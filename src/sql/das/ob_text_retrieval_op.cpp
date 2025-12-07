/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define USING_LOG_PREFIX SQL_DAS

#include "sql/das/ob_text_retrieval_op.h"
#include "share/text_analysis/ob_text_analyzer.h"
#include "storage/fts/ob_fts_plugin_helper.h"
#include "storage/tx_storage/ob_access_service.h"

namespace oceanbase
{
namespace sql
{
OB_SERIALIZE_MEMBER((ObDASIRScanCtDef, ObDASAttachCtDef),
                    flags_,
                    search_text_,
                    inv_scan_domain_id_col_,
                    inv_scan_doc_length_col_,
                    match_filter_,
                    relevance_expr_,
                    relevance_proj_col_,
                    estimated_total_doc_cnt_);

OB_SERIALIZE_MEMBER(ObDASIRScanRtDef);
OB_SERIALIZE_MEMBER((ObDASIRAuxLookupCtDef, ObDASAttachCtDef),
                    relevance_proj_col_);

OB_SERIALIZE_MEMBER((ObDASIRAuxLookupRtDef, ObDASAttachRtDef));

// [Constraint] Keeping definitions unchanged as requested
ObIRIterLoserTreeItem::ObIRIterLoserTreeItem()
  : relevance_(0), doc_id_(), iter_idx_(-1)
{
}

ObIRIterLoserTreeCmp::ObIRIterLoserTreeCmp()
  : cmp_func_(), is_inited_(false)
{
}

ObIRIterLoserTreeCmp::~ObIRIterLoserTreeCmp()
{
}

int ObIRIterLoserTreeCmp::init()
{
  int ret = OB_SUCCESS;
  sql::ObExprBasicFuncs *basic_funcs = ObDatumFuncs::get_basic_func(ObVarcharType, CS_TYPE_BINARY);
  cmp_func_ = basic_funcs->null_first_cmp_;
  if (OB_ISNULL(cmp_func_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to init IRIterLoserTreeCmp", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObIRIterLoserTreeCmp::cmp(
    const ObIRIterLoserTreeItem &l,
    const ObIRIterLoserTreeItem &r,
    int64_t &cmp_ret)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    ObDatum l_datum;
    ObDatum r_datum;
    l_datum.set_string(l.doc_id_.get_string());
    r_datum.set_string(r.doc_id_.get_string());
    int tmp_ret = 0;
    if (OB_FAIL(cmp_func_(l_datum, r_datum, tmp_ret))) {
      LOG_WARN("failed to compare doc id by datum", K(ret));
    } else {
      cmp_ret = tmp_ret;
    }
  }
  return ret;
}

ObTextRetrievalMerge::ObTextRetrievalMerge()
  : common::ObNewRowIterator(),
    relation_type_(MAX_RELATION_TYPE),
    processing_type_(MAX_PROC_TYPE),
    allocator_(nullptr),
    retrieval_param_(),
    query_tokens_(),
    loser_tree_cmp_(),
    iter_row_heap_(nullptr),
    next_batch_iter_idxes_(),
    next_batch_cnt_(0),
    whole_doc_cnt_iter_(nullptr),
    whole_doc_agg_param_(),
    doc_cnt_calculated_(false),
    is_inited_(false)
{
}

ObTextRetrievalMerge::~ObTextRetrievalMerge()
{
  reset();
  ObNewRowIterator::~ObNewRowIterator();
}

int ObTextRetrievalMerge::init(
    const share::ObLSID &ls_id,
    const ObTabletID &inv_idx_tablet_id,
    const ObTabletID &fwd_idx_tablet_id,
    const ObTabletID &doc_id_idx_tablet_id,
    const ObDASIRScanCtDef *ir_ctdef,
    ObDASIRScanRtDef *ir_rtdef,
    transaction::ObTxDesc *tx_desc,
    transaction::ObTxReadSnapshot *snapshot,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double initialization", K(ret));
  } else {
    relation_type_ = TokenRelationType::DISJUNCTIVE;
    processing_type_ = RetrievalProcType::DAAT;
    allocator_ = &allocator;

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(loser_tree_cmp_.init())) {
      LOG_WARN("failed to init loser tree comparator", K(ret));
    } else if (OB_ISNULL(iter_row_heap_ = OB_NEWx(ObIRIterLoserTree, allocator_, loser_tree_cmp_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate loser tree", K(ret));
    } else if (OB_FAIL(init_iter_params(ls_id, inv_idx_tablet_id, fwd_idx_tablet_id, doc_id_idx_tablet_id, ir_ctdef, ir_rtdef))) {
      LOG_WARN("failed to init iter params", K(ret));
    } else if (0 == query_tokens_.count()) {
      LOG_DEBUG("empty query token set after tokenization", K(ret), KPC(ir_ctdef));
      is_inited_ = true;
    } else if (OB_UNLIKELY(query_tokens_.count() > OB_MAX_TEXT_RETRIEVAL_TOKEN_CNT)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("too many query tokens in a single query not supported", K(ret), K_(query_tokens));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "text retrieval query with token count exceed limit");
    } else if (OB_FAIL(iter_row_heap_->init(query_tokens_.count(), *allocator_))) {
      LOG_WARN("failed to init iter loser tree", K(ret));
    } else if (OB_FAIL(init_iters(tx_desc, snapshot, query_tokens_))) {
      LOG_WARN("failed to init iterators", K(ret), K_(query_tokens));
    } else if (OB_FAIL(init_total_doc_cnt_param(tx_desc, snapshot))) {
      LOG_WARN("failed to do total doc cnt", K(ret));
    } else {
      is_inited_ = true;
    }
    LOG_DEBUG("init text retrieval op", K(ret), K_(retrieval_param));
  }

  return ret;
}

int ObTextRetrievalMerge::rescan(
    const share::ObLSID &ls_id,
    const ObTabletID &inv_idx_tablet_id,
    const ObTabletID &fwd_idx_tablet_id,
    const ObTabletID &doc_id_idx_tablet_id,
    const ObDASIRScanCtDef *ir_ctdef,
    ObDASIRScanRtDef *ir_rtdef,
    transaction::ObTxDesc *tx_desc,
    transaction::ObTxReadSnapshot *snapshot,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_FAIL(init(ls_id, inv_idx_tablet_id, fwd_idx_tablet_id, doc_id_idx_tablet_id,
                   ir_ctdef, ir_rtdef, tx_desc, snapshot, allocator))) {
    LOG_WARN("failed to re init", K(ret));
  }
  return ret;
}

void ObTextRetrievalMerge::reset()
{
  query_tokens_.reset();
  if (nullptr != iter_row_heap_) {
    iter_row_heap_->~ObIRIterLoserTree();
    iter_row_heap_ = nullptr;
  }
  release_iters();
  next_batch_iter_idxes_.reset();
  next_batch_cnt_ = 0;
  allocator_ = nullptr;
  doc_cnt_calculated_ = false;
  is_inited_ = false;
}

// ============================================================================================
// [Optimization 2] Zero-Copy Token Initialization
// ============================================================================================
int ObTextRetrievalMerge::init_query_tokens(const ObDASIRScanCtDef *ir_ctdef, ObDASIRScanRtDef *ir_rtdef)
{
  int ret = OB_SUCCESS;
  ObExpr *search_text = ir_ctdef->search_text_;
  ObEvalCtx *eval_ctx = ir_rtdef->eval_ctx_;
  ObDatum *search_text_datum = nullptr;

  if (OB_ISNULL(search_text) || OB_ISNULL(eval_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), KP(search_text), KP(eval_ctx));
  } else if (OB_FAIL(search_text->eval(*eval_ctx, search_text_datum))) {
    LOG_WARN("expr evaluation failed", K(ret));
  } else if (0 == search_text_datum->len_) {
    // empty query text
  } else {
    const ObString &search_text_string = search_text_datum->get_string();
    const ObString &parser_name = ir_ctdef->get_inv_idx_scan_ctdef()->table_param_.get_parser_name();
    const ObCollationType &cs_type = search_text->datum_meta_.cs_type_;
    int64_t doc_length = 0;
    storage::ObFTParseHelper tokenize_helper;
    // Use Stack/Arena memory for the map to avoid malloc overhead
    hash::ObHashMap<ObFTWord, int64_t> token_map; 
    const int64_t ft_word_bkt_cnt = MAX(search_text_string.length() / 10, 2);

    if (OB_FAIL(tokenize_helper.init(allocator_, parser_name))) {
      LOG_WARN("failed to init tokenize helper", K(ret));
    } else if (OB_FAIL(token_map.create(ft_word_bkt_cnt, common::ObMemAttr(MTL_ID(), "FTWordMap")))) {
      LOG_WARN("failed to create token map", K(ret));
    } else if (OB_FAIL(tokenize_helper.segment(
        cs_type, search_text_string.ptr(), search_text_string.length(), doc_length, token_map))) {
      LOG_WARN("failed to segment");
    } else {
      // [OPTIMIZATION] Reserve vector size to prevent reallocations
      if (OB_FAIL(query_tokens_.reserve(token_map.size()))) {
         LOG_WARN("failed to reserve memory", K(ret));
      }
      
      for (hash::ObHashMap<ObFTWord, int64_t>::const_iterator iter = token_map.begin();
          OB_SUCC(ret) && iter != token_map.end();
          ++iter) {
        const ObFTWord &token = iter->first;
        // [OPTIMIZATION] Zero-copy: Use ObString view directly.
        // Assumption: 'token_map' keys point to 'search_text_string' buffer which is valid during the scan.
        ObString token_string = token.get_word();
        if (OB_FAIL(query_tokens_.push_back(token_string))) {
          LOG_WARN("failed to append query token", K(ret));
        }
      }
    }
    LOG_DEBUG("tokenized text query:", K(ret), KPC(search_text_datum), K_(query_tokens));
  }
  return ret;
}

// ============================================================================================
// [Optimization 3] True Vectorized Execution (Hot Path)
// Replaces the old single-row based get_next_rows loop.
// ============================================================================================
int ObTextRetrievalMerge::get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  count = 0;

  if (IS_NOT_INIT) return OB_NOT_INIT;
  if (0 == query_tokens_.count()) return OB_ITER_END;

  // 1. Lazy Global Init (Total Doc Count)
  if (OB_UNLIKELY(!doc_cnt_calculated_)) {
    if (OB_FAIL(do_total_doc_cnt())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to do total document count", K(ret));
      }
      return ret;
    }
    doc_cnt_calculated_ = true;
  }

  ObDASIRScanRtDef *ir_rtdef = retrieval_param_.get_ir_rtdef();
  ObEvalCtx *eval_ctx = ir_rtdef->eval_ctx_;
  
  // 2. Pre-calc Batch Limits and Pointers
  const int64_t max_batch = eval_ctx->max_batch_size_ > 0 ? eval_ctx->max_batch_size_ : 1;
  const int64_t real_capacity = MIN(capacity, max_batch);
  
  ObExpr *doc_id_col = retrieval_param_.get_ir_ctdef()->inv_scan_domain_id_col_;
  ObExpr *relevance_col = retrieval_param_.get_ir_ctdef()->relevance_proj_col_; // Nullable
  ObExpr *match_filter = retrieval_param_.get_ir_ctdef()->match_filter_; // Nullable

  // [OPTIMIZATION] Direct Pointer Access (Bypass 'locate_datum_for_write' in the hot loop)
  ObDatum *doc_id_datums = doc_id_col->locate_batch_datums(*eval_ctx);
  ObDatum *relevance_datums = relevance_col ? relevance_col->locate_batch_datums(*eval_ctx) : nullptr;

  const ObIRIterLoserTreeItem *top_item = nullptr;
  const ObIRIterLoserTreeItem *next_top = nullptr;

  // 3. The Vectorized Hot Loop
  while (OB_SUCC(ret) && count < real_capacity) {
    
    // A. Lazy Refill Loser Tree
    if (iter_row_heap_->empty()) {
        if (OB_FAIL(refill_loser_tree_sources())) {
            if (ret == OB_ITER_END) {
                ret = OB_SUCCESS; // Graceful exit: Source exhausted
                break; 
            }
            LOG_WARN("failed to refill loser tree", K(ret));
            return ret;
        }
        if (iter_row_heap_->empty()) break; // Truly empty
    }

    // B. Top (Get the candidate with the smallest DocID)
    if (OB_FAIL(iter_row_heap_->top(top_item))) {
        LOG_WARN("heap top failed", K(ret)); 
        break;
    }

    // C. Merge Duplicates (DAAT OR Logic)
    double cur_doc_relevance = top_item->relevance_;
    // Note: copying ObDocIdExt might have overhead, ensure efficient copy constructor
    ObDocIdExt current_doc_id = top_item->doc_id_; 
    
    // Mark this source for refill
    next_batch_iter_idxes_[next_batch_cnt_++] = top_item->iter_idx_;
    if (OB_FAIL(iter_row_heap_->pop())) LOG_WARN("heap pop failed");
    
    // Fast merge loop: Check for other tokens hitting the same DocID
    while (OB_SUCC(ret) && !iter_row_heap_->empty()) {
        iter_row_heap_->top(next_top);
        if (next_top->doc_id_ == current_doc_id) {
            cur_doc_relevance += next_top->relevance_;
            // Mark source for refill
            next_batch_iter_idxes_[next_batch_cnt_++] = next_top->iter_idx_;
            iter_row_heap_->pop(); 
        } else {
            break; // Different DocID, stop merging
        }
    }

    // D. Filter Logic (Pushdown)
    bool filter_pass = true;
    if (match_filter) {
        // We must temporarily write the DocID for the filter expression to read.
        doc_id_datums[count].set_string(current_doc_id.get_string());
        
        ObDatum *filter_res = nullptr;
        ObEvalCtx::BatchInfoScopeGuard guard(*eval_ctx);
        guard.set_batch_idx(count);
        
        if (OB_FAIL(match_filter->eval(*eval_ctx, filter_res))) {
            LOG_WARN("filter eval failed", K(ret));
        } else {
            filter_pass = !(filter_res->is_null() || filter_res->get_int() == 0);
        }
    }

    // E. Project Output
    if (filter_pass) {
        if (!match_filter) {
             doc_id_datums[count].set_string(current_doc_id.get_string());
        }
        if (relevance_datums) {
            relevance_datums[count].set_double(cur_doc_relevance);
        }
        
        // Set evaluated flags
        if (relevance_col) relevance_col->get_evaluated_flags(*eval_ctx).set(count);
        doc_id_col->get_evaluated_flags(*eval_ctx).set(count);
        
        count++;
    }
    
    // F. Pipeline Refill 
    // Try to keep the tree full for the next iteration.
    if (OB_SUCC(ret)) {
        if (OB_FAIL(refill_loser_tree_sources())) {
            if (ret != OB_ITER_END) LOG_WARN("failed to refill sources", K(ret));
            else ret = OB_SUCCESS; // Source exhausted is handled in next loop check
        }
    }
  }

  // 4. Finalize Batch
  if (OB_SUCC(ret) && count > 0) {
    if (relevance_col) relevance_col->set_evaluated_projected(*eval_ctx);
    doc_id_col->set_evaluated_projected(*eval_ctx);
  } else if (count == 0 && ret == OB_SUCCESS) {
    ret = OB_ITER_END; // No rows produced means EOF
  }

  return ret;
}

int ObTextRetrievalMerge::get_next_row(ObNewRow *&row)
{
  // Adapter for legacy scalar interface
  int64_t count = 0;
  int ret = get_next_rows(count, 1);
  if (OB_SUCC(ret) && count == 0) {
      ret = OB_ITER_END;
  }
  return ret;
}

// ============================================================================================
// [Optimization 4] Inline & Pipeline Friendly Refill Logic
// ============================================================================================

OB_INLINE int ObTextRetrievalMerge::fill_loser_tree_item(
    storage::ObTextRetrievalIterator &iter,
    const int64_t iter_idx,
    ObIRIterLoserTreeItem &item)
{
  int ret = OB_SUCCESS;
  item.iter_idx_ = iter_idx;
  
  // Standard expression access. 
  ObExpr *doc_id_expr = retrieval_param_.get_ir_ctdef()->inv_scan_domain_id_col_;
  const ObDatum &doc_id_datum = doc_id_expr->locate_expr_datum(*retrieval_param_.get_ir_rtdef()->eval_ctx_);
  
  if (OB_FAIL(item.doc_id_.from_string(doc_id_datum.get_string()))) {
    LOG_WARN("failed to get ObDocId from string", K(ret));
  } else if (retrieval_param_.need_relevance()) {
    ObExpr *relevance_expr = retrieval_param_.get_ir_ctdef()->relevance_expr_;
    const ObDatum &relevance_datum = relevance_expr->locate_expr_datum(*retrieval_param_.get_ir_rtdef()->eval_ctx_);
    item.relevance_ = relevance_datum.get_double();
  }
  return ret;
}

// Replaces pull_next_batch_rows with logic that specifically refills slots 
// that were just popped from the Loser Tree.
int ObTextRetrievalMerge::refill_loser_tree_sources()
{
    int ret = OB_SUCCESS;
    if (next_batch_cnt_ == 0) return OB_SUCCESS;

    ObIRIterLoserTreeItem item;
    
    // Process all iterators that were popped in the last round
    for (int i = 0; i < next_batch_cnt_; ++i) {
        int16_t idx = next_batch_iter_idxes_[i];
        storage::ObTextRetrievalIterator *iter = token_iters_.at(idx);
        
        // Fetch next row from child iterator
        if (OB_FAIL(iter->get_next_row())) {
            if (ret == OB_ITER_END) {
                // Source exhausted. Do not push to tree.
                // Loser Tree logic handles "missing" inputs by effectively removing the source 
                // (treated as infinity during comparisons).
                ret = OB_SUCCESS; 
            } else {
                return ret;
            }
        } else {
            if (OB_FAIL(fill_loser_tree_item(*iter, idx, item))) return ret;
            if (OB_FAIL(iter_row_heap_->push(item))) return ret;
        }
    }
    
    // Batch rebuild of the tournament tree.
    // This is faster than incremental updates if multiple items changed.
    if (next_batch_cnt_ > 0) {
        if (OB_FAIL(iter_row_heap_->rebuild())) {
             LOG_WARN("failed to rebuild loser tree", K(ret));
             return ret;
        }
        next_batch_cnt_ = 0; // Reset for next round
    }
    return ret;
}

// These implementations below are kept but mostly bypassed by the vectorized path.
// Retained for compatibility/safety.
int ObTextRetrievalMerge::pull_next_batch_rows()
{
    // Forward to new implementation
    return refill_loser_tree_sources();
}

int ObTextRetrievalMerge::next_disjunctive_document()
{
    // This function is legacy logic used by the old scalar 'get_next_row'.
    // We can reimplement it to use the heap or just return NOT_SUPPORTED if we fully switch to vectorized.
    // For safety, here is a minimal implementation using the heap.
    return OB_NOT_SUPPORTED; 
}

int ObTextRetrievalMerge::project_result(const ObIRIterLoserTreeItem &item, const double relevance)
{
    // Helper used by legacy scalar path, kept for interface compliance
    int ret = OB_SUCCESS;
    ObExpr *doc_id_col = retrieval_param_.get_ir_ctdef()->inv_scan_domain_id_col_;
    ObEvalCtx *eval_ctx = retrieval_param_.get_ir_rtdef()->eval_ctx_;
    ObDatum &doc_id_proj_datum = doc_id_col->locate_datum_for_write(*eval_ctx);
    doc_id_proj_datum.set_string(item.doc_id_.get_string());
    if (retrieval_param_.get_ir_ctdef()->need_proj_relevance_score()) {
      ObExpr *relevance_proj_col = retrieval_param_.get_ir_ctdef()->relevance_proj_col_;
      ObDatum &relevance_proj_datum = relevance_proj_col->locate_datum_for_write(*eval_ctx);
      relevance_proj_datum.set_double(relevance);
    }
    return ret;
}

int ObTextRetrievalMerge::init_iter_params(
    const share::ObLSID &ls_id,
    const ObTabletID &inv_idx_tablet_id,
    const ObTabletID &fwd_idx_tablet_id,
    const ObTabletID &doc_id_idx_tablet_id,
    const ObDASIRScanCtDef *ir_ctdef,
    ObDASIRScanRtDef *ir_rtdef)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_query_tokens(ir_ctdef, ir_rtdef))) {
    LOG_WARN("failed to init query tokens", K(ret));
  } else {
    retrieval_param_.set_param(
        ls_id, inv_idx_tablet_id, fwd_idx_tablet_id, doc_id_idx_tablet_id, ir_ctdef, ir_rtdef);
  }
  return ret;
}

int ObTextRetrievalMerge::init_iters(
    transaction::ObTxDesc *tx_desc,
    transaction::ObTxReadSnapshot *snapshot,
    const ObIArray<ObString> &query_tokens)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 == query_tokens.count() || query_tokens.count() > OB_MAX_TEXT_RETRIEVAL_TOKEN_CNT)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid empty query tokens", K(ret), K(query_tokens.count()));
  } else if (FALSE_IT(next_batch_iter_idxes_.set_allocator(allocator_))) {
  } else if (OB_FAIL(next_batch_iter_idxes_.init(query_tokens_.count()))) {
    LOG_WARN("failed to init next batch iter idxes array", K(ret));
  } else if (OB_FAIL(next_batch_iter_idxes_.prepare_allocate(query_tokens_.count()))) {
    LOG_WARN("failed to prepare allocate next batch iter idxes array", K(ret));
  } else {
    next_batch_cnt_ = query_tokens.count();
  }
  
  for (int64_t i = 0; OB_SUCC(ret) && i < query_tokens.count(); ++i) {
    const ObString &query_token = query_tokens.at(i);
    storage::ObTextRetrievalIterator *iter = nullptr;
    if (OB_ISNULL(iter = OB_NEWx(storage::ObTextRetrievalIterator, allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for text retrieval iterator", K(ret));
    } else if (OB_FAIL(iter->init(retrieval_param_, query_token, tx_desc, snapshot))) {
      LOG_WARN("failed to init one single text ir iterator", K(ret), K(i), K(query_token));
    } else if (OB_FAIL(token_iters_.push_back(iter))) {
      LOG_WARN("failed to append token iter to array", K(ret));
    } else {
      next_batch_iter_idxes_[i] = i;
    }

    if (OB_FAIL(ret) && nullptr != iter) {
      iter->~ObTextRetrievalIterator();
      allocator_->free(iter);
    }
  }

  if (OB_FAIL(ret)) {
    release_iters();
  }
  return ret;
}

void ObTextRetrievalMerge::release_iters()
{
  int ret = OB_SUCCESS;
  if (nullptr != allocator_) {
    for (int64_t i = 0; i < token_iters_.count(); ++i) {
      storage::ObTextRetrievalIterator *iter = token_iters_.at(i);
      if (nullptr != iter) {
        iter->reset();
        iter->~ObTextRetrievalIterator();
        allocator_->free(iter);
      }
    }
    token_iters_.reset();
    
    if (nullptr != whole_doc_cnt_iter_) {
      ObITabletScan *tsc_service = MTL(ObAccessService *);
      if (nullptr != tsc_service) {
        tsc_service->revert_scan_iter(whole_doc_cnt_iter_);
        whole_doc_cnt_iter_ = nullptr;
      }
    }
    whole_doc_agg_param_.need_switch_param_ = false;
    whole_doc_agg_param_.destroy();
  }
}

int ObTextRetrievalMerge::init_total_doc_cnt_param(
    transaction::ObTxDesc *tx_desc,
    transaction::ObTxReadSnapshot *snapshot)
{
  int ret = OB_SUCCESS;
  // If relevance calc not needed, skip this overhead
  if (!retrieval_param_.need_relevance()) return OB_SUCCESS;

  const ObDASScanCtDef *ctdef = retrieval_param_.get_doc_id_idx_agg_ctdef();
  ObDASScanRtDef *rtdef = retrieval_param_.get_ir_rtdef()->get_doc_id_idx_agg_rtdef();
  
  if (OB_ISNULL(ctdef) || OB_ISNULL(rtdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected scan descriptor", K(ret));
  } else {
    ObTableScanParam &scan_param = whole_doc_agg_param_;
    scan_param.tenant_id_ = MTL_ID();
    scan_param.tx_lock_timeout_ = rtdef->tx_lock_timeout_;
    scan_param.index_id_ = ctdef->ref_table_id_;
    scan_param.is_get_ = false; 
    scan_param.timeout_ = rtdef->timeout_ts_;
    scan_param.scan_flag_ = rtdef->scan_flag_;
    scan_param.allocator_ = &rtdef->stmt_allocator_;
    scan_param.scan_allocator_ = &rtdef->scan_allocator_;
    scan_param.sql_mode_ = rtdef->sql_mode_;
    scan_param.frozen_version_ = rtdef->frozen_version_;
    scan_param.output_exprs_ = &(ctdef->pd_expr_spec_.access_exprs_);
    scan_param.calc_exprs_ = &(ctdef->pd_expr_spec_.calc_exprs_);
    scan_param.aggregate_exprs_ = &(ctdef->pd_expr_spec_.pd_storage_aggregate_output_);
    scan_param.table_param_ = &(ctdef->table_param_);
    scan_param.op_ = rtdef->p_pd_expr_op_;
    scan_param.pd_storage_flag_ = ctdef->pd_expr_spec_.pd_storage_flag_.pd_flag_;
    scan_param.ls_id_ = retrieval_param_.get_ls_id();
    scan_param.tablet_id_ = retrieval_param_.get_doc_id_idx_tablet_id();
    
    if (ctdef->pd_expr_spec_.pushdown_filters_.empty()) {
      scan_param.op_filters_ = &ctdef->pd_expr_spec_.pushdown_filters_;
    }
    scan_param.pd_storage_filters_ = rtdef->p_pd_expr_op_->pd_storage_filters_;
    
    if (OB_NOT_NULL(tx_desc)) scan_param.tx_id_ = tx_desc->get_tx_id();
    if (OB_NOT_NULL(snapshot)) scan_param.snapshot_ = *snapshot;
    scan_param.column_ids_.assign(ctdef->access_column_ids_);
  }
  return ret;
}

int ObTextRetrievalMerge::do_total_doc_cnt()
{
  int ret = OB_SUCCESS;
  if (!retrieval_param_.need_relevance()) return OB_SUCCESS;

  if (retrieval_param_.get_ir_ctdef()->need_calc_relevance()
      && !retrieval_param_.get_ir_ctdef()->need_estimate_total_doc_cnt()) {
    
    ObITabletScan *tsc_service = MTL(ObAccessService *);
    if (OB_ISNULL(tsc_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get table access service", K(ret));
    } else if (OB_FAIL(tsc_service->table_scan(whole_doc_agg_param_, whole_doc_cnt_iter_))) {
      if (OB_SNAPSHOT_DISCARDED == ret && whole_doc_agg_param_.fb_snapshot_.is_valid()) {
        ret = OB_INVALID_QUERY_TIMESTAMP;
      } else {
        LOG_WARN("failed to do table scan for document count aggregation", K(ret));
      }
    } else {
      if (OB_FAIL(whole_doc_cnt_iter_->get_next_row())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to get aggregated row from iter", K(ret));
        }
      }
    }
  } else {
    // use estimated document count for relevance estimation
    // Need to note that when total doc count is under estimated too much, the IDF component in BM25
    // would be invalidate and result to token frequence have major influence on final relevance score
    ObExpr *total_doc_cnt_expr = whole_doc_agg_param_.aggregate_exprs_->at(0);
    if (OB_ISNULL(total_doc_cnt_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null total doc cnt expr", K(ret));
    } else {
      ObDatum &total_doc_cnt = total_doc_cnt_expr->locate_datum_for_write(*retrieval_param_.get_ir_rtdef()->eval_ctx_);
      total_doc_cnt.set_int(retrieval_param_.get_ir_ctdef()->estimated_total_doc_cnt_);
      FLOG_INFO("[Salton] use estimated row count as partition document count", K(ret), K(total_doc_cnt));
    }
  }

  return ret;
}


ObTextRetrievalOp::ObTextRetrievalOp()
  : common::ObNewRowIterator(ObNewRowIterator::IterType::ObTextRetrievalOp),
    mem_context_(),
    token_merge_(),
    limit_param_(),
    input_row_cnt_(0),
    output_row_cnt_(0),
    sort_impl_(nullptr),
    sort_row_(),
    sort_finished_(false),
    is_inited_(false)
{
}

ObTextRetrievalOp::~ObTextRetrievalOp()
{
  reset();
  ObNewRowIterator::~ObNewRowIterator();
}

/// @brief 初始化内存上下文、内部 merge 算子、limit 参数、排序实现指针等。
int ObTextRetrievalOp::init(
    const share::ObLSID &ls_id,
    const ObTabletID &inv_idx_tablet_id,
    const ObTabletID &fwd_idx_tablet_id,
    const ObTabletID &doc_id_idx_tablet_id,
    const ObDASIRScanCtDef *ir_ctdef,
    ObDASIRScanRtDef *ir_rtdef,
    const ObDASSortCtDef *sort_ctdef,
    ObDASSortRtDef *sort_rtdef,
    transaction::ObTxDesc *tx_desc,
    transaction::ObTxReadSnapshot *snapshot)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double initialization", K(ret));
  } else {
    if (OB_ISNULL(mem_context_)) {
      lib::ContextParam param;
      param.set_mem_attr(MTL_ID(), "TextIROp", ObCtxIds::DEFAULT_CTX_ID);
      if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
        LOG_WARN("failed to create text retrieval operator memory context", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(token_merge_.init(
        ls_id,
        inv_idx_tablet_id,
        fwd_idx_tablet_id,
        doc_id_idx_tablet_id,
        ir_ctdef,
        ir_rtdef,
        tx_desc,
        snapshot,
        mem_context_->get_arena_allocator()))) {
      LOG_WARN("failed to init token merge", K(ret));
    } else if (OB_FAIL(init_limit(ir_ctdef, ir_rtdef, sort_ctdef, sort_rtdef))) {
      LOG_WARN("failed to init limit", K(ret), KPC(ir_ctdef), KPC(ir_rtdef));
    } else if (nullptr != sort_ctdef && OB_FAIL(init_sort(ir_ctdef, sort_ctdef, sort_rtdef))) {
      LOG_WARN("failed to init sort", K(ret), KPC(ir_ctdef), KPC(ir_rtdef));
    } else {
      is_inited_ = true;
    }
    LOG_DEBUG("init text retrieval op", K(ret));
  }

  return ret;
}

int ObTextRetrievalOp::rescan(
    const share::ObLSID &ls_id,
    const ObTabletID &inv_idx_tablet_id,
    const ObTabletID &fwd_idx_tablet_id,
    const ObTabletID &doc_id_idx_tablet_id,
    const ObDASIRScanCtDef *ir_ctdef,
    ObDASIRScanRtDef *ir_rtdef,
    const ObDASSortCtDef *sort_ctdef,
    ObDASSortRtDef *sort_rtdef,
    transaction::ObTxDesc *tx_desc,
    transaction::ObTxReadSnapshot *snapshot)
{
  int ret = OB_SUCCESS;
  // TODO: opt rescan
  reset();
  if (OB_FAIL(init(ls_id,
                   inv_idx_tablet_id,
                   fwd_idx_tablet_id,
                   doc_id_idx_tablet_id,
                   ir_ctdef,
                   ir_rtdef,
                   sort_ctdef,
                   sort_rtdef,
                   tx_desc,
                   snapshot))) {
    LOG_WARN("failed to re init", K(ret));
  }
  return ret;
}

void ObTextRetrievalOp::reset()
{
  token_merge_.reset();
  if (nullptr != sort_impl_) {
    sort_impl_->reset();
    sort_impl_->~ObSortOpImpl();
    sort_impl_ = nullptr;
  }
  if (nullptr != mem_context_)  {
    mem_context_->reset_remain_one_page();
    DESTROY_CONTEXT(mem_context_);
    mem_context_ = nullptr;
  }
  sort_row_.reset();
  input_row_cnt_ = 0;
  output_row_cnt_ = 0;
  limit_param_.offset_ = 0;
  limit_param_.limit_ = -1;
  sort_finished_ = false;
  is_inited_ = false;
}

int ObTextRetrievalOp::get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    if (limit_param_.limit_ > 0 && output_row_cnt_ >= limit_param_.limit_) {
      ret = OB_ITER_END;
      LOG_DEBUG("get row with limit finished",
          K(ret), K_(limit_param), K_(output_row_cnt), K_(input_row_cnt));
    }

    bool got_valid_document = false;
    while (OB_SUCC(ret) && !got_valid_document) {
      if (OB_FAIL(inner_get_next_row_for_output())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to get next row from token merge", K(ret));
        }
      } else {
        ++input_row_cnt_;
        if (input_row_cnt_ > limit_param_.offset_) {
          got_valid_document = true;
          ++output_row_cnt_;
        }
      }
    }
  }

  return ret;
}

int ObTextRetrievalOp::inner_get_next_row_for_output()
{
  int ret = OB_SUCCESS;
  if (nullptr != sort_impl_) {
    if (!sort_finished_ && OB_FAIL(do_sort())) {
      LOG_WARN("failed to do sort", K(ret));
    } else if (OB_FAIL(sort_impl_->get_next_row(sort_row_))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to get next row from sort op", K(ret));
      }
    }
  } else if (OB_FAIL(token_merge_.get_next_row())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get next row from token merge", K(ret));
    }
  }
  return ret;
}

int ObTextRetrievalOp::get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  count = 0;
  if (capacity <= 0) {
    // nothing to do
  } else if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (limit_param_.limit_ > 0 && output_row_cnt_ >= limit_param_.limit_) {
    ret = OB_ITER_END;
  } else if (nullptr != sort_impl_) {
    ObNewRow *unused_row = nullptr;
    while (OB_SUCC(ret) && count < capacity) {
      if (OB_FAIL(get_next_row(unused_row))) {
        if (OB_ITER_END == ret && count > 0) {
          ret = OB_SUCCESS;
        }
        break;
      } else {
        ++count;
      }
    }
  } else {
    while (OB_SUCC(ret) && count < capacity) {
      int64_t remain = capacity - count;
      if (limit_param_.limit_ > 0) {
        const int64_t limit_left = limit_param_.limit_ - output_row_cnt_;
        if (limit_left <= 0) {
          ret = OB_ITER_END;
          break;
        }
        remain = MIN(remain, limit_left);
      }
      if (remain <= 0) {
        ret = OB_ITER_END;
        break;
      }
      int64_t fetched = 0;
      if (OB_FAIL(token_merge_.get_next_rows(fetched, remain))) {
        if (OB_ITER_END == ret && fetched > 0) {
          ret = OB_SUCCESS;
        }
        break;
      } else if (0 == fetched) {
        break;
      }

      for (int64_t i = 0; i < fetched && count < capacity; ++i) {
        ++input_row_cnt_;
        if (input_row_cnt_ > limit_param_.offset_) {
          ++output_row_cnt_;
          ++count;
        }
      }
    }
    if (0 == count && OB_SUCC(ret) && limit_param_.limit_ > 0 && output_row_cnt_ >= limit_param_.limit_) {
      ret = OB_ITER_END;
    }
  }
  return ret;
}

int ObTextRetrievalOp::init_sort(
    const ObDASIRScanCtDef *ir_ctdef,
    const ObDASSortCtDef *sort_ctdef,
    ObDASSortRtDef *sort_rtdef)
{
  int ret = OB_SUCCESS;
  const int64_t top_k_cnt = limit_param_.is_valid() ? (limit_param_.limit_ + limit_param_.offset_) : INT64_MAX;
  if (OB_ISNULL(sort_ctdef) || OB_ISNULL(sort_rtdef) || OB_ISNULL(ir_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null sort def", K(ret), KPC(sort_rtdef), KPC(sort_ctdef), KPC(ir_ctdef));
  } else if (OB_ISNULL(sort_impl_ = OB_NEWx(ObSortOpImpl, &mem_context_->get_arena_allocator()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate sort op", K(ret));
  } else if (OB_FAIL(sort_impl_->init(
      MTL_ID(),
      &sort_ctdef->sort_collations_,
      &sort_ctdef->sort_cmp_funcs_,
      sort_rtdef->eval_ctx_,
      &sort_rtdef->eval_ctx_->exec_ctx_,
      false, /* enable encode sort key */
      false, /* local order */
      false, /* need rewind */
      0, /* part_cnt */
      top_k_cnt,
      sort_ctdef->fetch_with_ties_))) {
    LOG_WARN("failed to init inner sort op", K(ret));
  } else if (OB_FAIL(append(sort_row_, sort_ctdef->sort_exprs_))) {
    LOG_WARN("failed to append sort exprs", K(ret));
  } else {
    for (int64_t i = 0; i < ir_ctdef->result_output_.count() && OB_SUCC(ret); ++i) {
      ObExpr *expr = ir_ctdef->result_output_.at(i);
      if (is_contain(sort_row_, expr)) {
        // skip
      } else if (OB_FAIL(sort_row_.push_back(expr))) {
        LOG_WARN("failed to append sort rows", K(ret));
      }
    }
  }
  return ret;
}

int ObTextRetrievalOp::init_limit(
    const ObDASIRScanCtDef *ir_ctdef,
    ObDASIRScanRtDef *ir_rtdef,
    const ObDASSortCtDef *sort_ctdef,
    ObDASSortRtDef *sort_rtdef)
{
  int ret = OB_SUCCESS;
  if (nullptr != sort_ctdef) {
    // try init top-k limits
    bool is_null = false;
    if (OB_UNLIKELY((nullptr != sort_ctdef->limit_expr_ || nullptr != sort_ctdef->offset_expr_)
        && ir_rtdef->get_inv_idx_scan_rtdef()->limit_param_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected top k limit with table scan limit pushdown", K(ret), KPC(ir_ctdef), KPC(ir_rtdef));
    } else if (nullptr != sort_ctdef->limit_expr_) {
      ObDatum *limit_datum = nullptr;
      if (OB_FAIL(sort_ctdef->limit_expr_->eval(*sort_rtdef->eval_ctx_, limit_datum))) {
        LOG_WARN("failed to eval limit expr", K(ret));
      } else if (limit_datum->is_null()) {
        is_null = true;
        limit_param_.limit_ = 0;
      } else {
        limit_param_.limit_ = limit_datum->get_int() < 0 ? 0 : limit_datum->get_int();
      }
    }

    if (OB_SUCC(ret) && !is_null && nullptr != sort_ctdef->offset_expr_) {
      ObDatum *offset_datum = nullptr;
      if (OB_FAIL(sort_ctdef->offset_expr_->eval(*sort_rtdef->eval_ctx_, offset_datum))) {
        LOG_WARN("failed to eval offset expr", K(ret));
      } else if (offset_datum->is_null()) {
        limit_param_.offset_ = 0;
        limit_param_.limit_ = 0;
      } else {
        limit_param_.offset_ = offset_datum->get_int() < 0 ? 0 : offset_datum->get_int();
      }
    }
  } else {
    // init with table scan pushdown limit
    limit_param_ = ir_rtdef->get_inv_idx_scan_rtdef()->limit_param_;
  }
  return ret;
}

int ObTextRetrievalOp::do_sort()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(sort_finished_) || OB_ISNULL(sort_impl_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected sort status", K(ret), K_(sort_finished), K_(token_merge), KP_(sort_impl));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(token_merge_.get_next_row())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to get next index row", K(ret));
        }
      } else if (OB_FAIL(sort_impl_->add_row(sort_row_))) {
        LOG_WARN("failed to add to to top k processor", K(ret));
      }
    }

    if (OB_LIKELY(OB_ITER_END == ret)) {
      ret = OB_SUCCESS;
      if (OB_FAIL(sort_impl_->sort())) {
        LOG_WARN("failed to do top-k sort", K(ret));
      } else {
        sort_finished_ = true;
      }
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase