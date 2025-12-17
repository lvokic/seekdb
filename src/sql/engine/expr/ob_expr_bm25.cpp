/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_bm25.h"
#include "sql/resolver/expr/ob_raw_expr.h"

namespace oceanbase
{
namespace sql
{
ObExprBM25::ObExprBM25(ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_BM25, N_BM25, 4, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

int ObExprBM25::calc_result_typeN(
    ObExprResType &result_type,
    ObExprResType *types,
    int64_t param_num,
    common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  if (OB_UNLIKELY(param_num != 5)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("BM25 expr should have 4 parameters", K(ret), K(param_num));
  } else {
    types[TOKEN_DOC_CNT_PARAM_IDX].set_calc_type(ObIntType);
    types[TOTAL_DOC_CNT_PARAM_IDX].set_calc_type(ObIntType);
    types[DOC_TOKEN_CNT_PARAM_IDX].set_calc_type(ObIntType);
    types[AVG_DOC_CNT_PARAM_IDX].set_calc_type(ObDoubleType);
    types[RELATED_TOKEN_CNT_PARAM_IDX].set_calc_type(ObUInt64Type);
    result_type.set_double();
  }
  return ret;
}

int ObExprBM25::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  CK(5 == raw_expr.get_param_count());
  rt_expr.eval_func_ = eval_bm25_relevance_expr;
  rt_expr.eval_batch_func_ = eval_batch_bm25_relevance_expr;
  return ret;
}

int ObExprBM25::eval_bm25_relevance_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *token_doc_cnt_datum = nullptr;
  ObDatum *total_doc_cnt_datum = nullptr;
  ObDatum *doc_token_cnt_datum = nullptr;
  ObDatum *avg_doc_token_cnt_datum = nullptr;
  ObDatum *related_token_cnt_datum = nullptr;
  if (OB_FAIL(expr.eval_param_value(
      ctx,
      token_doc_cnt_datum,
      total_doc_cnt_datum,
      doc_token_cnt_datum,
      avg_doc_token_cnt_datum,
      related_token_cnt_datum))) {
    LOG_WARN("evaluate parameter value failed", K(ret));
  } else if (OB_UNLIKELY(token_doc_cnt_datum->is_null() || total_doc_cnt_datum->is_null()
      || doc_token_cnt_datum->is_null() || avg_doc_token_cnt_datum->is_null() || related_token_cnt_datum->is_null())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null datum", K(ret), KPC(token_doc_cnt_datum), KPC(total_doc_cnt_datum),
        KPC(doc_token_cnt_datum), KPC(avg_doc_token_cnt_datum), KPC(related_token_cnt_datum));
  } else {
    const int64_t token_doc_cnt = token_doc_cnt_datum->get_int();
    const int64_t total_doc_cnt = total_doc_cnt_datum->get_int();
    const int64_t related_token_cnt = related_token_cnt_datum->get_uint();
    const int64_t doc_token_cnt = doc_token_cnt_datum->get_int();
    const double avg_doc_token_cnt = avg_doc_token_cnt_datum->get_double();
    const double norm_len = doc_token_cnt / avg_doc_token_cnt;
    const double token_weight = query_token_weight(token_doc_cnt, total_doc_cnt);
    const double doc_weight = doc_token_weight(related_token_cnt, norm_len);
    const double relevance = token_weight * doc_weight;
    res_datum.set_double(relevance);
    LOG_DEBUG("show bm25 parameters for current document",
        K(token_doc_cnt), K(total_doc_cnt), K(related_token_cnt), K(doc_token_cnt), K(avg_doc_token_cnt),
        K(norm_len), K(token_weight), K(doc_weight), K(relevance));
  }
  return ret;
}

int ObExprBM25::eval_batch_bm25_relevance_expr(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const int64_t size)
{
  int ret = OB_SUCCESS;
  ObDatumVector token_doc_cnt_datum;
  ObDatumVector total_doc_cnt_datum;
  ObDatumVector doc_token_cnt_datum;
  ObDatumVector avg_doc_token_cnt_datum;
  ObDatumVector related_token_cnt_datum;
  if (OB_FAIL(expr.eval_batch_param_value(
      ctx, skip, size,
      token_doc_cnt_datum, total_doc_cnt_datum, doc_token_cnt_datum,
      avg_doc_token_cnt_datum, related_token_cnt_datum))) {
    LOG_WARN("evaluate parameter value failed", K(ret));
  } else if (OB_UNLIKELY(token_doc_cnt_datum.at(0)->is_null() || total_doc_cnt_datum.at(0)->is_null()
             || avg_doc_token_cnt_datum.at(0)->is_null())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null datum", K(ret), KPC(token_doc_cnt_datum.at(0)),
             KPC(total_doc_cnt_datum.at(0)), KPC(avg_doc_token_cnt_datum.at(0)));
  } else {
    const int64_t token_doc_cnt = token_doc_cnt_datum.at(0)->get_int();
    const int64_t total_doc_cnt = total_doc_cnt_datum.at(0)->get_int();
    const double token_weight = query_token_weight(token_doc_cnt, total_doc_cnt);
    const double avg_doc_token_cnt = avg_doc_token_cnt_datum.at(0)->get_double();
    ObDatum *res_datum = expr.locate_batch_datums(ctx);
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    int64_t i = 0;

    const __m512d v_avg_doc_token_cnt = _mm512_set1_pd(avg_doc_token_cnt);
    const __m512d v_token_weight = _mm512_set1_pd(token_weight);
    const __m512d v_p_k1 = _mm512_set1_pd(p_k1);
    const __m512d v_p_b = _mm512_set1_pd(p_b);
    const __m512d v_one = _mm512_set1_pd(1.0);
    const __m512d v_one_minus_p_b = _mm512_sub_pd(v_one, v_p_b); 

    for (; OB_SUCC(ret) && i <= size - 8; i += 8) {
        const __m512d v_related_token_cnt = _mm512_set_pd(
            (double)related_token_cnt_datum.at(i+7)->get_uint(), (double)related_token_cnt_datum.at(i+6)->get_uint(), 
            (double)related_token_cnt_datum.at(i+5)->get_uint(), (double)related_token_cnt_datum.at(i+4)->get_uint(),
            (double)related_token_cnt_datum.at(i+3)->get_uint(), (double)related_token_cnt_datum.at(i+2)->get_uint(),
            (double)related_token_cnt_datum.at(i+1)->get_uint(), (double)related_token_cnt_datum.at(i)->get_uint());
        const __m512d v_doc_token_cnt = _mm512_set_pd(
            (double)doc_token_cnt_datum.at(i+7)->get_int(), (double)doc_token_cnt_datum.at(i+6)->get_int(), 
            (double)doc_token_cnt_datum.at(i+5)->get_int(), (double)doc_token_cnt_datum.at(i+4)->get_int(),
            (double)doc_token_cnt_datum.at(i+3)->get_int(), (double)doc_token_cnt_datum.at(i+2)->get_int(),
            (double)doc_token_cnt_datum.at(i+1)->get_int(), (double)doc_token_cnt_datum.at(i)->get_int());
        // norm_len = doc_token_cnt / avg_doc_token_cnt
        const __m512d v_norm_len = _mm512_div_pd(v_doc_token_cnt, v_avg_doc_token_cnt);
        // denominator_term = p_k1 * ( (1.0 - p_b) + p_b * norm_len )
        const __m512d v_p_b_norm_len = _mm512_mul_pd(v_p_b, v_norm_len);
        const __m512d v_inner_sum = _mm512_add_pd(v_one_minus_p_b, v_p_b_norm_len);
        const __m512d v_denominator_term = _mm512_mul_pd(v_p_k1, v_inner_sum);
        // denominator = related_token_cnt + denominator_term
        const __m512d v_denominator = _mm512_add_pd(v_related_token_cnt, v_denominator_term);
        // doc_weight = related_token_cnt / denominator
        const __m512d v_doc_weight = _mm512_div_pd(v_related_token_cnt, v_denominator);
        const __m512d v_relevance = _mm512_mul_pd(v_token_weight, v_doc_weight);
        // 假设 res_datum[i] 处的内存是可写的，且结果需要存入 ObDatum 内部的 double 区域。
        double *start_addr = const_cast<double*>(res_datum[i].ptr().double_);
        _mm512_storeu_pd(start_addr, v_relevance); 
        eval_flags.set(i, 8); 
    }
    
    // for (; OB_SUCC(ret) && i < size; ++i) {
    //   if (OB_UNLIKELY(doc_token_cnt_datum.at(i)->is_null() || related_token_cnt_datum.at(i)->is_null())) {
    //     ret = OB_ERR_UNEXPECTED;
    //     LOG_WARN("unexpected null datum", K(ret), KPC(doc_token_cnt_datum.at(i)), KPC(related_token_cnt_datum.at(i)));
    //   } else if (!skip.contain(i) && !eval_flags.at(i)) {
    //     // 标量计算逻辑
    //     const int64_t related_token_cnt = related_token_cnt_datum.at(i)->get_uint();
    //     const int64_t doc_token_cnt = doc_token_cnt_datum.at(i)->get_int();
    //     const double norm_len = doc_token_cnt / avg_doc_token_cnt;
    //     const double doc_weight = doc_token_weight(related_token_cnt, norm_len);
    //     const double relevance = token_weight * doc_weight;
    //     res_datum[i].set_double(relevance);
    //     eval_flags.set(i);
    //     LOG_DEBUG("show bm25 parameters for current document", K(token_doc_cnt), K(total_doc_cnt),
    //               K(related_token_cnt), K(doc_token_cnt), K(avg_doc_token_cnt),
    //               K(norm_len), K(token_weight), K(doc_weight), K(relevance));
    //   }
    // }
  }
  return ret;
}

double ObExprBM25::doc_token_weight(const int64_t token_freq, const double norm_len)
{
  const double tf = static_cast<double>(token_freq);
  return tf / (tf + p_k1 * (1.0 - p_b + p_b * norm_len));
}

double ObExprBM25::query_token_weight(const int64_t doc_freq, const int64_t doc_cnt)
{
  const double df = static_cast<double>(doc_freq);
  const double len = static_cast<double>(doc_cnt);
  // Since we might use approximate count statistic for total doc cnt, possibilities there are
  //   document frequencies larger than total doc cnt
  const double diff = (len - df) > 0 ? (len - df) : 0;
  const double idf = std::log((diff + 0.5) / (df + 0.5));
  return MAX(p_epsilon, idf) * (1.0 + p_k1);
}


} // namespace sql
} // namespace oceanbase
