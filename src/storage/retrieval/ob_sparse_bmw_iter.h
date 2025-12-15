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

#ifndef OB_SPARSE_BMW_ITER_H_
#define OB_SPARSE_BMW_ITER_H_

#include "lib/container/ob_heap.h"
#include "ob_sparse_daat_iter.h"
#include "ob_sparse_retrieval_util.h"
#include "ob_scalar_filter_candidate_set.h"

namespace oceanbase
{
namespace storage
{
class ObSRBMWIterImpl : public ObSRDaaTIterImpl
{
public:
  ObSRBMWIterImpl();
  virtual ~ObSRBMWIterImpl() {}

  virtual void reuse(const bool switch_tablet = false) override;
  void reset();
  int init(
      ObSparseRetrievalMergeParam &iter_param,
      ObIArray<ObISRDaaTDimIter *> &dim_iters,
      ObIAllocator &iter_allocator,
      ObSRDaaTRelevanceCollector &relevance_collector);
  virtual int get_next_rows(const int64_t capacity, int64_t &count) override;
protected:
  virtual int process_collected_row(const ObDatum &id_datum, const double relevance) override;
  virtual int init_before_wand_process() { return OB_SUCCESS; }

  int top_k_search();
  int build_top_k_heap();
  int next_pivot(int64_t &pivot_iter_idx);
  int next_pivot_range(int64_t &skip_range_cnt);
  int evaluate_pivot(const int64_t pivot_iter_idx);
  int evaluate_pivot_range(const int64_t pivot_iter_idx, bool &is_candidate);
  double get_top_k_threshold() const
  {
    return top_k_heap_.empty() ? 0.0 : top_k_heap_.top().relevance_;
  }

private:
  ObISRDimBlockMaxIter *get_iter(const int64_t iter_idx) const
  {
    return static_cast<ObISRDimBlockMaxIter *>(dim_iters_->at(iter_idx));
  }
  int fill_merge_heap_with_shallow_dims(const ObDatum *last_range_border_id, const bool inclusive);
  int try_generate_next_range_from_merge_heap(
      bool &is_candidate_range,
      const ObDatum *&min_domain_id_with_pivot,
      const ObDatum *&max_domain_id_without_pivot);
  int project_rows_from_top_k_heap(const int64_t capacity, int64_t &count);
  int unify_dim_iters_for_next_round();
  int advance_dim_iters_for_next_round(const ObDatum &target_id, const bool iter_end_available);
  void set_next_round_iter_end(const int64_t idx) { next_round_iter_idxes_[idx] = -1; }
  bool is_next_round_iter_end(const int64_t idx) const { return next_round_iter_idxes_[idx] == -1; }

protected:
  enum BMWStatus{
    FIND_NEXT_PIVOT,
    EVALUATE_PIVOT_RANGE,
    EVALUATE_PIVOT,
    FIND_NEXT_PIVOT_RANGE,
    FINISHED,
    MAX_STATUS,
  };

  // Two-phase optimization configuration
  struct TwoPhaseConfig {
    TwoPhaseConfig() : enabled_(false), baseline_ratio_(1.0), reserve_ratio_(1.2),
                       use_adaptive_threshold_(true), min_baseline_count_(10) {}
    bool enabled_;                    // Enable two-phase optimization
    double baseline_ratio_;           // Ratio of baseline to target topk (default 1.0)
    double reserve_ratio_;            // Reserve count ratio for phase 2 (default 1.2)
    bool use_adaptive_threshold_;     // Dynamically adjust threshold based on selectivity
    int64_t min_baseline_count_;      // Minimum baseline count to enable optimization
    TO_STRING_KV(K_(enabled), K_(baseline_ratio), K_(reserve_ratio), 
                 K_(use_adaptive_threshold), K_(min_baseline_count));
  };

  // Statistics for two-phase optimization
  struct TwoPhaseStats {
    TwoPhaseStats() { reset(); }
    void reset() {
      phase1_row_count_ = 0;
      phase1_min_score_ = 0.0;
      phase1_max_score_ = 0.0;
      phase1_avg_score_ = 0.0;
      phase2_row_count_ = 0;
      phase2_pruned_count_ = 0;
      phase2_pivot_pruned_count_ = 0;
      phase2_total_pivots_ = 0;
      phase2_evaluated_pivots_ = 0;
      fallback_count_ = 0;
      final_threshold_ = 0.0;
      early_termination_ = false;
      selectivity_ratio_ = 0.0;
      scalar_filtered_count_ = 0;  // 新增: 被标量过滤掉的文档数
    }
    int64_t phase1_row_count_;          // Rows collected in phase 1
    double phase1_min_score_;           // Minimum score from phase 1
    double phase1_max_score_;           // Maximum score from phase 1
    double phase1_avg_score_;           // Average score from phase 1
    int64_t phase2_row_count_;          // Rows collected in phase 2
    int64_t phase2_pruned_count_;       // Rows pruned by baseline threshold
    int64_t phase2_pivot_pruned_count_; // Pivots pruned by baseline threshold
    int64_t phase2_total_pivots_;       // Total pivots examined in phase 2
    int64_t phase2_evaluated_pivots_;   // Pivots fully evaluated in phase 2
    int64_t fallback_count_;            // Times fallback to baseline results
    double final_threshold_;            // Final threshold used
    bool early_termination_;            // Whether phase 2 terminated early
    double selectivity_ratio_;          // Selectivity = pruned / total
    int64_t scalar_filtered_count_;     // Rows filtered by scalar conditions
    
    double get_pruning_ratio() const {
      return phase2_total_pivots_ > 0 
          ? static_cast<double>(phase2_pivot_pruned_count_) / phase2_total_pivots_ 
          : 0.0;
    }
    
    TO_STRING_KV(K_(phase1_row_count), K_(phase1_min_score), K_(phase1_max_score),
                 K_(phase1_avg_score), K_(phase2_row_count), K_(phase2_pruned_count), 
                 K_(phase2_pivot_pruned_count), K_(phase2_total_pivots), 
                 K_(phase2_evaluated_pivots), K_(fallback_count), K_(final_threshold),
                 K_(early_termination), K_(selectivity_ratio), K_(scalar_filtered_count),
                 "pruning_ratio", get_pruning_ratio());
  };

  struct TopKItem {
    TopKItem() : relevance_(0.0), cache_idx_(-1) {}
    TopKItem(const double &relevance, const int64_t &cache_idx) : relevance_(relevance), cache_idx_(cache_idx) {}
    ~TopKItem() = default;
    TO_STRING_KV(K_(relevance), K_(cache_idx));
    double relevance_;
    int64_t cache_idx_;
  };

  struct TopKItemCmp {
    bool operator()(const TopKItem &a, const TopKItem &b) const {
      return a.relevance_ > b.relevance_;
    }
    int get_error_code() { return common::OB_SUCCESS; }
  };
  typedef common::ObBinaryHeap<TopKItem, TopKItemCmp> TopKHeap;

  // Two-phase optimization methods
  int init_two_phase_config();
  int execute_phase1_baseline();
  int execute_phase2_bmw();
  bool should_use_two_phase() const { return two_phase_config_.enabled_ && baseline_top_k_count_ > 0; }
  double get_adaptive_threshold() const;
  int merge_baseline_and_phase2_results();
  void update_two_phase_stats();
  
  // Advanced optimization methods
  bool should_terminate_phase2_early() const;
  double predict_optimal_threshold() const;
  void adjust_threshold_dynamically();
  double calculate_selectivity_ratio() const;

  DISALLOW_COPY_AND_ASSIGN(ObSRBMWIterImpl);
  ObArenaAllocator allocator_;
  TopKItemCmp less_score_cmp_;
  // search_count may be greater than baseline_count_ to reserve rows for post-filtering
  // Maybe domain id not need to be cached within the topk heap since there would be top-n sort with datum store above
  TopKHeap top_k_heap_;
  int64_t top_k_count_;
  int64_t baseline_top_k_count_;
  double baseline_min_score_;
  ObDomainIdCmp domain_id_cmp_;
  ObFixedArray<ObDocIdExt, ObIAllocator> id_cache_;
  ObFixedArray<ObDocIdExt, ObIAllocator> baseline_id_cache_;
  ObFixedArray<double, ObIAllocator> baseline_relevances_;
  BMWStatus status_;
  // Two-phase optimization members
  TwoPhaseConfig two_phase_config_;
  TwoPhaseStats two_phase_stats_;
  bool is_phase1_completed_;
  bool is_phase2_completed_;
  
  // Scalar filter prefetch optimization
  const ObScalarFilterCandidateSet *scalar_candidates_;
  bool enable_scalar_filter_;
  
  // Simple ID range filter (for primary key id)
  int64_t id_lower_bound_;
  int64_t id_upper_bound_;
  bool enable_id_range_filter_;

  static constexpr double PREDICTION_ALPHA = 0.2;
};


} // namespace storage
} // namespace oceanbase
#endif
