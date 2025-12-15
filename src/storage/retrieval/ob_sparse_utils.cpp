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

#define USING_LOG_PREFIX STORAGE

#include "ob_sparse_utils.h"

namespace oceanbase
{
namespace storage
{

int ObSRDaaTInnerProductRelevanceCollector::init(int64_t should_match)
{
  int ret = OB_SUCCESS;
  total_relevance_ = 0.0;
  should_match_ = should_match;
  return ret;
}

void ObSRDaaTInnerProductRelevanceCollector::reset()
{
  total_relevance_ = 0.0;
  matched_cnt_ = 0;
}

void ObSRDaaTInnerProductRelevanceCollector::reuse()
{
  total_relevance_ = 0.0;
  matched_cnt_ = 0;
}

int ObSRDaaTInnerProductRelevanceCollector::collect_one_dim(const int64_t dim_idx, const double relevance)
{
  int ret = OB_SUCCESS;
  total_relevance_ += relevance;
  matched_cnt_ ++;
  return ret;
}

int ObSRDaaTInnerProductRelevanceCollector::collect_batch_dims(
  const int64_t *dim_idxs,
  const double *relevances,
  int64_t count)
{
  int ret = OB_SUCCESS;
  matched_cnt_ += count;
  __m512d total_vec = _mm512_setzero_pd(); // 初始化 8 个 double 累加器
  int64_t i = 0;
  for (; i <= count - 8; i += 8) {
    __m512d scores = _mm512_loadu_pd(&relevances[i]);
    total_vec = _mm512_add_pd(total_vec, scores);
  }
  // 1. 将 512-bit 归约到 256-bit
  __m256d vlow_256 = _mm512_castpd512_pd256(total_vec);
  __m256d vhigh_256 = _mm512_extractf64x4_pd(total_vec, 1); // 提取高 256 位
  __m256d vsum_256 = _mm256_add_pd(vlow_256, vhigh_256); // [d3..d0] + [d7..d4]
  // 2. 将 256-bit 归约到 128-bit (使用 AVX2 逻辑)
  __m256d vsum_1 = _mm256_hadd_pd(vsum_256, vsum_256);
  __m128d vlow = _mm256_castpd256_pd128(vsum_1);
  __m128d vhigh = _mm256_extractf128_pd(vsum_1, 1);
  __m128d vsum_2 = _mm_add_pd(vlow, vhigh);
  double simd_sum = _mm_cvtsd_f64(vsum_2); // 从寄存器中取出标量结果
  // 3. 处理剩余的非 SIMD 部分
  double scalar_sum = 0.0;
  for (; i < count; ++i) {
    scalar_sum += relevances[i];
  }
  // 4. 更新总分数
  total_relevance_ += simd_sum + scalar_sum;

  return ret;
}

int ObSRDaaTInnerProductRelevanceCollector::get_result(double &relevance, bool &is_valid)
{
  int ret = OB_SUCCESS;
  relevance = total_relevance_;
  is_valid = matched_cnt_ >= should_match_;
  total_relevance_ = 0.0;
  matched_cnt_ = 0;
  return ret;
}

int ObSRDaaTBooleanRelevanceCollector::init(ObIAllocator *allocator, const int64_t dim_cnt, ObFtsEvalNode *node)
{
  int ret = OB_SUCCESS;
  allocator_ = allocator;
  dim_cnt_ = dim_cnt;
  boolean_compute_node_ = node;
  if (FALSE_IT(boolean_relevances_.set_allocator(allocator))) {
  } else if (OB_FAIL(boolean_relevances_.init(dim_cnt_))) {
    LOG_WARN("failed to init boolean relevances array", K(ret));
  } else if (OB_FAIL(boolean_relevances_.prepare_allocate(dim_cnt_))) {
    LOG_WARN("failed to prepare allocate boolean relevacnes array", K(ret));
  } else {
    for (int64_t i = 0; i < dim_cnt_; ++i) {
      boolean_relevances_[i] = 0.0;
    }
  }
  return ret;
}

void ObSRDaaTBooleanRelevanceCollector::reset()
{
  boolean_relevances_.reset();
  if (OB_NOT_NULL(boolean_compute_node_)) {
    boolean_compute_node_->release();
    boolean_compute_node_ = nullptr;
  }
}

void ObSRDaaTBooleanRelevanceCollector::reuse()
{
  for (int64_t i = 0; i < dim_cnt_; ++i) {
    boolean_relevances_[i] = 0.0;
  }
}

int ObSRDaaTBooleanRelevanceCollector::collect_one_dim(const int64_t dim_idx, const double relevance)
{
  int ret = OB_SUCCESS;
  boolean_relevances_[dim_idx] = relevance;
  return ret;
}

int ObSRDaaTBooleanRelevanceCollector::collect_batch_dims(
    const int64_t *dim_idxs,
    const double *relevances,
    int64_t count)
{
    int ret = OB_SUCCESS;
    const int64_t dim_count = dim_cnt_; 
    int64_t i = 0;
    // 循环展开 (步长为 4)，且必须保证每一步操作后都检查 ret 
    // 这将减少指令级并行性 (ILP)，但确保了严格的错误处理顺序，符合您的风格。
    for (; OB_SUCC(ret) && i <= count - 4; i += 4) {
      // ------------------ 块 1 ------------------
      const int64_t dim_idx_0 = dim_idxs[i];
      if (OB_UNLIKELY(dim_idx_0 < 0 || dim_idx_0 >= dim_count)) {
        ret = OB_ARRAY_OUT_OF_RANGE;
        LOG_WARN("dim index out of range", K(ret), K(dim_idx_0), K(dim_count));
      } else {
        boolean_relevances_[dim_idx_0] = relevances[i];
      }
      // ------------------ 块 2 ------------------
      if (OB_SUCC(ret)) { // 必须检查上一步是否成功
        const int64_t dim_idx_1 = dim_idxs[i+1];
        if (OB_UNLIKELY(dim_idx_1 < 0 || dim_idx_1 >= dim_count)) {
          ret = OB_ARRAY_OUT_OF_RANGE;
          LOG_WARN("dim index out of range", K(ret), K(dim_idx_1), K(dim_count));
        } else {
          boolean_relevances_[dim_idx_1] = relevances[i+1];
        }
      }
      // ------------------ 块 3 ------------------
      if (OB_SUCC(ret)) { // 必须检查上一步是否成功
        const int64_t dim_idx_2 = dim_idxs[i+2];
        if (OB_UNLIKELY(dim_idx_2 < 0 || dim_idx_2 >= dim_count)) {
          ret = OB_ARRAY_OUT_OF_RANGE;
          LOG_WARN("dim index out of range", K(ret), K(dim_idx_2), K(dim_count));
        } else {
          boolean_relevances_[dim_idx_2] = relevances[i+2];
        }
      }
      // ------------------ 块 4 ------------------
      if (OB_SUCC(ret)) { // 必须检查上一步是否成功
        const int64_t dim_idx_3 = dim_idxs[i+3];
        if (OB_UNLIKELY(dim_idx_3 < 0 || dim_idx_3 >= dim_count)) {
          ret = OB_ARRAY_OUT_OF_RANGE;
          LOG_WARN("dim index out of range", K(ret), K(dim_idx_3), K(dim_count));
        } else {
          boolean_relevances_[dim_idx_3] = relevances[i+3];
        }
      }
    }
    for (; OB_SUCC(ret) && i < count; ++i) { // 循环控制中包含 ret 检查
      const int64_t dim_idx = dim_idxs[i];
      if (OB_UNLIKELY(dim_idx < 0 || dim_idx >= dim_count)) {
        ret = OB_ARRAY_OUT_OF_RANGE;
        LOG_WARN("dim index out of range", K(ret), K(dim_idx), K(dim_count));
      } else {
        boolean_relevances_[dim_idx] = relevances[i];
      }
    }

    return ret;
}

int ObSRDaaTBooleanRelevanceCollector::get_result(double &relevance, bool &is_valid)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(boolean_compute_node_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null boolean compute node", K(ret));
  } else if (OB_FAIL(ObFtsEvalNode::fts_boolean_eval(boolean_compute_node_, boolean_relevances_, relevance))) {
    LOG_WARN("failed to evaluate boolean relevance");
  } else {
    is_valid = relevance > 0;
    const int64_t dim_count = dim_cnt_;
    __m512d zero = _mm512_setzero_pd();
    int64_t i = 0;
    for (; i <= dim_count - 8; i += 8) {
      _mm512_storeu_pd(&boolean_relevances_[i], zero);
    }    
    // 处理剩余的非 SIMD 部分
    for (; i < dim_count; ++i) {
      boolean_relevances_[i] = 0.0; // 标量赋值
    }
  }  
  return ret;
}

} // namespace storage
} // namespace oceanbase
