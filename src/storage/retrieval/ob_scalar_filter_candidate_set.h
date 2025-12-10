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

#ifndef OB_SCALAR_FILTER_CANDIDATE_SET_H_
#define OB_SCALAR_FILTER_CANDIDATE_SET_H_

#include "lib/hash/ob_hashset.h"
#include "lib/container/ob_bit_set.h"
#include "lib/utility/ob_print_utils.h"
#include "src/share/datum/ob_datum.h"

namespace oceanbase
{
namespace storage
{

typedef std::function<bool(const ObDatum &doc_id)> ObScalarFilterFunc;

/**
 * @brief 标量过滤候选集合
 * 
 * 简化实现：支持两种模式
 * 1. HashSet模式: 预构建候选集（用于离线场景）
 * 2. Callback模式: 动态过滤函数（用于在线场景）
 */
class ObScalarFilterCandidateSet
{
public:
  ObScalarFilterCandidateSet()
    : allocator_(nullptr),
      use_hashset_(true),
      use_callback_(false),
      filter_func_(nullptr),
      doc_id_set_(),
      total_doc_count_(0),
      candidate_count_(0),
      selectivity_(1.0),
      is_inited_(false)
  {
  }
  
  ~ObScalarFilterCandidateSet() { destroy(); }
  
  /**
   * @brief 初始化候选集
   * @param allocator 内存分配器
   * @param estimated_count 预估候选数量
   * @param use_hashset 是否使用HashSet (false则使用BitSet)
   * @param max_doc_id BitSet模式下的最大文档ID（用于确定bitmap大小）
   */
  int init(ObIAllocator *allocator, 
           int64_t estimated_count = 10000,
           bool use_hashset = true,
           int64_t max_doc_id = -1);
  
  /**
   * @brief 设置过滤回调函数（用于动态过滤）
   * @param func 过滤函数，返回true表示文档通过过滤
   */
  void set_filter_func(const ObScalarFilterFunc &func) {
    filter_func_ = func;
    use_callback_ = true;
  }
  
  /**
   * @brief 检查文档ID是否通过过滤
   * @param doc_id 文档ID
   * @return true 通过过滤（应该保留）
   */
  bool contains(const ObDatum &doc_id) const;
  bool contains(uint64_t doc_id_val) const;
  
  /**
   * @brief 添加文档ID到候选集
   * @param doc_id 文档ID
   */
  int add(const ObDatum &doc_id);
  int add(uint64_t doc_id_val);
  
  /**
   * @brief 批量添加文档ID
   * @param doc_ids 文档ID数组
   * @param count 数量
   */
  int add_batch(const ObDatum *doc_ids, int64_t count);
  
  /**
   * @brief 设置总文档数 (用于计算选择性)
   */
  void set_total_doc_count(int64_t total_count) { 
    total_doc_count_ = total_count;
    calculate_selectivity();
  }
  
  /**
   * @brief 获取候选集大小
   */
  int64_t size() const { return candidate_count_; }
  
  /**
   * @brief 判断候选集是否为空
   */
  bool empty() const { return candidate_count_ == 0; }
  
  /**
   * @brief 获取选择性 (候选数/总数)
   */
  double get_selectivity() const { return selectivity_; }
  
  /**
   * @brief 判断是否应该启用标量前置优化
   * 
   * 规则:
   * 1. 选择性 < 50% (过滤掉超过一半文档)
   * 2. 候选数 >= 最小阈值 (避免候选集太小导致开销大于收益)
   * 3. 总文档数足够大
   */
  bool should_enable_prefetch(int64_t min_candidates = 1000) const {
    return is_inited_ && 
           selectivity_ < 0.5 && 
           candidate_count_ >= min_candidates &&
           total_doc_count_ > min_candidates * 2;
  }
  
  /**
   * @brief 清空候选集
   */
  void reuse();
  void destroy();
  
  TO_STRING_KV(K_(use_hashset), K_(total_doc_count), K_(candidate_count), 
               K_(selectivity), K_(is_inited));

private:
  void calculate_selectivity() {
    if (total_doc_count_ > 0) {
      selectivity_ = static_cast<double>(candidate_count_) / total_doc_count_;
    }
  }

private:
  ObIAllocator *allocator_;
  bool use_hashset_;                        // 是否使用HashSet
  bool use_callback_;                       // 是否使用回调函数模式
  ObScalarFilterFunc filter_func_;          // 过滤回调函数
  hash::ObHashSet<uint64_t> doc_id_set_;    // HashSet实现
  ObBitSet<> doc_id_bitset_;                // BitSet实现（更高效）
  int64_t max_doc_id_;                      // BitSet模式下的最大文档ID
  int64_t total_doc_count_;                 // 总文档数
  int64_t candidate_count_;                 // 候选文档数
  double selectivity_;                      // 选择性 (候选数/总数)
  bool is_inited_;
  
  DISALLOW_COPY_AND_ASSIGN(ObScalarFilterCandidateSet);
};

} // namespace storage
} // namespace oceanbase

#endif // OB_SCALAR_FILTER_CANDIDATE_SET_H_
