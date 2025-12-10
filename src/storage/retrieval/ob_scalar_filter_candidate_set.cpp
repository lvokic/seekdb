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

#include "ob_scalar_filter_candidate_set.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace storage
{

int ObScalarFilterCandidateSet::init(
    ObIAllocator *allocator,
    int64_t estimated_count,
    bool use_hashset,
    int64_t max_doc_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("candidate set already initialized", K(ret));
  } else if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid allocator", K(ret));
  } else {
    allocator_ = allocator;
    use_hashset_ = use_hashset;
    max_doc_id_ = max_doc_id;
    if (use_hashset_) {
      // 初始化HashSet，预估容量
      if (OB_FAIL(doc_id_set_.create(estimated_count, 
                                      ObMemAttr(MTL_ID(), "ScalarCandSet")))) {
        LOG_WARN("failed to create doc id hashset", K(ret), K(estimated_count));
      }
    } else {
      // 初始化BitSet，根据max_doc_id确定大小
      if (max_doc_id <= 0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid max_doc_id for bitset mode", K(ret), K(max_doc_id));
      } else if (OB_FAIL(doc_id_bitset_.reserve(max_doc_id + 1))) {
        LOG_WARN("failed to init doc id bitset", K(ret), K(max_doc_id));
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
      LOG_DEBUG("scalar candidate set initialized", 
                K(estimated_count), K(use_hashset_), K(max_doc_id_));
    }
  }
  return ret;
}

bool ObScalarFilterCandidateSet::contains(const ObDatum &doc_id) const
{
  bool ret = false;
  if (!doc_id.is_null()) {
    if (use_callback_ && filter_func_) {
      ret = filter_func_(doc_id);
    } else if (IS_INIT) {
      uint64_t doc_id_val = doc_id.get_uint64();
      ret = contains(doc_id_val);
    }
  }
  return ret;
}

bool ObScalarFilterCandidateSet::contains(uint64_t doc_id_val) const
{
  bool ret = false;
  if (use_callback_ && filter_func_) {
    ObDatum datum;
    datum.set_uint(doc_id_val);
    ret = filter_func_(datum);
  } else if (IS_INIT) {
    if (use_hashset_) {
      int tmp_ret = doc_id_set_.exist_refactored(doc_id_val);
      ret = (tmp_ret == OB_HASH_EXIST);
    } else {
      if (doc_id_val <= max_doc_id_) {
        ret = doc_id_bitset_.has_member(doc_id_val);
      }
    }
  }
  return ret;
}

int ObScalarFilterCandidateSet::add(const ObDatum &doc_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("candidate set not initialized", K(ret));
  } else if (doc_id.is_null()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid doc id", K(ret));
  } else {
    uint64_t doc_id_val = doc_id.get_uint64();
    ret = add(doc_id_val);
  }
  return ret;
}

int ObScalarFilterCandidateSet::add(uint64_t doc_id_val)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("candidate set not initialized", K(ret));
  } else if (use_hashset_) {
    int hash_ret = doc_id_set_.set_refactored(doc_id_val);
    if (OB_HASH_EXIST == hash_ret) {
      // 文档已存在，不算错误
      LOG_DEBUG("doc id already exists in candidate set", K(doc_id_val));
    } else if (OB_SUCCESS != hash_ret) {
      ret = hash_ret;
      LOG_WARN("failed to add doc id to hashset", K(ret), K(doc_id_val));
    } else {
      // 成功添加新文档
      ++candidate_count_;
      calculate_selectivity();
      LOG_DEBUG("added doc id to candidate set", 
                K(doc_id_val), K_(candidate_count));
    }
  } else {
    // BitSet模式
    if (doc_id_val > max_doc_id_) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("doc id exceeds max bitset range", K(ret), K(doc_id_val), K_(max_doc_id));
    } else if (OB_FAIL(doc_id_bitset_.add_member(doc_id_val))) {
      LOG_WARN("failed to add doc id to bitset", K(ret), K(doc_id_val));
    } else {
      ++candidate_count_;
      calculate_selectivity();
      LOG_DEBUG("added doc id to bitset", K(doc_id_val), K_(candidate_count));
    }
  }
  return ret;
}

int ObScalarFilterCandidateSet::add_batch(const ObDatum *doc_ids, int64_t count)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("candidate set not initialized", K(ret));
  } else if (OB_ISNULL(doc_ids) || count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(doc_ids), K(count));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      if (OB_FAIL(add(doc_ids[i]))) {
        LOG_WARN("failed to add doc id", K(ret), K(i), K(doc_ids[i]));
      }
    }
    LOG_DEBUG("batch added doc ids to candidate set", 
              K(count), K_(candidate_count));
  }
  return ret;
}

void ObScalarFilterCandidateSet::reuse()
{
  if (IS_INIT) {
    if (use_hashset_) {
      doc_id_set_.reuse();
    } else {
      doc_id_bitset_.reuse();
    }
  }
  candidate_count_ = 0;
  selectivity_ = 1.0;
}

void ObScalarFilterCandidateSet::destroy()
{
  if (IS_INIT && use_hashset_) {
    doc_id_set_.destroy();
  }
  allocator_ = nullptr;
  candidate_count_ = 0;
  total_doc_count_ = 0;
  selectivity_ = 1.0;
  is_inited_ = false;
}

} // namespace storage
} // namespace oceanbase
