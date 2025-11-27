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

#include "object/ob_object.h"
#define USING_LOG_PREFIX STORAGE_FTS

#include "share/rc/ob_tenant_base.h"
#include "plugin/sys/ob_plugin_mgr.h"
#include "storage/fts/ob_fts_stop_word.h"
#include "storage/fts/ob_fts_plugin_helper.h"
#include "storage/fts/ob_fts_parser_property.h"

namespace oceanbase
{
namespace storage
{

int get_global_stopword_check_result(const ObFTWord &word, bool &is_stopword, common::ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  ObStopWordChecker *stop_word_checker = ObFTParsePluginData::instance().stop_word_checker();
  if (OB_ISNULL(stop_word_checker)) {
    is_stopword = false; 
  } else if (OB_FAIL(stop_word_checker->check_stopword(word, is_stopword, allocator))) {
    LOG_WARN("fail to check stopword", K(ret));
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////
// ObAddWordT Implementation
template <typename WordMapT>
ObAddWordT<WordMapT>::ObAddWordT(
      const ObFTParserProperty &property,
      const ObObjMeta &meta,
      const ObAddWordFlag &flag,
      common::ObIAllocator &allocator,
      WordMapT &word_map)
      : word_meta_(meta),
        allocator_(allocator),
        word_map_(word_map),
        min_max_word_cnt_(0),
        non_stopword_cnt_(0),
        stopword_cnt_(0),
        min_token_size_(property.min_token_size_),
        max_token_size_(property.max_token_size_),
        flag_(flag)
{
}

template <typename WordMapT>
int ObAddWordT<WordMapT>::process_word(
    const char *word,
    const int64_t word_len,
    const int64_t char_cnt,
    const int64_t word_freq)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(word) || OB_UNLIKELY(word_len <= 0 || char_cnt <= 0 || word_freq <= 0)) {
    // invalid input, skip
  } else if (flag_.min_max_word() && !is_min_max_word(char_cnt)) {
    min_max_word_cnt_ += word_freq;
  } else {
    const int64_t STACK_BUF_SIZE = 128;
    char stack_buf[STACK_BUF_SIZE];
    char *process_buf = nullptr;
    if (OB_LIKELY(word_len <= STACK_BUF_SIZE)) {
      process_buf = stack_buf;
    } else {
      if (OB_ISNULL(process_buf = (char *)allocator_.alloc(word_len))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret), K(word_len));
      }
    }
    if (OB_SUCC(ret)) {
      if (flag_.casedown()) {
        if (OB_FAIL(casedown_word(word, word_len, process_buf))) {
          LOG_WARN("failed to casedown word", K(ret));
        }
      } else {
        MEMCPY(process_buf, word, word_len);
      }
    }
    if (OB_SUCC(ret)) {
      ObFTWord tmp_word(word_len, process_buf, word_meta_);
      bool is_stopword = false;
      if (flag_.stopword() && OB_FAIL(check_stopword(tmp_word, is_stopword))) {
        LOG_WARN("failed to check stopword", K(ret));
      } else if (is_stopword) {
        stopword_cnt_ += word_freq;
      } else {
        if (flag_.groupby_word()) {
          if (OB_FAIL(groupby_word(tmp_word, word_freq))) {
            LOG_WARN("failed to groupby word", K(ret));
          }
          if (OB_SUCC(ret)) {
            non_stopword_cnt_ += word_freq;
          }
        } else {
          non_stopword_cnt_ += word_freq;
        }
      }
    }
  }
  return ret;
}

template <typename WordMapT>
bool ObAddWordT<WordMapT>::is_min_max_word(const int64_t c_len) const
{
  return (c_len >= min_token_size_ && c_len <= max_token_size_);
}

template <typename WordMapT>
int ObAddWordT<WordMapT>::casedown_word(const char* src_ptr, int64_t src_len, char* dst_buf)
{
  int ret = OB_SUCCESS;
  ObString src_str(src_len, src_len, const_cast<char*>(src_ptr));
  ObString dst_str;
  if (OB_FAIL(common::ObCharset::tolower(word_meta_.get_collation_type(), src_str, dst_str, allocator_))) {
    // log
  } else {
    int64_t copy_len = std::min(src_len, (int64_t)dst_str.length());
    MEMCPY(dst_buf, dst_str.ptr(), copy_len);
  }
  return ret;
}

template <typename WordMapT>
int ObAddWordT<WordMapT>::check_stopword(const ObFTWord &word, bool &is_stopword)
{
  return get_global_stopword_check_result(word, is_stopword, &allocator_);
}

template <typename WordMapT>
int ObAddWordT<WordMapT>::groupby_word(const ObFTWord &word, const int64_t word_cnt)
{
  int ret = OB_SUCCESS;
  int64_t *cnt = nullptr;
  if (OB_NOT_NULL(cnt = word_map_.get(word))) {
    *cnt += word_cnt;
  } else {
    const ObString &str = word.get_word().get_string();
    int64_t len = str.length();
    char *persist_buf = nullptr;
    if (OB_ISNULL(persist_buf = (char *)allocator_.alloc(len))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "failed to alloc key memory", K(ret));
    } else {
        MEMCPY(persist_buf, str.ptr(), len);
        uint64_t cached_hash;
        word.hash(cached_hash);
        ObFTWord persist_key(len, persist_buf, word_meta_, cached_hash);
        if (OB_FAIL(word_map_.set_refactored(persist_key, word_cnt))) {
            STORAGE_LOG(WARN, "failed to set word map", K(ret));
        }
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////
// class ObStopWordChecker
ObStopWordChecker::~ObStopWordChecker()
{
  destroy();
}

int ObStopWordChecker::init()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  if (OB_INVALID_TENANT_ID == tenant_id) {
    tenant_id = OB_SERVER_TENANT_ID;
  }

  if (inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(stopword_set_.create(DEFAULT_STOPWORD_BUCKET_NUM, "StopWordSet", "StopWordSet", tenant_id))) {
    LOG_WARN("fail to create stop word set", K(ret));
  } else {
    ObObjMeta stop_meta;
    stop_meta.set_varchar();
    stop_meta.set_collation_type(ObCollationType::CS_TYPE_UTF8MB4_GENERAL_CI);

    stopword_type_.set_meta(stop_meta);
    const int64_t stopword_count = sizeof(ob_stop_word_list) / sizeof(ob_stop_word_list[0]);
    for (int64_t i = 0; OB_SUCC(ret) && i < stopword_count; ++i) {
      ObFTWord stopword(STRLEN(ob_stop_word_list[i]), ob_stop_word_list[i], stopword_type_);
      if (OB_FAIL(stopword_set_.set_refactored(stopword))) {
        LOG_WARN("fail to set stop word", K(ret), K(stopword));
      }
    }

    if (OB_SUCC(ret)) {
      inited_ = true;
    }
  }
  return ret;
}

void ObStopWordChecker::destroy()
{
  if (inited_) {
    stopword_set_.destroy();
    inited_ = false;
  }
}

int ObStopWordChecker::check_stopword(const ObFTWord &word, bool &is_stopword, common::ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObStopWordChecker hasn't been initialized", K(ret), K(inited_));
  } else if (OB_UNLIKELY(word.empty())) {
    // 空词直接返回 false，不做处理
  } else {
    ObObjMeta lookup_meta;
    lookup_meta.set_varchar();
    lookup_meta.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    const ObDatum &datum = word.get_word();
    ObFTWord lookup_key(datum.len_, datum.ptr_, lookup_meta);
    int hash_ret = stopword_set_.exist_refactored(lookup_key);
    if (hash_ret == OB_HASH_EXIST) {
        is_stopword = true;
    } else if (hash_ret == OB_HASH_NOT_EXIST) {
        is_stopword = false;
    } else {
        ret = hash_ret;
        if (ret != OB_HASH_NOT_EXIST) {
             LOG_WARN("check exist in hash set failed", K(ret));
        }
    }
  }
  return ret;
}

template class ObAddWordT<ObFTWordMap>;

template class ObAddWordT<ObFTSArenaWordMap>;

} // end namespace storage
} // end namespace oceanbase