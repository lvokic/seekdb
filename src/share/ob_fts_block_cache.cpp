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

#define USING_LOG_PREFIX SHARE

#include "share/ob_fts_block_cache.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace share
{

// ============================================================================
// ObFtsBlockCacheKey Implementation
// ============================================================================

ObFtsBlockCacheKey::ObFtsBlockCacheKey()
  : tenant_id_(OB_INVALID_ID),
    table_id_(OB_INVALID_ID),
    tablet_id_(OB_INVALID_ID),
    term_hash_(0),
    block_id_(0)
{
}

ObFtsBlockCacheKey::ObFtsBlockCacheKey(uint64_t tenant_id, 
                                       uint64_t table_id, 
                                       uint64_t tablet_id, 
                                       int64_t term_hash, 
                                       int64_t block_id)
  : tenant_id_(tenant_id),
    table_id_(table_id),
    tablet_id_(tablet_id),
    term_hash_(term_hash),
    block_id_(block_id)
{
}

bool ObFtsBlockCacheKey::operator ==(const common::ObIKVCacheKey &other) const
{
  const ObFtsBlockCacheKey &other_key = reinterpret_cast<const ObFtsBlockCacheKey &>(other);
  return tenant_id_ == other_key.tenant_id_ &&
         table_id_ == other_key.table_id_ &&
         tablet_id_ == other_key.tablet_id_ &&
         term_hash_ == other_key.term_hash_ &&
         block_id_ == other_key.block_id_;
}

uint64_t ObFtsBlockCacheKey::hash() const
{
  uint64_t hash_val = 0;
  hash_val = common::murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
  hash_val = common::murmurhash(&table_id_, sizeof(table_id_), hash_val);
  hash_val = common::murmurhash(&tablet_id_, sizeof(tablet_id_), hash_val);
  hash_val = common::murmurhash(&term_hash_, sizeof(term_hash_), hash_val);
  hash_val = common::murmurhash(&block_id_, sizeof(block_id_), hash_val);
  return hash_val;
}

int64_t ObFtsBlockCacheKey::size() const
{
  return sizeof(*this);
}

int ObFtsBlockCacheKey::deep_copy(char *buf, const int64_t buf_len, common::ObIKVCacheKey *&key) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || buf_len < size()) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("buffer not enough", K(ret), K(buf_len), "need_size", size());
  } else {
    // 使用 placement new 在预分配的 buffer 上构造对象
    ObFtsBlockCacheKey *new_key = new (buf) ObFtsBlockCacheKey(
        tenant_id_, table_id_, tablet_id_, term_hash_, block_id_);
    key = new_key;
  }
  return ret;
}

// ============================================================================
// ObFtsBlockCacheValue Implementation
// ============================================================================

ObFtsBlockCacheValue::ObFtsBlockCacheValue()
  : max_score_(0.0)
{
}

ObFtsBlockCacheValue::ObFtsBlockCacheValue(double score)
  : max_score_(score)
{
}

int64_t ObFtsBlockCacheValue::size() const
{
  return sizeof(*this);
}

int ObFtsBlockCacheValue::deep_copy(char *buf, const int64_t buf_len, common::ObIKVCacheValue *&value) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || buf_len < size()) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("buffer not enough", K(ret), K(buf_len), "need_size", size());
  } else {
    // 使用 placement new
    ObFtsBlockCacheValue *new_val = new (buf) ObFtsBlockCacheValue(max_score_);
    value = new_val;
  }
  return ret;
}

// ============================================================================
// ObFtsBlockCache Implementation
// ============================================================================

const char *const ObFtsBlockCache::CACHE_NAME = "fts_block_cache";

ObFtsBlockCache &ObFtsBlockCache::get_instance()
{
  static ObFtsBlockCache instance;
  return instance;
}

ObFtsBlockCache::ObFtsBlockCache()
  : is_inited_(false)
{
}

ObFtsBlockCache::~ObFtsBlockCache()
{
  destroy();
}

int ObFtsBlockCache::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(cache_.init(CACHE_NAME))) { // 默认优先级
    LOG_WARN("init kv cache failed", K(ret), K(CACHE_NAME));
  } else {
    is_inited_ = true;
    LOG_INFO("ObFtsBlockCache init success");
  }
  return ret;
}

void ObFtsBlockCache::destroy()
{
  if (is_inited_) {
    cache_.destroy();
    is_inited_ = false;
  }
}

int ObFtsBlockCache::get_block_max_score(const ObFtsBlockCacheKey &key, 
                                         const ObFtsBlockCacheValue *&value, 
                                         ObFtsBlockCacheHandle &handle)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(cache_.get(key, value, handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get token df from cache failed", K(ret), K(key));
    }
  }
  return ret;
}

int ObFtsBlockCache::put_block_max_score(const ObFtsBlockCacheKey &key, 
                                         double max_score)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    // 构造 Value 对象，KVCache 会调用其 deep_copy 方法将数据拷贝到底层内存
    ObFtsBlockCacheValue value(max_score);
    if (OB_FAIL(cache_.put(key, value))) {
      LOG_WARN("put block max score to cache failed", K(ret), K(key), K(value));
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase