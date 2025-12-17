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

#define USING_LOG_PREFIX SHARE

#include "share/ob_token_df_cache.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace share
{

// 全局变量定义
ObTokenDFCache g_token_df_cache;

ObTokenDFCacheKey::ObTokenDFCacheKey()
  : tenant_id_(common::OB_INVALID_TENANT_ID),
    table_id_(common::OB_INVALID_ID),
    index_id_(common::OB_INVALID_ID),
    schema_version_(0),
    token_()
{
}

ObTokenDFCacheKey::ObTokenDFCacheKey(uint64_t tenant_id, uint64_t table_id, uint64_t index_id, int64_t ver, const common::ObString &tok)
  : tenant_id_(tenant_id),
    table_id_(table_id),
    index_id_(index_id),
    schema_version_(ver),
    token_(tok)
{
}

void ObTokenDFCacheKey::set_token(const char* str)
{
  token_ = common::ObString::make_string(str);
}

bool ObTokenDFCacheKey::operator==(const common::ObIKVCacheKey &other) const
{
  const ObTokenDFCacheKey &other_key = reinterpret_cast<const ObTokenDFCacheKey &>(other);
  return tenant_id_ == other_key.tenant_id_ &&
         table_id_ == other_key.table_id_ &&
         index_id_ == other_key.index_id_ &&
         schema_version_ == other_key.schema_version_ &&
         token_ == other_key.token_;
}

uint64_t ObTokenDFCacheKey::hash() const
{
  uint64_t hash_val = 0;
  hash_val = common::murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
  hash_val = common::murmurhash(&table_id_, sizeof(table_id_), hash_val);
  hash_val = common::murmurhash(&index_id_, sizeof(index_id_), hash_val);
  hash_val = common::murmurhash(&schema_version_, sizeof(schema_version_), hash_val);
  hash_val = common::murmurhash(token_.ptr(), token_.length(), hash_val);
  return hash_val;
}

int64_t ObTokenDFCacheKey::size() const
{
  return sizeof(*this) + token_.length();
}

int ObTokenDFCacheKey::deep_copy(char *buf, const int64_t buf_len, common::ObIKVCacheKey *&key) const
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(buf) || buf_len < size()) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    // 1. 在 buffer 头部 placement new 构造对象
    ObTokenDFCacheKey *new_key = new (buf) ObTokenDFCacheKey();
    *new_key = *this;
    // 2. 将 Token 字符串内容紧跟在结构体后面
    if (token_.length() > 0) {
      char *token_buf = buf + sizeof(*this);
      MEMCPY(token_buf, token_.ptr(), token_.length());
      // 3. 让新对象的 ObString 指向 buffer 内部的内存
      new_key->token_.assign_ptr(token_buf, token_.length());
    }
    
    key = new_key;
  }
  return ret;
}

ObTokenDFCacheValue::ObTokenDFCacheValue() 
  : token_doc_cnt_(0), max_token_relevance_(0.0) 
{
}

ObTokenDFCacheValue::ObTokenDFCacheValue(int64_t cnt, double relevance) 
  : token_doc_cnt_(cnt), max_token_relevance_(relevance) 
{
}

int64_t ObTokenDFCacheValue::size() const
{
  return sizeof(*this);
}

int ObTokenDFCacheValue::deep_copy(char *buf, const int64_t buf_len, common::ObIKVCacheValue *&value) const
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(buf) || buf_len < size()) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    // POD 类型，直接构造并赋值即可
    ObTokenDFCacheValue *new_val = new (buf) ObTokenDFCacheValue();
    *new_val = *this;
    value = new_val;
  }
  return ret;
}

ObTokenDFCache::ObTokenDFCache() : is_inited_(false) 
{
}

ObTokenDFCache::~ObTokenDFCache() 
{ 
  destroy(); 
}

int ObTokenDFCache::init()
{
  int ret = common::OB_SUCCESS;
  if (is_inited_) {
    ret = common::OB_INIT_TWICE;
  } else if (OB_FAIL(cache_.init("TokenDFCache", 1))) {
    LOG_WARN("init token df cache failed", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObTokenDFCache::destroy()
{
  if (is_inited_) {
    cache_.destroy();
    is_inited_ = false;
  }
}

int ObTokenDFCache::get_token_df(const ObTokenDFCacheKey &key, const ObTokenDFCacheValue *&value, ObTokenDFCacheHandle &handle)
{
  if (!is_inited_) return common::OB_NOT_INIT;
  return cache_.get(key, value, handle);
}

int ObTokenDFCache::put_token_df(const ObTokenDFCacheKey &key, const ObTokenDFCacheValue &value)
{
  if (!is_inited_) return common::OB_NOT_INIT;
  return cache_.put(key, value);
}

} // namespace share
} // namespace oceanbase