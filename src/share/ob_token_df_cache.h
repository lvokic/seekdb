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

#ifndef OCEANBASE_SHARE_OB_TOKEN_DF_CACHE_H_
#define OCEANBASE_SHARE_OB_TOKEN_DF_CACHE_H_

#include "share/cache/ob_kv_storecache.h"
#include "share/cache/ob_kvcache_struct.h"
#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace share
{

// -------------------------------------------------------------------------
// 1. 缓存 Key 定义
// -------------------------------------------------------------------------
struct ObTokenDFCacheKey : public common::ObIKVCacheKey
{
  uint64_t tenant_id_;
  uint64_t table_id_;
  uint64_t index_id_;
  int64_t schema_version_;
  common::ObString token_;

  ObTokenDFCacheKey();
  ObTokenDFCacheKey(uint64_t tenant_id, uint64_t table_id, uint64_t index_id, int64_t ver, const common::ObString &tok);

  void set_table_id(uint64_t id) { table_id_ = id; }
  void set_index_id(uint64_t id) { index_id_ = id; }
  void set_schema_version(int64_t v) { schema_version_ = v; }
  void set_token(const common::ObString &t) { token_ = t; }
  void set_token(const char* str);
  bool has_token() const { return !token_.empty(); }
  virtual uint64_t get_tenant_id() const override { return tenant_id_; }
  virtual bool operator ==(const common::ObIKVCacheKey &other) const override;
  virtual uint64_t hash() const override;
  virtual int64_t size() const override;
  virtual int deep_copy(char *buf, const int64_t buf_len, common::ObIKVCacheKey *&key) const override;

  TO_STRING_KV(K_(tenant_id), K_(table_id), K_(index_id), K_(schema_version), K_(token));
};

// -------------------------------------------------------------------------
// 2. 缓存 Value 定义
// -------------------------------------------------------------------------
struct ObTokenDFCacheValue : public common::ObIKVCacheValue
{
  int64_t token_doc_cnt_;
  double max_token_relevance_;

  ObTokenDFCacheValue();
  ObTokenDFCacheValue(int64_t cnt, double relevance);

  int64_t get_token_doc_cnt() const { return token_doc_cnt_; }
  double get_max_token_relevance() const { return max_token_relevance_; }
  virtual int64_t size() const override;
  virtual int deep_copy(char *buf, const int64_t buf_len, common::ObIKVCacheValue *&value) const override;

  TO_STRING_KV(K_(token_doc_cnt), K_(max_token_relevance));
};

// 定义 Handle 类型别名
typedef common::ObKVCacheHandle ObTokenDFCacheHandle;

// -------------------------------------------------------------------------
// 3. 缓存管理类声明
// -------------------------------------------------------------------------
class ObTokenDFCache
{
public:
  ObTokenDFCache();
  virtual ~ObTokenDFCache();

  int init();
  void destroy();

  // 获取缓存
  // handle: 输出参数，用于持有缓存对象的引用计数
  int get_token_df(const ObTokenDFCacheKey &key, const ObTokenDFCacheValue *&value, ObTokenDFCacheHandle &handle);

  // 写入缓存
  int put_token_df(const ObTokenDFCacheKey &key, const ObTokenDFCacheValue &value);

public:
  bool is_inited_;

private:
  common::ObKVCache<ObTokenDFCacheKey, ObTokenDFCacheValue> cache_;
};

// 声明全局变量（如果在单例模式下使用）
extern ObTokenDFCache g_token_df_cache;

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_OB_TOKEN_DF_CACHE_H_