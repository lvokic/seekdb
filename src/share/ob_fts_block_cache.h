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

#ifndef OCEANBASE_SHARE_FTS_OB_FTS_BLOCK_CACHE_H_
#define OCEANBASE_SHARE_FTS_OB_FTS_BLOCK_CACHE_H_

#include "share/cache/ob_kv_storecache.h"
#include "share/cache/ob_kvcache_struct.h"

namespace oceanbase
{
namespace share
{

// -------------------------------------------------------------------------
// 1. 缓存 Key 定义 (ObFtsBlockCacheKey)
// -------------------------------------------------------------------------
struct ObFtsBlockCacheKey : public common::ObIKVCacheKey
{
  uint64_t tenant_id_;
  uint64_t table_id_;
  uint64_t tablet_id_;  // 增加 TabletID (分区级别缓存)
  int64_t term_hash_;   // 倒排词的 Hash (维度 ID)
  int64_t block_id_;    // 微块标识 (通常是 Block 的起始 DocID 或 Offset)

  ObFtsBlockCacheKey();
  ObFtsBlockCacheKey(uint64_t tenant_id, 
                     uint64_t table_id, 
                     uint64_t tablet_id, 
                     int64_t term_hash, 
                     int64_t block_id);
  virtual uint64_t get_tenant_id() const override { return tenant_id_; }
  virtual bool operator ==(const common::ObIKVCacheKey &other) const override;
  virtual uint64_t hash() const override;
  virtual int64_t size() const override;
  virtual int deep_copy(char *buf, const int64_t buf_len, common::ObIKVCacheKey *&key) const override;

  TO_STRING_KV(K_(tenant_id), K_(table_id), K_(tablet_id), K_(term_hash), K_(block_id));
};

// -------------------------------------------------------------------------
// 2. 缓存 Value 定义 (ObFtsBlockCacheValue)
// -------------------------------------------------------------------------
struct ObFtsBlockCacheValue : public common::ObIKVCacheValue
{
  double max_score_; // 该 Block 的分数上界 (WAND 剪枝核心)

  ObFtsBlockCacheValue();
  explicit ObFtsBlockCacheValue(double score);

  virtual int64_t size() const override;
  virtual int deep_copy(char *buf, const int64_t buf_len, common::ObIKVCacheValue *&value) const override;

  TO_STRING_KV(K_(max_score));
};

// 定义 Handle 类型别名 (用于持有引用计数)
typedef common::ObKVCacheHandle ObFtsBlockCacheHandle;

// -------------------------------------------------------------------------
// 3. 缓存管理类声明 (ObFtsBlockCache)
// -------------------------------------------------------------------------
class ObFtsBlockCache
{
public:
  static ObFtsBlockCache &get_instance();

  ObFtsBlockCache();
  virtual ~ObFtsBlockCache();

  int init();
  void destroy();

  // 获取 Block Max Score
  // [in] key: 查询 Key
  // [out] value: 输出 Value 指针 (生命周期由 handle 管理)
  // [out] handle: 缓存句柄，使用完后会自动释放引用
  int get_block_max_score(const ObFtsBlockCacheKey &key, 
                          const ObFtsBlockCacheValue *&value, 
                          ObFtsBlockCacheHandle &handle);

  // 写入 Block Max Score
  int put_block_max_score(const ObFtsBlockCacheKey &key, 
                          double max_score);

public:
  bool is_inited_;

private:
  // KVCache 名字，用于监控和内存统计
  static const char *const CACHE_NAME; 
  // 核心 Cache 实例
  common::ObKVCache<ObFtsBlockCacheKey, ObFtsBlockCacheValue> cache_;
};

// 全局访问宏 (可选)
#define OB_FTS_BLOCK_CACHE (oceanbase::share::ObFtsBlockCache::get_instance())

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_FTS_OB_FTS_BLOCK_CACHE_H_