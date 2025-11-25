/*
 * Copyright (c) 2025 OceanBase.
 * ... License ...
 */

#ifndef OB_FTS_STOP_WORD_H_
#define OB_FTS_STOP_WORD_H_

#include "lib/container/ob_iarray.h"
#include "lib/hash/ob_hashset.h"
#include "object/ob_object.h"
#include "storage/fts/ob_fts_struct.h"
#include "storage/fts/ob_fts_parser_property.h"

namespace oceanbase
{
namespace storage
{

class ObFTParserProperty;

#define FTS_STOP_WORD_MAX_LENGTH 10
static const char ob_stop_word_list[][FTS_STOP_WORD_MAX_LENGTH] = {
  "a", "about", "an", "are", "as", "at", "be", "by", "com", "de",
  "en", "for", "from", "how", "i", "in", "is", "it", "la", "of",
  "on", "or", "that", "the", "this", "to", "was", "what", "when", "where",
  "who", "will", "with", "und", "the", "www"
};

class ObStopWordChecker final
{
public:
  ObStopWordChecker() = default;
  ~ObStopWordChecker();
  int init();
  void destroy();
  int check_stopword(const ObFTWord &word, bool &is_stopword, common::ObIAllocator *allocator);
private:
  static const int64_t DEFAULT_STOPWORD_BUCKET_NUM = 37L;
  typedef common::hash::ObHashSet<storage::ObFTWord> StopWordSet;
  StopWordSet stopword_set_;
  ObObjMeta stopword_type_;
  bool inited_ = false;
  static_assert(sizeof(ob_stop_word_list) / sizeof(ob_stop_word_list[0]) <= DEFAULT_STOPWORD_BUCKET_NUM,
              "ob_stop_word_list's number shouldn't be greater than DEFAULT_STOPWORD_BUCKET_NUM");
};

int get_global_stopword_check_result(const ObFTWord &word, bool &is_stopword, common::ObIAllocator *allocator);

template <typename WordMapT>
class ObAddWordT final
{
public:
  ObAddWordT(
      const ObFTParserProperty &property,
      const ObObjMeta &meta,
      const ObAddWordFlag &flag,
      common::ObIAllocator &allocator,
      WordMapT &word_map);
  ~ObAddWordT() = default;
  int process_word(
      const char *word,
      const int64_t word_len,
      const int64_t char_cnt,
      const int64_t word_freq);
  virtual int64_t get_add_word_count() const { return non_stopword_cnt_; }
  VIRTUAL_TO_STRING_KV(
      K_(word_meta),
      K_(min_max_word_cnt),
      K_(non_stopword_cnt),
      K_(stopword_cnt),
      K_(min_token_size),
      K_(max_token_size),
      K(word_map_.size()));

private:
  bool is_min_max_word(const int64_t c_len) const;
  int casedown_word(const char* src_ptr, int64_t src_len, char* dst_buf); 
  int check_stopword(const ObFTWord &word, bool &is_stopword);
  int groupby_word(const ObFTWord &word, const int64_t word_cnt);

private:
  ObObjMeta word_meta_;
  common::ObIAllocator &allocator_;
  WordMapT &word_map_;
  int64_t min_max_word_cnt_;
  int64_t non_stopword_cnt_;
  int64_t stopword_cnt_;
  int64_t min_token_size_;
  int64_t max_token_size_;
  ObAddWordFlag flag_;
};

typedef ObAddWordT<ObFTWordMap> ObAddWord;

} // end namespace storage
} // end namespace oceanbase

#endif // OB_FTS_STOP_WORD_H_