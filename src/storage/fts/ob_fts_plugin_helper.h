/*
 * Copyright (c) 2025 OceanBase.
 * ... License ...
 */

#ifndef OB_FTS_PLUGIN_HELPER_H_
#define OB_FTS_PLUGIN_HELPER_H_

#define USING_LOG_PREFIX STORAGE_FTS

#include "lib/allocator/ob_fifo_allocator.h"
#include "lib/charset/ob_charset.h"
#include "lib/string/ob_string.h"
#include "object/ob_object.h"
#include "share/ob_plugin_helper.h"
#include "storage/fts/ob_fts_parser_property.h"
#include "storage/fts/ob_fts_struct.h"
#include "share/ob_force_print_log.h"
#include "plugin/interface/ob_plugin_ftparser_intf.h"
#include "storage/fts/ob_fts_stop_word.h"

namespace oceanbase
{
namespace common
{
class ObIJsonBase;
}

namespace plugin
{
class ObIFTParserDesc;
class ObPluginParam;
}

namespace storage
{

class ObStopWordChecker;
class ObFTDictHub;

#define FTS_BUILD_IN_PARSER_LIST                                                                   \
  FT_PARSER_TYPE(FTP_SPACE, space)                                                                 \
  FT_PARSER_TYPE(FTP_NGRAM, ngram)                                                                 \
  FT_PARSER_TYPE(FTP_BENG, beng)                                                                   \
  FT_PARSER_TYPE(FTP_IK, ik)                                                                       \
  FT_PARSER_TYPE(FTP_NGRAM2, ngram2)

class ObFTParser final
{
public:
  enum ParserType : int64_t {
    FTP_NON_BUILDIN = 0,
#define FT_PARSER_TYPE(ftp_type, parser_name) ftp_type,
    FTS_BUILD_IN_PARSER_LIST
#undef FT_PARSER_TYPE
    FTP_MAX
  };
  static const char *NAME_STR[ParserType::FTP_MAX + 1];

public:
  ObFTParser() : parser_name_(), parser_version_(-1) {}
  ~ObFTParser() = default;
  int parse_from_str(const char *plugin_name, const int64_t buf_len);
  int serialize_to_str(char *buf, const int64_t buf_len);

#define FT_PARSER_TYPE(fts_type, parser_name)                          \
  OB_INLINE bool is_##parser_name() const {                            \
    ParserType type = fts_type;                                        \
    return share::ObPluginName(NAME_STR[type]) == parser_name_;        \
  }
  FTS_BUILD_IN_PARSER_LIST
#undef FT_PARSER_TYPE

  OB_INLINE const share::ObPluginName &get_parser_name() const { return parser_name_; }
  OB_INLINE int64_t get_parser_version() const { return parser_version_; }
  OB_INLINE bool is_valid() const { return parser_name_.is_valid() && parser_version_ >= 0; }
  OB_INLINE bool is_type_before_4_3_5_1() const { return is_space() || is_beng() || is_ngram(); }
  OB_INLINE void set_name_and_version(const share::ObPluginName &name, const int64_t version)
  {
    parser_name_ = name;
    parser_version_ = version;
  }
  OB_INLINE bool operator ==(const ObFTParser &other) const
  {
    bool is_equal = true;
    if (this != &other) {
      is_equal = parser_name_ == other.get_parser_name() && parser_version_ == other.parser_version_;
    }
    return is_equal;
  }
  OB_INLINE bool operator !=(const ObFTParser &other) const { return !(*this == other); }
  TO_STRING_KV(K_(parser_name), K_(parser_version));
private:
  share::ObPluginName parser_name_;
  int64_t parser_version_;
};

class ObFTParsePluginData final
{
public:
  ObFTParsePluginData() = default;
  ~ObFTParsePluginData();
  static int  init_global();
  static void deinit_global();
  static ObFTParsePluginData &instance();
  int init();
  void destroy();
  ObStopWordChecker *stop_word_checker() const { return stop_word_checker_; }
  int get_dict_hub(ObFTDictHub *&hub);
private:
  int init_and_set_stopword_list();
  int init_dict_hub();
private:
  ObStopWordChecker * stop_word_checker_ = nullptr;
  ObFTDictHub * dict_hub_          = nullptr;
  common::ObFIFOAllocator handler_allocator_;
  bool                    is_inited_         = false;
};

class ObFTParseHelper final
{
public:
  ObFTParseHelper();
  ~ObFTParseHelper();

  int init(
      common::ObIAllocator *allocator,
      const common::ObString &plugin_name,
      const common::ObString &plugin_properties);

  template <typename WordMapT>
  int segment(
      const common::ObObjMeta &meta,
      const char *fulltext,
      const int64_t fulltext_len,
      int64_t &doc_length,
      WordMapT &words) const;

  int check_is_the_same(
      const common::ObString &plugin_name,
      const common::ObString &plugin_properties,
      bool &is_same) const;

  int make_detail_json(
      const ObFTWordMap &words,
      const int64_t doc_length,
      common::ObIJsonBase *&json_root);

  int make_token_array_json(
      const ObFTWordMap &words,
      common::ObIJsonBase *&json_root);

  void reset();

  TO_STRING_KV(KP_(allocator), K_(parser_name), KP_(parser_desc), K_(is_inited));

private:
  template <typename AddWordT>
  static int segment(
      const ObFTParserProperty &property,
      const int64_t parser_version,
      const plugin::ObIFTParserDesc *parser_desc,
      plugin::ObPluginParam *plugin_param,
      const ObCharsetInfo *cs,
      const char *fulltext,
      const int64_t fulltext_len,
      common::ObIAllocator &allocator,
      AddWordT &add_word);

  int set_add_word_flag(const plugin::ObIFTParserDesc &ftparser_desc);

private:
  common::ObIAllocator *allocator_;
  plugin::ObIFTParserDesc *parser_desc_;
  plugin::ObPluginParam *plugin_param_;
  ObFTParser parser_name_;
  ObAddWordFlag add_word_flag_;
  ObFTParserProperty parser_property_;
  bool is_inited_;

private:
  static constexpr const char *ENTRY_NAME_DOC_LEN = "doc_len";
  static constexpr const char *ENTRY_NAME_TOKENS = "tokens";
  DISALLOW_COPY_AND_ASSIGN(ObFTParseHelper);
};

template <typename AddWordT>
int ObFTParseHelper::segment(
    const ObFTParserProperty &property,
    const int64_t parser_version,
    const plugin::ObIFTParserDesc *parser_desc,
    plugin::ObPluginParam *plugin_param,
    const ObCharsetInfo *cs,
    const char *fulltext,
    const int64_t fulltext_len,
    common::ObIAllocator &allocator,
    AddWordT &add_word)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(parser_version < 0 || nullptr == parser_desc || nullptr == cs || nullptr == fulltext || 0 >= fulltext_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(parser_version), KP(parser_desc), KP(cs), K(fulltext), K(fulltext_len));
  } else {
    plugin::ObFTParserParam param;
    plugin::ObITokenIterator *iter = nullptr;
    param.allocator_ = &allocator;
    param.cs_ = cs;
    param.fulltext_ = fulltext;
    param.ft_length_ = fulltext_len;
    param.parser_version_ = parser_version;
    param.plugin_param_ = plugin_param;
    param.ngram_token_size_ = property.ngram_token_size_;
    param.ik_param_.mode_
        = (property.ik_mode_smart_ ? plugin::ObFTIKParam::Mode::SMART : plugin::ObFTIKParam::Mode::MAX_WORD);
    param.min_ngram_size_ = property.min_ngram_token_size_;
    param.max_ngram_size_ = property.max_ngram_token_size_;

    if (OB_FAIL(parser_desc->segment(&param, iter))) {
      LOG_WARN("fail to segment", K(ret), K(param));
    } else if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, token iterator is nullptr", K(ret), KP(iter));
    } else {
      const char *word = nullptr;
      int64_t word_len = 0;
      int64_t char_cnt = 0;
      int64_t word_freq = 0;
      while (OB_SUCC(ret)) {
        if (OB_FAIL(iter->get_next_token(word, word_len, char_cnt, word_freq))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fail to get next token", K(ret), KPC(iter));
          }
        } else if (OB_FAIL(add_word.process_word(word, word_len, char_cnt, word_freq))) {
          LOG_WARN("fail to process one word", K(ret), KP(word), K(word_len), K(char_cnt), K(word_freq));
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
    if (OB_NOT_NULL(iter)) {
      parser_desc->free_token_iter(&param, iter);
      iter = nullptr;
    }
  }
  return ret;
}

template <typename WordMapT>
int ObFTParseHelper::segment(
    const common::ObObjMeta &meta,
    const char *fulltext,
    const int64_t fulltext_len,
    int64_t &doc_length,
    WordMapT &words) const
{
  int ret = OB_SUCCESS;
  const ObCharsetInfo *cs = nullptr;
  ObCollationType type = meta.get_collation_type();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("this fulltext parser helper hasn't been initialized", K(ret), K(is_inited_));
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator ptr is nullptr", K(ret), KP_(allocator), K_(is_inited));
  } else if (OB_UNLIKELY(CS_TYPE_INVALID == type || type >= CS_TYPE_PINYIN_BEGIN_MARK)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(type));
  } else if (OB_ISNULL(cs = common::ObCharset::get_charset(type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, charset info is nullptr", K(ret), K(type));
  } else {
    words.reuse();
    ObAddWordT<WordMapT> add_word(parser_property_, meta, add_word_flag_, *allocator_, words);
    if (OB_FAIL(segment(
                    parser_property_,
                    parser_name_.get_parser_version(),
                    parser_desc_,
                    plugin_param_,
                    cs,
                    fulltext,
                    fulltext_len,
                    *allocator_,
                    add_word))) {
      LOG_WARN("fail to segment fulltext", K(ret), K(parser_name_), KP(parser_desc_), KP(cs), KP(fulltext),
          K(fulltext_len), KP(allocator_), K(parser_property_));
    } else {
      doc_length = add_word.get_add_word_count();
    }
  }
  LOG_DEBUG("ft parse segment", K(ret), K(type), K(add_word_flag_), K(parser_name_),
      K(common::ObString(fulltext_len, fulltext)), K(words.size()));
  return ret;
}

} // end namespace storage
} // end namespace oceanbase

#endif // OB_FTS_PLUGIN_HELPER_H_