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
#include "ob_scan_merge_loser_tree.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
namespace storage
{
void ObScanMergeLoserTreeCmp::reset()
{
  datum_utils_ = nullptr;
  rowkey_size_ = 0;
  reverse_ = false;
  is_inited_ = false;
}

int ObScanMergeLoserTreeCmp::init(const int64_t rowkey_size, const ObStorageDatumUtils &datum_utils, const bool reverse)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (rowkey_size <= 0 || datum_utils.get_rowkey_count() < rowkey_size) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(rowkey_size), K(datum_utils));
  } else {
    datum_utils_ = &datum_utils;
    rowkey_size_ = rowkey_size;
    reverse_ = reverse;
    is_inited_ = true;
  }
  return ret;
}

int ObScanMergeLoserTreeCmp::compare_rowkey(const ObDatumRow &l_row, const ObDatumRow &r_row, int64_t &cmp_result)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!l_row.is_valid() || !r_row.is_valid() || nullptr == datum_utils_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(l_row), K(r_row), KP(datum_utils_));
  } else if (OB_UNLIKELY(l_row.get_column_count() < rowkey_size_ || r_row.get_column_count() < rowkey_size_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected row column cnt", K(ret), K(l_row), K(r_row), K_(rowkey_size));
  } else {
    int cmp_ret_int = 0;
    if (1 == rowkey_size_) {
      const ObStorageDatum &l_datum = l_row.storage_datums_[0];
      const ObStorageDatum &r_datum = r_row.storage_datums_[0];
      if (OB_FAIL(datum_utils_->get_cmp_funcs().at(0).get_cmp_func().cmp_func_(l_datum, r_datum, cmp_ret_int))) {
        STORAGE_LOG(WARN, "Failed to compare rowkey", K(ret), K(l_datum), K(r_datum));
      }
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && 0 == cmp_ret_int && i < rowkey_size_; ++i) {
        const ObStorageDatum &l_datum = l_row.storage_datums_[i];
        const ObStorageDatum &r_datum = r_row.storage_datums_[i];
        if (OB_FAIL(datum_utils_->get_cmp_funcs().at(i).get_cmp_func().cmp_func_(l_datum, r_datum, cmp_ret_int))) {
           STORAGE_LOG(WARN, "Failed to compare datum", K(ret), K(i));
        }
      }
    }
    if (OB_SUCC(ret)) {
      cmp_result = cmp_ret_int;
    }
  }
  return ret;
}

int ObScanMergeLoserTreeCmp::cmp(
    const ObScanMergeLoserTreeItem &l,
    const ObScanMergeLoserTreeItem &r,
    int64_t &cmp_ret)
{
  int ret = OB_SUCCESS;
  cmp_ret = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (nullptr == l.row_ || nullptr == r.row_ || l.row_->scan_index_ < 0 || r.row_->scan_index_ < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(l.row_), KP(r.row_));
  } else {
    cmp_ret = l.row_->scan_index_ - r.row_->scan_index_;
    if (0 == cmp_ret) {
      if (OB_FAIL(compare_rowkey(*l.row_, *r.row_, cmp_ret))) {
        LOG_WARN("compare rowkey error", K(ret));
      } else if (reverse_) {
        cmp_ret = -cmp_ret;
      }
    }
  }
  return ret;
}

}
}

