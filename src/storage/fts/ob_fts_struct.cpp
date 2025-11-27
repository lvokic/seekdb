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

#include "storage/fts/ob_fts_struct.h"

#include "lib/charset/ob_charset.h"
#include "share/datum/ob_datum_funcs.h"
#include "storage/ob_storage_util.h"

namespace oceanbase
{
namespace storage
{
void ObFTWord::calc_hash()
{
  if (OB_LIKELY(!word_.is_null())) {
    hash_val_ = common::murmurhash64A(word_.ptr_, word_.len_, 0);
  } else {
    hash_val_ = 0;
  }
}

bool ObFTWord::inner_equal(const ObFTWord &other) const
{
  return common::ObDatum::binary_equal(word_, other.word_);
}

} // namespace storage
} // namespace oceanbase
