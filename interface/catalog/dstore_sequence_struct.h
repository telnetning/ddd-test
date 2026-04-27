/*
 * Copyright (C) 2026 Huawei Technologies Co.,Ltd.
 *
 * dstore is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * dstore is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. if not, see <https://www.gnu.org/licenses/>.
 *
 * ---------------------------------------------------------------------------------------
 *
 * dstore_fake_sequence.h
 *
 * IDENTIFICATION
 *        include/catalog/dstore_fake_sequence.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef STRUCT_SEQUENCE_STRUCT_H
#define STRUCT_SEQUENCE_STRUCT_H

#include "common/dstore_common_utils.h"
#include "page/dstore_page_struct.h"

namespace DSTORE {
#pragma pack (push, 1)
struct FormData_gs_sequence {
    DstoreNameData sequence_name;
    int128 last_value;
    int128 start_value;
    int128 increment_by;
    int128 max_value;
    int128 min_value;
    int128 cache_value;
    bool is_cycled;
    bool is_called;
    Oid seq_oid;
    char scale;
};
#pragma pack (pop)

static const uint8 ATTR_NUM_SEQ_NAME = 1;
static const uint8 ATTR_NUM_SEQ_LAST = 2;
static const uint8 ATTR_NUM_SEQ_START = 3;
static const uint8 ATTR_NUM_SEQ_INCREMENT = 4;
static const uint8 ATTR_NUM_SEQ_MAX = 5;
static const uint8 ATTR_NUM_SEQ_MIN = 6;
static const uint8 ATTR_NUM_SEQ_CACHE = 7;
static const uint8 ATTR_NUM_SEQ_CYCLE = 8;
static const uint8 ATTR_NUM_SEQ_CALLED = 9;
static const uint8 ATTR_NUM_SEQ_OID = 10;
static const uint8 ATTR_NUM_SEQ_SCALE = 11;

static const uint8_t MAX_GS_SEQUENCE_ATTRS = 11;
} /* namespace DSTORE */

#endif /* STRUCT_SEQUENCE_STRUCT_H */
