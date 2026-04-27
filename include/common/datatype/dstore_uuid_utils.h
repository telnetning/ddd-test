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
 * dstore_uuid_utils.h
 *  This file define uuid.
 *
 *
 * IDENTIFICATION
 *        include/common/datatype/dstore_uuid_utils.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_DSTORE_UUID_UTILS_H
#define DSTORE_DSTORE_UUID_UTILS_H

#include "common/dstore_datatype.h"
#include <pthread.h>

namespace DSTORE {

#define UUID_FIRST_PART_LEN 8
#define UUID_MID_PART_LEN 4
#define UUID_LAST_PART_LEN 12

#define UUID_timestampMid_OFFSET 32
#define UUID_TIMESTAMP_HIGH_OFFSET 48

#define HUNDRED_NANO_SECOND 10

#define INT_RANGE_REVISE_PARAM 128
#define HEX_BASE 16
#define HEX_BYTES 4
#define URANDOM_FILE_PATH "/dev/urandom"

#define CLOCK_SEQ_CHAR_NUM 2
#define MAX_MAC_ADDR_LIST 10
#define HEX_CHARS "0123456789abcdef"

#define MAC_ADDR_LEN 6
#define MAC_ADDR_A_INDEX 0
#define MAC_ADDR_B_INDEX 1
#define MAC_ADDR_C_INDEX 2
#define MAC_ADDR_D_INDEX 3
#define MAC_ADDR_E_INDEX 4
#define MAC_ADDR_F_INDEX 5

#define MAC_GET_UINT64(X)                                                                                         \
    (((uint64)X.a << 40) | ((uint64)X.b << 32) | ((uint64)X.c << 24) | ((uint64)X.d << 16) | ((uint64)X.e << 8) | \
     (uint64)X.f)

#define PDB_DEFAULT_UUID_STR "00000000-0000-0000-0000-000000000000\0"

/*
 *	This is the internal storage format for MAC addresses:
 */
typedef struct macaddr {
    unsigned char a;
    unsigned char b;
    unsigned char c;
    unsigned char d;
    unsigned char e;
    unsigned char f;
} macaddr;

uint64 GetCurNodeId();
void UuidGenerate(char *pdbUuid);

}

#endif  // DSTORE_DSTORE_UUID_UTILS_H
