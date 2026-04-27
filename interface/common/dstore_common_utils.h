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
 * IDENTIFICATION
 *        interface/common/dstore_common_utils.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_COMMON_STRUCT
#define DSTORE_COMMON_STRUCT

#include <stdint.h>
#include <pthread.h>
#include <stddef.h>

namespace DSTORE {

constexpr uint8_t NAME_DATA_LEN = 64;
/*
 * Representation of a Name: effectively just a C string, but null-padded to
 * exactly NAME_DATA_LEN bytes.  The use of a struct is historical.
 */
struct DstoreNameData {
    char data[NAME_DATA_LEN];
};


using DstoreName = DstoreNameData*;

using CommitSeqNo = uint64_t;
using CommandId = uint32_t;
using PdbId = uint32_t;
using NodeId = uint32_t;
using TablespaceId = uint16_t;
using Timestamp = int64_t;
using WalPlsn = uint64_t;
using WalId = uint64_t;
using Datea = int64_t;

constexpr PdbId INVALID_PDB_ID = 0;

using Oid = uint32_t;
constexpr Oid DSTORE_INVALID_OID = 0;

/*
 * user defined attribute numbers start at 1.   -ay 2/95
 */
using AttrNumber = int16_t;

/* registered function, references registered procedure */
using RegFunc = Oid;

using Datum = uintptr_t;
constexpr Datum INVALID_DATUM = 0;

using bytea = struct varlena;

using ItemType = uint32_t;
/*
 * ErrorCode is a 64-bit unsigned error code composed of:
 * 20 bits - Not used
 *  4 bits - Severity
 *  8 bits - Module ID
 * 32 bits - Module 0-based error code
 *
 * The value of 0 is always success.
 */
using ErrorCode = long long;

using ThreadId = pthread_t;

/*
 * BlockNumber:
 *
 * each data file (heap or index) is divided into openGauss disk blocks
 * (which may be thought of as the unit of i/o -- a openGauss buffer
 * contains exactly one disk block).  the blocks are numbered
 * sequentially, 0 to 0xFFFFFFFE.
 *
 * DSTORE_INVALID_BLOCK_NUMBER is the same thing as P_NEW in buf.h.
 *
 * the access methods, the buffer manager and the storage manager are
 * more or less the only pieces of code that should be accessing disk
 * blocks directly.
 */
using BlockNumber = uint32_t;
using FileId = uint16_t;

constexpr BlockNumber DSTORE_INVALID_BLOCK_NUMBER = 0xFFFFFFFFU;
constexpr BlockNumber DSTORE_MAX_BLOCK_NUMBER = 0xFFFFFFFEU;
constexpr FileId INVALID_VFS_FILE_ID = 0U;
constexpr FileId MAX_VFS_FILE_ID = 0XFFFFU;
constexpr uint32_t MAX_VFS_FILE_NUMBER = (1U << 16);
constexpr int VFS_FILE_PATH_MAX_LEN = 1024;
constexpr int NAME_MAX_LEN = 64;
constexpr int DSTORE_FLEXIBLE_ARRAY_MEMBER = 1;
constexpr uint16_t FIRST_NORMAL_OBJECT_ID = 16384;
constexpr uint16_t FIRST_BOOTSTRAP_OBJECT_ID = 10000;
#define BLCKSZ 8192

enum RetStatus : int {
    DSTORE_FAIL = -1,
    DSTORE_SUCC = 0,
};

const pthread_t INVALID_THREAD_ID = (static_cast<ThreadId>(-1));

/*
 * ScanDirection was an int8 for no apparent reason. I kept the original
 * values because I'm not sure if I'll break anything otherwise.  -ay 2/95
 */
enum class ScanDirection { BACKWARD_SCAN_DIRECTION = -1, NO_MOVEMENT_SCAN_DIRECTION = 0, FORWARD_SCAN_DIRECTION = 1 };

/* Macros to disable copying and moving */
#define DISALLOW_COPY(cname)       \
    cname(const cname &) = delete; \
    cname &operator=(const cname &) = delete;

#define DISALLOW_MOVE(cname)  \
    cname(cname &&) = delete; \
    cname &operator=(cname &&) = delete;

#define DISALLOW_COPY_AND_MOVE(cname) \
    DISALLOW_COPY(cname);             \
    DISALLOW_MOVE(cname);

#define DSTORE_EXPORT __attribute__((visibility("default")))
#define DSTORE_LOCAL __attribute__((visibility("hidden")))

using DstoreMemoryContext = struct DstoreMemoryContextData *;

struct OidVector {
    int32_t vlLen;       /* these fields must match ArrayType! */
    int ndim;              /* always 1 for oidvector */
    int32_t dataoffset;    /* always 0 for oidvector */
    Oid elemtype;
    int dim1;
    int lbound1;
    Oid values[DSTORE_FLEXIBLE_ARRAY_MEMBER];
};

struct Int32Vector {
    int32_t vlLen;        /* these fields must match ArrayType! */
    int ndim;               /* always 1 for int2vector */
    int32_t dataoffset;     /* always 0 for int2vector */
    Oid elemtype;
    int dim1;
    int lbound1;
    int16_t values[DSTORE_FLEXIBLE_ARRAY_MEMBER];
};

/* remove it later */
DSTORE_EXPORT RetStatus CreateMemoryContextForTool(const char *name);

/* The unit size can be adjusted by changing these three declarations: */
/* hard code, cause SIZEOF_VOID_P always equal to 8 in dstore and we consistent with sql engine */
#define DSTORE_BITS_PER_BITMAPWORD 64
typedef uint64_t bitmapword;      /* must be an unsigned type */
typedef int64_t signedbitmapword; /* must be the matching signed type */

struct Bitmapset {
    int nwords;                                      /* number of words in array */
    bitmapword words[DSTORE_FLEXIBLE_ARRAY_MEMBER];  /* really [nwords] */
};

inline size_t CalculateDstoreBitmapsetSize(int nwords)
{
    return offsetof(Bitmapset, words) + static_cast<size_t>(nwords) * sizeof(bitmapword);
}

using AllocMemFunc = void* (*)(uint32_t);
using FreeMemFunc = void (*)(void *ptr);

extern void DstoreSetMemoryOutOfControl();
extern void DstoreSetMemoryInControl();

}  // namespace DSTORE
#endif
