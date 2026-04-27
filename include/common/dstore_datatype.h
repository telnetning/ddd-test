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
 *        include/common/dstore_datatype.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_DATATYPE_H
#define DSTORE_DATATYPE_H

#include <cassert>
#include <cerrno>
#include <climits> /* include LONG_MAX, etc. */
#include <cstdarg>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <pthread.h>
#include <type_traits>
#include <sys/time.h>
#include <sys/file.h>
#include <execinfo.h>
#include <unistd.h>
#include "securec.h"
#include "config.h"
#include "common/dstore_common_utils.h"
#include "types/data_types.h"
#include "log/dstore_log.h"

namespace DSTORE {

#define STATIC_ASSERT_TRIVIAL(type)                     \
    static_assert(std::is_trivial<type>::value == true, \
                  #type " must be trivial struct, forbid to add virtual function!")

#define STATIC_ASSERT_VAL_IN_RANGE(val, range_val)                         \
    static_assert(((std::numeric_limits<decltype(range_val)>::max() >= (val)) && \
                   (std::numeric_limits<decltype(range_val)>::min() <= (val))), \
                        #val " is not in " #range_val " type range")

#define STATIC_CAST_PTR_TYPE(ptr, type) (static_cast<type>(static_cast<void *>(ptr)))

using float32 = float;
using float64 = double;
using int8 = int8_t ;
using int16 = int16_t;
using int32 = int32_t;
using int64 = int64_t;
using uint8 = uint8_t;
using uint16 = uint16_t;
using uint32 = uint32_t;
using uint64 = uint64_t;
using uint128_t = __uint128_t;

#define INVALID_THREAD_CORE_ID INT_MAX
#define PointerGetDatum(X) ((Datum)(X))
#define DatumGetPointer(X) ((Pointer)(X))

#ifdef UT
#define final
#endif
/*
 * bitsN
 *      Unit of bitwise operation, AT LEAST N BITS IN SIZE.
 */
typedef uint8 bits8;    /* >= 8 bits */
typedef uint16 bits16;  /* >= 16 bits */
typedef uint32 bits32;  /* >= 32 bits */

constexpr uint32 BITS_PER_BYTE = 8;
constexpr uint32 MAX_INT_LEN = 12;
constexpr uint32 MAX_INT8_LEN = 20;
constexpr uint32 MAX_FLOAT_WIDTH = 64;
constexpr uint32 MAX_DOUBLE_WIDTH = 128;
constexpr uint32 MAX_DATE_LEN = 128;

typedef char* Pointer;

constexpr uint16 MAXPGPATH = 1024;

constexpr uint16 REMOTE_MEMORY_NODE_MAX = 16;

#ifdef __aarch64__
    #define DSTORE_CACHELINE_SIZE 128
#else
    #define DSTORE_CACHELINE_SIZE (SIZEOF_VOID_P == 8 ? 64 : 32)
#endif

/* Select timestamp representation (float8 or int64) */
#ifdef USE_INTEGER_DATETIMES
#define HAVE_INT64_TIMESTAMP
#endif

/* todo: need define in Wal module in the future */
using WalId = uint64;
using TimeLineID = uint16;

/** @define Utility macro for defining aligned structures. */
#define ALIGNED(size) __attribute__((aligned (size)))

#define UNUSE_PARAM __attribute__((__unused__))

#define HOTFUNCTION __attribute__((hot))

#define PACKED __attribute__((packed))

#define NOINLINE __attribute__((noinline))

typedef size_t Size;

const int INVALID_BACKEND_ID = -1;

inline bool DstoreOidIsValid(Oid objectId)
{
    return objectId != DSTORE_INVALID_OID;
}
constexpr Oid DEFAULT_COLLATION_OID = 100;
constexpr Oid C_COLLATION_OID = 950;
constexpr Oid POSIX_COLLATION_OID = 951;

constexpr uint32 SIZE_CARRY = 1024;
inline uint32 SizeK(uint64 n)
{
    return static_cast<uint32>(n * SIZE_CARRY);
}
inline uint32 SizeM(uint64 n)
{
    return static_cast<uint32>(SIZE_CARRY * SizeK(n));
}
inline uint64 SizeG(uint64 n)
{
    return SIZE_CARRY * SizeM(n);
}
inline uint64 SizeT(uint64 n)
{
    return SIZE_CARRY * SizeG(n);
}

constexpr const int VARLEAN_DATATYPE_LENGTH = -1;
constexpr const int CSTRING_DATATYPE_LENGTH = -2;

typedef Datum* DatumPtr;

/*
 * A NullableDatum is used in places where both a Datum and its nullness needs
 * to be stored. This can be more efficient than storing datums and nullness
 * in separate arrays, due to better spatial locality, even if more space may
 * be wasted due to padding.
 */
struct NullableDatum {
#define FIELDNO_NULLABLE_DATUM_DATUM 0
    Datum value;
#define FIELDNO_NULLABLE_DATUM_ISNULL 1
    bool isnull;
    /* due to alignment padding this could be used for flags for free */
};

/*
 * Specialized array types.  These are physically laid out just the same
 * as regular arrays (so that the regular array subscripting code works
 * with them).	They exist as distinct types, mainly for historical reasons:
 * they have nonstandard I/O behavior which we don't want to change for fear
 * of breaking applications that look at the system catalogs.  There is another
 * implementation issue for oidvector: it's part of the primary key for
 * pg_proc, and we can't use the normal btree array support routines for that
 * without circularity.
 */
typedef struct {
    int32 vlLen;    /* these fields must match ArrayType! */
    int ndim;         /* always 1 for int2vector */
    int32 dataoffset; /* always 0 for int2vector */
    Oid elemtype;
    int dim1;
    int lbound1;
    int16 values[DSTORE_FLEXIBLE_ARRAY_MEMBER];
} int2vector;

typedef int32 DateADT;
typedef int64 TimeADT;
typedef struct {
    TimeADT time; /* all time units other than months and years */
    int32 zone;   /* numeric time zone, in seconds */
} TimeTzADT;

/*
 * Timestamp represents absolute time.
 *
 * Interval represents delta time. Keep track of months (and years), days,
 * and hours/minutes/seconds separately since the elapsed time spanned is
 * unknown until instantiated relative to an absolute time.
 *
 * Note that Postgres uses "time interval" to mean a bounded interval,
 * consisting of a beginning and ending time, not a time span - thomas 97/03/20
 *
 * Timestamps, as well as the h/m/s fields of intervals, are stored as
 * int64 values with units of microseconds.  (Once upon a time they were
 * double values with units of seconds.)
 *
 * TimeOffset and fsec_t are convenience typedefs for temporary variables.
 * Do not use fsec_t in values stored on-disk.
 * Also, fsec_t is only meant for *fractional* seconds; beware of overflow
 * if the value you need to store could be many seconds.
 */

typedef int64 TimestampTz;
typedef int64 TimeOffset;
const TimestampTz TIMESTAMPTZ_MAX = INT64_MAX;

#ifndef likely
#define likely(x) __builtin_expect((long)(!!(x)), 1)
#endif
#ifndef unlikely
#define unlikely(x) __builtin_expect((long)(!!(x)), 0)
#endif

#define DstoreDatumGetTimeTzADTP(X) ((TimeTzADT *) DatumGetPointer(X))

#define DateADTGetDatum(X) Int32GetDatum(X)
#define TimeADTGetDatum(X) Int64GetDatum(X)
#define TimeTzADTPGetDatum(X) PointerGetDatum(X)

/*
 * Macros for fmgr-callable functions.
 *
 * For Timestamp, we make use of the same support routines as for int64.
 * Therefore Timestamp is pass-by-reference if and only if int64 is!
 */
using TimeOffset = int64;
struct Interval {
    TimeOffset time; /* all time units other than days, months and
                      * years */
    int32 day;       /* days, after time for alignment */
    int32 month;     /* months and years, after time for alignment */
} ;
inline Interval *DatumGetIntervalP(Datum x)
{
    return reinterpret_cast<Interval *>(DatumGetPointer(x));
}

#define TimestampGetDatum(X) Int64GetDatum(X)
#define TimestampTzGetDatum(X) Int64GetDatum(X)
#define IntervalPGetDatum(X) PointerGetDatum(X)

#define DSTORESHORTALIGN(LEN) TYPEALIGN(sizeof(short), (LEN))
#define DSTOREINTALIGN(LEN) TYPEALIGN(sizeof(int), (LEN))
#define DSTORELONGALIGN(LEN) TYPEALIGN(sizeof(long), (LEN))
#define DSTOREDOUBLEALIGN(LEN) TYPEALIGN(sizeof(double), (LEN))
constexpr uintptr_t TypeAlignDown(uintptr_t alignval, uintptr_t len)
{
    return (static_cast<uintptr_t>(len)) & ~(static_cast<uintptr_t>((alignval) - 1));
}

constexpr uint8 ALIGNOF_SHORT = 2;
constexpr uint8 ALIGNOF_INT = 4;
constexpr uint8 ALIGNOF_LONG = 8;
constexpr uint8 ALIGNOF_DOUBLE = 8;

constexpr uintptr_t ShortAlignDown(uintptr_t len)
{
    return TypeAlignDown(ALIGNOF_SHORT, (len));
}

constexpr uintptr_t IntAlignDown(uintptr_t len)
{
    return TypeAlignDown(ALIGNOF_INT, (len));
}

constexpr uintptr_t LongAlignDown(uintptr_t len)
{
    return TypeAlignDown(ALIGNOF_LONG, (len));
}

constexpr uintptr_t DoubleAlignDown(uintptr_t len)
{
    return TypeAlignDown(ALIGNOF_DOUBLE, (len));
}

constexpr uintptr_t MaxAlignDown(uintptr_t len)
{
    return  TypeAlignDown(MAXIMUM_ALIGNOF, (len));
}
/*
 * Max
 *      Return the maximum of two numbers.
 */
#define DstoreMax(x, y) ((x) > (y) ? (x) : (y))

/*
 * Min
 *		Return the minimum of two numbers.
 */
#define DstoreMin(x, y) ((x) < (y) ? (x) : (y))

/*
 * Round up x to the next multiples of y.
 */
template <class T>
inline T DstoreRoundUp(T x, T y)
{
    if (y != static_cast<T>(0)) {
        return ((x + y - 1) / y) * y;
    } else {
        return x;
    }
}

/*
 * Round down x to the previous multiples of y.
 */
template <class T>
inline T DstoreRoundDown(T x, T y)
{
    if (y != static_cast<T>(0)) {
        return (x / y) * y;
    } else {
        return x;
    }
}

#ifdef ENABLE_DEFAULT_GCC
#ifdef __linux__
#if __GNUC__ >= 7
typedef __int128 int128;
typedef unsigned __int128 uint128;
#endif
#endif
#else
#if !defined(WIN32)
typedef __int128 int128;
typedef unsigned __int128 uint128;
#endif
#endif

typedef union {
    uint128   u128;
    uint64    u64[2];
    uint32    u32[4];
} uint128_u;

#define MAX_RANDOM_VALUE (0x7FFFFFFF)
#define MAX_BACKENDS 0x3FFFF

#ifdef DSTORE_USE_ASSERT_CHECKING
#define DSTORE_PG_USED_FOR_ASSERTS_ONLY __attribute__((unused))
#else
#define DSTORE_PG_USED_FOR_ASSERTS_ONLY __attribute__((unused))
#endif

typedef void (*pg_on_exit_callback)(int code, Datum arg);

/* PClint */
#ifdef PC_LINT
#define THR_LOCAL
#endif

#ifndef THR_LOCAL
#ifndef WIN32
#define THR_LOCAL __thread
#else
#define THR_LOCAL __declspec(thread)
#endif
#endif

struct varlena {
    char vlLen[4]; /* Do not touch this field directly! */
    char vl_dat[DSTORE_FLEXIBLE_ARRAY_MEMBER];
};

/* ----------------------------------------------------------------
 *              Section 1:  variable-length datatypes (TOAST support)
 * ----------------------------------------------------------------
 */

/* LOB_TODO: create a GUC variable MAX_INLINE_LOB_SIZE */
/* The type size of ItemPointerData is 64 bits, equal to an uint64 variable */
constexpr Size MAX_INLINE_LOB_SIZE = 2048;
struct VarattLobLocator {
    uint32_t relid;
    int32_t rawsize; /* varlena header + data payload (uncompressed) */
    int32_t extsize; /* data payload (potentially compressed) */
    uint64_t ctid;
};

/*
 * Type of external toast datum stored.
 * DSTORE only has one external storage type.
 * The enum values must be consistent with the definitions in the SQL engine.
 * See gaussdb_clouddb/src/include/postgres.h:vartag_external
 */
enum class VartagExternal {
    VARTAG_EXPANDED_RO = 2,
    VARTAG_EXPANDED_RW = 3,
    VARTAG_DLOB_LOCATOR = 38
};

/*
 * These structs describe the header of a varlena object that may have been
 * TOASTed.  Generally, don't reference these structs directly, but use the
 * macros below.
 *
 * We use separate structs for the aligned and unaligned cases because the
 * compiler might otherwise think it could generate code that assumes
 * alignment while touching fields of a 1-byte-header varlena.
 */
typedef union {
    struct { /* Normal varlena (4-byte length) */
        uint32 va_header;
        char vaData[DSTORE_FLEXIBLE_ARRAY_MEMBER];
    } va_4byte;
    struct { /* Compressed-in-line format */
        uint32 va_header;
        uint32 va_rawsize;                   /* Original data size (excludes header) */
        char vaData[DSTORE_FLEXIBLE_ARRAY_MEMBER]; /* Compressed data */
    } va_compressed;
} varattrib_4b;

typedef struct {
    uint8 va_header;
    char vaData[DSTORE_FLEXIBLE_ARRAY_MEMBER]; /* Data begins here */
} varattrib_1b;

/* inline portion of a short varlena pointing to an external resource */
typedef struct {
    uint8 va_header;                     /* Always 0x80 or 0x01 */
    uint8 va_tag;                        /* Type of datum */
    char vaData[DSTORE_FLEXIBLE_ARRAY_MEMBER]; /* Type-specific data */
} varattrib_1b_e;

typedef struct ExpandedObjectHeader ExpandedObjectHeader;
uint32 DstoreExpandedVarSize(void* ptr);
void DstoreformExpandedVar(void* ptr, void *result, Size allocatedSize);
 
/*
 * struct varatt_expanded is a "TOAST pointer" representing an out-of-line
 * Datum that is stored in memory, in some type-specific, not necessarily
 * physically contiguous format that is convenient for computation not
 * storage.  APIs for this, in particular the definition of struct
 * ExpandedObjectHeader, are in src/include/utils/expandeddatum.h.
 *
 * Note that just as for struct varatt_external, this struct is stored
 * unaligned within any containing tuple.
 */
typedef struct varatt_expanded {
    ExpandedObjectHeader *eohptr;
} varatt_expanded;

#define VARHDRSZ (static_cast<uint32>(sizeof(int32)))

#define VARHDRSZ_COMPRESSED offsetof(varattrib_4b, va_compressed.vaData)

inline bool VarAttIs4B(void *ptr)
{
    return (static_cast<varattrib_1b*>(ptr)->va_header & 0x01) == 0x00;
}

inline bool VarAttIs4BU(void *ptr)
{
    return (static_cast<varattrib_1b*>(ptr)->va_header & 0x03) == 0x00;
}

inline bool VarAttIs4BC(const void *ptr)
{
    return (static_cast<const varattrib_1b *>(ptr)->va_header & 0x03) == 0x02;
}

inline bool VarAttIs1B(const void *ptr)
{
    return (static_cast<const varattrib_1b *>(ptr)->va_header & 0x01) == 0x01;
}

inline bool VarAttIs1BE(const void *ptr)
{
    return (static_cast<const varattrib_1b *>(ptr)->va_header) == 0x01;
}

inline bool VarAttNotPadByte(const void *ptr)
{
    return *(static_cast<const uint8 *>(ptr)) != 0;
}

/* VARSIZE_4B() should only be used on known-aligned data */
inline uint32 DstoreVarSize4B(const void *ptr)
{
    if (ptr != nullptr) {
        return ((static_cast<const varattrib_4b *>(ptr))->va_4byte.va_header >> 2) & 0x3FFFFFFF;
    } else {
        return 0;
    }
}

inline uint32 DstoreVarSize1B(const void *ptr)
{
    return ((static_cast<const varattrib_1b *>(ptr))->va_header >> 1) & 0x7F;
}

inline uint8 DstoreVarTag1BE(const void *ptr)
{
    return (static_cast<const varattrib_1b_e*>(ptr))->va_tag;
}

inline void DstoreSetVarSize4B(void *ptr, uint32 len)
{
    (static_cast<varattrib_4b*>(ptr))->va_4byte.va_header = (len << 2);
}

inline void DstoreSetVarSize4BC(void *ptr, uint32 len)
{
    (static_cast<varattrib_4b*>(ptr))->va_4byte.va_header = (len << 2) | 0x02;
}

inline void DstoreSetVarSize1B(void *ptr, uint32 len)
{
    (static_cast<varattrib_1b*>(ptr))->va_header = static_cast<uint8>((len << 1) | 0x01);
}

inline void DstoreSetVarTag1BE(void *ptr, VartagExternal tag)
{
    (static_cast<varattrib_1b_e*>(ptr))->va_header = 0x01;
    (static_cast<varattrib_1b_e*>(ptr))->va_tag = static_cast<uint8>(tag);
}

#define VARHDRSZ_SHORT offsetof(varattrib_1b, vaData)
constexpr uint8 VARATT_SHORT_MAX = 0x7f;
#define DSTORE_VARATT_CAN_MAKE_SHORT(PTR) \
    (VarAttIs4BU(PTR) && ((DstoreVarSize(PTR) + VARHDRSZ_SHORT) - VARHDRSZ) <= VARATT_SHORT_MAX)

#define VARHDRSZ_EXTERNAL offsetof(varattrib_1b_e, vaData)

inline char *VarData4B(void *ptr)
{
    return (static_cast<varattrib_4b *>(ptr))->va_4byte.vaData;
}

inline char *VarData4BC(void *ptr)
{
    return (static_cast<varattrib_4b *>(ptr))->va_compressed.vaData;
}

inline char *VarData1B(void *ptr)
{
    return (static_cast<varattrib_1b *>(ptr))->vaData;
}

inline char *VarData1BE(void *ptr)
{
    return (static_cast<varattrib_1b_e *>(ptr))->vaData;
}

inline uint32 DstoreVarRawSize4BC(const void *ptr)
{
    if (ptr != nullptr) {
        return (static_cast<const varattrib_4b *>(ptr))->va_compressed.va_rawsize;
    } else {
        return 0;
    }
}

/* Externally visible macros */

/*
 * VARDATA, VARSIZE, and SET_VARSIZE are the recommended API for most code
 * for varlena datatypes.  Note that they only work on untoasted,
 * 4-byte-header Datums!
 *
 * Code that wants to use 1-byte-header values without detoasting should
 * use DSTORE_VARSIZE_ANY/DSTORE_VARSIZE_ANY_EXHDR/VARDATA_ANY.  The other macros here
 * should usually be used only by tuple assembly/disassembly code and
 * code that specifically wants to work with still-toasted Datums.
 *
 * WARNING: It is only safe to use VARDATA_ANY() -- typically with
 * PG_DETOAST_DATUM_PACKED() -- if you really don't care about the alignment.
 * Either because you're working with something like text where the alignment
 * doesn't matter or because you're not going to access its constituent parts
 * and just use things like memcpy on it anyways.
 */
inline char *VarData(void *ptr)
{
    return VarData4B(ptr);
}

inline uint32 DstoreVarSize(void *ptr)
{
    return DstoreVarSize4B(ptr);
}

inline uint32 DstoreVarSizeShort(void *ptr)
{
    return DstoreVarSize1B(ptr);
}

inline char *VarDataShort(void *ptr)
{
    return VarData1B(ptr);
}

inline uint8 DstoreVarTagExternal(const void *ptr)
{
    return DstoreVarTag1BE(ptr);
}

inline char *VarDataExternal(void *ptr)
{
    return VarData1BE(ptr);
}

inline bool DstoreVarAttIsExternal(void *ptr)
{
    return VarAttIs1BE(ptr);
}

inline bool DstoreVarAttIsExternalDlob(void *ptr)
{
    return (DstoreVarAttIsExternal(ptr) &&
            DstoreVarTagExternal(ptr) == static_cast<uint8>(VartagExternal::VARTAG_DLOB_LOCATOR));
}

/*
 * Check whether ptr is a datum of the expanded varlena structure.
 * See src/include/gaussdb.h: VARATT_IS_EXTERNAL_EXPANDED
 */
inline bool DstoreVarAttIsExternalExpanded(void *ptr)
{
    if (!DstoreVarAttIsExternal(ptr)) {
        return false;
    }

    if ((DstoreVarTagExternal(ptr) & ~1) != static_cast<uint8>(VartagExternal::VARTAG_EXPANDED_RO)) {
        return false;
    }
    return *(uint32 *)(VarDataExternal(ptr)) == 0x00000000;
}

inline bool DstoreVarAttIsShort(const void *ptr)
{
    return VarAttIs1B(ptr);
}

inline bool DstoreVarAttIsExtended(void *ptr)
{
    return (!VarAttIs4BU(ptr));
}

inline uint32 DstoreVarTagSize(uint32 tag)
{
    uint32 maskedTag = tag;
    if ((tag & 0x80) != 0x00) {
        maskedTag = tag & 0x7f;
    }

    uint32 size = 0;
    switch (maskedTag) {
        case static_cast<uint8>(VartagExternal::VARTAG_DLOB_LOCATOR):
            size = sizeof(VarattLobLocator);
            break;
        default:
            ErrLog(DSTORE_PANIC, MODULE_COMMON, ErrMsg("maskedTag : %u", maskedTag));
            break;
    }

    return size;
}

/* All possible sizes are less than 32. */
constexpr uint32 MAX_VAR_TAG_SIZE = 32;
inline uint32 DstoreVarSizeExternal(void *ptr)
{
    if (DstoreVarAttIsExternalExpanded(ptr)) {
        return DstoreExpandedVarSize(ptr);
    }

    uint32 size = DstoreVarTagSize(DstoreVarTagExternal(ptr));
    /*
     * We do check here to ensure there is no overflow.
     */
    if (unlikely(size >= MAX_VAR_TAG_SIZE)) {
        return 0;
    }
    return (VARHDRSZ_EXTERNAL + size);
}

inline void DstoreSetVarSize(void *ptr, uint32 len)
{
    DstoreSetVarSize4B(ptr, len);
}

inline void DstoreSetVarSizeShort(void *ptr, uint32 len)
{
    DstoreSetVarSize1B(ptr, len);
}

inline void DstoreSetVarSizeCompressed(void *ptr, uint32 len)
{
    DstoreSetVarSize4BC(ptr, len);
}

inline uint32 DstoreVarSizeAny(void *ptr)
{
    return (VarAttIs1BE(ptr) ? DstoreVarSizeExternal(ptr) :
            (VarAttIs1B(ptr) ? DstoreVarSize1B(ptr) : DstoreVarSize4B(ptr)));
}

#define DSTORE_VARSIZE_ANY_EXHDR(PTR) \
    (VarAttIs1BE(PTR) ? DstoreVarSizeExternal(PTR) - VARHDRSZ_EXTERNAL : \
        (VarAttIs1B(PTR) ? DstoreVarSize1B(PTR) - VARHDRSZ_SHORT : DstoreVarSize4B(PTR) - VARHDRSZ))

/* caution: this will not work on an external or compressed-in-line Datum */
/* caution: this will return a possibly unaligned pointer */
inline char *VarDataAny(void *ptr)
{
    return (VarAttIs1B(ptr) ? VarData1B(ptr) : VarData4B(ptr));
}

inline Size DstoreVarAttConvertedShortSize(void *ptr)
{
    return ((DstoreVarSize(ptr) + VARHDRSZ_SHORT) - VARHDRSZ);
}

/*
 * These widely-used datatypes are just a varlena header and the data bytes.
 * There is no terminating null or anything like that --- the data length is
 * always VARSIZE(ptr) - VARHDRSZ.
 */
typedef struct varlena byteawithoutorderwithequalcol;
typedef struct varlena text;
typedef struct varlena BpChar;    /* blank-padded char, ie SQL char(n) */
typedef struct varlena VarChar;   /* var-length char, ie SQL varchar(n) */
typedef struct varlena NVarChar2; /* var-length char, ie SQL nvarchar2(n) */

/*
 * Invert the sign of a qsort-style comparison result, ie, exchange negative
 * and positive integer values, being careful not to get the wrong answer
 * for INT_MIN.  The argument should be an integral variable.
 */
inline void InvertCompareResult(int *var)
{
    *var = *var < 0 ? 1 : -(*var);
}

/*
 * DstorePointerIsValid
 *      True iff pointer is valid.
 */
inline bool DstorePointerIsValid(const void *ptr)
{
    return ptr != nullptr;
}

constexpr uint8 HIGHBIT = 0x80;
inline unsigned char IsHighBitSet(unsigned char ch)
{
    return static_cast<unsigned char>((ch) & HIGHBIT);
}

/*
 * DatumGetBool
 *      Returns boolean value of a datum.
 *
 * Note: any nonzero value will be considered TRUE, but we ignore bits to
 * the left of the width of bool, per comment above.
 */

inline bool DatumGetBool(Datum x)
{
    return (static_cast<bool>(x) != 0);
}

/*
 * BoolGetDatum
 *      Returns datum representation for a boolean.
 *
 * Note: any nonzero value will be considered TRUE.
 */
#ifndef BoolGetDatum
inline Datum BoolGetDatum(bool x)
{
    return (x) ? 1 : 0;
}
#endif
/*
 * DatumGetChar
 *      Returns character value of a datum.
 */
inline char DatumGetChar(Datum x)
{
    return static_cast<char>(GET_1_BYTE(x));
}

/*
 * CharGetDatum
 *      Returns datum representation for a character.
 */
inline Datum CharGetDatum(char x)
{
    return SET_1_BYTE(static_cast<Datum>(static_cast<unsigned char>(x)));
}

/*
 * DstoreInt8GetDatum
 *      Returns datum representation for an 8-bit integer.
 */
inline Datum DstoreInt8GetDatum(int8 x)
{
    return SET_1_BYTE(static_cast<Datum>(static_cast<uint8>(x)));
}

/*
 * DATUM_GET_INT8
 *             Returns 8-bit integer value of a datum.
 */
#define DATUM_GET_INT8(X) (static_cast<int8>GET_1_BYTE(X))

/*
 * DATUM_GET_UINT8
 *      Returns 8-bit unsigned integer value of a datum.
 */

#define DATUM_GET_UINT8(X) (static_cast<uint8>GET_1_BYTE(X))

/*
 * UINT8_GET_DATUM
 *      Returns datum representation for an 8-bit unsigned integer.
 */

#define UINT8_GET_DATUM(X) (static_cast<Datum>(SET_1_BYTE(static_cast<uint8>(X))))

/*
 * DatumGetInt16
 *      Returns 16-bit integer value of a datum.
 */
inline int16 DatumGetInt16(Datum x)
{
    return static_cast<int16>(GET_2_BYTES(x));
}

/*
 * Int16GetDatum
 *      Returns datum representation for a 16-bit integer.
 */
inline Datum Int16GetDatum(int16 x)
{
    return SET_2_BYTES(static_cast<Datum>(static_cast<uint16>(x)));
}

/*
 * DatumGetUInt16
 *      Returns 16-bit unsigned integer value of a datum.
 */

#define DatumGetUInt16(X) ((uint16)GET_2_BYTES(X))

/*
 * UInt16GetDatum
 *      Returns datum representation for a 16-bit unsigned integer.
 */

#define UInt16GetDatum(X) (static_cast<Datum>(SET_2_BYTES(X)))

/*
 * DatumGetInt32
 *      Returns 32-bit integer value of a datum.
 */
inline int32 DatumGetInt32(Datum x)
{
    return static_cast<int32>(GET_4_BYTES(x));
}

/*
 * Int32GetDatum
 *      Returns datum representation for a 32-bit integer.
 */
inline Datum Int32GetDatum(int32 x)
{
    return SET_4_BYTES(static_cast<Datum>(static_cast<uint32>(x)));
}

/*
 * DatumGetInt64
 *      Returns 64-bit integer value of a datum.
 */
inline int64 DatumGetInt64(Datum x)
{
    return static_cast<int64>(GET_8_BYTES(x));
}

/*
 * DatumGetInt64
 *      Returns 64-bit integer value of a datum.
 */
inline uint64 DatumGetUInt64(Datum x)
{
    return static_cast<uint64>(GET_8_BYTES(x));
}

/*
 * Int64GetDatum
 *		Returns datum representation for a 32-bit integer.
 */
inline Datum Int64GetDatum(int64 x)
{
    return SET_8_BYTES(x);
}

/*
 * DatumGetInt128
 *      Returns 128-bit integer value of a datum.
 */
inline int128 DatumGetInt128(Datum x)
{
    return (*(static_cast<int128 *>(static_cast<void *>(DatumGetPointer(x)))));
}

/*
 * Int128GetDatum
 *		Returns datum representation for a 128-bit integer.
 */
Datum Int128GetDatum(int128 x);


/*
 * UInt64GetDatum
 *      Returns datum representation for a 64-bit unsigned integer.
 */
inline Datum UInt64GetDatum(uint64 x)
{
    return SET_8_BYTES(x);
}

/*
 * ObjectIdGetDatum
 *              Returns datum representation for an object identifier.
 */

inline Datum ObjectIdGetDatum(Oid x)
{
    return SET_4_BYTES(x);
}
/*
 * DatumGetObjectId
 *      Returns object identifier value of a datum.
 */
inline Oid DatumGetObjectId(Datum x)
{
    return static_cast<Oid>(GET_4_BYTES(x));
}

inline Timestamp DstoreDatumGetTimestamp(Datum x)
{
    return static_cast<Timestamp>(DatumGetInt64(x));
}

inline Datea DstoreDatumGetDatea(Datum x)
{
    return static_cast<Datea>(DatumGetInt64(x));
}

inline DateADT DstoreDatumGetDateADT(Datum x)
{
    return static_cast<DateADT>(DatumGetInt32(x));
}

inline char *DatumGetCString(Datum x)
{
    return static_cast<char *>(DatumGetPointer(x));
}
inline TimeADT DstoreDatumGetTimeADT(Datum x)
{
    return static_cast<TimeADT>(DatumGetInt64(x));
}

inline TimeTzADT *DatumGetTimeTzADTP(Datum x)
{
    return reinterpret_cast<TimeTzADT *>(DatumGetPointer(x));
}

inline TimestampTz DstoreDatumGetTimestampTz(Datum x)
{
    return static_cast<TimestampTz>(DatumGetInt64(x));
}
inline varlena *DatumGetText(Datum x)
{
    return static_cast<varlena*>(static_cast<void*>(DatumGetPointer(x)));
}

/*
 * DatumGetFloat32
 *		Returns 32-bit floating point value of a datum.
 */
inline float32 DatumGetFloat32(Datum dx)
{
    union {
        int32 value;
        float32 retval;
    } myunion;

    myunion.value = DatumGetInt32(dx);
    return myunion.retval;
}

/*
 * Float32GetDatum
 *		Returns datum representation for a 32-bit floating point number.
 */
inline Datum Float32GetDatum(float32 fx)
{
    union {
        float32 value;
        int32 retval;
    } myunion;

    myunion.value = fx;
    return Int32GetDatum(myunion.retval);
}
/*
 * DatumGetFloat64
 *		Returns 64-bit floating point value of a datum.
 *
 * Note: this macro hides whether float64 is pass by value or by reference.
 */
inline float64 DatumGetFloat64(Datum dx)
{
    union {
        int64 value;
        float64 retval;
    } myunion;

    myunion.value = DatumGetInt64(dx);
    return myunion.retval;
}

/*
 * Float64GetDatum
 *		Returns datum representation for an 64-bit floating point number.
 *
 * Note: if float64 is pass by reference, this function returns a reference
 * to palloc'd space.
 */
inline Datum Float64GetDatum(float64 fx)
{
    union {
        float64 value;
        int64 retval;
    } myunion;

    myunion.value = fx;
    return Int64GetDatum(myunion.retval);
}


typedef int64 Cash;

/* Cash is pass-by-reference if and only if int64 is */
inline Cash DatumGetCash(Datum x)
{
    return static_cast<Cash>(DatumGetInt64(x));
}
#define CashGetDatum(X) Int64GetDatum(X)
/*
 * CStringGetDatum
 *      Returns datum representation for a C string (null-terminated string).
 *
 * Note: C string is not a full-fledged openGauss type at present,
 * but type output functions use this conversion for their outputs.
 * Note: CString is pass-by-reference; caller must ensure the pointed-to
 * value has adequate lifetime.
 */

#define CStringGetDatum(X) PointerGetDatum(X)
#define CppAsString(identifier) #identifier
#define CppConcat(x, y) x##y

extern Size datumGetSize(Datum value, bool typByVal, int typLen);
extern Datum datumCopy(Datum value, bool typByVal, int typLen);
extern bool datumImageEq(Datum value1, Datum value2, bool typByVal, int typLen);

/* suppress warning */
#define UNUSED_VARIABLE(x) ((void)(x))

#define STORAGE_FUNC_SUCC(func) (likely((func) == DSTORE_SUCC))
#define STORAGE_FUNC_FAIL(func) (unlikely((func) == DSTORE_FAIL))
#define STORAGE_VAR_NULL(var) (unlikely((var) == nullptr))

constexpr int DSTORE_EPOCH_JDATE = 2451545;          /* == date2j(2000, 1, 1) */
constexpr int STORAGE_UNIX_EPOCH_JDATE = 2440588;    /* == date2j(1970, 1, 1) */

constexpr int STORAGE_SECS_PER_MIN = 60;
constexpr int STORAGE_SECS_PER_HOUR = 3600;
constexpr int STORAGE_SECS_PER_DAY = 86400;

constexpr int STORAGE_MSECS_PER_SEC = 1000;
constexpr int STORAGE_USECS_PER_MSEC = 1000;
constexpr int STORAGE_USECS_PER_SEC = 1000000;
constexpr int64 STORAGE_USECS_PER_HOUR = static_cast<int64>(3600000000);
constexpr int64 STORAGE_USECS_PER_DAY = static_cast<int64>(86400000000);

const int STORAGE_DAYS_PER_MONTH = 30;     /* assumes exactly 30 days per month */
const int STORAGE_HOURS_PER_DAY = 24;      /* assume no daylight savings time changes */

/*
 * Get the current operating system time
 *
 * @return TimestampTz Expressed to the full precision of the gettimeofday() syscall
 */
inline TimestampTz GetCurrentTimestamp(void)
{
    TimestampTz result;
    struct timeval tp;

    (void)gettimeofday(&tp, nullptr);

    result = static_cast<TimestampTz>(tp.tv_sec) -
        ((DSTORE_EPOCH_JDATE - STORAGE_UNIX_EPOCH_JDATE) * STORAGE_SECS_PER_DAY);

#ifdef HAVE_INT64_TIMESTAMP
    result = (result * STORAGE_USECS_PER_SEC) + tp.tv_usec;
#else
    result = static_cast<TimestampTz>(result + (tp.tv_usec / static_cast<double>(STORAGE_USECS_PER_SEC)));
#endif

    return result;
}

inline TimestampTz GetCurrentTimestampInSecond(void)
{
    TimestampTz result;
    struct timeval tp;

    (void)gettimeofday(&tp, nullptr);

    result = static_cast<TimestampTz>(tp.tv_sec) -
        ((DSTORE_EPOCH_JDATE - STORAGE_UNIX_EPOCH_JDATE) * STORAGE_SECS_PER_DAY);

    result = static_cast<TimestampTz>(result + (tp.tv_usec / static_cast<double>(STORAGE_USECS_PER_SEC)));

    return result;
}


inline uint64 GetSystemTimeInMicrosecond()
{
    struct timespec tv;
    uint64 res;
    const int thousands = 1000;

    /* According to POSIX.1-2008, Applications should use the clock_gettime() function
     * instead of the obsolescent gettimeofday() function.
     * Use CLOCK_MONOTONIC_RAW to prevent the system time from being adjusted by NTP and the like.
     */
    (void)clock_gettime(CLOCK_MONOTONIC_RAW, &tv);
    res = static_cast<uint64>(tv.tv_sec * thousands * thousands + tv.tv_nsec / thousands);

    return res;
}

/* copied from timestamp.c */
time_t timestamptzTotime_t(TimestampTz t);
const char* TimestamptzTostr(TimestampTz dt);

inline void dstore_usleep(long microsec)
{
    if (microsec > 0) {
        struct timeval delay;
        delay.tv_sec = microsec / 1000000L;
        delay.tv_usec = microsec % 1000000L;
        (void)select(0, nullptr, nullptr, nullptr, &delay);
    }
}

inline void PrintBackTrace()
{
    constexpr uint8_t backtraceMaxSize = 100;
    int backtraceSize;
    void *buffer[backtraceMaxSize];
    char **info;

    /* Obtain the backtrack information of the current function */
    backtraceSize = backtrace(buffer, backtraceMaxSize);

    /* Corresponds the return address to the specific function name. */
    info = backtrace_symbols(buffer, backtraceSize);
    if (info == nullptr) {
        return;
    }

    for (int i = 0; i < backtraceSize; i++) {
        (void)fprintf(stderr, "[%d] %s\n", i, info[i]);
    }
    free(info);
}

/* Unblocking random number generator */
inline RetStatus DstoreGetRandomNum(uint8 &randomNum)
{
    int fd = open("/dev/urandom", O_RDONLY);
    if (fd == -1) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
            ErrMsg("Failed to open file \"/dev/urandom\"."));
        randomNum = 0;
        return DSTORE_FAIL;
    }

    if (fd > 0) {
        ssize_t len = read(fd, &randomNum, sizeof(randomNum));
        if (len != static_cast<ssize_t>(sizeof(randomNum))) {
            (void)close(fd);
            ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
                ErrMsg("Failed to open file \"/dev/random\"."));
            randomNum = 0;
            return DSTORE_FAIL;
        }
    }

    (void)close(fd);
    return DSTORE_SUCC;
}

#define DSTORE_TRAP(condition, errorType)                                                                    \
    do {                                                                                                     \
        StorageReleasePanic((condition), MODULE_FRAMEWORK, ErrMsg("condition: %s", CppAsString(condition))); \
    } while (0)

/* FIXME , the compile script to generate.msk */
#ifdef DSTORE_USE_ASSERT_CHECKING
#define StorageAssert(condition) DSTORE_TRAP(!(condition), "FailedAssertion")
#else
#define StorageAssert(condition)
#endif

#define StorageExit0(condition, module, errorMsg)                      \
    if (unlikely((condition))) {                                       \
        PrintBackTrace();                                              \
        ErrLog(DSTORE_ERROR, module, errorMsg);                        \
        ErrLog(DSTORE_ERROR, module, ErrMsg("Process suicide, exit 0."));      \
        FlushLogger();                                                 \
        _exit(0);                                                      \
    }

#define StorageExit1(condition, module, errorMsg)                      \
    if (unlikely((condition))) {                                       \
        PrintBackTrace();                                              \
        ErrLog(DSTORE_ERROR, module, errorMsg);                        \
        ErrLog(DSTORE_ERROR, module, ErrMsg("Process suicide, exit 1."));      \
        FlushLogger();                                                 \
        _exit(1);                                                       \
    }

#define StorageReleasePanic(condition, module, errorMsg)  \
    if (unlikely((condition))) {                          \
        PrintBackTrace();                       \
        ErrLog(DSTORE_PANIC, module, errorMsg); \
        exit(1);                                \
    }

/* Only can be called when not hold pd lock. */
#define StorageReleaseBufferCheckPanic(condition, module, bufTag, errMsg)                                \
    if (unlikely((condition))) {                                                                         \
        PrintBackTrace();                                                                                \
        char *clusterBufferInfo = g_storageInstance->GetBufferMgr()->GetClusterBufferInfo(bufTag.pdbId,  \
            bufTag.pageId.m_fileId, bufTag.pageId.m_blockId);                                            \
        if (likely(clusterBufferInfo != nullptr)) {                                                      \
            ErrLog(DSTORE_PANIC, module, ErrMsg("%s. %s", errMsg, clusterBufferInfo));                   \
        }                                                                                                \
        ErrLog(DSTORE_PANIC, module, ErrMsg("%s", errMsg));                                              \
    }

#define STORAGE_RELEASE_EXIT(condition, ...)  \
    if (unlikely((condition))) {            \
        PrintBackTrace();                   \
        (void)fprintf(stderr, __VA_ARGS__); \
        (void)fprintf(stderr, "Process suicide."); \
        (void)fflush(stdout);               \
        (void)fflush(stderr);               \
        exit(1);                            \
    }

#define STORAGE_PROCESS_FORCE_EXIT(condition, ...)  \
    if (unlikely((condition))) {            \
        PrintBackTrace();                   \
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Process suicide: " __VA_ARGS__)); \
        (void)fprintf(stderr, __VA_ARGS__); \
        (void)fflush(stdout);               \
        (void)fflush(stderr);               \
        FlushLogger();                      \
        _exit(1);                            \
    }

#define STORAGE_CHECK_BUFFER_PANIC(bufferDesc, module, pageId)          \
    StorageReleasePanic(((bufferDesc) == INVALID_BUFFER_DESC), (module), \
        ErrMsg("Buffer(%hu, %u) invalid!", (pageId).m_fileId, (pageId).m_blockId))

#define storage_securec_check(errno, charList, ...)                                      \
    {                                                                                    \
        StorageReleasePanic((errno != 0), MODULE_FRAMEWORK, ErrMsg("errno: %d", errno)); \
    }
/* Only used in sprintf_s or scanf_s cluster function */
#define storage_securec_check_ss(errno)                                                     \
    {                                                                                       \
        StorageReleasePanic(((errno) == -1), MODULE_FRAMEWORK, ErrMsg("errno: %d", errno)); \
    }
/*
 * Macros to support compile-time assertion checks.
 *
 * If the "condition" (a compile-time-constant expression) evaluates to false,
 * will throw a compile error using the "errmessage" (a string literal).
 *
 * gcc 4.6 and up supports _Static_assert(), but there are bizarre syntactic
 * placement restrictions.  These macros make it safe to use as statements
 * or in expressions, respectively.
 *
 * Otherwise we fall back on a kluge that assumes the compiler will complain
 * about a negative width for a struct bit-field.  This will not include a
 * helpful error message, but it beats not getting an error at all.
 */
#define DSTORE_STATIC_ASSERT_STMT(condition, errmessage) (static_cast<void>(1 / static_cast<int>(!!(condition))))
#define DSTORE_STATIC_ASSERT_EXPR(condition, errmessage) DSTORE_STATIC_ASSERT_STMT(condition, errmessage)

/*
 * Compile-time checks that a variable (or expression) has the specified type.
 *
 * AssertVariableIsOfType() can be used as a statement.
 * AssertVariableIsOfTypeMacro() is for use in macros, eg
 *      #define foo(x) (AssertVariableIsOfTypeMacro(x, int), bar(x))
 *
 * If we don't have __builtin_types_compatible_p, we can still assert that
 * the types have the same size.  This is far from ideal (especially on 32-bit
 * platforms) but it at least provides  some coverage.
 */
#define ASSERT_POINTER_ALIGNMENT(ptr, bndr) \
    StorageAssert((TYPEALIGN(bndr, reinterpret_cast<uintptr_t>(ptr)) == reinterpret_cast<uintptr_t>(ptr)))
#define AssertVariableIsOfType(varname, typename) \
    DSTORE_STATIC_ASSERT_STMT(                             \
        sizeof(varname) == sizeof(typename), CppAsString(varname) " does not have type " CppAsString(typename))
#define AssertVariableIsOfTypeMacro(varname, typename) \
    (DSTORE_STATIC_ASSERT_EXPR(                           \
        sizeof(varname) == sizeof(typename), CppAsString(varname) " does not have type " CppAsString(typename)))

#define CONTAINER_OF(type, membername, ptr) ((type *)((char *)(ptr) - offsetof(type, membername)))

constexpr uint16_t MAX_FILE_NAME_LEN = 256;

constexpr uint16 HEX_ALIGN{16};

#define OFFSETOF(type, member) ((long)(&((type*)0)->member))

constexpr uint8 BYTE_TO_BIT_SHIFT = 3;
constexpr uint8 BYTE_TO_BIT_MULTIPLIER = 8;
constexpr uint8 BYTE_TO_MB_SHIFT = 20;

constexpr uint64 INVALID_FILE_VERSION = 0xFFFFFFFFFFFFFFFFU;

#define FUNCTION_DELETE_LATER
} // namespace DSTORE

/*
 * there is a Max(x, y) in types/data_types.h
 * and it is conflict with gtest-internal.h::Max() function
 * so I undef Max there
 */
#ifdef UT
#undef Max
#endif

#endif  // STORAGE_DATATYPE_H
