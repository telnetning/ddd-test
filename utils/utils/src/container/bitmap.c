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
 * Description: Implement bitmap
 */

#include <strings.h>
#include "securec.h"
#include "container/hash_table.h"
#include "memory/memory_ctx.h"
#include "types/data_types.h"
#include "container/bitmap.h"

struct Bitmap {
    /* Later Optimize Orientation:
     * if extend memory more frequency, can change this single block memory to multi memory connect by dlist */
    uint32_t size;    /* flexible array size */
    uint64_t bits[0]; /* flexible array */
};

/* if used in 32bit OS, uint64_t -> uint32_t, ffsll -> ffs, __builtin_popcountll -> __builtin_popcount */
/* __typeof__ is support in clang and gun, use it to define the element type in bitmap for change it to
 * 32bit element easily. also, the type must be unsigned, becase we use -1 to express max value */
#define ELEMENT_TYPE __typeof__(((Bitmap *)0)->bits[0])

static inline bool TestBit(const ELEMENT_TYPE *bits, uint32_t pos)
{
    return (*bits & ((ELEMENT_TYPE)1 << pos)) != 0;
}

static inline void SetBit(ELEMENT_TYPE *bits, uint32_t pos)
{
    *bits |= ((ELEMENT_TYPE)1 << pos);
}

static inline void ClearBit(ELEMENT_TYPE *bits, uint32_t pos)
{
    *bits &= (~((ELEMENT_TYPE)1 << pos));
}

/* 
 * (x & ((~x) + 1)) to get right most setbit, this is a very classic algorithm, keep the rightmost 1,
 * clear other bits, for example
 *  x           :  01110000
 *  ~x          :  10001111
 *  -x or ~x + 1:  10010000
 *  x & -x      :  00010000
 */
static inline bool HasMultipleOnes(ELEMENT_TYPE x)
{
    return (x & ((~x) + 1)) != x;
}

static inline uint32_t RightMostOnePos(ELEMENT_TYPE x)
{
    /* for detail, man ffsll. if x == 0, will not call this function, so this function never return 0. if we implement
     * this function at work later, we can optimize the subtraction (-1) operation and an internal judgment of zero */
    return (uint32_t)ffsll((long long)x) - 1;
}

/* 
 * gcc builtin function is like method 4, the blogger in the first link had proved performance is best in most cases
 */
static inline uint32_t CountBitsOne(ELEMENT_TYPE x)
{
    return (uint32_t)__builtin_popcountll(x);
}

/* the following variables, bitpos is relative to the entire Bitmap, pos is relative to the element in Bitmap
 * |bitpos 0~63| |bitpos 64~127| |bitpos 128~191| ...
 * |-----------| |-------------| |--------------|
 * |bits idx 0 | |bits idx 1   | |bits idx 2    | ...
 */
static inline uint32_t GetBitsIdx(uint32_t bitPosition)
{
    // CountBitsOne((ELEMENT_TYPE)-1) optimized to (sizeof(bits[0]) * 8)
    return bitPosition / CountBitsOne((ELEMENT_TYPE)-1);
}

static inline uint32_t GetBitPosition(uint32_t bitsIdx)
{
    return bitsIdx * CountBitsOne((ELEMENT_TYPE)-1);
}

static inline uint32_t GetPosInElement(uint32_t bitPosition)
{
    return bitPosition % CountBitsOne((ELEMENT_TYPE)-1);
}

static inline Bitmap *BitmapCreate(uint32_t size)
{
    ASSERT(size > 0);
    Bitmap *result = (Bitmap *)MemAlloc(sizeof(Bitmap) + size * sizeof(ELEMENT_TYPE));
    if (unlikely(result == NULL)) {
        return NULL;
    }
    result->size = size;
    return result;
}

static Bitmap *BitmapExtend(Bitmap *bitmap, uint32_t size)
{
    ASSERT(bitmap != NULL);
    ASSERT(size > 0);
    /* like memory realloc. get fast path, the size is enough, no need extend */
    if (size <= bitmap->size) {
        return bitmap;
    }
    /* slow path, sadly, have to realloc space */
    Bitmap *result = (Bitmap *)MemAlloc(sizeof(Bitmap) + size * sizeof(ELEMENT_TYPE));
    if (unlikely(result == NULL)) {
        return NULL;
    }
    result->size = size;
    /* copy old content, from bitmap to result */
    size_t bytes = bitmap->size * sizeof(ELEMENT_TYPE);
    if (memcpy_s(result->bits, bytes, bitmap->bits, bytes) != EOK) {
        MemFree(result);
        return NULL;
    }
    /* clear the remain space */
    /* had checked size <= bitmap->size before, bitmap->size is guarantee to access */
    bytes = (result->size - bitmap->size) * sizeof(ELEMENT_TYPE);
    (void)memset_s(&result->bits[bitmap->size], bytes, 0, bytes);
    /* BitmapDestroy old */
    MemFree(bitmap);
    return result;
}

UTILS_EXPORT Bitmap *BitmapCreateWithSet(uint32_t bitPosition)
{
    uint32_t idx = GetBitsIdx(bitPosition);
    // assert not warp around
    ASSERT(idx < (idx + 1));

    Bitmap *result = BitmapCreate(idx + 1);
    if (unlikely(result == NULL)) {
        return NULL;
    }
    size_t bytes = result->size * sizeof(ELEMENT_TYPE);
    (void)memset_s(result->bits, bytes, 0, bytes);
    SetBit(&result->bits[idx], GetPosInElement(bitPosition));
    return result;
}

UTILS_EXPORT void BitmapDestroy(Bitmap *bitmap)
{
    MemFree(bitmap);
}

UTILS_EXPORT Bitmap *BitmapCopy(const Bitmap *bitmap)
{
    if (unlikely(bitmap == NULL)) {
        return NULL;
    }
    Bitmap *result = BitmapCreate(bitmap->size);
    if (unlikely(result == NULL)) {
        return NULL;
    }
    ASSERT(result->size == bitmap->size);
    size_t bytes = bitmap->size * sizeof(ELEMENT_TYPE);
    if (memcpy_s(result->bits, bytes, bitmap->bits, bytes) != EOK) {
        MemFree(result);
        return NULL;
    }
    return result;
}

UTILS_EXPORT Bitmap *BitmapSetBit(Bitmap *bitmap, uint32_t bitPosition)
{
    if (unlikely(bitmap == NULL)) {
        return BitmapCreateWithSet(bitPosition);
    }

    uint32_t idx = GetBitsIdx(bitPosition);
    // assert not warp around
    ASSERT(idx < (idx + 1));

    Bitmap *result = BitmapExtend(bitmap, idx + 1);
    if (unlikely(result == NULL)) {
        return NULL;
    }
    SetBit(&result->bits[idx], GetPosInElement(bitPosition));
    return result;
}

UTILS_EXPORT void BitmapClearBit(Bitmap *bitmap, uint32_t bitPosition)
{
    if (unlikely(bitmap == NULL)) {
        return;
    }

    uint32_t idx = GetBitsIdx(bitPosition);
    // assert not warp around
    ASSERT(idx < (idx + 1));

    if (unlikely(idx + 1 > bitmap->size)) {
        /* sorry, bitmap space too small, not include this bitPosition, no need to clear */
        return;
    }
    ClearBit(&bitmap->bits[idx], GetPosInElement(bitPosition));
}

UTILS_EXPORT Bitmap *BitmapOr(const Bitmap *a, const Bitmap *b)
{
    /* if a or b is NULL, copy the other one, it's ok as copy NULL bitmap, will return NULL */
    if (unlikely(a == NULL)) {
        return BitmapCopy(b);
    }

    if (unlikely(b == NULL)) {
        return BitmapCopy(a);
    }

    bool isFirstBigger = (a->size > b->size);
    /* result size is same as the longer one */
    const Bitmap *longer = isFirstBigger ? a : b;
    Bitmap *result = BitmapCreate(longer->size);
    if (unlikely(result == NULL)) {
        return NULL;
    }
    ASSERT(result->size == longer->size);

    uint32_t shorterSize = isFirstBigger ? b->size : a->size;
    for (uint32_t i = 0; i < shorterSize; i++) {
        result->bits[i] = a->bits[i] | b->bits[i];
    }

    /* copy thre remain space */
    ASSERT(longer->size >= shorterSize);
    uint32_t remainSpace = longer->size - shorterSize;
    if (remainSpace == 0) {
        return result;
    }
    size_t bytes = remainSpace * sizeof(ELEMENT_TYPE);
    if (memcpy_s(&result->bits[shorterSize], bytes, &longer->bits[shorterSize], bytes) != EOK) {
        MemFree(result);
        return NULL;
    }
    return result;
}

UTILS_EXPORT Bitmap *BitmapAnd(const Bitmap *a, const Bitmap *b)
{
    if (unlikely((a == NULL) || (b == NULL))) {
        return NULL;
    }

    /* result size is same as the shorter one */
    uint32_t shorterSize = Min(a->size, b->size);
    Bitmap *result = BitmapCreate(shorterSize);
    if (unlikely(result == NULL)) {
        return NULL;
    }
    ASSERT(result->size == shorterSize);

    for (uint32_t i = 0; i < shorterSize; i++) {
        result->bits[i] = a->bits[i] & b->bits[i];
    }
    return result;
}

UTILS_EXPORT Bitmap *BitmapAndComplement(const Bitmap *a, const Bitmap *b)
{
    if (unlikely(a == NULL)) {
        return NULL;
    }

    if (unlikely(b == NULL)) {
        return BitmapCopy(a);
    }

    Bitmap *result = BitmapCreate(a->size);
    if (unlikely(result == NULL)) {
        return NULL;
    }
    ASSERT(result->size == a->size);

    if (a->size > b->size) {
        for (uint32_t i = 0; i < b->size; i++) {
            result->bits[i] = a->bits[i] & (~b->bits[i]);
        }
        /* copy thre remain space */
        uint32_t remainSpace = a->size - b->size;
        size_t bytes = remainSpace * sizeof(ELEMENT_TYPE);
        if (memcpy_s(&result->bits[b->size], bytes, &a->bits[b->size], bytes) != EOK) {
            MemFree(result);
            return NULL;
        }
    } else {
        for (uint32_t i = 0; i < a->size; i++) {
            result->bits[i] = a->bits[i] & (~b->bits[i]);
        }
    }
    return result;
}

/**
 *  ELEMENT_TYPE is
 *  a : 0000 0000 0000 0000 0000 1110 0000 1110
 * ~a : 1111 1111 1111 1111 1111 0001 1111 0001
 *  b : 0000 0000 0000 0000 1010 1110 0000 1110
 * ~b : 1111 1111 1111 1111 0101 0001 1111 0001
 * a is subset of b, (a & ~b) == 0,
 * b is not subset of a, and (b & ~a) != 0,
 * a = 0 is any number subset, and a == b is subset of each other
 */
static inline bool IsSubSet(ELEMENT_TYPE a, ELEMENT_TYPE b)
{
    return (a & ~b) == 0;
}

static inline bool IsEmptyFromTo(const Bitmap *bitmap, uint32_t from, uint32_t to)
{
    ASSERT(bitmap != NULL);
    for (uint32_t i = from; i < to; i++) {
        if (bitmap->bits[i] != 0) {
            return false;
        }
    }
    return true;
}

UTILS_EXPORT BitmapCmpResult BitmapCompare(const Bitmap *a, const Bitmap *b)
{
    if (unlikely((a == NULL) && (b == NULL))) {
        return BITMAP_EQUAL;
    }

    /* a is the FIRST, b is the SECOND */
    if (unlikely(a == NULL)) {
        return BitmapIsEmpty(b) ? BITMAP_EQUAL : BITMAP_SECOND_CONTAIN_FIRST;
    }

    if (unlikely(b == NULL)) {
        return BitmapIsEmpty(a) ? BITMAP_EQUAL : BITMAP_FIRST_CONTAIN_SECOND;
    }

    BitmapCmpResult result = BITMAP_EQUAL;

    /* a and b are not NULL, check the shorter size firstly */
    uint32_t shorterSize = (a->size > b->size) ? b->size : a->size;
    for (uint32_t i = 0; i < shorterSize; i++) {
        if (!IsSubSet(a->bits[i], b->bits[i])) {
            if (result == BITMAP_SECOND_CONTAIN_FIRST) {
                return BITMAP_DIFFERENT;
            }
            result = BITMAP_FIRST_CONTAIN_SECOND;
        }
        if (!IsSubSet(b->bits[i], a->bits[i])) {
            if (result == BITMAP_FIRST_CONTAIN_SECOND) {
                return BITMAP_DIFFERENT;
            }
            result = BITMAP_SECOND_CONTAIN_FIRST;
        }
    }

    /* then, check extra bits */
    if (a->size > b->size) {
        /* a is the longer one */
        if (!IsEmptyFromTo(a, b->size, a->size)) {
            /* a is not a subset of b */
            return result == BITMAP_SECOND_CONTAIN_FIRST ? BITMAP_DIFFERENT : BITMAP_FIRST_CONTAIN_SECOND;
        }
    } else {
        /* b is the longer one */
        if (!IsEmptyFromTo(b, a->size, b->size)) {
            /* b is not a subset of a */
            return result == BITMAP_FIRST_CONTAIN_SECOND ? BITMAP_DIFFERENT : BITMAP_SECOND_CONTAIN_FIRST;
        }
    }
    return result;
}

UTILS_EXPORT int64_t BitmapGetPositionForSingleton(const Bitmap *bitmap)
{
    if (unlikely(bitmap == NULL)) {
        return -1;
    }

    int64_t result = -1;
    for (uint32_t i = 0; i < bitmap->size; i++) {
        ELEMENT_TYPE current = bitmap->bits[i];
        if (current == 0) {
            continue;
        }

        if ((result >= 0) || HasMultipleOnes(current)) {
            return -1;
        }

        result = GetBitPosition(i) + RightMostOnePos(current);
    }
    return result;
}

UTILS_EXPORT uint64_t BitmapCountBits(const Bitmap *bitmap)
{
    if (unlikely(bitmap == NULL)) {
        return 0;
    }

    uint64_t result = 0;
    for (uint32_t i = 0; i < bitmap->size; i++) {
        ELEMENT_TYPE current = bitmap->bits[i];
        if (current == 0) {
            continue;
        }
        result += CountBitsOne(current);
    }
    return result;
}

UTILS_EXPORT BitmapCountShip BitmapCountBitsFast(const Bitmap *bitmap)
{
    if (unlikely(bitmap == NULL)) {
        return BITMAP_ALL_NOT_SET;
    }

    BitmapCountShip result = BITMAP_ALL_NOT_SET;
    for (uint32_t i = 0; i < bitmap->size; i++) {
        ELEMENT_TYPE current = bitmap->bits[i];
        if (current == 0) {
            continue;
        }

        if ((result == BITMAP_SINGLE_BIT_SET) || (HasMultipleOnes(current))) {
            return BITMAP_MULTIPLE_BITS_SET;
        }
        result = BITMAP_SINGLE_BIT_SET;
    }
    return result;
}

UTILS_EXPORT bool BitmapIsSubset(const Bitmap *a, const Bitmap *b)
{
    if (unlikely(a == NULL)) {
        return true; /* empty bitmap is a subset of anything bitmap */
    }

    if (unlikely(b == NULL)) {
        return BitmapIsEmpty(a);
    }

    /* a and b are not NULL, check the shorter size firstly */
    bool isFirstBigger = (a->size > b->size);
    uint32_t shorterSize = isFirstBigger ? b->size : a->size;
    for (uint32_t i = 0; i < shorterSize; i++) {
        if (!IsSubSet(a->bits[i], b->bits[i])) {
            return false;
        }
    }

    /* then, check extra bits */
    if (isFirstBigger) {
        /* a is the longer one and extra bits all zero */
        return IsEmptyFromTo(a, shorterSize, a->size);
    }
    return true;
}

UTILS_EXPORT bool BitmapEqual(const Bitmap *a, const Bitmap *b)
{
    if (unlikely((a == NULL) && (b == NULL))) {
        return true;
    }

    if (unlikely(a == NULL)) {
        return IsEmptyFromTo(b, 0, b->size);
    }

    if (unlikely(b == NULL)) {
        return IsEmptyFromTo(a, 0, a->size);
    }

    /* a and b are not NULL, check the shorter size firstly */
    bool isFirstBigger = (a->size > b->size);
    uint32_t shorterSize = isFirstBigger ? b->size : a->size;
    for (uint32_t i = 0; i < shorterSize; i++) {
        if (a->bits[i] != b->bits[i]) {
            return false;
        }
    }

    /* then, check extra bits */
    if (isFirstBigger) {
        /* a is the longer one and extra bits all zero */
        return IsEmptyFromTo(a, shorterSize, a->size);
    } else {
        return IsEmptyFromTo(b, shorterSize, b->size);
    }
}

UTILS_EXPORT bool BitmapIsBitSet(const Bitmap *bitmap, uint32_t bitPosition)
{
    if (unlikely(bitmap == NULL)) {
        return false;
    }

    uint32_t idx = GetBitsIdx(bitPosition);
    // assert not warp around
    ASSERT(idx < (idx + 1));

    if (idx >= bitmap->size) {
        /* the bitmap not include the bitPosition */
        return false;
    }

    return TestBit(&bitmap->bits[idx], GetPosInElement(bitPosition));
}

UTILS_EXPORT bool BitmapOverlap(const Bitmap *a, const Bitmap *b)
{
    if (unlikely((a == NULL) || (b == NULL))) {
        return false;
    }

    uint32_t shorterSize = Min(a->size, b->size);
    for (uint32_t i = 0; i < shorterSize; i++) {
        if ((a->bits[i] & b->bits[i]) != 0) {
            return true;
        }
    }
    return false;
}

UTILS_EXPORT bool BitmapAndComplementIsNonEmpty(const Bitmap *a, const Bitmap *b)
{
    if (unlikely(a == NULL)) {
        return false;
    }

    if (unlikely(b == NULL)) {
        return !IsEmptyFromTo(a, 0, a->size);
    }

    /* a and b are not NULL, check the shorter size firstly */
    uint32_t shorterSize = Min(a->size, b->size);
    for (uint32_t i = 0; i < shorterSize; i++) {
        if ((a->bits[i] & (~b->bits[i])) != 0) {
            return true;
        }
    }

    /* then, check extra bits */
    return !IsEmptyFromTo(a, shorterSize, a->size);
}

UTILS_EXPORT bool BitmapIsEmpty(const Bitmap *bitmap)
{
    if (unlikely(bitmap == NULL)) {
        return true;
    }

    return IsEmptyFromTo(bitmap, 0, bitmap->size);
}

UTILS_EXPORT Bitmap *BitmapOrInplace(Bitmap *a, const Bitmap *b)
{
    if (unlikely(a == NULL)) {
        return BitmapCopy(b);
    }

    if (unlikely(b == NULL)) {
        return a;
    }

    Bitmap *result;
    if (a->size < b->size) {
        /* the bitmap a is shorter */
        result = BitmapCreate(b->size);
        if (unlikely(result == NULL)) {
            return NULL;
        }
        for (uint32_t i = 0; i < a->size; i++) {
            result->bits[i] = a->bits[i] | b->bits[i];
        }
        /* copy thre remain space */
        uint32_t remainSpace = b->size - a->size;
        size_t bytes = remainSpace * sizeof(ELEMENT_TYPE);
        if (memcpy_s(&result->bits[a->size], bytes, &b->bits[a->size], bytes) != EOK) {
            MemFree(result);
            return NULL;
        }
        MemFree(a);
    } else {
        /* a is longer, reuse it */
        result = a;
        for (uint32_t i = 0; i < b->size; i++) {
            result->bits[i] = a->bits[i] | b->bits[i];
        }
    }
    return result;
}

UTILS_EXPORT Bitmap *BitmapAndInplace(Bitmap *a, const Bitmap *b)
{
    if (unlikely(a == NULL)) {
        return NULL;
    }

    if (unlikely(b == NULL)) {
        MemFree(a);
        return NULL;
    }

    /* a and b are not NULL, check the shorter size firstly */
    uint32_t shorterSize = Min(a->size, b->size);
    for (uint32_t i = 0; i < shorterSize; i++) {
        a->bits[i] &= b->bits[i];
    }

    /* set a remain space to 0 */
    if (a->size > b->size) {
        ASSERT(shorterSize == b->size);
        size_t bytes = (a->size - b->size) * sizeof(ELEMENT_TYPE);
        (void)memset_s(&a->bits[b->size], bytes, 0, bytes);
    }
    return a;
}

UTILS_EXPORT Bitmap *BitmapAndComplementInplace(Bitmap *a, const Bitmap *b)
{
    if (unlikely(a == NULL)) {
        return NULL;
    }

    if (unlikely(b == NULL)) {
        return a;
    }

    /* a and b are not NULL, check the shorter size firstly */
    uint32_t shorterSize = Min(a->size, b->size);
    for (uint32_t i = 0; i < shorterSize; i++) {
        a->bits[i] &= (~b->bits[i]);
    }

    return a;
}

UTILS_EXPORT Bitmap *BitmapOrWithDestroy(Bitmap *a, Bitmap *b)
{
    if (unlikely(a == NULL)) {
        return b;
    }

    if (unlikely(b == NULL)) {
        return a;
    }

    Bitmap *result;
    if (a->size < b->size) {
        /* the bitmap a is shorter */
        result = b;
        for (uint32_t i = 0; i < a->size; i++) {
            result->bits[i] = a->bits[i] | b->bits[i];
        }
        MemFree(a);
    } else {
        /* a is longer, reuse it */
        result = a;
        for (uint32_t i = 0; i < b->size; i++) {
            result->bits[i] = a->bits[i] | b->bits[i];
        }
        MemFree(b);
    }
    return result;
}

UTILS_EXPORT int64_t BitmapGetFirstBit(const Bitmap *bitmap)
{
    if (unlikely(bitmap == NULL)) {
        return -1;
    }

    for (uint32_t i = 0; i < bitmap->size; i++) {
        ELEMENT_TYPE current = bitmap->bits[i];
        if (current == 0) {
            continue;
        }
        return GetBitPosition(i) + RightMostOnePos(current);
    }
    return -1;
}

UTILS_EXPORT int64_t BitmapGetNextBit(const Bitmap *bitmap, uint32_t startPosition)
{
    if (unlikely(bitmap == NULL)) {
        return -1;
    }

    ASSERT(bitmap->size > 0);
    uint32_t fromPosition = startPosition + 1;
    if (unlikely(fromPosition < startPosition)) {
        /* wrap around */
        return -1;
    }
    /* the bitmap element index where fromPosition bit is located */
    uint32_t fromIdx = GetBitsIdx(fromPosition);
    if (unlikely(fromIdx >= bitmap->size)) {
        /* bitmap is not include the startPosition */
        return -1;
    }
    /* firstly, handle the fromIdx */
    ELEMENT_TYPE mask = (~(ELEMENT_TYPE)0) << GetPosInElement(fromPosition);
    ELEMENT_TYPE valueMasked = bitmap->bits[fromIdx] & mask;
    if (valueMasked != 0) {
        return GetBitPosition(fromIdx) + RightMostOnePos(valueMasked);
    }
    /* then, handle the remained element without mask bit */
    ASSERT(fromIdx < fromIdx + 1);
    for (uint32_t i = fromIdx + 1; i < bitmap->size; i++) {
        ELEMENT_TYPE current = bitmap->bits[i];
        if (current == 0) {
            continue;
        }
        return GetBitPosition(i) + RightMostOnePos(current);
    }
    return -1;
}

UTILS_EXPORT uint32_t BitmapHashValue(const Bitmap *bitmap)
{
    if (unlikely(bitmap == NULL)) {
        return 0;
    }

    /* get non zero bits at last */
    for (int64_t i = (int64_t)bitmap->size - 1; i >= 0; i--) {
        if (bitmap->bits[i] == 0) {
            /* zero in tail no need to hash */
            continue;
        }

        uintptr_t address = (uintptr_t)bitmap + offsetof(Bitmap, bits); /* the start address of bits */
        size_t len = (size_t)(i + 1) * sizeof(ELEMENT_TYPE);            /* the length need to hash */
        return HashAny((const unsigned char *)address, (int)len);
    }
    return 0; /* the bitmap is empty */
}
