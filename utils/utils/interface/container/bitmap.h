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
 * Description: implement the external header of bitmap.c
 */
#ifndef UTILS_BITMAP_H
#define UTILS_BITMAP_H

#include "defines/common.h"

GSDB_BEGIN_C_CODE_DECLS

typedef struct Bitmap Bitmap;

/**
 * Some Definition Of Concept
 * EMPTY    : a bitmap no set any bit, or bitmap pointer is null, that implies an empty bitmap of any size
 * EQUAL    : the positions of the bits be setbit are all the same, although the size of the bitmap is different
 * SUBSET   : subset, easy to understand
 * DIFFERENT: the positions of the bits be setbit, have partial same or all different, and not subsets of each other
 */

/* two bitmap set relations, as the result of comparison API */
typedef enum {
    BITMAP_EQUAL,                /* a '==' b, see BitmapEqual */
    BITMAP_SECOND_CONTAIN_FIRST, /* second is include first (first set is a subset of the second) */
    BITMAP_FIRST_CONTAIN_SECOND, /* first is include second (second set is a subset of the first) */
    BITMAP_DIFFERENT,            /* not subsets of each other */
} BitmapCmpResult;

/* rough statistics the quantity of member's set bit count */
typedef enum {
    BITMAP_ALL_NOT_SET,       /* no set any bit, or bitmap pointer is null, that implies an empty bitmap of any size */
    BITMAP_SINGLE_BIT_SET,    /* only one bit had set */
    BITMAP_MULTIPLE_BITS_SET, /* multi bit had set, >1 */
} BitmapCountShip;

/**
 * create a Bitmap object and initialize set the bitPosition to '1', bitPosition start from 0,
 * so the quantity of Bitmap bit slots at least is bitPosition + 1
 *
 * @param[in] bitPosition    - the Bitmap position of bit slots need to setbit
 * @return                   - NULL is initialize fail (allocate memory fail), not NULL is success initialize a object
 */
Bitmap *BitmapCreateWithSet(uint32_t bitPosition);

/**
 * destroy a Bitmap object
 *
 * @param[in] bitmap         - the Bitmap object pointer
 */
void BitmapDestroy(Bitmap *bitmap);

/**
 * copy a Bitmap object
 *
 * @param[in] bitmap         - the Bitmap object pointer
 * @return                   - the copied Bitmap object pointer
 */
Bitmap *BitmapCopy(const Bitmap *bitmap);

/**
 * set bit is set bitmap bit bitPosition to 1, if bitPosition is exceeded the maximum bit slot that the bitmap can
 * accommodate will be automatically expanded to accommodate bit bitPosition.
 *
 * @param[in] bitmap         - the Bitmap object pointer
 * @param[in] bitPosition    - the bits need to set, first bitPosition is 0
 * @return                   - return Bitmap pointer after operate success, if happened Bitmap size extend, the old
 *                             Bitmap will Destroyd when extend success, but if bitPosition is too big, and extend
                               Bitmap size fail (allocate memory fail) will return NULL, old Bitmap not Destroyd.
 *
 * @constraint               - don't attempt to Destroy bitmap if return not NULL
 */
Bitmap *BitmapSetBit(Bitmap *bitmap, uint32_t bitPosition);

/**
 * clear bit is set bitmap bitPosition to 0, if bitPosition is exceeded the maximum bit slot that the bitmap can
 * accommodate, will be return directly. this API always success.
 *
 * @param[in] bitmap         - the Bitmap object pointer
 * @param[in] bitPosition    - the bits need to clear, first bitPosition is 0
 */
void BitmapClearBit(Bitmap *bitmap, uint32_t bitPosition);

/* below is set operates */
/**
 * union two Bitmap object, return unionset, that is say, a | b
 *
 * @param[in] a             - the Bitmap object pointer
 * @param[in] b             - the another Bitmap object pointer
 * @return                  - the union set Bitmap object pointer
 */
Bitmap *BitmapOr(const Bitmap *a, const Bitmap *b);

/**
 * evaluate the intersect set of two Bitmap object, that is say, a & b
 *
 * @param[in] a             - the Bitmap object pointer
 * @param[in] b             - the another Bitmap object pointer
 * @return                  - the intersect set Bitmap object pointer
 */
Bitmap *BitmapAnd(const Bitmap *a, const Bitmap *b);

/**
 * evaluate the symmetric difference set of two Bitmap object, that is say, a & (~b)
 *
 * @param[in] a             - the Bitmap object pointer
 * @param[in] b             - the another Bitmap object pointer
 * @return                  - the difference set Bitmap object pointer
 */
Bitmap *BitmapAndComplement(const Bitmap *a, const Bitmap *b);

/**
 * the relationship compare of two Bitmap object
 *
 * @param[in] a             - the Bitmap object pointer
 * @param[in] b             - the another Bitmap object pointer
 * @return                  - the comparsion relationship, see BitmapCmpResult prototype define
 */
BitmapCmpResult BitmapCompare(const Bitmap *a, const Bitmap *b);

/**
 * get the pos of single bit that had been set in Bitmap, like BitmapSingletonMember
 *
 * @param[in] bitmap        - the Bitmap object pointer
 * @return                  - -1, bitmap is not only Singleton setbit (multi bits set to 1) or the setbit slot position
 */
int64_t BitmapGetPositionForSingleton(const Bitmap *bitmap);

/**
 * count the quantity of setbit in Bitmap
 *
 * @param[in] bitmap        - the Bitmap object pointer
 * @return                  - the count of setbit in Bitmap
 */
uint64_t BitmapCountBits(const Bitmap *bitmap);

/**
 * rough count the quantity of setbit in Bitmap, optimized tests when we don't need to know exact count
 *
 * @param[in] bitmap        - the Bitmap object pointer
 * @return                  - see BitmapCountShip prototype define
 */
BitmapCountShip BitmapCountBitsFast(const Bitmap *bitmap);

/**
 * determine whether a is a subset of b
 *
 * @param[in] a             - the Bitmap object pointer
 * @param[in] b             - the another Bitmap object pointer
 * @return                  - true, a is the subset of b. false, not
 */
bool BitmapIsSubset(const Bitmap *a, const Bitmap *b);

/**
 * determine whether a is equal of b, such as
 * a: 0b0101100010
 * b: 0b0101100010000000
 * even though the two bitmaps are not equal in size, but the two of them,
 * the setbit '1' positions are the same. we say, a '==' b
 *
 * @param[in] a             - the Bitmap object pointer
 * @param[in] b             - the another Bitmap object pointer
 * @return                  - true, a '==' b. false, not
 */
bool BitmapEqual(const Bitmap *a, const Bitmap *b);

/**
 * determine whether bitPosition is set to 1
 *
 * @param[in] bitmap        - the Bitmap object pointer
 * @param[in] bitPosition   - the bits need query whether setbit, first bitPosition is 0
 * @return                  - ture, bits x is setbit. false, not
 */
bool BitmapIsBitSet(const Bitmap *bitmap, uint32_t bitPosition);

/**
 * determine whether a and b is overlay
 *
 * @param[in] a             - the Bitmap object pointer
 * @param[in] b             - the another Bitmap object pointer
 * @return                  - true, is overlap. false, not
 */
bool BitmapOverlap(const Bitmap *a, const Bitmap *b);

/**
 * determine whether a and b sets have a nonempty difference
 *
 * @param[in] a             - the Bitmap object pointer
 * @param[in] b             - the another Bitmap object pointer
 * @return                  - true, have a nonempty difference. false, not, that is to say (a & (~b)) is empty
 */
bool BitmapAndComplementIsNonEmpty(const Bitmap *a, const Bitmap *b);

/**
 * determine whether bitmap is empty Bitmap
 *
 * @param[in] bitmap        - the Bitmap object pointer
 * @return                  - true, empty, see Definition of EMPTY. false, not
 */
bool BitmapIsEmpty(const Bitmap *bitmap);

/**
 * add Bitmap b to Bitmap a in place, but if happened size extend, the Bitmap a will destroyed and return a new create
 * bitmap, just in case, create new bitmap fail, will return NULL and Bitmap a not destroyed. so you need check the
 * return value, and if not NULL, can guarantee assigned to a is safety
 *
 * @param[in] a             - the Bitmap object pointer
 * @param[in] b             - the another Bitmap object pointer
 * @return                  - NULL, operate fail. not NULL, success
 *
 * @constraint              - don't attempt to Destroy a if return not NULL
 */
Bitmap *BitmapOrInplace(Bitmap *a, const Bitmap *b);

/**
 * evaluate Bitmap a and b intersection for a in place, (like a = a & b)
 *
 * @param[in] a             - the Bitmap object pointer
 * @param[in] b             - the another Bitmap object pointer
 * @return                  - this function won't happened size extend never, and alway return Bitmap a
 */
Bitmap *BitmapAndInplace(Bitmap *a, const Bitmap *b);

/**
 * delete Bitmap b from Bitmap a in place, logic operation is (a = a & (~b))
 *
 * @param[in] a             - the Bitmap object pointer
 * @param[in] b             - the another Bitmap object pointer
 * @return                  - this function won't happened size extend never, and alway return Bitmap a
 */
Bitmap *BitmapAndComplementInplace(Bitmap *a, const Bitmap *b);

/**
 * like BitmapAnd, but *both* inputs are recycled, c = (a & b)
 *
 * @param[in] a             - the Bitmap object pointer
 * @param[in] b             - the another Bitmap object pointer
 * @return                  - NULL, operate fail, the input not Destroyd. not NULL, success
 *
 * @constraint              - don't attempt to Destroy a and b, if return not NULL
 */
Bitmap *BitmapOrWithDestroy(Bitmap *a, Bitmap *b);

/**
 * Get the first setbit slot position, support for iterating through the integer elements of a set
 *
 * @param[in] bitmap        - the Bitmap object pointer
 * @return                  - the first setbit slot position, if not exist, return -1
 */
int64_t BitmapGetFirstBit(const Bitmap *bitmap);

/**
 * Get the next setbit slot position after startPosition, not include the startPosition
 *
 * @param[in] bitmap        - the Bitmap object pointer
 * @param[in] startPosition - the search starting position
 * @return                  - the next first setbit slot position after startPosition, if not exist, return -1
 */
int64_t BitmapGetNextBit(const Bitmap *bitmap, uint32_t startPosition);

/**
 * support for hashtables using Bitmaps as keys
 *
 * @param[in] bitmap        - the Bitmap object pointer
 * @return                  - 32bit hash value
 */
uint32_t BitmapHashValue(const Bitmap *bitmap);

GSDB_END_C_CODE_DECLS

#endif /* UTILS_BITMAP_H */
