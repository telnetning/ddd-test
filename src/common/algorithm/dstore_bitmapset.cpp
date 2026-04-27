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
 *        src/common/algorithm/dstore_bitmapset.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "common/algorithm/dstore_bitmapset.h"
#include "securec.h"
#include "common/dstore_datatype.h"
#include "common/memory/dstore_mctx.h"
#include "common/algorithm/dstore_hsearch.h"

namespace DSTORE {

#define WORDNUM(x) ((x) / DSTORE_BITS_PER_BITMAPWORD)
#define BITNUM(x) ((x) % DSTORE_BITS_PER_BITMAPWORD)

/* --------------------------------------------------------------------------------
 * This is a well-known cute trick for isolating the rightmost one-bit in a word.
 * The value is then something like
 *     xxxxxx10000
 * where x's are unspecified bits.
 * value - 1  gives
 *     xxxxxx01111
 * value & (value - 1) gives
 *     xxxxxx00000
 * (value & (value - 1)) ^ value gives
 *     00000010000
 * This works for all cases except original value = zero, where of course we get zero.
 * --------------------------------------------------------------------------------
 */
#define RIGHTMOST_ONE(x) (((x) & ((x) - 1)) ^ (x))
#define HAS_MULTIPLE_ONES(x) ((bitmapword)RIGHTMOST_ONE(x) != (x))

/*
 * Lookup tables to avoid need for bit-by-bit groveling
 *
 * rightmost_one_pos[x] gives the bit number (0-7) of the rightmost one bit in a nonzero byte value x.
 * The entry for x=0 is never used.
 *
 * number_of_ones[x] gives the number of one-bits (0-8) in a byte value x.
 *
 * We could make these tables larger and reduce the number of iterations in the functions that use them, but bytewise
 * shifts and masks are especially fast on many machines, so working a byte at a time seems best.
 */
static const uint8 rightmost_one_pos[256] = {
    0, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 0, 2,
    0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 6, 0, 1, 0, 2, 0, 1, 0, 3, 0,
    1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1,
    0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 7, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0,
    2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3,
    0, 1, 0, 2, 0, 1, 0, 6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0,
    1, 0, 5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0};

static const uint8 number_of_ones[256] = {
    0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 1, 2, 2, 3, 2,
    3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3,
    3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5,
    6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4,
    3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4,
    5, 5, 6, 5, 6, 6, 7, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6,
    6, 7, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8};

/*
 * BmsCopy - make a palloc'd copy of a bitmapset
 */
Bitmapset* BmsCopy(const Bitmapset* a)
{
    Bitmapset* result = nullptr;
    size_t size;

    if (a == nullptr) {
        return nullptr;
    }
    size = CalculateDstoreBitmapsetSize(a->nwords);
    result = static_cast<Bitmapset *>(DstorePalloc(size));
    if (STORAGE_VAR_NULL(result)) {
        return nullptr;
    }
    errno_t rc = memcpy_s(result, size, a, size);
    storage_securec_check(rc, "\0", "\0");
    return result;
}

/*
 * BmsEqual - are two bitmapsets equal?
 *
 * This is logical not physical equality; in particular, a NULL pointer will
 * be reported as equal to a palloc'd value containing no members.
 */
bool BmsEqual(const Bitmapset* a, const Bitmapset* b)
{
    const Bitmapset* longer = nullptr;
    const Bitmapset* shorter = nullptr;
    int longLen;
    int shortLen;
    int i;

    /* Handle cases where either input is NULL */
    if (a == nullptr) {
        if (b == nullptr) {
            return true;
        }
        return BmsIsEmpty(b);
    } else if (b == nullptr) {
        return BmsIsEmpty(a);
    }
    /* Identify shorter and longer input */
    if (a->nwords <= b->nwords) {
        longer = b;
        shorter = a;
    } else {
        longer = a;
        shorter = b;
    }
    /* And process */
    shortLen = shorter->nwords;
    for (i = 0; i < shortLen; i++) {
        if (longer->words[i] != shorter->words[i]) {
            return false;
        }
    }
    longLen = longer->nwords;
    for (; i < longLen; i++) {
        if (longer->words[i] != 0) {
            return false;
        }
    }
    return true;
}

/*
 * BmsMakeSingleton - build a bitmapset containing a single member
 */
Bitmapset* BmsMakeSingleton(int x)
{
    Bitmapset* result = nullptr;
    int wordnum, bitnum;

    if (x < 0) {
        return nullptr;
    }
    wordnum = WORDNUM(x);
    bitnum = BITNUM(x);
    result = static_cast<Bitmapset *>(DstorePalloc0(CalculateDstoreBitmapsetSize(wordnum + 1)));
    if (STORAGE_VAR_NULL(result)) {
        return nullptr;
    }
    result->nwords = wordnum + 1;
    result->words[wordnum] = ((bitmapword)1 << (unsigned int)bitnum);
    return result;
}

/*
 * BmsFree - free a bitmapset
 *
 * Same as DstorePfree except for allowing NULL input
 */
void BmsFree(Bitmapset* a)
{
    if (a != nullptr) {
        DstorePfreeExt(a);
    }
}

/*
 * These operations all make a freshly palloc'd result,
 * leaving their inputs untouched
 */

/*
 * BmsUnion - set union
 */
Bitmapset* BmsUnion(const Bitmapset* a, const Bitmapset* b)
{
    Bitmapset* result = nullptr;
    const Bitmapset* other = nullptr;
    int otherLen;
    int i;

    /* Handle cases where either input is NULL */
    if (a == nullptr) {
        return BmsCopy(b);
    }
    if (b == nullptr) {
        return BmsCopy(a);
    }
    /* Identify shorter and longer input; copy the longer one */
    if (a->nwords <= b->nwords) {
        result = BmsCopy(b);
        other = a;
    } else {
        result = BmsCopy(a);
        other = b;
    }
    if (STORAGE_VAR_NULL(result)) {
        return nullptr;
    }
    /* And union the shorter input into the result */
    otherLen = other->nwords;
    for (i = 0; i < otherLen; i++) {
        result->words[i] |= other->words[i];
    }
    return result;
}

/*
 * BmsIntersect - set intersection
 */
Bitmapset* BmsIntersect(const Bitmapset* a, const Bitmapset* b)
{
    Bitmapset* result = nullptr;
    const Bitmapset* other = nullptr;
    int resultLen;
    int i;

    /* Handle cases where either input is NULL */
    if (a == nullptr || b == nullptr) {
        return nullptr;
    }
    /* Identify shorter and longer input; copy the shorter one */
    if (a->nwords <= b->nwords) {
        result = BmsCopy(a);
        other = b;
    } else {
        result = BmsCopy(b);
        other = a;
    }
    if (STORAGE_VAR_NULL(result)) {
        return nullptr;
    }
    /* And intersect the longer input with the result */
    resultLen = result->nwords;
    for (i = 0; i < resultLen; i++) {
        result->words[i] &= other->words[i];
    }
    return result;
}

/*
 * BmsDifference - set difference (ie, A without members of B)
 */
Bitmapset* BmsDifference(const Bitmapset* a, const Bitmapset* b)
{
    Bitmapset* result = nullptr;
    int shortLen;
    int i;

    /* Handle cases where either input is NULL */
    if (a == nullptr) {
        return nullptr;
    }
    if (b == nullptr) {
        return BmsCopy(a);
    }
    /* Copy the left input */
    result = BmsCopy(a);
    if (STORAGE_VAR_NULL(result)) {
        return nullptr;
    }
    /* And remove b's bits from result */
    shortLen = DstoreMin(a->nwords, b->nwords);
    for (i = 0; i < shortLen; i++) {
        result->words[i] &= ~b->words[i];
    }
    return result;
}

/*
 * BmsIsSubset - is A a subset of B?
 */
bool BmsIsSubset(const Bitmapset* a, const Bitmapset* b)
{
    int shortLen;
    int longLen;
    int i;

    /* Handle cases where either input is NULL */
    if (a == nullptr) {
        return true; /* empty set is a subset of anything */
    }
    if (b == nullptr) {
        return BmsIsEmpty(a);
    }
    /* Check common words */
    shortLen = DstoreMin(a->nwords, b->nwords);
    for (i = 0; i < shortLen; i++) {
        if ((a->words[i] & (~b->words[i])) != 0) {
            return false;
        }
    }
    /* Check extra words */
    if (a->nwords > b->nwords) {
        longLen = a->nwords;
        for (; i < longLen; i++) {
            if (a->words[i] != 0) {
                return false;
            }
        }
    }
    return true;
}

/*
 * BmsSubsetCompare - compare A and B for equality/subset relationships
 *
 * This is more efficient than testing BmsIsSubset in both directions.
 */
BMS_Comparison BmsSubsetCompare(const Bitmapset* aSet, const Bitmapset* bSet)
{
    BMS_Comparison result;
    int shortLen;
    int longLen;
    int i;

    /* Handle cases where either input is NULL */
    if (aSet == nullptr) {
        if (bSet == nullptr) {
            return BMS_EQUAL;
        }
        return BmsIsEmpty(bSet) ? BMS_EQUAL : BMS_SUBSET1;
    }
    if (bSet == nullptr) {
        return BmsIsEmpty(aSet) ? BMS_EQUAL : BMS_SUBSET2;
    }
    /* Check common words */
    result = BMS_EQUAL; /* status so far */
    shortLen = DstoreMin(aSet->nwords, bSet->nwords);
    for (i = 0; i < shortLen; i++) {
        bitmapword aword = aSet->words[i];
        bitmapword bword = bSet->words[i];

        if ((aword & ~bword) != 0) {
            /* aSet is not aSet subset of bSet */
            if (result == BMS_SUBSET1) {
                return BMS_DIFFERENT;
            }
            result = BMS_SUBSET2;
        }
        if ((bword & ~aword) != 0) {
            /* bSet is not aSet subset of aSet */
            if (result == BMS_SUBSET2) {
                return BMS_DIFFERENT;
            }
            result = BMS_SUBSET1;
        }
    }
    /* Check extra words */
    if (aSet->nwords > bSet->nwords) {
        longLen = aSet->nwords;
        for (; i < longLen; i++) {
            if (aSet->words[i] != 0) {
                /* aSet is not aSet subset of bSet */
                if (result == BMS_SUBSET1) {
                    return BMS_DIFFERENT;
                }
                result = BMS_SUBSET2;
            }
        }
    } else if (aSet->nwords < bSet->nwords) {
        longLen = bSet->nwords;
        for (; i < longLen; i++) {
            if (bSet->words[i] != 0) {
                /* bSet is not aSet subset of aSet */
                if (result == BMS_SUBSET2) {
                    return BMS_DIFFERENT;
                }
                result = BMS_SUBSET1;
            }
        }
    }
    return result;
}

/*
 * BmsIsMember - is X a member of A?
 */
bool BmsIsMember(int x, const Bitmapset* a)
{
    int wordnum, bitnum;

    /* XXX better to just return false for x<0 ? */
    if (x < 0) {
        return false;
    }
    if (a == nullptr) {
        return false;
    }
    wordnum = WORDNUM(x);
    bitnum = BITNUM(x);
    if (wordnum >= a->nwords)  {
        return false;
    }
    if ((a->words[wordnum] & ((bitmapword)1 << (unsigned int)bitnum)) != 0) {
        return true;
    }
    return false;
}

/*
 * BmsOverlap - do sets overlap (ie, have a nonempty intersection)?
 */
bool BmsOverlap(const Bitmapset* a, const Bitmapset* b)
{
    int shortLen;
    int i;

    /* Handle cases where either input is NULL */
    if (a == nullptr || b == nullptr) {
        return false;
    }
    /* Check words in common */
    shortLen = DstoreMin(a->nwords, b->nwords);
    for (i = 0; i < shortLen; i++) {
        if ((a->words[i] & b->words[i]) != 0) {
            return true;
        }
    }
    return false;
}

/*
 * BmsNonemptyDifference - do sets have a nonempty difference?
 */
bool BmsNonemptyDifference(const Bitmapset* aSet, const Bitmapset* bSet)
{
    int shortLen;
    int i;

    /* Handle cases where either input is NULL */
    if (aSet == nullptr) {
        return false;
    }
    if (bSet == nullptr) {
        return !BmsIsEmpty(aSet);
    }
    /* Check words in common */
    shortLen = DstoreMin(aSet->nwords, bSet->nwords);
    for (i = 0; i < shortLen; i++) {
        if ((aSet->words[i] & ~bSet->words[i]) != 0) {
            return true;
        }
    }
    /* Check extra words in aSet */
    for (; i < aSet->nwords; i++) {
        if (aSet->words[i] != 0) {
            return true;
        }
    }
    return false;
}

/*
 * BmsSingletonMember - return the sole integer member of set
 *
 * Raises error if |a| is not 1.
 */
int BmsSingletonMember(const Bitmapset* a)
{
    int result = -1;
    int nwords;
    int wordnum;

    if (a == nullptr) {
        return -1;
    }
    nwords = a->nwords;
    for (wordnum = 0; wordnum < nwords; wordnum++) {
        bitmapword w = a->words[wordnum];

        if (w != 0) {
            if (result >= 0 || HAS_MULTIPLE_ONES(w)) {
                return -1;
            }
            result = wordnum * DSTORE_BITS_PER_BITMAPWORD;
            while ((w & 255) == 0) {
                w >>= 8;
                result += 8;
            }
            result += rightmost_one_pos[w & 255];
        }
    }
    if (result < 0) {
        return -1;
    }
    return result;
}

/*
 * BmsNumMembers - count members of set
 */
int BmsNumMembers(const Bitmapset* aSet)
{
    int result = 0;
    int nwords;
    int wordnum;

    if (aSet == nullptr) {
        return 0;
    }
    nwords = aSet->nwords;
    for (wordnum = 0; wordnum < nwords; wordnum++) {
        bitmapword w = aSet->words[wordnum];
        /* we assume here that bitmapword is an unsigned type */
        while (w != 0) {
            result += number_of_ones[w & 255];
            w >>= 8;
        }
    }
    return result;
}

/*
 * BmsMembership - does a set have zero, one, or multiple members?
 *
 * This is faster than making an exact count with BmsNumMembers().
 */
BMS_Membership BmsMembership(const Bitmapset* aSet)
{
    BMS_Membership result = BMS_EMPTY_SET;
    int nwords;
    int wordnum;

    if (aSet == nullptr) {
        return BMS_EMPTY_SET;
    }
    nwords = aSet->nwords;
    for (wordnum = 0; wordnum < nwords; wordnum++) {
        bitmapword w = aSet->words[wordnum];
        if (w != 0) {
            if (result != BMS_EMPTY_SET || HAS_MULTIPLE_ONES(w)) {
                return BMS_MULTIPLE;
            }
            result = BMS_SINGLETON;
        }
    }
    return result;
}

/*
 * BmsIsEmpty - is a set empty?
 *
 * This is even faster than BmsMembership().
 */
bool BmsIsEmpty(const Bitmapset* a)
{
    int nwords;
    int wordnum;

    if (a == nullptr) {
        return true;
    }
    nwords = a->nwords;
    for (wordnum = 0; wordnum < nwords; wordnum++) {
        bitmapword word = a->words[wordnum];
        if (word != 0) {
            return false;
        }
    }
    return true;
}

/*
 * These operations all "recycle" their non-const inputs, ie, either return the modified input or pfree it if it can't
 * hold the result.
 *
 * These should generally be used in the style
 *
 *  foo = BmsAddMember(foo, x);
 */

/*
 * BmsAddMember - add a specified member to set
 *
 * Input set is modified or recycled!
 */
Bitmapset* BmsAddMember(Bitmapset* a, int x)
{
    int wordnum, bitnum;

    if (x < 0) {
        return nullptr;
    }
    if (a == nullptr) {
        return BmsMakeSingleton(x);
    }
    wordnum = WORDNUM(x);
    bitnum = BITNUM(x);
    if (wordnum >= a->nwords) {
        /* Slow path: make a larger set and union the input set into it */
        Bitmapset* result = nullptr;
        int nwords;
        int i;

        result = BmsMakeSingleton(x);
        if (STORAGE_VAR_NULL(result)) {
            return nullptr;
        }
        nwords = a->nwords;
        for (i = 0; i < nwords; i++) {
            result->words[i] |= a->words[i];
        }
        DstorePfreeExt(a);
        return result;
    }
    /* Fast path: x fits in existing set */
    a->words[wordnum] |= ((bitmapword)1 << (unsigned int)bitnum);
    return a;
}

/*
 * BmsDelMember - remove a specified member from set
 *
 * No error if x is not currently a member of set
 *
 * Input set is modified in-place!
 */
Bitmapset* BmsDelMember(Bitmapset* a, int x)
{
    int wordnum, bitnum;

    if (x < 0) {
        return nullptr;
    }
    if (a == nullptr) {
        return nullptr;
    }
    wordnum = WORDNUM(x);
    bitnum = BITNUM(x);
    if (wordnum < a->nwords) {
        a->words[wordnum] &= ~((bitmapword)1 << bitnum);
    }
    return a;
}

/*
 * BmsAddMembers - like BmsUnion, but left input is recycled
 */
Bitmapset* BmsAddMembers(Bitmapset* a, const Bitmapset* b)
{
    Bitmapset* result = nullptr;
    const Bitmapset* other = nullptr;
    int otherlen;
    int i;

    /* Handle cases where either input is NULL */
    if (a == nullptr) {
        return BmsCopy(b);
    }
    if (b == nullptr) {
        return a;
    }
    /* Identify shorter and longer input; copy the longer one if needed */
    if (a->nwords < b->nwords) {
        other = a;
        result = BmsCopy(b);
    } else {
        other = b;
        result = a;
    }
    if (STORAGE_VAR_NULL(result)) {
        return nullptr;
    }
    /* And union the shorter input into the result */
    otherlen = other->nwords;
    for (i = 0; i < otherlen; i++) {
        result->words[i] |= other->words[i];
    }
    if (result != a) {
        DstorePfreeExt(a);
    }
    return result;
}

/*
 * BmsIntMembers - like BmsIntersect, but left input is recycled
 */
Bitmapset* BmsIntMembers(Bitmapset* a, const Bitmapset* b)
{
    int shortLen;
    int i;

    /* Handle cases where either input is NULL */
    if (a == nullptr) {
        return nullptr;
    }
    if (b == nullptr) {
        DstorePfreeExt(a);
        return nullptr;
    }
    /* Intersect b into a; we need never copy */
    shortLen = DstoreMin(a->nwords, b->nwords);
    for (i = 0; i < shortLen; i++) {
        a->words[i] &= b->words[i];
    }
    for (; i < a->nwords; i++) {
        a->words[i] = 0;
    }
    return a;
}

/*
 * BmsDelMembers - like BmsDifference, but left input is recycled
 */
Bitmapset* BmsDelMembers(Bitmapset* a, const Bitmapset* b)
{
    int shortLen;
    int i;

    /* Handle cases where either input is NULL */
    if (a == nullptr) {
        return nullptr;
    }
    if (b == nullptr) {
        return a;
    }
    /* Remove b's bits from a; we need never copy */
    shortLen = DstoreMin(a->nwords, b->nwords);
    for (i = 0; i < shortLen; i++) {
        a->words[i] &= ~b->words[i];
    }
    return a;
}

/*
 * BmsJoin - like BmsUnion, but *both* inputs are recycled
 */
Bitmapset* BmsJoin(Bitmapset* a, Bitmapset* b)
{
    Bitmapset* result = nullptr;
    Bitmapset* other = nullptr;
    int otherlen;
    int i;

    /* Handle cases where either input is NULL */
    if (a == nullptr) {
        return b;
    }
    if (b == nullptr) {
        return a;
    }
    /* Identify shorter and longer input; use longer one as result */
    if (a->nwords < b->nwords) {
        other = a;
        result = b;
    } else {
        other = b;
        result = a;
    }
    /* And union the shorter input into the result */
    otherlen = other->nwords;
    for (i = 0; i < otherlen; i++) {
        result->words[i] |= other->words[i];
    }
    if (other != result) {  /* pure paranoia */
        DstorePfreeExt(other);
    }
    return result;
}

/* ----------
 * BmsFirstMember - find and remove first member of a set
 *
 * Returns -1 if set is empty. NB: set is destructively modified!
 *
 * This is intended as support for iterating through the members of a set.
 * The typical pattern is
 *
 *   tmpset = BmsCopy(inputset);
 *   while ((x = BmsFirstMember(tmpset)) >= 0)
 *    process member x;
 *   bms_free_ext(tmpset);
 * ----------
 */
int BmsFirstMember(Bitmapset* a)
{
    int nwords;
    int wordnum;
    const uint32_t mask255 = 255;
    const uint32_t bitsPerByte = 8;
    if (a == nullptr) {
        return -1;
    }
    nwords = a->nwords;
    for (wordnum = 0; wordnum < nwords; wordnum++) {
        bitmapword word = a->words[wordnum];
        if (word != 0) {
            int result;
            word = RIGHTMOST_ONE(word);
            a->words[wordnum] &= ~word;

            result = wordnum * DSTORE_BITS_PER_BITMAPWORD;
            while ((word & mask255) == 0) {
                word >>= bitsPerByte;
                result += bitsPerByte;
            }
            result += rightmost_one_pos[word & mask255];
            return result;
        }
    }
    return -1;
}

/*
 * BmsHashValue - compute a hash key for a Bitmapset
 *
 * Note: we must ensure that any two bitmapsets that are BmsEqual() will
 * hash to the same value; in practice this means that trailing all-zero
 * words must not affect the result.  Hence we strip those before applying
 * hash_any().
 */
uint32 BmsHashValue(const Bitmapset* a)
{
    int lastword;

    if (a == nullptr) {
        /* All empty sets hash to 0 */
        return 0;
    }
    for (lastword = a->nwords; --lastword >= 0;) {
        if (a->words[lastword] != 0) {
            break;
        }
    }
    if (lastword < 0) {
        /* All empty sets hash to 0 */
        return 0;
    }
    return DatumGetUInt32(hash_any((const unsigned char*)a->words, (lastword + 1) * sizeof(bitmapword)));
}

/*
 * BmsNextMember - find next member of a set
 *
 * Returns smallest member greater than "prevbit", or -2 if there is none.
 * "prevbit" must NOT be less than -1, or the behavior is unpredictable.
 *
 * This is intended as support for iterating through the members of a set.
 * The typical pattern is
 *
 *   x = -1;
 *   while ((x = BmsNextMember(inputset, x)) >= 0)
 *    process member x;
 *
 * Notice that when there are no more members, we return -2, not -1 as you might expect.  The rationale for that is to
 * allow distinguishing the loop-not-started state (x == -1) from the loop-completed state (x == -2).
 * It makes no difference in simple loop usage, but complex iteration logic might need such an ability.
 */
int BmsNextMember(const Bitmapset* a, int prevbit)
{
    int nwords;
    int wordnum;
    bitmapword mask;
    const uint32_t mask255 = 255;
    const uint32_t bitsPerByte = 8;

    if (prevbit < -1) {
        return -1;
    }

    if (a == nullptr) {
        return -2;
    }
    nwords = a->nwords;
    prevbit++;
    mask = (~(bitmapword)0) << BITNUM((unsigned int)prevbit);
    for (wordnum = WORDNUM(prevbit); wordnum < nwords; wordnum++) {
        bitmapword word = a->words[wordnum];
        /* ignore bits before prevbit */
        word &= mask;

        if (word != 0) {
            int result;

            result = wordnum * DSTORE_BITS_PER_BITMAPWORD;
            while ((word & mask255) == 0) {
                word >>= bitsPerByte;
                result += bitsPerByte;
            }
            result += rightmost_one_pos[word & mask255];
            return result;
        }

        /* in subsequent words, consider all bits */
        mask = (~(bitmapword)0);
    }
    return -2;
}

}
