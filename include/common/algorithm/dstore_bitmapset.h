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
 *        include/common/algorithm/dstore_bitmapset.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_BITMAPSET_H
#define DSTORE_BITMAPSET_H

#include "common/dstore_datatype.h"

namespace DSTORE {

/* result of BmsSubsetCompare */
enum BMS_Comparison : uint8 {
    BMS_EQUAL,    /* sets are equal */
    BMS_SUBSET1,  /* first set is a subset of the second */
    BMS_SUBSET2,  /* second set is a subset of the first */
    BMS_DIFFERENT /* neither set is a subset of the other */
};

/* result of BmsMembership */
enum BMS_Membership : uint8 {
    BMS_EMPTY_SET, /* 0 members */
    BMS_SINGLETON, /* 1 member */
    BMS_MULTIPLE   /* >1 member */
};

/*
 * function prototypes in nodes/bitmapset.c
 */
extern Bitmapset* BmsCopy(const Bitmapset* a);
extern bool BmsEqual(const Bitmapset* a, const Bitmapset* b);
extern Bitmapset* BmsMakeSingleton(int x);
extern void BmsFree(Bitmapset* a);

#define bms_free_ext(bms)    \
    do {                     \
        if ((bms) != NULL) { \
            BmsFree(bms);   \
            bms = NULL;      \
        }                    \
    } while (0)

extern Bitmapset* BmsUnion(const Bitmapset* a, const Bitmapset* b);
extern Bitmapset* BmsIntersect(const Bitmapset* a, const Bitmapset* b);
extern Bitmapset* BmsDifference(const Bitmapset* a, const Bitmapset* b);
extern bool BmsIsSubset(const Bitmapset* a, const Bitmapset* b);
extern BMS_Comparison BmsSubsetCompare(const Bitmapset* aSet, const Bitmapset* bSet);
extern bool BmsIsMember(int x, const Bitmapset* a);
extern bool BmsOverlap(const Bitmapset* a, const Bitmapset* b);
extern bool BmsNonemptyDifference(const Bitmapset* aSet, const Bitmapset* bSet);
extern int BmsSingletonMember(const Bitmapset* a);
extern int BmsNumMembers(const Bitmapset* aSet);

/* optimized tests when we don't need to know exact membership count: */
extern BMS_Membership BmsMembership(const Bitmapset* aSet);
extern bool BmsIsEmpty(const Bitmapset* a);

/* these routines recycle (modify or free) their non-const inputs: */
extern Bitmapset* BmsAddMember(Bitmapset* a, int x);
extern Bitmapset* BmsDelMember(Bitmapset* a, int x);
extern Bitmapset* BmsAddMembers(Bitmapset* a, const Bitmapset* b);
extern Bitmapset* BmsIntMembers(Bitmapset* a, const Bitmapset* b);
extern Bitmapset* BmsDelMembers(Bitmapset* a, const Bitmapset* b);
extern Bitmapset* BmsJoin(Bitmapset* a, Bitmapset* b);

/* support for iterating through the integer elements of a set: */
extern int BmsFirstMember(Bitmapset* a);
extern int BmsNextMember(const Bitmapset* a, int prevbit);

/* support for hashtables using Bitmapsets as keys: */
extern uint32 BmsHashValue(const Bitmapset* a);
}

#endif // STORAGE_BITMAPSET_H