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
 *        src/common/algorithm/dstore_sorttemplate.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "common/algorithm/dstore_sorttemplate.h"

namespace DSTORE {

constexpr int ST_POINTER_STEP = 1;
int SortTupleCompare(SortTuple *a, SortTuple *b, TuplesortMgr *tsMgr)
{
    return tsMgr->CompareSortTuple(a, b);
}

SortTuple *GetMedianOfThree(SortTuple *a, SortTuple *b, SortTuple *c, TuplesortMgr *tsMgr)
{
    return SortTupleCompare(a, b, tsMgr) < 0 ?
        (SortTupleCompare(b, c, tsMgr) < 0 ? b : (SortTupleCompare(a, c, tsMgr) < 0 ? c : a))
        : (SortTupleCompare(b, c, tsMgr) > 0 ? b : (SortTupleCompare(a, c, tsMgr) < 0 ? a : c));
}

inline void SwapSortTuple(SortTuple *a, SortTuple *b)
{
    SortTuple tmp = *a;
    *a = *b;
    *b = tmp;
}

inline void SwapSortTupleArray(SortTuple *a, SortTuple *b, size_t n)
{
    for (size_t i = 0; i < n; ++i) {
        SwapSortTuple(&a[i], &b[i]);
    }
}

bool IsPreSorted(SortTuple *data, size_t n, TuplesortMgr *tsMgr)
{
    int presorted = 1;
    for (SortTuple *pm = data + ST_POINTER_STEP; pm < data + n * ST_POINTER_STEP; pm += ST_POINTER_STEP) {
        if (SortTupleCompare(pm - ST_POINTER_STEP, pm, tsMgr) > 0) {
            presorted = 0;
            break;
        }
    }
    if (presorted) {
        return true;
    }
    return false;
}

SortTuple* GetPivot(SortTuple *data,  size_t n, uint32 threshold1, TuplesortMgr *tsMgr)
{
    const uint32 threshold2 = 40;
    const uint32 half = 2;
    SortTuple *pivot = data + (n / half) * ST_POINTER_STEP;
    if (n > threshold1) {
        SortTuple *pl = data;
        SortTuple *pn = data + (n - 1) * ST_POINTER_STEP;
        if (n > threshold2) {
            size_t d = (n / 8) * ST_POINTER_STEP;
            constexpr size_t two = 2;
            pl = GetMedianOfThree(pl, pl + d, pl + two * d, tsMgr);
            pivot = GetMedianOfThree(pivot - d, pivot, pivot + d, tsMgr);
            pn = GetMedianOfThree(pn - two * d, pn - d, pn, tsMgr);
        }
        pivot = GetMedianOfThree(pl, pivot, pn, tsMgr);
    }
    return pivot;
}

void FindRightTuple(SortTuple *data, SortTuple **pa, SortTuple **pb, SortTuple **pc, TuplesortMgr *tsMgr)
{
    int r = 0;
    while (*pb <= *pc) {
        r = SortTupleCompare(*pb, data, tsMgr);
        if (r > 0) {
            break;
        }
        if (r == 0) {
            SwapSortTuple(*pa, *pb);
            *pa += ST_POINTER_STEP;
        }
        *pb += ST_POINTER_STEP;
    }
}

void FindLeftTuple(SortTuple *data, SortTuple **pb, SortTuple **pc, SortTuple **pd, TuplesortMgr *tsMgr)
{
    int r = 0;
    while (*pb <= *pc) {
        r = SortTupleCompare(*pc, data, tsMgr);
        if (r < 0) {
            break;
        }
        if (r == 0) {
            SwapSortTuple(*pc, *pd);
            *pd -= ST_POINTER_STEP;
        }
        *pc -= ST_POINTER_STEP;
    }
}

void DoQuickSortTuples(SortTuple *data, size_t n, TuplesortMgr *tsMgr)
{
    SortTuple *pa = nullptr;
    SortTuple *pb = nullptr;
    SortTuple *pc = nullptr;
    SortTuple *pd = nullptr;
    SortTuple *pl = nullptr;
    SortTuple *pm = nullptr;

    const uint32 threshold1 = 7;

loop:
    if (n < threshold1) {
        for (pm = data + ST_POINTER_STEP; pm < data + n * ST_POINTER_STEP; pm += ST_POINTER_STEP) {
            for (pl = pm; pl > data && SortTupleCompare(pl - ST_POINTER_STEP, pl, tsMgr) > 0; pl -= ST_POINTER_STEP) {
                SwapSortTuple(pl, pl - ST_POINTER_STEP);
            }
        }
        return;
    }
    if (IsPreSorted(data, n, tsMgr)) {
        return;
    }
    pm = GetPivot(data, n, threshold1, tsMgr);
    SwapSortTuple(data, pm);
    pa = pb = data + ST_POINTER_STEP;
    pc = pd = data + (n - 1) * ST_POINTER_STEP;
    for (;;) {
        FindRightTuple(data, &pa, &pb, &pc, tsMgr);
        FindLeftTuple(data, &pb, &pc, &pd, tsMgr);
        if (pb > pc) {
            break;
        }
        SwapSortTuple(pb, pc);
        pb += ST_POINTER_STEP;
        pc -= ST_POINTER_STEP;
    }
    SortTuple *pn = data + n * ST_POINTER_STEP;
    size_t d1 = static_cast<size_t>(DstoreMin(pa - data, pb - pa));
    SwapSortTupleArray(data, pb - d1, d1);
    d1 = static_cast<size_t>(DstoreMin(pd - pc, (pn - pd) - ST_POINTER_STEP));
    SwapSortTupleArray(pb, pn - d1, d1);
    d1 = static_cast<size_t>(pb - pa);
    size_t d2 = static_cast<size_t>(pd - pc);
    if (d1 <= d2) {
        /* Recurse on left partition, then iterate on right partition */
        if (d1 > ST_POINTER_STEP) {
            DoQuickSortTuples(data, d1 / ST_POINTER_STEP, tsMgr);
        }
        if (d2 > ST_POINTER_STEP) {
            /* Iterate rather than recurse to save stack space */
            data = pn - d2;
            n = d2 / ST_POINTER_STEP;
            goto loop;
        }
    } else {
        /* Recurse on right partition, then iterate on left partition */
        if (d2 > ST_POINTER_STEP) {
            DoQuickSortTuples(pn - d2, d2 / ST_POINTER_STEP, tsMgr);
        }
        if (d1 > ST_POINTER_STEP) {
            /* Iterate rather than recurse to save stack space */
            n = d1 / ST_POINTER_STEP;
            goto loop;
        }
    }
}
#undef ST_POINTER_STEP
}
