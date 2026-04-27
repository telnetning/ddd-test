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
 * IDENTIFICATION
 *        src/include/lib/binaryheap.h
 *
 * binaryheap.h
 *
 * A simple binary heap implementation
 */

#ifndef DSTORE_DSTORE_BINARYHEAP_H
#define DSTORE_DSTORE_BINARYHEAP_H

#include "common/dstore_datatype.h"

namespace DSTORE {

/*
 * For a max-heap, the comparator must return <0 iff a < b, 0 iff a == b,
 * and >0 iff a > b.  For a min-heap, the conditions are reversed.
 */
typedef int (*binaryheap_comparator) (Datum a, Datum b, void *arg);

typedef struct binaryheap {
    int bh_size;                        /* how many nodes are currently in "nodes" */
    int bh_space;                       /* how many nodes can be stored in "nodes" */
    bool bh_has_heap_property;          /* no unordered operations since last heap build, debugging cross-check */
    binaryheap_comparator bh_compare;   /* comparison function to define the heap property */
    void *bh_arg;                                 /* user data for comparison function */
    Datum bh_nodes[DSTORE_FLEXIBLE_ARRAY_MEMBER]; /* variable-length array of "space" nodes */
} binaryheap;

extern binaryheap *BinaryheapAllocate(int capacity, binaryheap_comparator compare, void *arg);
extern void BinaryheapReset(binaryheap *heap);
extern void BinaryheapFree(binaryheap *heap);
extern void BinaryheapAddUnordered(binaryheap *heap, Datum d);
extern void BinaryheapBuild(binaryheap *heap);
extern void BinaryheapAdd(binaryheap *heap, Datum d);
extern Datum BinaryheapFirst(binaryheap *heap);
extern void BinaryheapRemoveFirst(binaryheap *heap);
extern void BinaryheapReplaceFirst(binaryheap *heap, Datum d);

#define binaryheap_empty(h) ((h)->bh_size == 0)
}

#endif // DSTORE_STORAGE_BINARYHEAP_H
