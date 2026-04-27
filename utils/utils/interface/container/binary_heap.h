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
 * Description:
 * 1. A simple binary heap implementation.
 *
 * ---------------------------------------------------------------------------------
 */

#ifndef UTILS_BINARY_HEAP_H
#define UTILS_BINARY_HEAP_H

#include "types/data_types.h"
#include "memory/memory_ctx.h"
#include "syslog/err_log.h"

GSDB_BEGIN_C_CODE_DECLS

/*
 * For a max-heap, the comparator must return <0 iff a < b, 0 iff a == b,
 * and >0 iff a > b.  For a min-heap, the conditions are reversed.
 */
typedef int (*BinaryHeapComparator)(Datum a, Datum b, void *arg);

/*
 * BinaryHeap
 *
 *		bhSize			how many nodes are currently in "nodes"
 *		bhSpace		how many nodes can be stored in "nodes"
 *		bhHasHeapProperty	no unordered operations since last heap build
 *		bhCompare		comparison function to define the heap property
 *		bhArg			user data for comparison function
 *		bhNodes		variable-length array of "space" nodes
 */
typedef struct BinaryHeap {
    int bhSize;
    size_t bhSpace;
    bool bhHasHeapProperty; /* debugging cross-check */
    BinaryHeapComparator bhCompare;
    void *bhArg;
    Datum bhNodes[FLEXIBLE_ARRAY_MEMBER];
} BinaryHeap;

/*
 * BinaryHeapAllocate
 *
 * Returns a pointer to a newly-allocated heap that has the capacity to
 * store the given number of nodes, with the heap property defined by
 * the given comparator function, which will be invoked with the additional
 * argument specified by 'arg'.
 */
BinaryHeap *BinaryHeapAllocate(size_t capacity, BinaryHeapComparator compare, void *arg);

/*
 * BinaryHeapReset
 *
 * Resets the heap to an empty state, losing its data content but not the
 * parameters passed at allocation.
 */
void BinaryHeapReset(BinaryHeap *heap);

/*
 * BinaryHeapFree
 *
 * Releases memory used by the given BinaryHeap.
 */
void BinaryHeapFree(BinaryHeap *heap);

/*
 * BinaryHeapAddUnordered
 *
 * Adds the given datum to the end of the heap's list of nodes in O(1) without
 * preserving the heap property. This is a convenience to add elements quickly
 * to a new heap. To obtain a valid heap, one must call BinaryHeapBuild()
 * afterwards.
 */
void BinaryHeapAddUnordered(BinaryHeap *heap, Datum d);

/*
 * BinaryHeapBuild
 *
 * Assembles a valid heap in O(n) from the nodes added by
 * BinaryHeapAddUnordered(). Not needed otherwise.
 */
void BinaryHeapBuild(BinaryHeap *heap);

/*
 * BinaryHeapAdd
 *
 * Adds the given datum to the heap in O(log n) time, while preserving
 * the heap property.
 */
void BinaryHeapAdd(BinaryHeap *heap, Datum d);

/*
 * BinaryHeapFirst
 *
 * Returns a pointer to the first (root, topmost) node in the heap
 * without modifying the heap. The caller must ensure that this
 * routine is not used on an empty heap. Always O(1).
 */
Datum BinaryHeapFirst(BinaryHeap *heap);

/*
 * BinaryHeapRemoveFirst
 *
 * Removes the first (root, topmost) node in the heap and returns a
 * pointer to it after rebalancing the heap. The caller must ensure
 * that this routine is not used on an empty heap. O(log n) worst
 * case.
 */
Datum BinaryHeapRemoveFirst(BinaryHeap *heap);

/*
 * BinaryHeapReplaceFirst
 *
 * Replace the topmost element of a non-empty heap, preserving the heap
 * property.  O(1) in the best case, or O(log n) if it must fall back to
 * sifting the new node down.
 */
void BinaryHeapReplaceFirst(BinaryHeap *heap, Datum d);

bool BinaryHeapEmpty(BinaryHeap *heap);

GSDB_END_C_CODE_DECLS

#endif /* UTILS_BINARY_HEAP_H */
