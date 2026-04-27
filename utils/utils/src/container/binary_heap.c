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

#include "types/type_object.h"
#include "container/binary_heap.h"

#define LEFT_CHILD_OFFSET  1
#define RIGHT_CHILD_OFFSET 2
#define PARENT_OFFSET      1
#define BH_NODES_OFFSET    1
#define DOUBLE             2

#ifdef LOCAL_MODULE_NAME
#undef LOCAL_MODULE_NAME
#endif
#define LOCAL_MODULE_NAME "binary_heap"

static void SiftDown(BinaryHeap *heap, int nodeOff);
static void SiftUp(BinaryHeap *heap, int nodeOff);
static inline void SwapNodes(BinaryHeap *heap, int a, int b);
static bool OperationCheck(BinaryHeap *heap);

BinaryHeap *BinaryHeapAllocate(size_t capacity, BinaryHeapComparator compare, void *arg)
{
    size_t sz;
    BinaryHeap *heap = NULL;

    if (compare == NULL) {
        return NULL;
    }
    sz = offsetof(BinaryHeap, bhNodes) + sizeof(Datum) * capacity;
    heap = (BinaryHeap *)malloc(sz);
    if (heap == NULL) {
        return NULL;
    }
    heap->bhSpace = capacity;
    heap->bhCompare = compare;
    heap->bhArg = arg;

    heap->bhSize = 0;
    heap->bhHasHeapProperty = true;

    return heap;
}

void BinaryHeapReset(BinaryHeap *heap)
{
    if (heap == NULL) {
        return;
    }
    heap->bhSize = 0;
    heap->bhHasHeapProperty = true;
}

void BinaryHeapFree(BinaryHeap *heap)
{
    if (heap == NULL) {
        return;
    }
    free(heap);
}

/*
 * These utility functions return the offset of the left child, right
 * child, and parent of the node at the given index, respectively.
 *
 * The heap is represented as an array of nodes, with the root node
 * stored at index 0. The left child of node i is at index 2*i+1, and
 * the right child at 2*i+2. The parent of node i is at index (i-1)/2.
 */
static inline int GetLeftOffset(int i)
{
    return DOUBLE * i + LEFT_CHILD_OFFSET;
}

static inline int GetRightOffset(int i)
{
    return DOUBLE * i + RIGHT_CHILD_OFFSET;
}

static inline int GetParentOffset(int i)
{
    return (i - PARENT_OFFSET) / DOUBLE;
}

void BinaryHeapAddUnordered(BinaryHeap *heap, Datum d)
{
    if (heap == NULL) {
        return;
    }
    if (heap->bhSize >= (int)heap->bhSpace) {
        ErrLog(ERROR, ErrMsg("out of binary heap slots"));
        return;
    }
    heap->bhHasHeapProperty = false;
    heap->bhNodes[heap->bhSize] = d;
    heap->bhSize++;
}

void BinaryHeapBuild(BinaryHeap *heap)
{
    int i;

    if (heap == NULL) {
        return;
    }
    for (i = GetParentOffset(heap->bhSize - 1); i >= 0; i--) {
        SiftDown(heap, i);
    }
    heap->bhHasHeapProperty = true;
}

void BinaryHeapAdd(BinaryHeap *heap, Datum d)
{
    if (heap == NULL) {
        return;
    }
    if (heap->bhSize >= (int)heap->bhSpace) {
        ErrLog(ERROR, ErrMsg("out of binary heap slots"));
        return;
    }
    heap->bhNodes[heap->bhSize] = d;
    heap->bhSize++;
    SiftUp(heap, heap->bhSize - 1);
}

bool BinaryHeapEmpty(BinaryHeap *heap)
{
    ASSERT(heap != NULL);
    return heap->bhSize == 0;
}

Datum BinaryHeapFirst(BinaryHeap *heap)
{
    if (!OperationCheck(heap)) {
        return (Datum)NULL;
    }
    return heap->bhNodes[0];
}

Datum BinaryHeapRemoveFirst(BinaryHeap *heap)
{
    if (!OperationCheck(heap)) {
        return (Datum)NULL;
    }
    if (heap->bhSize == BH_NODES_OFFSET) {
        heap->bhSize--;
        return heap->bhNodes[0];
    }

    /*
     * Swap the root and last nodes, decrease the size of the heap (i.e.
     * remove the former root node) and sift the new root node down to its
     * correct position.
     */
    SwapNodes(heap, 0, heap->bhSize - 1);
    heap->bhSize--;
    SiftDown(heap, 0);

    return heap->bhNodes[heap->bhSize];
}

void BinaryHeapReplaceFirst(BinaryHeap *heap, Datum d)
{
    if (!OperationCheck(heap)) {
        return;
    }
    heap->bhNodes[0] = d;

    if (heap->bhSize > BH_NODES_OFFSET) {
        SiftDown(heap, 0);
    }
}

/*
 * the check operation before touching the Datum.
 */
static bool OperationCheck(BinaryHeap *heap)
{
    if (heap == NULL) {
        ErrLog(ERROR, ErrMsg("binary heap is NULL"));
        return false;
    }
    if (BinaryHeapEmpty(heap)) {
        ErrLog(ERROR, ErrMsg("binary heap is empty"));
        return false;
    }
    if (!heap->bhHasHeapProperty) {
        ErrLog(ERROR, ErrMsg("heap property is not set"));
        return false;
    }
    return true;
}

/*
 * Swap the contents of two nodes.
 */
static inline void SwapNodes(BinaryHeap *heap, int a, int b)
{
    Datum swap;
    ASSERT(heap != NULL);
    swap = heap->bhNodes[a];
    heap->bhNodes[a] = heap->bhNodes[b];
    heap->bhNodes[b] = swap;
}

/*
 * Sift a node up to the highest position it can hold according to the
 * comparator.
 */
static void SiftUp(BinaryHeap *heap, int nodeOff)
{
    ASSERT(heap != NULL);
    int nodeOffset = nodeOff;
    while (nodeOffset != 0) {
        int cmp;
        int parentOff;

        /*
         * If this node is smaller than its parent, the heap condition is
         * satisfied, and we're done.
         */
        parentOff = GetParentOffset(nodeOffset);
        cmp = heap->bhCompare(heap->bhNodes[nodeOffset], heap->bhNodes[parentOff], heap->bhArg);
        if (cmp <= 0) {
            break;
        }

        /*
         * Otherwise, swap the node and its parent and go on to check the
         * node's new parent.
         */
        SwapNodes(heap, nodeOffset, parentOff);
        nodeOffset = parentOff;
    }
}

/*
 * Sift a node down from its current position to satisfy the heap
 * property.
 */
static void SiftDown(BinaryHeap *heap, int nodeOff)
{
    ASSERT(heap != NULL);
    int nodeOffset = nodeOff;
    for (;;) {
        int leftOff = GetLeftOffset(nodeOffset);
        int rightOff = GetRightOffset(nodeOffset);
        int swapOff = 0;

        /* Is the left child larger than the parent? */
        if (leftOff < heap->bhSize &&
            heap->bhCompare(heap->bhNodes[nodeOffset], heap->bhNodes[leftOff], heap->bhArg) < 0) {
            swapOff = leftOff;
        }

        /* Is the right child larger than the parent? */
        if (rightOff < heap->bhSize &&
            heap->bhCompare(heap->bhNodes[nodeOffset], heap->bhNodes[rightOff], heap->bhArg) < 0) {
            /* swap with the larger child */
            if (!swapOff || heap->bhCompare(heap->bhNodes[leftOff], heap->bhNodes[rightOff], heap->bhArg) < 0) {
                swapOff = rightOff;
            }
        }

        /*
         * If we didn't find anything to swap, the heap condition is
         * satisfied, and we're done.
         */
        if (!swapOff) {
            break;
        }

        /*
         * Otherwise, swap the node with the child that violates the heap
         * property; then go on to check its children.
         */
        SwapNodes(heap, swapOff, nodeOffset);
        nodeOffset = swapOff;
    }
}