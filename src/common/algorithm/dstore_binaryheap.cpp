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
 *        src/common/algorithm/dstore_binaryheap.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "common/algorithm/dstore_binaryheap.h"
#include "common/memory/dstore_mctx.h"
#include "errorcode/dstore_common_error_code.h"
#include "common/error/dstore_error.h"

namespace DSTORE {

static void sift_down(binaryheap *heap, int node_off);
static void sift_up(binaryheap *heap, int node_off);
static inline void swap_nodes(binaryheap *heap, int a, int b);

/*
 * BinaryheapAllocate
 *
 * Returns a pointer to a newly-allocated heap that has the capacity to
 * store the given number of nodes, with the heap property defined by
 * the given comparator function, which will be invoked with the additional
 * argument specified by 'arg'.
 */
binaryheap *BinaryheapAllocate(int capacity, binaryheap_comparator compare, void *arg)
{
    int sz;
    binaryheap *heap;

    sz = offsetof(binaryheap, bh_nodes) + sizeof(Datum) * capacity;
    heap = static_cast<binaryheap *>(DstorePalloc(sz));
    if (STORAGE_VAR_NULL(heap)) {
        return nullptr;
    }
    heap->bh_space = capacity;
    heap->bh_arg = arg;
    heap->bh_compare = compare;

    heap->bh_size = 0;
    heap->bh_has_heap_property = true;

    return heap;
}

/*
 * BinaryheapReset
 *
 * Resets the heap to an empty state, losing its data content but not the
 * parameters passed at allocation.
 */
void BinaryheapReset(binaryheap *heap)
{
    heap->bh_size = 0;
    heap->bh_has_heap_property = true;
}

/*
 * BinaryheapFree
 *
 * Releases memory used by the given binaryheap.
 */
void BinaryheapFree(binaryheap *heap)
{
    DstorePfree(heap);
}

/*
 * These utility functions return the offset of the left child, right
 * child, and parent of the node at the given index, respectively.
 *
 * The heap is represented as an array of nodes, with the root node
 * stored at index 0. The left child of node i is at index 2*i+1, and
 * the right child at 2*i+2. The parent of node i is at index (i-1)/2.
 */

static inline int left_offset(int i)
{
    return 2 * i + 1;
}

static inline int right_offset(int i)
{
    return 2 * i + 2;
}

static inline int parent_offset(int i)
{
    return (i - 1) / 2;
}

/*
 * BinaryheapAddUnordered
 *
 * Adds the given datum to the end of the heap's list of nodes in O(1) without
 * preserving the heap property. This is a convenience to add elements quickly
 * to a new heap. To obtain a valid heap, one must call BinaryheapBuild()
 * afterwards.
 */
void BinaryheapAddUnordered(binaryheap *heap, Datum d)
{
    if (heap->bh_size >= heap->bh_space) {
        storage_set_error(BINARYHEAP_ERROR_OUT_OF_BINARY_HEAP_SLOTS);
        StorageAssert(0);
    }
    heap->bh_has_heap_property = false;
    heap->bh_nodes[heap->bh_size] = d;
    heap->bh_size++;
}

/*
 * BinaryheapBuild
 *
 * Assembles a valid heap in O(n) from the nodes added by
 * BinaryheapAddUnordered(). Not needed otherwise.
 */
void BinaryheapBuild(binaryheap *heap)
{
    int i;

    for (i = parent_offset(heap->bh_size - 1); i >= 0; i--) {
        sift_down(heap, i);
    }
    heap->bh_has_heap_property = true;
}

/*
 * BinaryheapAdd
 *
 * Adds the given datum to the heap in O(log n) time, while preserving
 * the heap property.
 */
void BinaryheapAdd(binaryheap *heap, Datum d)
{
    if (heap->bh_size >= heap->bh_space) {
        storage_set_error(BINARYHEAP_ERROR_OUT_OF_BINARY_HEAP_SLOTS);
        StorageAssert(0);
    }
    heap->bh_nodes[heap->bh_size] = d;
    heap->bh_size++;
    sift_up(heap, heap->bh_size - 1);
}

/*
 * BinaryheapFirst
 *
 * Returns a pointer to the first (root, topmost) node in the heap
 * without modifying the heap. The caller must ensure that this
 * routine is not used on an empty heap. Always O(1).
 */
Datum BinaryheapFirst(binaryheap *heap)
{
    StorageAssert(!binaryheap_empty(heap) && heap->bh_has_heap_property);
    return heap->bh_nodes[0];
}

/*
 * BinaryheapRemoveFirst
 *
 * Removes the first (root, topmost) node in the heap and returns a
 * pointer to it after rebalancing the heap. The caller must ensure
 * that this routine is not used on an empty heap. O(log n) worst
 * case.
 */
void BinaryheapRemoveFirst(binaryheap *heap)
{
    StorageAssert(!binaryheap_empty(heap) && heap->bh_has_heap_property);

    if (heap->bh_size == 1) {
        heap->bh_size--;
        return;
    }

    /*
     * Swap the root and last nodes, decrease the size of the heap (i.e.
     * remove the former root node) and sift the new root node down to 
     * its correct position.
     */
    swap_nodes(heap, 0, heap->bh_size - 1);
    heap->bh_size--;
    sift_down(heap, 0);
}

/*
 * BinaryheapReplaceFirst
 *
 * Replace the topmost element of a non-empty heap, preserving the heap
 * property.  O(1) in the best case, or O(log n) if it must fall back to
 * sifting the new node down.
 */
void BinaryheapReplaceFirst(binaryheap *heap, Datum d)
{
    StorageAssert(!binaryheap_empty(heap) && heap->bh_has_heap_property);

    heap->bh_nodes[0] = d;

    if (heap->bh_size > 1) {
        sift_down(heap, 0);
    }
}

/*
 * Swap the contents of two nodes.
 */
static inline void swap_nodes(binaryheap *heap, int a, int b)
{
    Datum swap = heap->bh_nodes[a];
    heap->bh_nodes[a] = heap->bh_nodes[b];
    heap->bh_nodes[b] = swap;
}

/*
 * Sift a node up to the highest position it can hold according to the comparator.
 */
static void sift_up(binaryheap *heap, int node_off)
{
    while (node_off != 0) {
        int cmp;
        int parent_off;

        /*
         * If this node is smaller than its parent, the heap condition is
         * satisfied and we're done.
         */
        parent_off = parent_offset(node_off);
        cmp = heap->bh_compare(heap->bh_nodes[node_off], heap->bh_nodes[parent_off], heap->bh_arg);
        if (cmp <= 0) {
            break;
        }

        /*
         * Otherwise, swap the node and its parent, and go on to check 
         * the node's new parent.
         */
        swap_nodes(heap, node_off, parent_off);
        node_off = parent_off;
    }
}

/*
 * Sift a node down from its current position to satisfy the heap
 * property.
 */
static void sift_down(binaryheap *heap, int node_off)
{
    while (true) {
        int swap_off = 0;
        int left_off = left_offset(node_off);
        int right_off = right_offset(node_off);

        /* Is the left child larger than the parent? */
        if (left_off < heap->bh_size &&
            heap->bh_compare(heap->bh_nodes[node_off], heap->bh_nodes[left_off], heap->bh_arg) < 0) {
            swap_off = left_off;
        }
        /* Is the right child larger than the parent? */
        if (right_off < heap->bh_size &&
            heap->bh_compare(heap->bh_nodes[node_off], heap->bh_nodes[right_off], heap->bh_arg) < 0) {
            /* swap with the larger child */
            if (!swap_off || heap->bh_compare(heap->bh_nodes[left_off], heap->bh_nodes[right_off], heap->bh_arg) < 0) {
                swap_off = right_off;
            }
        }

        /*
         * If we didn't find anything to swap, the heap condition is
         * satisfied and we're done.
         */
        if (!swap_off) {
            break;
        }

        /*
         * Otherwise, swap the node with the child that violates the heap
         * property; and then go on to check its children.
         */
        swap_nodes(heap, swap_off, node_off);
        node_off = swap_off;
    }
}
}  // namespace DSTORE
