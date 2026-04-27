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
 * 1. test binary heap
 *
 * ---------------------------------------------------------------------------------
 */
#include "gtest/gtest.h"
#include "stdio.h"
#include "stdlib.h"
#include "time.h"
#include "container/binary_heap.h"
#include "memory/memory_ctx.h"

#define NUM_SPACE 15
#define OVER_LENGTH 16
#define LENGTH 10
#define MAX_NUM 1000001

static int node_value_comparator(Datum a, Datum b, void *arg)
{
    if (a < b)
        return -1;
    else if (a == b)
        return 0;
    else
        return 1;
}

bool TestBinaryHeapCreate(BinaryHeap *heap, BinaryHeapComparator compare)
{
    heap = BinaryHeapAllocate(NUM_SPACE, compare, NULL);
    if (heap == NULL) {
        return false;
    }

    return true;
}

bool TestBinaryHeapAdd(BinaryHeap *heap, Datum d)
{
    int pre_size = heap->bhSize;
    BinaryHeapAdd(heap, d);
    if (heap->bhSize != pre_size + 1) {
        return false;
    }

    return true;
}

bool TestBinaryHeapAddUnordered(BinaryHeap *heap, Datum d)
{
    int pre_size = heap->bhSize;
    BinaryHeapAddUnordered(heap, d);
    if (heap->bhSize != pre_size + 1) {
        return false;
    }

    return true;
}

bool TestBinaryHeapPop(BinaryHeap *heap)
{
    int pre_size = heap->bhSize;
    Datum pre_val = heap->bhNodes[1];
    BinaryHeapRemoveFirst(heap);
    Datum cur_val = heap->bhNodes[1];
    int cur_size = heap->bhSize;
    if (cur_size != pre_size - 1 || pre_val == cur_val) {
        return false;
    }

    return true;
}

bool TestBinaryHeapDestory(BinaryHeap *heap)
{
    if (heap->bhSize > 1) {
        BinaryHeapReset(heap);
    }
    BinaryHeapFree(heap);

    return true;
}

class BinaryHeapTest : public testing::Test {
public:
    void SetUp() override {

    };

    void TearDown() override {

    };
};


TEST_F(BinaryHeapTest, BinaryHeapCreateTest)
{
    BinaryHeap *heap = BinaryHeapAllocate(NUM_SPACE, node_value_comparator, NULL);
    ASSERT_NE(heap, (BinaryHeap *)NULL);

    /* destroy binary heap */
    bool ret = TestBinaryHeapDestory(heap);
    ASSERT_EQ(ret, true);
    heap = NULL;
}

TEST_F(BinaryHeapTest, BinaryHeapAddTest)
{
    BinaryHeap *heap = BinaryHeapAllocate(NUM_SPACE, node_value_comparator, NULL);
    ASSERT_NE(heap, (BinaryHeap *)NULL);
    
    int nodes[LENGTH] = {7, 5, 34, 13, 9, 2, 6, 16, 22, 15};
    /* insert node in heap */
    for (int i = 0; i < LENGTH; i++) {
        bool ret = TestBinaryHeapAdd(heap, nodes[i]);
        ASSERT_EQ(ret, true);
    }

    /* destroy heap */
    bool ret = TestBinaryHeapDestory(heap);
    ASSERT_EQ(ret, true);
    heap = NULL;
}

TEST_F(BinaryHeapTest, BinaryHeapDelTest)
{
    BinaryHeap *heap = BinaryHeapAllocate(NUM_SPACE, node_value_comparator, NULL);
    ASSERT_NE(heap, (BinaryHeap *)NULL);
    
    int nodes[LENGTH] = {7, 5, 34, 13, 9, 2, 6, 16, 22, 15};
    /* insert node in heap */
    for (int i = 0; i < LENGTH; i++) {
        bool ret = TestBinaryHeapAdd(heap, nodes[i]);
        ASSERT_EQ(ret, true);
    }

    bool ret = TestBinaryHeapPop(heap);
    ASSERT_EQ(ret, true);

    /* destroy heap */
    ret = TestBinaryHeapDestory(heap);
    ASSERT_EQ(ret, true);
    heap = NULL;
}

TEST_F(BinaryHeapTest, BinaryHeapDestoryTest)
{
    BinaryHeap *heap = BinaryHeapAllocate(NUM_SPACE, node_value_comparator, NULL);
    ASSERT_NE(heap, (BinaryHeap *)NULL);
    
    bool ret = TestBinaryHeapDestory(heap);
    ASSERT_EQ(ret, true);
    heap = NULL;
}

TEST_F(BinaryHeapTest, BinaryHeapResetTest)
{
    BinaryHeap *heap = BinaryHeapAllocate(NUM_SPACE, node_value_comparator, NULL);
    ASSERT_NE(heap, (BinaryHeap *)NULL);
    
    int nodes[LENGTH] = {7, 5, 34, 13, 9, 2, 6, 16, 22, 15};
    /* insert node in heap */
    for (int i = 0; i < LENGTH; i++) {
        bool ret = TestBinaryHeapAdd(heap, nodes[i]);
        ASSERT_EQ(ret, true);
    }

    BinaryHeapReset(heap);
    ASSERT_EQ(heap->bhSize, 0);
    ASSERT_EQ(heap->bhHasHeapProperty, true);

    bool ret = TestBinaryHeapDestory(heap);
    ASSERT_EQ(ret, true);
    heap = NULL;
}

TEST_F(BinaryHeapTest, BinaryHeapReplaceTest)
{
    BinaryHeap *heap = BinaryHeapAllocate(NUM_SPACE, node_value_comparator, NULL);
    ASSERT_NE(heap, (BinaryHeap *)NULL);
    
    int nodes[LENGTH] = {7, 5, 34, 13, 9, 2, 6, 16, 22, 15};
    /* insert node in heap */
    for (int i = 0; i < LENGTH; i++) {
        bool ret = TestBinaryHeapAdd(heap, nodes[i]);
        ASSERT_EQ(ret, true);
    }

    Datum pre_val = BinaryHeapFirst(heap);
    int pre_size = heap->bhSize;
    /* replace first node in heap */
    BinaryHeapReplaceFirst(heap, 11);
    Datum cur_val = BinaryHeapFirst(heap);
    ASSERT_EQ(pre_size, heap->bhSize);
    for (int i = 0; i < heap->bhSize; i++) {
        ASSERT_NE(heap->bhNodes[i], pre_val);
    }
    /* destroy heap */
    bool ret = TestBinaryHeapDestory(heap);
    ASSERT_EQ(ret, true);
    heap = NULL;
}

TEST_F(BinaryHeapTest, BinaryHeapAddUnorderedTest)
{
    BinaryHeap *heap = BinaryHeapAllocate(NUM_SPACE, node_value_comparator, NULL);
    ASSERT_NE(heap, (BinaryHeap *)NULL);
    
    int nodes[LENGTH] = {7, 5, 34, 13, 9, 2, 6, 16, 22, 15};
    /* insert node in heap */
    for (int i = 0; i < LENGTH; i++) {
        bool ret = TestBinaryHeapAddUnordered(heap, nodes[i]);
        ASSERT_EQ(ret, true);
    }

    for (int i = 0; i < LENGTH; i++) {
        ASSERT_EQ(heap->bhNodes[i], nodes[i]);
    }

    /* destroy heap */
    bool ret = TestBinaryHeapDestory(heap);
    ASSERT_EQ(ret, true);
    heap = NULL;
}

TEST_F(BinaryHeapTest, BinaryHeapBuildTest)
{
    BinaryHeap *heap = BinaryHeapAllocate(NUM_SPACE, node_value_comparator, NULL);
    BinaryHeap *heap1 = BinaryHeapAllocate(NUM_SPACE, node_value_comparator, NULL);
    ASSERT_NE(heap, (BinaryHeap *)NULL);
    
    int nodes[LENGTH] = {7, 5, 34, 13, 9, 2, 6, 16, 22, 15};
    /* insert node in heap */
    for (int i = 0; i < LENGTH; i++) {
        bool ret = TestBinaryHeapAddUnordered(heap, nodes[i]);
        ASSERT_EQ(ret, true);
    }

    /* insert node in heap1 */
    for (int i = 0; i < LENGTH; i++) {
        bool ret = TestBinaryHeapAdd(heap1, nodes[i]);
        ASSERT_EQ(ret, true);
    }

    /* Order the heap */
    BinaryHeapBuild(heap);

    for (int i = 0; i < LENGTH; i++) {
        ASSERT_EQ(heap->bhNodes[i], heap1->bhNodes[i]);
    }

    /* destroy heap */
    bool ret = TestBinaryHeapDestory(heap);
    ASSERT_EQ(ret, true);
    ret = TestBinaryHeapDestory(heap1);
    ASSERT_EQ(ret, true);
    heap = NULL;
    heap1 = NULL;
}

TEST_F(BinaryHeapTest, BinaryHeapAddExceedTest)
{
    BinaryHeap *heap = BinaryHeapAllocate(NUM_SPACE, node_value_comparator, NULL);
    ASSERT_NE(heap, (BinaryHeap *)NULL);
    
    int nodes[OVER_LENGTH] = {7, 5, 34, 13, 9, 2, 6, 16, 22, 15, 45, 4, 67, 52, 23, 41};
    /* insert node in heap */
    for (int i = 0; i < OVER_LENGTH; i++) {
        bool ret = TestBinaryHeapAdd(heap, nodes[i]);
        if (i > (heap->bhSpace - 1)) {
            ASSERT_EQ(ret, false);
        } else {
            ASSERT_EQ(ret, true);
        }
    }

    /* destroy heap */
    bool ret = TestBinaryHeapDestory(heap);
    ASSERT_EQ(ret, true);
    heap = NULL;
}

TEST_F(BinaryHeapTest, BinaryHeapIsOrderedTest)
{
    BinaryHeap *heap = BinaryHeapAllocate(MAX_NUM, node_value_comparator, NULL);
    ASSERT_NE(heap, (BinaryHeap *)NULL);
    
    /* insert node in heap in a rand order*/
    srand((unsigned)time(NULL));
    for (int i = 0; i < MAX_NUM - 1; i++) {
        int randVal = rand();
        bool ret = TestBinaryHeapAdd(heap, randVal);
        ASSERT_EQ(ret, true);
    }
    
    /* test if node is ordered */
    for (int i = 0; i < MAX_NUM - 1; i++) {
        int max;
        if (i != 0) {
             ASSERT_GE(max, BinaryHeapFirst(heap));
        }
        max = BinaryHeapRemoveFirst(heap);
    }

    /* destroy heap */
    bool ret = TestBinaryHeapDestory(heap);
    ASSERT_EQ(ret, true);
    heap = NULL;
}