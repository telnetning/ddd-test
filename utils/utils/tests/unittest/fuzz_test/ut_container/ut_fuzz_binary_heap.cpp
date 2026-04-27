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
#include "ut_fuzz_common.h"
#include "secodeFuzz.h"

static int node_value_comparator(Datum a, Datum b, void *arg)
{
    if (a < b) {
        return -1;
    } else if (a == b) {
        return 0;
    } else {
        return 1;
    }
}

class BinaryHeapFuzzTest : public testing::Test {
public:
    void SetUp() override{};

    void TearDown() override{};
    BinaryHeap *binaryHeap;
};

static void InitBinaryHeap(int *i_num, BinaryHeap** binaryHeap)
{
    unsigned long binaryHeapAllocatePara0_0 = *(u32 *)DT_SetGetU32(&g_Element[*i_num], 0x123456);
    (*i_num)++;
    *binaryHeap = BinaryHeapAllocate(binaryHeapAllocatePara0_0, node_value_comparator, NULL);
}

TEST_F(BinaryHeapFuzzTest, testForBinaryHeapAllocate)
{
    int seed = 0, count = COUNT_NUM;
    DT_Enable_Support_Loop(1);
    DT_Set_Is_Dump_Coverage(1);
    DT_FUZZ_START(seed, count, "testForBinaryHeapAllocate", 0)
    {
        int i_num = 0;
        InitBinaryHeap(&i_num, &binaryHeap);
        BinaryHeapFree(binaryHeap);
    }
    DT_FUZZ_END()
}

TEST_F(BinaryHeapFuzzTest, testForBinaryHeapReset)
{
    int seed = 0, count = COUNT_NUM;
    DT_Enable_Support_Loop(1);
    DT_Set_Is_Dump_Coverage(1);
    DT_FUZZ_START(seed, count, "testForBinaryHeapReset", 0)
    {
        int i_num = 0;
        InitBinaryHeap(&i_num, &binaryHeap);
        BinaryHeapReset(binaryHeap);
        BinaryHeapFree(binaryHeap);
    }
    DT_FUZZ_END()
}

TEST_F(BinaryHeapFuzzTest, testForBinaryHeapFree)
{
    int seed = 0, count = COUNT_NUM;
    DT_Enable_Support_Loop(1);
    DT_Set_Is_Dump_Coverage(1);
    DT_FUZZ_START(seed, count, "testForBinaryHeapFree", 0)
    {
        int i_num = 0;
        InitBinaryHeap(&i_num, &binaryHeap);
        BinaryHeapFree(binaryHeap);
    }
    DT_FUZZ_END()
}

TEST_F(BinaryHeapFuzzTest, testForBinaryHeapAddUnordered)
{
    int seed = 0, count = COUNT_NUM;
    DT_Enable_Support_Loop(1);
    DT_Set_Is_Dump_Coverage(1);
    DT_FUZZ_START(seed, count, "testForBinaryHeapAddUnordered", 0)
    {
        int i_num = 0;
        InitBinaryHeap(&i_num, &binaryHeap);
        unsigned long binaryHeapAddUnorderedPara1_0 = *(u32 *)DT_SetGetU32(&g_Element[i_num++], 0x123456);
        BinaryHeapAddUnordered(binaryHeap, binaryHeapAddUnorderedPara1_0);
        BinaryHeapFree(binaryHeap);
    }
    DT_FUZZ_END()
}

TEST_F(BinaryHeapFuzzTest, testForBinaryHeapBuild)
{
    int seed = 0, count = COUNT_NUM;
    DT_Enable_Support_Loop(1);
    DT_Set_Is_Dump_Coverage(1);
    DT_FUZZ_START(seed, count, "testForBinaryHeapBuild", 0)
    {
        int i_num = 0;
        InitBinaryHeap(&i_num, &binaryHeap);
        BinaryHeapBuild(binaryHeap);
        BinaryHeapFree(binaryHeap);
    }
    DT_FUZZ_END()
}

TEST_F(BinaryHeapFuzzTest, testForBinaryHeapAdd)
{
    int seed = 0, count = COUNT_NUM;
    DT_Enable_Support_Loop(1);
    DT_Set_Is_Dump_Coverage(1);
    DT_FUZZ_START(seed, count, "testForBinaryHeapAdd", 0)
    {
        int i_num = 0;
        InitBinaryHeap(&i_num, &binaryHeap);
        unsigned long binaryHeapAddPara1_0 = *(u32 *)DT_SetGetU32(&g_Element[i_num++], 0x123456);
        BinaryHeapAdd(binaryHeap, binaryHeapAddPara1_0);
        BinaryHeapFree(binaryHeap);
    }
    DT_FUZZ_END()
}

TEST_F(BinaryHeapFuzzTest, testForBinaryHeapFirst)
{
    int seed = 0, count = COUNT_NUM;
    DT_Enable_Support_Loop(1);
    DT_Set_Is_Dump_Coverage(1);
    DT_FUZZ_START(seed, count, "testForBinaryHeapFirst", 0)
    {
        int i_num = 0;
        InitBinaryHeap(&i_num, &binaryHeap);
        BinaryHeapFirst(binaryHeap);
        BinaryHeapFree(binaryHeap);
    }
    DT_FUZZ_END()
}

TEST_F(BinaryHeapFuzzTest, testForBinaryHeapRemoveFirst)
{
    int seed = 0, count = COUNT_NUM;
    DT_Enable_Support_Loop(1);
    DT_Set_Is_Dump_Coverage(1);
    DT_FUZZ_START(seed, count, "testForBinaryHeapRemoveFirst", 0)
    {
        int i_num = 0;
        InitBinaryHeap(&i_num, &binaryHeap);
        BinaryHeapRemoveFirst(binaryHeap);
        BinaryHeapFree(binaryHeap);
    }
    DT_FUZZ_END()
}

TEST_F(BinaryHeapFuzzTest, testForBinaryHeapReplaceFirst)
{
    int seed = 0, count = COUNT_NUM;
    DT_Enable_Support_Loop(1);
    DT_Set_Is_Dump_Coverage(1);
    DT_FUZZ_START(seed, count, "testForBinaryHeapReplaceFirst", 0)
    {
        int i_num = 0;
        InitBinaryHeap(&i_num, &binaryHeap);
        unsigned long binaryHeapReplaceFirstPara1_0 = *(u32 *)DT_SetGetU32(&g_Element[i_num++], 0x123456);
        BinaryHeapReplaceFirst(binaryHeap, binaryHeapReplaceFirstPara1_0);
        BinaryHeapFree(binaryHeap);
    }
    DT_FUZZ_END()
}

TEST_F(BinaryHeapFuzzTest, testForBinaryHeapEmpty)
{
    int seed = 0, count = COUNT_NUM;
    DT_Enable_Support_Loop(1);
    DT_Set_Is_Dump_Coverage(1);
    DT_FUZZ_START(seed, count, "testForBinaryHeapEmpty", 0)
    {
        int i_num = 0;
        InitBinaryHeap(&i_num, &binaryHeap);
        BinaryHeapEmpty(binaryHeap);
        BinaryHeapFree(binaryHeap);
    }
    DT_FUZZ_END()
}