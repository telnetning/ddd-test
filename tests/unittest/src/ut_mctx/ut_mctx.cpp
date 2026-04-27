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
 */
#include <gtest/gtest.h>
#include "framework/dstore_pdb.h"
#include "ut_mock/ut_mock.h"
#include "ut_utilities/ut_dstore_framework.h"
#include "buffer/dstore_buf_table.h"
#include "common/memory/dstore_memory_allocator_generic.h"
#include "common/instrument/perf/dstore_perf.h"
#include "diagnose/dstore_memory_diagnose.h"
#include "common/memory/dstore_memory_allocator_stack.h"

class MctxTest : public DSTORETEST {
protected:
    void SetUp() override
    {
        DSTORETEST::SetUp();
        UtInstance *instance = DstoreNew(m_ut_memory_context)UtInstance();
        instance->Install(&DSTORETEST::m_guc, m_ut_memory_context);
        instance->Startup(&DSTORETEST::m_guc);
    }
    void TearDown() override
    {
        UtInstance *instance = (UtInstance *)g_storageInstance;
        instance->Shutdown();
        delete instance;
        DSTORETEST::TearDown();
    }
    
    char *GetMemctxName(uint32 level, uint32 nodeNumber)
    {
        int err = memset_s(m_contextName, MAX_CONTEXT_NAME_LENGTH, 0, MAX_CONTEXT_NAME_LENGTH);
        storage_securec_check(err, "", "");
        errno_t rc = sprintf_s(m_contextName, MAX_CONTEXT_NAME_LENGTH, "level%uMctx%u", level, nodeNumber);
        storage_securec_check_ss(rc);
        return m_contextName;
    }

    StorageInstance *m_instance;
    ThreadContext *m_thrd;
    char m_contextName[MAX_CONTEXT_NAME_LENGTH];

};

TEST_F(MctxTest, NewBufferTable)
{
    static const int entryPerPartition = 1;
    int totalBuf = entryPerPartition * NUM_BUFFER_PARTITIONS;
    PerfCpuMonitor cpumonitor;
    cpumonitor.Init();

    PerfTracePoint  trace_buffer;
    trace_buffer.Init("trace buffer", &cpumonitor);
    trace_buffer.Reset();
    trace_buffer.Start();
    BufTable *table = DstoreNew(g_dstoreCurrentMemoryContext) BufTable(totalBuf);
    table->Initialize();
    EXPECT_NE(table, nullptr);
    trace_buffer.End();
    trace_buffer.Print();
    cpumonitor.Destroy();

    Size table_chunk_size = DstoreGetMemoryChunkSpace(table);
    EXPECT_GE(table_chunk_size, sizeof(BufTable));
    table->Destroy();
    delete table;
}

TEST_F(MctxTest, MctxCreateAndDeleteTest)
{
    /* create shared memory context */
    DstoreMemoryContext top_mcxt = g_storageInstance->GetMemoryMgr()->GetRoot();
    DstoreMemoryContext level1_mctx = DstoreAllocSetContextCreate(g_storageInstance->GetMemoryMgr()->GetRoot(),
                                                      "level1_mctx", ALLOCSET_DEFAULT_SIZES, MemoryContextType::SHARED_CONTEXT);
    EXPECT_TRUE(DstoreMemoryContextIsValid(level1_mctx));
    EXPECT_EQ(level1_mctx->type, MemoryContextType::SHARED_CONTEXT);
    PerfCpuMonitor cpumonitor;
    cpumonitor.Init();

    PerfTracePoint  trace_buffer;
    trace_buffer.Init("trace_mctx", &cpumonitor, PERF_TRACE_ALL);
    trace_buffer.Reset();
    trace_buffer.Start();

    /* create shared memory level2_mctx context under level1_mctx */
    DstoreMemoryContext level2_mctx = DstoreAllocSetContextCreate(level1_mctx,
                                                      "level2_mctx", ALLOCSET_DEFAULT_SIZES, MemoryContextType::SHARED_CONTEXT);
    EXPECT_TRUE(DstoreMemoryContextIsValid(level2_mctx));
    EXPECT_EQ(level2_mctx->type, MemoryContextType::SHARED_CONTEXT);
    EXPECT_EQ(level2_mctx->parent, level1_mctx);
    EXPECT_EQ(level1_mctx->firstChild, level2_mctx);

    /* delete level2_mctx */
    DstoreMemoryContextDelete(level2_mctx);
    EXPECT_EQ(level1_mctx->firstChild, nullptr);

    /* create standard memory level2_mctx context under level1_mctx */
    level2_mctx = DstoreAllocSetContextCreate(level1_mctx, "level2_mctx", ALLOCSET_DEFAULT_SIZES, MemoryContextType::THREAD_CONTEXT);
    EXPECT_TRUE(DstoreMemoryContextIsValid(level2_mctx));
    EXPECT_EQ(level2_mctx->type, MemoryContextType::THREAD_CONTEXT);
    EXPECT_EQ(level2_mctx->parent, level1_mctx);
    EXPECT_EQ(level1_mctx->firstChild, level2_mctx);

    /* Calling DstoreMemoryContextDelete to delete level1_mctx will also delete level2_mctx who is a child of level1_mctx */
    DstoreMemoryContextDelete(level1_mctx);
    trace_buffer.End();
    trace_buffer.Print();
    cpumonitor.Destroy();
}

TEST_F(MctxTest, MctxAllocAndFreeTest)
{
    /* allocate specific size from given memory context */
    Size small_size = (Size)SELF_GENRIC_MEMCTX_LIMITATION >> 1;
    Size large_size = (Size)SELF_GENRIC_MEMCTX_LIMITATION << 2;

/* method 1: using MemoryContextAlloc & allocate size < ALLOC_CHUNK_LIMIT */
    char *test_space = (char *)DstoreMemoryContextAlloc(g_dstoreCurrentMemoryContext, small_size);
    EXPECT_NE(test_space, nullptr);
    /* check size */
    Size space_size = DstoreGetMemoryChunkSpace(test_space);
    EXPECT_GT(space_size, small_size);
    /* check whether test_space is under the memory context of g_dstoreCurrentMemoryContext */
    /* then free_pointer */
    DstorePfreeExt(test_space);
    EXPECT_EQ(test_space, nullptr);

/* method 2: using DstorePalloc0 & allocate size > ALLOC_CHUNK_LIMIT */
    test_space = (char *)DstorePalloc0(large_size);
    EXPECT_NE(test_space, nullptr);
    /* check size */
    space_size = DstoreGetMemoryChunkSpace(test_space);
    EXPECT_GT(space_size, large_size);
    /* check whether test_space is under the memory context of g_dstoreCurrentMemoryContext */
    /* then free_pointer */
    DstorePfreeExt(test_space);
    EXPECT_EQ(test_space, nullptr);
}

TEST_F(MctxTest, MctxReallocTest)
{
    Size small_size = (Size)SELF_GENRIC_MEMCTX_LIMITATION >> 2;
    char *test_space = (char *)DstorePalloc(small_size);
    DstoreAllocSetContext *alloc_set_context_old = (DstoreAllocSetContext *)AllocPtrGetChunk(test_space)->alloc_set;
    EXPECT_NE(test_space, nullptr);
    /* check size */
    Size space_size = DstoreGetMemoryChunkSpace(test_space);
    EXPECT_GT(space_size, small_size);

    /* realloc */
    small_size = small_size * 2;
    test_space = (char *)DstoreRepalloc(test_space, small_size);
    space_size = DstoreGetMemoryChunkSpace(test_space);
    EXPECT_GT(space_size, small_size);
    DstoreAllocSetContext *alloc_set_context_new = (DstoreAllocSetContext *)AllocPtrGetChunk(test_space)->alloc_set;
    EXPECT_EQ(alloc_set_context_old, alloc_set_context_new);
}

TEST_F(MctxTest, StackMctxAllocAndFreeTest)
{
    Size size = (Size)100;
    AutoMemCxtSwitch autoMemCxtSwitch(thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_STACK));

    char *test_space = (char *)DstorePalloc(size);
    EXPECT_NE(test_space, nullptr);
    
    DstorePfreeExt(test_space);
    EXPECT_EQ(test_space, nullptr);
}

TEST_F(MctxTest, HugeSizeMemcpyTest)
{
    Size max_size = 0x7fffUL;
    Size size = max_size + 1;
    char source_buf[size];
    int rc = memset_s(source_buf, max_size, 'a', max_size);
    storage_securec_check(rc, "\0", "\0");
    source_buf[max_size] = 'b';
    char *target_buf = (char *)DstorePalloc(size);
    EXPECT_NE(target_buf, nullptr);
    DSTORE::DstoreMemcpySafelyForHugeSize(target_buf, size, source_buf, size, max_size);
    EXPECT_EQ(memcmp(target_buf, source_buf, size), 0);
    DstorePfreeExt(target_buf);
    EXPECT_EQ(target_buf, nullptr);
}

static uint32 AligneNumToN2(uint32 size)
{
    uint32 ans = 1;
    uint32 tmp = size;
    while (size >>= 1) {
        ans <<= 1;
    }
    return ans < tmp ? ans << 1 : ans;
}

TEST_F(MctxTest, GetSharedMctxTreeInfoTest)
{
    /* create shared memory context */
    DstoreMemoryContext backupContext = g_storageInstance->GetMemoryMgr()->GetRoot();
    g_storageInstance->GetMemoryMgr()->m_topContext = nullptr;
    DstoreMemoryContext rootContext =
        DstoreAllocSetContextCreate(nullptr, "rootMctx", ALLOCSET_DEFAULT_SIZES, MemoryContextType::SHARED_CONTEXT);
    ASSERT_TRUE(DstoreMemoryContextIsValid(rootContext));
    ASSERT_EQ(rootContext->type, MemoryContextType::SHARED_CONTEXT);
    g_storageInstance->GetMemoryMgr()->m_topContext = rootContext;

    SharedMemoryDetail *detailsArray = nullptr;
    uint32 detailsArrayLen = 0;
    ASSERT_EQ(MemoryDiagnose::GetAllSharedMemoryDetails(&detailsArray, &detailsArrayLen), DSTORE_SUCC);
    ASSERT_EQ(detailsArrayLen, 1);
    ASSERT_NE(detailsArray, nullptr);
    EXPECT_EQ(strcmp(detailsArray->contextName, "rootMctx"), 0);
    EXPECT_EQ(detailsArray->level, 0);
    EXPECT_EQ(strlen(detailsArray->parent), 0);
    EXPECT_EQ(detailsArray->totalSize, 0);
    EXPECT_EQ(detailsArray->freeSize, 0);
    EXPECT_EQ(detailsArray->usedSize, 0);
    EXPECT_EQ(strlen(detailsArray->fileName), 0);
    EXPECT_EQ(detailsArray->lineNumber, 0);
    EXPECT_EQ(detailsArray->allocSize, 0);
    DstorePfreeExt(detailsArray);
    detailsArray = nullptr;

    uint32 rootUsedSize = 0;
    uint32 rootTotalSize = 0;
    /* Build a 3 layer 4-tree */
    /* Root has 4 child, the far left is level1Mctx4, fat right is level1Mctx1 */
    DstoreMemoryContext level1Context1 = DstoreAllocSetContextCreate(rootContext, "level1Mctx1",
        ALLOCSET_DEFAULT_SIZES, MemoryContextType::SHARED_CONTEXT);
    rootTotalSize += BLCKSZ;
    rootUsedSize += AligneNumToN2(sizeof(GenericAllocSetContext)) + ALLOC_BLOCKHDRSZ + ALLOC_CHUNKHDRSZ;

    DstoreMemoryContext level1Context2 = DstoreAllocSetContextCreate(rootContext, "level1Mctx2",
        ALLOCSET_DEFAULT_SIZES, MemoryContextType::SHARED_CONTEXT);
    rootUsedSize += AligneNumToN2(sizeof(GenericAllocSetContext)) + ALLOC_CHUNKHDRSZ;

    DstoreMemoryContext level1Context3 = DstoreAllocSetContextCreate(rootContext, "level1Mctx3",
        ALLOCSET_DEFAULT_SIZES, MemoryContextType::SHARED_CONTEXT);
    rootUsedSize += AligneNumToN2(sizeof(GenericAllocSetContext)) + ALLOC_CHUNKHDRSZ;

    DstoreMemoryContext level1Context4 = DstoreAllocSetContextCreate(rootContext, "level1Mctx4",
        ALLOCSET_DEFAULT_SIZES, MemoryContextType::SHARED_CONTEXT);
    rootUsedSize += AligneNumToN2(sizeof(GenericAllocSetContext)) + ALLOC_CHUNKHDRSZ;

    /* Root's far right child has 3 child */
    DstoreMemoryContext level2Context1 = DstoreAllocSetContextCreate(level1Context1, "level2Mctx1",
        ALLOCSET_DEFAULT_SIZES, MemoryContextType::SHARED_CONTEXT);
    rootUsedSize += AligneNumToN2(sizeof(GenericAllocSetContext)) + ALLOC_CHUNKHDRSZ;

    DstoreMemoryContext level2Context2 = DstoreAllocSetContextCreate(level1Context1, "level2Mctx2",
        ALLOCSET_DEFAULT_SIZES, MemoryContextType::SHARED_CONTEXT);
    rootUsedSize += AligneNumToN2(sizeof(GenericAllocSetContext)) + ALLOC_CHUNKHDRSZ;

    DstoreMemoryContext level2Context3 = DstoreAllocSetContextCreate(level1Context1, "level2Mctx3",
        ALLOCSET_DEFAULT_SIZES, MemoryContextType::SHARED_CONTEXT);
    rootUsedSize += AligneNumToN2(sizeof(GenericAllocSetContext)) + ALLOC_CHUNKHDRSZ;

    /* Root's far left child has 2 child */
    DstoreMemoryContext level2Context4 = DstoreAllocSetContextCreate(level1Context4, "level2Mctx4",
        ALLOCSET_DEFAULT_SIZES, MemoryContextType::SHARED_CONTEXT);
    rootUsedSize += AligneNumToN2(sizeof(GenericAllocSetContext)) + ALLOC_CHUNKHDRSZ;

    DstoreMemoryContext level2Context5 = DstoreAllocSetContextCreate(level1Context4, "level2Mctx5",
        ALLOCSET_DEFAULT_SIZES, MemoryContextType::SHARED_CONTEXT);
    rootUsedSize += AligneNumToN2(sizeof(GenericAllocSetContext)) + ALLOC_CHUNKHDRSZ;

    ASSERT_EQ(MemoryDiagnose::GetAllSharedMemoryDetails(&detailsArray, &detailsArrayLen), DSTORE_SUCC);
    ASSERT_EQ(detailsArrayLen, 10);
    ASSERT_NE(detailsArray, nullptr);
    uint32 level1NodeId = 4;
    uint32 level2NodeId = 5;
    for (uint32 i = 0; i < 10; ++i) {
        if (i == 0) {
            EXPECT_EQ(strcmp(detailsArray[i].contextName, "rootMctx"), 0);
            EXPECT_EQ(detailsArray[i].level, 0);
            EXPECT_EQ(strlen(detailsArray[i].parent), 0);
        } else if (i == 1 || i == 4 || i == 5 || i == 6) {
            EXPECT_EQ(strcmp(detailsArray[i].contextName, GetMemctxName(1, level1NodeId--)), 0);
            EXPECT_EQ(detailsArray[i].level, 1);
            EXPECT_EQ(strcmp(detailsArray[i].parent, "rootMctx"), 0);
        } else {
            EXPECT_EQ(strcmp(detailsArray[i].contextName, GetMemctxName(2, level2NodeId--)), 0);
            EXPECT_EQ(detailsArray[i].level, 2);
            if (i == 2 || i == 3) {
                EXPECT_EQ(strcmp(detailsArray[i].parent, GetMemctxName(1, 4)), 0);
            } else {
                EXPECT_EQ(strcmp(detailsArray[i].parent, GetMemctxName(1, 1)), 0);
            }
        }
        
        if (i == 0) {
            EXPECT_EQ(detailsArray[i].totalSize, rootTotalSize);
            EXPECT_EQ(detailsArray[i].usedSize, rootUsedSize);
            EXPECT_EQ(detailsArray[i].freeSize, rootTotalSize - rootUsedSize);
        } else {
            EXPECT_EQ(detailsArray[i].totalSize, 0);
            EXPECT_EQ(detailsArray[i].usedSize, 0);
            EXPECT_EQ(detailsArray[i].freeSize, 0);
        }
        
        EXPECT_EQ(strlen(detailsArray[i].fileName), 0);
        EXPECT_EQ(detailsArray[i].lineNumber, 0);
        EXPECT_EQ(detailsArray[i].allocSize, 0);
    }
    DstorePfreeExt(detailsArray);
    detailsArray = nullptr;

    /* Calling DstoreMemoryContextDelete to delete level 1 will also delete level 2 who is a child of level1Mctx1 */
    DstoreMemoryContextDelete(level1Context1);
    DstoreMemoryContextDelete(level1Context2);
    DstoreMemoryContextDelete(level1Context3);
    DstoreMemoryContextDelete(level1Context4);
    DstoreMemoryContextDestroyTop(rootContext);
    g_storageInstance->GetMemoryMgr()->m_topContext = backupContext;
}

TEST_F(MctxTest, GetSharedMctxMallocAndFreeLittleSizeTest)
{
    /* create shared memory context */
    DstoreMemoryContext backupContext = g_storageInstance->GetMemoryMgr()->GetRoot();
    g_storageInstance->GetMemoryMgr()->m_topContext = nullptr;
    DstoreMemoryContext rootContext =
        DstoreAllocSetContextCreate(nullptr, "rootMctx", ALLOCSET_DEFAULT_SIZES, MemoryContextType::SHARED_CONTEXT);
    ASSERT_TRUE(DstoreMemoryContextIsValid(rootContext));
    ASSERT_EQ(rootContext->type, MemoryContextType::SHARED_CONTEXT);
    g_storageInstance->GetMemoryMgr()->m_topContext = rootContext;
    
    SharedMemoryDetail *detailsArray = nullptr;
    uint32 detailsArrayLen = 0;
    uint32 rootUsedSize = 0;
    uint32 rootTotalSize = 0;
    /* Build a 1 layer 2-tree */
    /* Root has 1 child */
    DstoreMemoryContext level1Context1 = DstoreAllocSetContextCreate(rootContext, "level1Mctx1",
        ALLOCSET_DEFAULT_SIZES, MemoryContextType::SHARED_CONTEXT);
    rootTotalSize += BLCKSZ;
    rootUsedSize += AligneNumToN2(sizeof(GenericAllocSetContext)) + ALLOC_BLOCKHDRSZ + ALLOC_CHUNKHDRSZ;
    
    /* alloc test: malloc two space, one size is 1 at root, another size is 64 at child */
    char *littleSize1 = (char *)DstoreMemoryContextAlloc(rootContext, 1);
    ASSERT_NE(littleSize1, nullptr);
    rootUsedSize += AligneNumToN2(1 + ALLOC_MAGICHDRSZ) + ALLOC_CHUNKHDRSZ;
    
    char *littleSize2 = (char *)DstoreMemoryContextAlloc(level1Context1, 64);
    ASSERT_NE(littleSize2, nullptr);
    uint32 childTotalSize = BLCKSZ;
    uint32 childUsedSize = AligneNumToN2(64 + ALLOC_MAGICHDRSZ) + ALLOC_BLOCKHDRSZ + ALLOC_CHUNKHDRSZ;
    
    ASSERT_EQ(MemoryDiagnose::GetAllSharedMemoryDetails(&detailsArray, &detailsArrayLen), DSTORE_SUCC);
    ASSERT_EQ(detailsArrayLen, 2);
    ASSERT_NE(detailsArray, nullptr);
    EXPECT_EQ(strcmp(detailsArray[0].contextName, "rootMctx"), 0);
    EXPECT_EQ(detailsArray[0].level, 0);
    EXPECT_EQ(strlen(detailsArray[0].parent), 0);
    EXPECT_EQ(detailsArray[0].totalSize, rootTotalSize);
    EXPECT_EQ(detailsArray[0].usedSize, rootUsedSize);
    EXPECT_EQ(detailsArray[0].freeSize, rootTotalSize - rootUsedSize);
    
    EXPECT_EQ(strcmp(detailsArray[1].contextName, "level1Mctx1"), 0);
    EXPECT_EQ(detailsArray[1].level, 1);
    EXPECT_EQ(strcmp(detailsArray[1].parent, "rootMctx"), 0);
    EXPECT_EQ(detailsArray[1].totalSize, childTotalSize);
    EXPECT_EQ(detailsArray[1].usedSize, childUsedSize);
    EXPECT_EQ(detailsArray[1].freeSize, childTotalSize - childUsedSize);
    DstorePfreeExt(detailsArray);
    detailsArray = nullptr;
    
    DstorePfreeExt(littleSize2);
    childUsedSize -= AligneNumToN2(64 + ALLOC_MAGICHDRSZ) + ALLOC_CHUNKHDRSZ;
    ASSERT_EQ(MemoryDiagnose::GetAllSharedMemoryDetails(&detailsArray, &detailsArrayLen), DSTORE_SUCC);
    ASSERT_EQ(detailsArrayLen, 2);
    ASSERT_NE(detailsArray, nullptr);
    EXPECT_EQ(strcmp(detailsArray[0].contextName, "rootMctx"), 0);
    EXPECT_EQ(detailsArray[0].level, 0);
    EXPECT_EQ(strlen(detailsArray[0].parent), 0);
    EXPECT_EQ(detailsArray[0].totalSize, rootTotalSize);
    EXPECT_EQ(detailsArray[0].usedSize, rootUsedSize);
    EXPECT_EQ(detailsArray[0].freeSize, rootTotalSize - rootUsedSize);
    
    EXPECT_EQ(strcmp(detailsArray[1].contextName, "level1Mctx1"), 0);
    EXPECT_EQ(detailsArray[1].level, 1);
    EXPECT_EQ(strcmp(detailsArray[1].parent, "rootMctx"), 0);
    EXPECT_EQ(detailsArray[1].totalSize, childTotalSize);
    EXPECT_EQ(detailsArray[1].usedSize, childUsedSize);
    EXPECT_EQ(detailsArray[1].freeSize, childTotalSize - childUsedSize);
    DstorePfreeExt(detailsArray);
    detailsArray = nullptr;
    
    /* alloc0 test: malloc one small space, size is 18 at child */
    char *littleSize3 = (char *)DstoreMemoryContextAllocZero(level1Context1, 18);
    ASSERT_NE(littleSize3, nullptr);
    childUsedSize += AligneNumToN2(18 + ALLOC_MAGICHDRSZ) + ALLOC_CHUNKHDRSZ;
    ASSERT_EQ(MemoryDiagnose::GetAllSharedMemoryDetails(&detailsArray, &detailsArrayLen), DSTORE_SUCC);
    ASSERT_EQ(detailsArrayLen, 2);
    ASSERT_NE(detailsArray, nullptr);
    EXPECT_EQ(strcmp(detailsArray[1].contextName, "level1Mctx1"), 0);
    EXPECT_EQ(detailsArray[1].level, 1);
    EXPECT_EQ(strcmp(detailsArray[1].parent, "rootMctx"), 0);
    EXPECT_EQ(detailsArray[1].totalSize, childTotalSize);
    EXPECT_EQ(detailsArray[1].usedSize, childUsedSize);
    EXPECT_EQ(detailsArray[1].freeSize, childTotalSize - childUsedSize);
    DstorePfreeExt(detailsArray);
    detailsArray = nullptr;
    
    DstorePfreeExt(littleSize1);
    DstorePfreeExt(littleSize3);
    DstoreMemoryContextDelete(level1Context1);
    DstoreMemoryContextDestroyTop(rootContext);
    g_storageInstance->GetMemoryMgr()->m_topContext = backupContext;
}

TEST_F(MctxTest, GetSharedMctxMallocAndFreeHugeSizeTest)
{
    /* create shared memory context */
    DstoreMemoryContext backupContext = g_storageInstance->GetMemoryMgr()->GetRoot();
    g_storageInstance->GetMemoryMgr()->m_topContext = nullptr;
    DstoreMemoryContext rootContext =
        DstoreAllocSetContextCreate(nullptr, "rootMctx", ALLOCSET_DEFAULT_SIZES, MemoryContextType::SHARED_CONTEXT);
    ASSERT_TRUE(DstoreMemoryContextIsValid(rootContext));
    ASSERT_EQ(rootContext->type, MemoryContextType::SHARED_CONTEXT);
    g_storageInstance->GetMemoryMgr()->m_topContext = rootContext;
    
    SharedMemoryDetail *detailsArray = nullptr;
    uint32 detailsArrayLen = 0;
    uint32 rootUsedSize = 0;
    uint32 rootTotalSize = 0;
    /* Build a 1 layer 2-tree */
    /* Root has 1 child */
    DstoreMemoryContext level1Context1 = DstoreAllocSetContextCreate(rootContext, "level1Mctx1",
        ALLOCSET_DEFAULT_SIZES, MemoryContextType::SHARED_CONTEXT);
    rootTotalSize += BLCKSZ;
    rootUsedSize += AligneNumToN2(sizeof(GenericAllocSetContext)) + ALLOC_BLOCKHDRSZ + ALLOC_CHUNKHDRSZ;
    
    /* alloc test: malloc one huge space, size is two BLCKSZ at child */
    char *hugeSize1 = (char *)DstoreMemoryContextAlloc(level1Context1, 2 * BLCKSZ);
    ASSERT_NE(hugeSize1, nullptr);
    uint32 childTotalSize = MAXALIGN(2 * BLCKSZ + ALLOC_MAGICHDRSZ) + ALLOC_BLOCKHDRSZ + ALLOC_CHUNKHDRSZ;
    uint32 childUsedSize = MAXALIGN(2 * BLCKSZ + ALLOC_MAGICHDRSZ) + ALLOC_BLOCKHDRSZ + ALLOC_CHUNKHDRSZ;
    ASSERT_EQ(MemoryDiagnose::GetAllSharedMemoryDetails(&detailsArray, &detailsArrayLen), DSTORE_SUCC);
    ASSERT_EQ(detailsArrayLen, 2);
    ASSERT_NE(detailsArray, nullptr);
    EXPECT_EQ(strcmp(detailsArray[0].contextName, "rootMctx"), 0);
    EXPECT_EQ(detailsArray[0].level, 0);
    EXPECT_EQ(strlen(detailsArray[0].parent), 0);
    EXPECT_EQ(detailsArray[0].totalSize, rootTotalSize);
    EXPECT_EQ(detailsArray[0].usedSize, rootUsedSize);
    EXPECT_EQ(detailsArray[0].freeSize, rootTotalSize - rootUsedSize);
    EXPECT_EQ(strcmp(detailsArray[1].contextName, "level1Mctx1"), 0);
    EXPECT_EQ(detailsArray[1].level, 1);
    EXPECT_EQ(strcmp(detailsArray[1].parent, "rootMctx"), 0);
    EXPECT_EQ(detailsArray[1].totalSize, childTotalSize);
    EXPECT_EQ(detailsArray[1].usedSize, childUsedSize);
    EXPECT_EQ(detailsArray[1].freeSize, childTotalSize - childUsedSize);
    DstorePfreeExt(detailsArray);
    detailsArray = nullptr;
    
    DstorePfreeExt(hugeSize1);
    /* The Huge size (which more than BLCKSZ) free will free total block, it will sub total size */
    childTotalSize -= MAXALIGN(2 * BLCKSZ + ALLOC_MAGICHDRSZ) + ALLOC_BLOCKHDRSZ + ALLOC_CHUNKHDRSZ;
    childUsedSize -= MAXALIGN(2 * BLCKSZ + ALLOC_MAGICHDRSZ) + ALLOC_BLOCKHDRSZ + ALLOC_CHUNKHDRSZ;
    ASSERT_EQ(MemoryDiagnose::GetAllSharedMemoryDetails(&detailsArray, &detailsArrayLen), DSTORE_SUCC);
    ASSERT_EQ(detailsArrayLen, 2);
    ASSERT_NE(detailsArray, nullptr);
    EXPECT_EQ(strcmp(detailsArray[0].contextName, "rootMctx"), 0);
    EXPECT_EQ(detailsArray[0].level, 0);
    EXPECT_EQ(strlen(detailsArray[0].parent), 0);
    EXPECT_EQ(detailsArray[0].totalSize, rootTotalSize);
    EXPECT_EQ(detailsArray[0].usedSize, rootUsedSize);
    EXPECT_EQ(detailsArray[0].freeSize, rootTotalSize - rootUsedSize);
    EXPECT_EQ(strcmp(detailsArray[1].contextName, "level1Mctx1"), 0);
    EXPECT_EQ(detailsArray[1].level, 1);
    EXPECT_EQ(strcmp(detailsArray[1].parent, "rootMctx"), 0);
    EXPECT_EQ(detailsArray[1].totalSize, childTotalSize);
    EXPECT_EQ(detailsArray[1].usedSize, childUsedSize);
    EXPECT_EQ(detailsArray[1].freeSize, childTotalSize - childUsedSize);
    
    /* alloc one space, size is more than half of BLCKSZ */
    char *hugeSize2 = (char *)DstoreMemoryContextAlloc(level1Context1, BLCKSZ / 2 + 1);
    ASSERT_NE(hugeSize2, nullptr);
    /* Require size will aligne to 2^N, that means (BLCKSZ / 2 + 1) will require BLCKSZ,
     * so it need BLCKSZ plus block and chunk header size, it will mallock double BLCKSZ
     */
    childTotalSize += 2 * BLCKSZ;
    childUsedSize += AligneNumToN2(BLCKSZ / 2 + 1 + ALLOC_MAGICHDRSZ) + ALLOC_BLOCKHDRSZ + ALLOC_CHUNKHDRSZ;
    
    ASSERT_EQ(MemoryDiagnose::GetAllSharedMemoryDetails(&detailsArray, &detailsArrayLen), DSTORE_SUCC);
    ASSERT_EQ(detailsArrayLen, 2);
    ASSERT_NE(detailsArray, nullptr);
    EXPECT_EQ(strcmp(detailsArray[1].contextName, "level1Mctx1"), 0);
    EXPECT_EQ(detailsArray[1].level, 1);
    EXPECT_EQ(strcmp(detailsArray[1].parent, "rootMctx"), 0);
    EXPECT_EQ(detailsArray[1].totalSize, childTotalSize);
    EXPECT_EQ(detailsArray[1].usedSize, childUsedSize);
    EXPECT_EQ(detailsArray[1].freeSize, childTotalSize - childUsedSize);
    DstorePfreeExt(detailsArray);
    detailsArray = nullptr;
    
    DstorePfreeExt(hugeSize2);
    /* Huge size (Less than BLCKSZ) free will keep the block, */
    childUsedSize -= AligneNumToN2(BLCKSZ / 2 + 1 + ALLOC_MAGICHDRSZ) + ALLOC_CHUNKHDRSZ;
    ASSERT_EQ(MemoryDiagnose::GetAllSharedMemoryDetails(&detailsArray, &detailsArrayLen), DSTORE_SUCC);
    ASSERT_EQ(detailsArrayLen, 2);
    ASSERT_NE(detailsArray, nullptr);
    EXPECT_EQ(detailsArray[1].level, 1);
    EXPECT_EQ(strcmp(detailsArray[1].parent, "rootMctx"), 0);
    EXPECT_EQ(detailsArray[1].totalSize, childTotalSize);
    EXPECT_EQ(detailsArray[1].usedSize, childUsedSize);
    EXPECT_EQ(detailsArray[1].freeSize, childTotalSize - childUsedSize);
    DstorePfreeExt(detailsArray);
    detailsArray = nullptr;
    
    DstoreMemoryContextDelete(level1Context1);
    DstoreMemoryContextDestroyTop(rootContext);
    g_storageInstance->GetMemoryMgr()->m_topContext = backupContext;
}

TEST_F(MctxTest, GetSharedMctxPutAndGetFreeListSizeTest)
{
    /* create shared memory context */
    DstoreMemoryContext backupContext = g_storageInstance->GetMemoryMgr()->GetRoot();
    g_storageInstance->GetMemoryMgr()->m_topContext = nullptr;
    DstoreMemoryContext rootContext =
        DstoreAllocSetContextCreate(nullptr, "rootMctx", ALLOCSET_DEFAULT_SIZES, MemoryContextType::SHARED_CONTEXT);
    ASSERT_TRUE(DstoreMemoryContextIsValid(rootContext));
    ASSERT_EQ(rootContext->type, MemoryContextType::SHARED_CONTEXT);
    g_storageInstance->GetMemoryMgr()->m_topContext = rootContext;
    
    SharedMemoryDetail *detailsArray = nullptr;
    uint32 detailsArrayLen = 0;
    /* Build a 1 layer 2-tree */
    /* Root has 1 child */
    DstoreMemoryContext level1Context1 = DstoreAllocSetContextCreate(rootContext, "level1Mctx1",
        ALLOCSET_DEFAULT_SIZES, MemoryContextType::SHARED_CONTEXT);

    /* alloc one space, these size is BLCKSZ / 4 */
    char *size1 = (char *)DstoreMemoryContextAlloc(level1Context1, BLCKSZ / 4);
    ASSERT_NE(size1, nullptr);
    uint32 childTotalSize = BLCKSZ;
    uint32 childUsedSize = AligneNumToN2(BLCKSZ / 4 + ALLOC_MAGICHDRSZ) + ALLOC_BLOCKHDRSZ + ALLOC_CHUNKHDRSZ;
    
    ASSERT_EQ(MemoryDiagnose::GetAllSharedMemoryDetails(&detailsArray, &detailsArrayLen), DSTORE_SUCC);
    ASSERT_EQ(detailsArrayLen, 2);
    ASSERT_NE(detailsArray, nullptr);
    EXPECT_EQ(strcmp(detailsArray[1].contextName, "level1Mctx1"), 0);
    EXPECT_EQ(detailsArray[1].level, 1);
    EXPECT_EQ(strcmp(detailsArray[1].parent, "rootMctx"), 0);
    EXPECT_EQ(detailsArray[1].totalSize, childTotalSize);
    EXPECT_EQ(detailsArray[1].usedSize, childUsedSize);
    EXPECT_EQ(detailsArray[1].freeSize, childTotalSize - childUsedSize);
    DstorePfreeExt(detailsArray);
    detailsArray = nullptr;
    
    /* If free list slot is empty and current block free space size is not enouth for this malloc,
     * put it into free list(free size 4000, cut it into 2048, 1024, 512, 256....) and create a new block
     */
    char *size2 = (char *)DstoreMemoryContextAlloc(level1Context1, BLCKSZ / 4 * 3 + 1);
    ASSERT_NE(size2, nullptr);
    /* Huge size, allock two BLCKSZ */
    childTotalSize += 2 * BLCKSZ;
    childUsedSize += AligneNumToN2(BLCKSZ / 4 * 3 + 1 + ALLOC_MAGICHDRSZ) + ALLOC_BLOCKHDRSZ + ALLOC_CHUNKHDRSZ;
    ASSERT_EQ(MemoryDiagnose::GetAllSharedMemoryDetails(&detailsArray, &detailsArrayLen), DSTORE_SUCC);
    ASSERT_EQ(detailsArrayLen, 2);
    ASSERT_NE(detailsArray, nullptr);
    EXPECT_EQ(strcmp(detailsArray[1].contextName, "level1Mctx1"), 0);
    EXPECT_EQ(detailsArray[1].level, 1);
    EXPECT_EQ(strcmp(detailsArray[1].parent, "rootMctx"), 0);
    EXPECT_EQ(detailsArray[1].totalSize, childTotalSize);
    EXPECT_EQ(detailsArray[1].usedSize, childUsedSize);
    EXPECT_EQ(detailsArray[1].freeSize, childTotalSize - childUsedSize);
    
    /* Now free list is not empty, get from free list */
    char *size3 = (char *)DstoreMemoryContextAlloc(level1Context1, 1024);
    ASSERT_NE(size3, nullptr);
    childUsedSize += AligneNumToN2(1024 + ALLOC_MAGICHDRSZ) + ALLOC_CHUNKHDRSZ;
    ASSERT_EQ(MemoryDiagnose::GetAllSharedMemoryDetails(&detailsArray, &detailsArrayLen), DSTORE_SUCC);
    ASSERT_EQ(detailsArrayLen, 2);
    ASSERT_NE(detailsArray, nullptr);
    EXPECT_EQ(strcmp(detailsArray[1].contextName, "level1Mctx1"), 0);
    EXPECT_EQ(detailsArray[1].level, 1);
    EXPECT_EQ(strcmp(detailsArray[1].parent, "rootMctx"), 0);
    EXPECT_EQ(detailsArray[1].totalSize, childTotalSize);
    EXPECT_EQ(detailsArray[1].usedSize, childUsedSize);
    EXPECT_EQ(detailsArray[1].freeSize, childTotalSize - childUsedSize);
  
    DstorePfreeExt(size1);
    DstorePfreeExt(size2);
    DstorePfreeExt(size3);
    DstoreMemoryContextDelete(level1Context1);
    DstoreMemoryContextDestroyTop(rootContext);
    g_storageInstance->GetMemoryMgr()->m_topContext = backupContext;
}

TEST_F(MctxTest, GetSharedMctxFileAndLineTest)
{
    /* create shared memory context */
    DstoreMemoryContext backupContext = g_storageInstance->GetMemoryMgr()->GetRoot();
    g_storageInstance->GetMemoryMgr()->m_topContext = nullptr;
    DstoreMemoryContext rootContext =
        DstoreAllocSetContextCreate(nullptr, "rootMctx", ALLOCSET_DEFAULT_SIZES, MemoryContextType::SHARED_CONTEXT);
    ASSERT_TRUE(DstoreMemoryContextIsValid(rootContext));
    ASSERT_EQ(rootContext->type, MemoryContextType::SHARED_CONTEXT);
    g_storageInstance->GetMemoryMgr()->m_topContext = rootContext;

    SharedMemoryDetail *detailsArray = nullptr;
    uint32 detailsArrayLen = 0;
    /* Root has 1 child */
    DstoreMemoryContext level1Context1 = DstoreAllocSetContextCreate(rootContext, "level1Mctx1",
        ALLOCSET_DEFAULT_SIZES, MemoryContextType::SHARED_CONTEXT);

    auto allocLine = __LINE__;
    char *littleSize = (char *)DstoreMemoryContextAlloc(level1Context1, 1);
    ASSERT_NE(littleSize, nullptr);
    uint32 childUsedSize = AligneNumToN2(1 + ALLOC_MAGICHDRSZ);

    ASSERT_EQ(
        MemoryDiagnose::GetSharedMemoryDetail("level1Mctx1", strlen("level1Mctx1"), &detailsArray, &detailsArrayLen),
        DSTORE_SUCC);
    ASSERT_EQ(detailsArrayLen, 1);
    ASSERT_NE(detailsArray, nullptr);
    EXPECT_EQ(strcmp(detailsArray[0].fileName, __FILE__), 0);
    EXPECT_EQ(detailsArray[0].lineNumber, allocLine + 1); /* littleSize malloc line num */
    EXPECT_EQ(detailsArray[0].allocSize, childUsedSize);
    DstorePfreeExt(littleSize);

    DstoreMemoryContextDelete(level1Context1);
    DstoreMemoryContextDestroyTop(rootContext);
    g_storageInstance->GetMemoryMgr()->m_topContext = backupContext;
    DstorePfreeExt(detailsArray);
}

#define TestDstoreMemoryContextAlloc(context, size) TestDstoreMemoryContextAllocDebug(context, size, __FILE__, __LINE__)
bool test_dstore_reserve_mem_sucess(size_t size, MemoryContextType type, int level)
{
    if (type != MemoryContextType::INVALID_CONTEXT) {
        return true;
    } else {
        return false;
    }
}

bool test_dstore_reserve_mem_fail(size_t size, MemoryContextType type, int level)
{
    return false;
}

void test_dstore_release_mem(size_t size, MemoryContextType type, int level)
{}

void *TestDstoreMemoryContextAllocDebug(DstoreMemoryContext context, Size size, const char *file, int line)
{
    if (!AllocSizeIsValid(size)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("invalid memory allocate request size %lu",
            static_cast<unsigned long>(size)));
        StorageAssert(0);
    }

    StorageAssert(DstoreMemoryContextIsValid(context));

    void *space = (context->allocSet)->allocate(0, size, file, line);
    return space;
}

void test_thread_sucess()
{
    (void)g_storageInstance->CreateThreadAndRegister(INVALID_PDB_ID, false, "test_thread", true,
                                                     ThreadMemoryLevel::THREADMEM_LOW_PRIORITY);
    DstoreAllocSetContext::get_MemContextCallBack().InitMemCallBack(
        reinterpret_cast<void *>(&test_dstore_reserve_mem_sucess), reinterpret_cast<void *>(&test_dstore_release_mem));
    ASSERT_EQ(DstoreAllocSetContext::get_MemContextCallBack().reserve_mem_callback_func,
              &test_dstore_reserve_mem_sucess);
    ASSERT_EQ(thrd->m_threadmemlevel, ThreadMemoryLevel::THREADMEM_LOW_PRIORITY);

    DstoreMemoryContext rootContext =
        DstoreAllocSetContextCreate(nullptr, "rootMctx", ALLOCSET_DEFAULT_SIZES, MemoryContextType::THREAD_CONTEXT);
    ASSERT_TRUE(DstoreMemoryContextIsValid(rootContext));
    ASSERT_EQ(rootContext->type, MemoryContextType::THREAD_CONTEXT);

    char *Size = (char *)DstoreMemoryContextAlloc(rootContext, 2 * ALLOC_CHUNK_LIMIT);
    ASSERT_NE(Size, nullptr);
    DstorePfreeExt(Size);

    g_storageInstance->UnregisterThread();
}

void test_thread_fail()
{
    DstoreAllocSetContext::get_MemContextCallBack().InitMemCallBack(nullptr, nullptr);
    ASSERT_EQ(DstoreAllocSetContext::get_MemContextCallBack().reserve_mem_callback_func, nullptr);
    (void)g_storageInstance->CreateThreadAndRegister(INVALID_PDB_ID, false, "test_thread_1", true,
                                                     ThreadMemoryLevel::THREADMEM_LOW_PRIORITY);
    DstoreAllocSetContext::get_MemContextCallBack().InitMemCallBack(
        reinterpret_cast<void *>(&test_dstore_reserve_mem_fail), reinterpret_cast<void *>(&test_dstore_release_mem));
    ASSERT_EQ(DstoreAllocSetContext::get_MemContextCallBack().reserve_mem_callback_func, &test_dstore_reserve_mem_fail);
    ASSERT_EQ(thrd->m_threadmemlevel, ThreadMemoryLevel::THREADMEM_LOW_PRIORITY);

    DstoreMemoryContext rootContext =
        DstoreAllocSetContextCreate(nullptr, "rootMctx", ALLOCSET_DEFAULT_SIZES, MemoryContextType::THREAD_CONTEXT);
    ASSERT_TRUE(DstoreMemoryContextIsValid(rootContext));
    ASSERT_EQ(rootContext->type, MemoryContextType::THREAD_CONTEXT);

    char *Size = (char *)TestDstoreMemoryContextAlloc(rootContext, 2 * ALLOC_CHUNK_LIMIT);
    ASSERT_EQ(Size, nullptr);
    DstorePfreeExt(Size);

    g_storageInstance->UnregisterThread();

    /*Space must also be allocated during destruction, and the pointer must be reset to null.*/
    DstoreAllocSetContext::get_MemContextCallBack().InitMemCallBack(nullptr, nullptr);
}

TEST_F(MctxTest, CreateMemoryOfSucessTest)
{
    std::thread t1(test_thread_sucess);
    t1.join();

    DstoreAllocSetContext::get_MemContextCallBack().InitMemCallBack(
        reinterpret_cast<void *>(&test_dstore_reserve_mem_sucess), reinterpret_cast<void *>(&test_dstore_release_mem));
    ASSERT_EQ(DstoreAllocSetContext::get_MemContextCallBack().reserve_mem_callback_func,
              &test_dstore_reserve_mem_sucess);

    /* create shared memory context */
    DstoreMemoryContext backupContext = g_storageInstance->GetMemoryMgr()->GetRoot();
    g_storageInstance->GetMemoryMgr()->m_topContext = nullptr;
    DstoreMemoryContext rootContext =
        DstoreAllocSetContextCreate(nullptr, "rootMctx", ALLOCSET_DEFAULT_SIZES, MemoryContextType::SHARED_CONTEXT);
    ASSERT_TRUE(DstoreMemoryContextIsValid(rootContext));
    ASSERT_EQ(rootContext->type, MemoryContextType::SHARED_CONTEXT);
    g_storageInstance->GetMemoryMgr()->m_topContext = rootContext;

    /* Root has 1 child */
    DstoreMemoryContext level1Context1 = DstoreAllocSetContextCreate(rootContext, "level1Mctx1", ALLOCSET_DEFAULT_SIZES,
                                                                     MemoryContextType::SHARED_CONTEXT);
    EXPECT_TRUE(DstoreMemoryContextIsValid(level1Context1));
    EXPECT_EQ(level1Context1->type, MemoryContextType::SHARED_CONTEXT);
    EXPECT_EQ(level1Context1->parent, rootContext);
    EXPECT_EQ(rootContext->firstChild, level1Context1);

    char *Size1 = (char *)DstoreMemoryContextAlloc(level1Context1, 2 * ALLOC_CHUNK_LIMIT);
    ASSERT_NE(Size1, nullptr);
    DstorePfreeExt(Size1);

    DstoreMemoryContextDelete(level1Context1);
    DstoreMemoryContextDestroyTop(rootContext);
    g_storageInstance->GetMemoryMgr()->m_topContext = backupContext;
}

TEST_F(MctxTest, CreateMemoryOfFailTest)
{
    std::thread t1(test_thread_fail);
    t1.join();

    /* create shared memory context */
    DstoreAllocSetContext::get_MemContextCallBack().InitMemCallBack(
        reinterpret_cast<void *>(&test_dstore_reserve_mem_fail), reinterpret_cast<void *>(&test_dstore_release_mem));
    ASSERT_EQ(DstoreAllocSetContext::get_MemContextCallBack().reserve_mem_callback_func, &test_dstore_reserve_mem_fail);

    DstoreMemoryContext backupContext = g_storageInstance->GetMemoryMgr()->GetRoot();
    g_storageInstance->GetMemoryMgr()->m_topContext = nullptr;
    DstoreMemoryContext rootContext =
        DstoreAllocSetContextCreate(nullptr, "rootMctx", ALLOCSET_DEFAULT_SIZES, MemoryContextType::SHARED_CONTEXT);
    ASSERT_TRUE(DstoreMemoryContextIsValid(rootContext));
    ASSERT_EQ(rootContext->type, MemoryContextType::SHARED_CONTEXT);
    g_storageInstance->GetMemoryMgr()->m_topContext = rootContext;

    char *Size = (char *)TestDstoreMemoryContextAlloc(rootContext, 2 * ALLOC_CHUNK_LIMIT);
    ASSERT_EQ(Size, nullptr);
    DstorePfreeExt(Size);

    DstoreMemoryContextDestroyTop(rootContext);
    g_storageInstance->GetMemoryMgr()->m_topContext = backupContext;

    /*Space must also be allocated during destruction, and the pointer must be reset to null.*/
    DstoreAllocSetContext::get_MemContextCallBack().InitMemCallBack(nullptr, nullptr);
}

enum ThreadTypeTest { STORAGE = 0, STORAGEPDB};
void test_thread_level(ThreadTypeTest type, ThreadMemoryLevel level)
{
    switch (type) {
        case STORAGE: {
            (void)g_storageInstance->CreateThreadAndRegister(INVALID_PDB_ID, false, "test_thread_1", true, level);
            ASSERT_EQ(thrd->GetLevel(), level);
            g_storageInstance->UnregisterThread(false);
            break;
        }
        case STORAGEPDB: {
            StoragePdb tmp(INVALID_PDB_ID);
            tmp.CreateThreadAndRegister(true, true, level, "test_thread_1");
            ASSERT_EQ(thrd->GetLevel(), level);
            tmp.UnregisterThread();
            break;
        }
    }
}
TEST_F(MctxTest, HighLevelTest)
{
    DstoreAllocSetContext::get_MemContextCallBack().InitMemCallBack(nullptr, nullptr);
    std::thread testThread1(test_thread_level, STORAGE, ThreadMemoryLevel::THREADMEM_HIGH_PRIORITY);
    testThread1.join();

    std::thread testThread2(test_thread_level, STORAGEPDB, ThreadMemoryLevel::THREADMEM_HIGH_PRIORITY);
    testThread2.join();
}

TEST_F(MctxTest, MediumLevelTest)
{
    DstoreAllocSetContext::get_MemContextCallBack().InitMemCallBack(nullptr, nullptr);
    std::thread testThread1(test_thread_level, STORAGE, ThreadMemoryLevel::THREADMEM_MEDIUM_PRIORITY);
    testThread1.join();

    std::thread testThread2(test_thread_level, STORAGEPDB, ThreadMemoryLevel::THREADMEM_MEDIUM_PRIORITY);
    testThread2.join();
}

TEST_F(MctxTest, LowLevelTest)
{
    DstoreAllocSetContext::get_MemContextCallBack().InitMemCallBack(nullptr, nullptr);
    std::thread testThread1(test_thread_level, STORAGE, ThreadMemoryLevel::THREADMEM_LOW_PRIORITY);
    testThread1.join();

    std::thread testThread2(test_thread_level, STORAGEPDB, ThreadMemoryLevel::THREADMEM_LOW_PRIORITY);
    testThread2.join();
}