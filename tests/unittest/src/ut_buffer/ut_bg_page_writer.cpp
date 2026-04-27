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
#include "ut_buffer/ut_buffer_fake_instance.h"
#include "ut_buffer/ut_buffer_util.h"
#include "ut_mock/ut_wal_stream_mock.h"
#include "ut_utilities/ut_thread_pool.h"

#include "framework/dstore_vfs_adapter.h"

struct FakeFile {
    FileId file_id;
    const char *path;
    BlockNumber max_block;
};

static const FakeFile FAKE_FILES[] = {
        {6100U, "6100", 1000U},
        {6200U, "6200", 1000U},
        {6300U, "6300", 1000U},
        {6400U, "6400", 1000U},
        {6500U, "6500", 1000U},
        {6600U, "6600", 1000U},
        {6700U, "6700", 1000U},
        {6800U, "6800", 1000U},
        {6900U, "6900", 1000U},
        {61000U, "61000", 1000U}
};

static const Size FAKE_SIZE = sizeof(FAKE_FILES) / sizeof(FakeFile);

class BgPageWriterTest : public DSTOREParamTest<std::vector<Size>> {
protected:
    uint64 m_makePageDirtyThreadNum;
    BgPageWriterFakeStorageInstance *m_storage_instance;
    void SetUp() override
    {
        SetDefaultPdbId(PDB_TEMPLATE1_ID);
        std::vector<Size> params = GetParam();
        DSTOREParamTest<std::vector<Size>>::SetUp();
        InstallDatabase(&DSTOREParamTest<std::vector<Size>>::m_guc, m_ut_memory_context);

        m_makePageDirtyThreadNum = params[0];
        Size maxBlock = 0;
        for (uint32 i = 0; i < m_makePageDirtyThreadNum; i++) {
            maxBlock += FAKE_FILES[i].max_block;
        }
        m_guc.buffer = maxBlock * 2;
        VfsInterface::ModuleInitialize();
        BgPageWriterFakeStorageInstance *instance = DstoreNew(m_ut_memory_context)BgPageWriterFakeStorageInstance();
        instance->Startup(&DSTOREParamTest<std::vector<Size>>::m_guc);

        m_storage_instance = instance;

        VFSAdapter *vfs = g_storageInstance->GetPdb(g_defaultPdbId)->GetVFS();
        for (Size i = 0; i < FAKE_SIZE; i++) {
            UtBufferUtils::prepare_fake_file(vfs, FAKE_FILES[i].file_id, FAKE_FILES[i].max_block, FAKE_FILES[i].path);
        }
    }
    void TearDown() override
    {
        VFSAdapter *vfs = g_storageInstance->GetPdb(g_defaultPdbId)->GetVFS();
        for (Size i = 0; i < FAKE_SIZE; i++) {
            UtBufferUtils::remove_fake_file(vfs, FAKE_FILES[i].file_id, FAKE_FILES[i].path);
        }
        BufferMgrFakeStorageInstance *bufMgrInstance = (BufferMgrFakeStorageInstance *)g_storageInstance;
        bufMgrInstance->Shutdown();
        delete bufMgrInstance;
        DSTOREParamTest<std::vector<Size>>::TearDown();
    }

    struct TestParam {
        WalStream *walStream;
        FileId fileId;
        uint64 maxBlockSize;
        WalGroupLsnInfo lastWalRecord;
    };

    static void MakePageDirtyThread(TestParam *params)
    {
        WalGroupLsnInfo walRecordGroupPtr;
        WalStream *walStream = params->walStream;
        BufMgrInterface *bufferPool = g_storageInstance->GetBufferMgr();

        pthread_setname_np(pthread_self(), "MakePageDirty");

        for (uint64 i = 0; i < params->maxBlockSize; i++) {
            PageId pageId = {params->fileId, uint32(i)};
            BufferDesc *bufferDesc = bufferPool->Read(g_defaultPdbId, pageId, DSTORE::LW_EXCLUSIVE);
            uint64 *block = (uint64 *)(reinterpret_cast<char *>(bufferDesc->GetPage()) + sizeof(Page::m_header));
            *block = *block + 1;
            bufferPool->MarkDirty(bufferDesc);
            walRecordGroupPtr = walStream->Append(nullptr, 100);
            bufferPool->UnlockAndRelease(bufferDesc);
        }
        params->lastWalRecord = walRecordGroupPtr;
    }
};

INSTANTIATE_TEST_SUITE_P(ThreadNumber, BgPageWriterTest,
                         testing::Values(
                             /* thread_number */
                             std::vector<Size>{1}, std::vector<Size>{5}, std::vector<Size>{10}));

TEST_P(BgPageWriterTest, BgPageWriterMultiThreadTest_TIER1)
{
    UTThreadPool threadPool;
    threadPool.Start(m_makePageDirtyThreadNum);

    TestParam params[m_makePageDirtyThreadNum];

    for (uint64 i = 0; i < m_makePageDirtyThreadNum; i++) {
        params[i].fileId = FAKE_FILES[i].file_id;
        params[i].maxBlockSize = FAKE_FILES[i].max_block;
        params[i].walStream = ((BgPageWriterFakeStorageInstance *)g_storageInstance)->GetWalStream();
        threadPool.AddTask(MakePageDirtyThread, &params[i]);
    }
    threadPool.WaitAllTaskFinish();
    threadPool.Shutdown();

    uint64 lastPLsn = 0;
    for (uint64 i = 0; i < m_makePageDirtyThreadNum; i++) {
        lastPLsn = DstoreMax(lastPLsn, params[i].lastWalRecord.m_endPlsn);
        params[i].walStream->WaitTargetPlsnPersist(params[i].lastWalRecord.m_endPlsn);
    }

    for (uint64 i = 0; i < m_makePageDirtyThreadNum; i++) {
        for (BlockNumber blockNumber = 0; blockNumber < FAKE_FILES[i].max_block; blockNumber++) {
            PageId pageId = {FAKE_FILES[i].file_id, blockNumber};
UT_TRY_AGAIN:
            BufferDesc *bufferDesc =
                g_storageInstance->GetBufferMgr()->Read(g_defaultPdbId, pageId, DSTORE::LW_SHARED);
            uint64 state = bufferDesc->LockHdr();
            if (state & Buffer::BUF_CONTENT_DIRTY) {
                bufferDesc->UnlockHdr(state);
                g_storageInstance->GetBufferMgr()->UnlockAndRelease(bufferDesc);
                goto UT_TRY_AGAIN;
            }
            bufferDesc->UnlockHdr(state);
            g_storageInstance->GetBufferMgr()->UnlockAndRelease(bufferDesc);
        }
    }

    VFSAdapter *vfs = g_storageInstance->GetPdb(g_defaultPdbId)->GetVFS();
    char block[BLCKSZ];
    for (uint64 i = 0; i < m_makePageDirtyThreadNum; i++) {
        for (BlockNumber blockNumber = 0; blockNumber < FAKE_FILES[i].max_block; blockNumber++) {
            vfs->ReadPageSync({.m_fileId = FAKE_FILES[i].file_id, .m_blockId = blockNumber}, block);
            uint64 *num = (uint64 *) (block + sizeof(Page::m_header));
            EXPECT_EQ(*num, 1);
        }
    }
}

TEST_P(BgPageWriterTest, FlushAllDirtyPagesTest_TIER1)
{
    UTThreadPool threadPool;
    threadPool.Start(m_makePageDirtyThreadNum);

    TestParam params[m_makePageDirtyThreadNum];

    for (uint64 i = 0; i < m_makePageDirtyThreadNum; i++) {
        params[i].fileId = FAKE_FILES[i].file_id;
        params[i].maxBlockSize = FAKE_FILES[i].max_block;
        params[i].walStream = ((BgPageWriterFakeStorageInstance *)g_storageInstance)->GetWalStream();
        threadPool.AddTask(MakePageDirtyThread, &params[i]);
    }
    threadPool.WaitAllTaskFinish();
    threadPool.Shutdown();

    for (uint64 i = 0, lastPLsn = 0; i < m_makePageDirtyThreadNum; i++) {
        lastPLsn = DstoreMax(lastPLsn, params[i].lastWalRecord.m_endPlsn);
        params[i].walStream->WaitTargetPlsnPersist(params[i].lastWalRecord.m_endPlsn);
    }

    static_cast<BgPageWriterFakeStorageInstance *>(g_storageInstance)->m_bgPageWriterMgr->FlushAllDirtyPages();

    VFSAdapter *vfs = g_storageInstance->GetPdb(g_defaultPdbId)->GetVFS();
    char block[BLCKSZ];
    for (uint64 i = 0; i < m_makePageDirtyThreadNum; i++) {
        for (BlockNumber blockNumber = 0; blockNumber < FAKE_FILES[i].max_block; blockNumber++) {
            vfs->ReadPageSync({.m_fileId = FAKE_FILES[i].file_id, .m_blockId = blockNumber}, block);
            uint64 *num = (uint64 *) (block + sizeof(Page::m_header));
            EXPECT_EQ(*num, 1);
        }
    }
}
