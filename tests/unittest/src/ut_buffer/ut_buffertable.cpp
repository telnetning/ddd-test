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
#include <thread>

#include "ut_buffer/ut_buffer_fake_instance.h"
#include "ut_buffer/ut_buffer_util.h"
#include "buffer/dstore_buf_table.h"

using namespace DSTORE;

class BufferTableTest : public DSTORETEST {
protected:
    void SetUp() override
    {
        DSTORETEST::SetUp();
        InstallDatabase(&DSTORETEST::m_guc, m_ut_memory_context);
        BufferMgrFakeStorageInstance *bufMgrInstance = DstoreNew(m_ut_memory_context)BufferMgrFakeStorageInstance();
        bufMgrInstance->Startup(&DSTORETEST::m_guc);

#ifdef ENABLE_FAULT_INJECTION

        FaultInjectionEntry entries[] = {
            FAULT_INJECTION_ENTRY(DstoreBufMgrFI::BUFTABLE_INSERT_SYNC1, false, nullptr),
            FAULT_INJECTION_ENTRY(DstoreBufMgrFI::BUFTABLE_INSERT_SYNC2, false, nullptr),
            FAULT_INJECTION_ENTRY(DstoreBufMgrFI::BUFTABLE_INSERT_SYNC3, false, nullptr),
            FAULT_INJECTION_ENTRY(DstoreBufMgrFI::BUFTABLE_INSERT_SYNC4, false, nullptr),
            FAULT_INJECTION_ENTRY(DstoreBufMgrFI::BUFTABLE_INSERT_SYNC5, false, nullptr),
            FAULT_INJECTION_ENTRY(DstoreBufMgrFI::BUFTABLE_REMOVE_SYNC1, false, nullptr),
            FAULT_INJECTION_ENTRY(DstoreBufMgrFI::BUFTABLE_REMOVE_SYNC2, false, nullptr),
        };
        ASSERT_EQ(RegisterFaultInjection(entries, sizeof(entries) / sizeof(entries[0]), FI_GLOBAL), ERROR_SYS_OK);

#endif
    }
    void TearDown() override
    {
        BufferMgrFakeStorageInstance *bufMgrInstance = (BufferMgrFakeStorageInstance *)g_storageInstance;
        bufMgrInstance->Shutdown();
        delete bufMgrInstance;
        DSTORETEST::TearDown();

#ifdef ENABLE_FAULT_INJECTION
        DestroyFaultInjectionHash(FI_GLOBAL);
#endif
    }
};

/* buffer table */
TEST_F(BufferTableTest, BaseTest)
{
    int total_buf = 1024;

    BufTable *table = DstoreNew(g_dstoreCurrentMemoryContext) BufTable(total_buf);
    table->Initialize();
    BufferDesc *bufferDescs = DstoreNew(g_dstoreCurrentMemoryContext) BufferDesc[total_buf];

    for (uint i = 0; i < total_buf; i++) {
        BufferTag bufTag = {0, {1, i}};
        bufferDescs[i].bufTag = bufTag;
        GsAtomicInitU64(&bufferDescs[i].state, 0);
        uint32 hashCode = table->GetHashCode(&bufTag);
        table->LockBufMapping(hashCode, LW_EXCLUSIVE);
        BufferDesc *buf = table->Insert(&bufTag, hashCode, &bufferDescs[i]);
        ASSERT_EQ(buf, INVALID_BUFFER_DESC);
        table->UnlockBufMapping(hashCode);
    }

    for (uint i = 0; i < total_buf; i++) {
        BufferTag bufTag = {0, {1, i}};

        uint32 hashCode = table->GetHashCode(&bufTag);
        table->LockBufMapping(hashCode, LW_EXCLUSIVE);
        BufferDesc *buf = table->Insert(&bufTag, hashCode, &bufferDescs[i]);
        ASSERT_EQ(buf, &bufferDescs[i]);
        table->UnlockBufMapping(hashCode);

        table->LockBufMapping(hashCode, LW_SHARED);
        buf = table->LookUp(&bufTag, hashCode);
        ASSERT_EQ(buf, &bufferDescs[i]);
        buf->Unpin();
        table->UnlockBufMapping(hashCode);

        table->LockBufMapping(hashCode, LW_EXCLUSIVE);
        table->Remove(&bufTag, hashCode);
        table->UnlockBufMapping(hashCode);

        table->LockBufMapping(hashCode, LW_SHARED);
        buf = table->LookUp(&bufTag, hashCode);
        table->UnlockBufMapping(hashCode);
        ASSERT_EQ(buf, INVALID_BUFFER_DESC);
    }

    delete[] bufferDescs;
    delete table;
}

struct BufferTableTestCtx {
    uint32 start;
    uint32 end;

    BufTable *table;
    BufferDesc **buffers;
};

void *hash_table_insert_proc(void *param)
{
    BufferTableTestCtx *ctx = static_cast<BufferTableTestCtx *>(param);
    BufTable *table = ctx->table;
    BufferDesc **buffers = ctx->buffers;

    create_thread_and_register();

    for (uint32 i = ctx->start; i < ctx->end; i++) {
        BufferDesc *buffer = buffers[i];
        BufferTag bufTag = buffer->bufTag;

        uint32 hash_code = table->GetHashCode(&bufTag);
        table->LockBufMapping(hash_code, LW_EXCLUSIVE);
        table->Insert(&bufTag, hash_code, buffer);
        table->UnlockBufMapping(hash_code);
    }

    unregister_thread();

    return nullptr;
}

/* We should enable this test when shared memory context is implemented */
TEST_F(BufferTableTest, BufferTableConcurrentInsertTest_TIER1)
{
    Size thread_num = 10;
    Size entry_num = 100000;

    entry_num = entry_num % thread_num == 0 ? entry_num : (entry_num / thread_num + 1) * (thread_num);

    Size delta = entry_num / thread_num;

    BufTable *table = DstoreNew(m_ut_memory_context) BufTable(entry_num);
    table->Initialize();

    BufferDesc **buffers = UtBufferUtils::prepare_buffer(entry_num);

    BufferTableTestCtx buffer_table_test_ctx[thread_num];
    std::thread threads[thread_num];

    for (Size i = 0; i < thread_num; i++) {
        buffer_table_test_ctx[i].start = i * delta;
        buffer_table_test_ctx[i].end = (i + 1) * delta;
        buffer_table_test_ctx[i].table = table;
        buffer_table_test_ctx[i].buffers = buffers;

        threads[i] = std::thread(hash_table_insert_proc, (void *)(&buffer_table_test_ctx[i]));
        pthread_setname_np(threads[i].native_handle(), "hash_table_insert_proc");
    }

    for (Size i = 0; i < thread_num; i++) {
        threads[i].join();
    }

    for (uint32 i = 0; i < entry_num; i++) {
        BufferTag bufTag = {g_defaultPdbId, {UtBufferUtils::DEFAULT_FILE_ID, i}};
        uint32 hash_code = table->GetHashCode(&bufTag);
        BufferDesc *buffer = table->LookUp(&bufTag, hash_code);
        ASSERT_EQ(buffer, buffers[i]);
        buffer->Unpin();
    }

    delete table;
    UtBufferUtils::free_buffer(buffers, entry_num);
}

void HashTableInsertProc(BufTable *table, BufferTag bufTag, BufferDesc **buffers)
{
    create_thread_and_register();
    uint32 hashCode = table->GetHashCode(&bufTag);
    table->Insert(&bufTag, hashCode, buffers[0]);
    unregister_thread();
}

void HashTableRemoveProc(BufTable *table, BufferTag bufTag)
{
    create_thread_and_register();
    uint32 hashCode = table->GetHashCode(&bufTag);
    table->Remove(&bufTag, hashCode);
    unregister_thread();
}

TEST_F(BufferTableTest, BufferTableLockLessLookup_TIER1)
{
    Size entryNum = 1024;
    BufTable *table = DstoreNew(m_ut_memory_context) BufTable(entryNum);
    table->Initialize();

    BufferTag oldBufferTag = {1, {1, 1}};
    BufferTag newBufferTag = {1, {1, 2}};
    BufferDesc **buffers = UtBufferUtils::prepare_buffer(1);
    buffers[0]->bufTag = oldBufferTag;
    uint32 oldHashCode = table->GetHashCode(&oldBufferTag);
    table->Insert(&oldBufferTag, oldHashCode, buffers[0]);
    BufferDesc *ret = table->LookUp(&oldBufferTag, oldHashCode);
    ASSERT_EQ(ret, buffers[0]);
    ASSERT_EQ(ret->bufTag, oldBufferTag);
    buffers[0]->Unpin();

    FAULT_INJECTION_ACTIVE(DstoreBufMgrFI::BUFTABLE_INSERT_SYNC1, FI_GLOBAL);
    FAULT_INJECTION_ACTIVE(DstoreBufMgrFI::BUFTABLE_INSERT_SYNC2, FI_GLOBAL);
    FAULT_INJECTION_ACTIVE(DstoreBufMgrFI::BUFTABLE_INSERT_SYNC3, FI_GLOBAL);
    FAULT_INJECTION_ACTIVE(DstoreBufMgrFI::BUFTABLE_INSERT_SYNC4, FI_GLOBAL);
    FAULT_INJECTION_ACTIVE(DstoreBufMgrFI::BUFTABLE_INSERT_SYNC5, FI_GLOBAL);
    FAULT_INJECTION_ACTIVE(DstoreBufMgrFI::BUFTABLE_REMOVE_SYNC1, FI_GLOBAL);
    FAULT_INJECTION_ACTIVE(DstoreBufMgrFI::BUFTABLE_REMOVE_SYNC2, FI_GLOBAL);

    std::thread insertThread = std::thread(HashTableInsertProc, table, newBufferTag, buffers);
    uint32 newHashCode = table->GetHashCode(&newBufferTag);

    FAULT_INJECTION_WAIT(DstoreBufMgrFI::BUFTABLE_INSERT_SYNC1);
    ret = table->LookUp(&newBufferTag, newHashCode);
    ASSERT_EQ(ret, nullptr);
    FAULT_INJECTION_NOTIFY(DstoreBufMgrFI::BUFTABLE_INSERT_SYNC2);

    FAULT_INJECTION_WAIT(DstoreBufMgrFI::BUFTABLE_INSERT_SYNC3);
    ret = table->LookUp(&newBufferTag, newHashCode);
    ASSERT_EQ((ret == nullptr || ret->bufTag != newBufferTag), true);
    FAULT_INJECTION_NOTIFY(DstoreBufMgrFI::BUFTABLE_INSERT_SYNC4);

    FAULT_INJECTION_WAIT(DstoreBufMgrFI::BUFTABLE_INSERT_SYNC5);
    ret = table->LookUp(&newBufferTag, newHashCode);
    ASSERT_EQ(ret, buffers[0]);
    ASSERT_EQ(ret->bufTag != newBufferTag, true);

    insertThread.join();
    buffers[0]->bufTag = newBufferTag;
    ret = table->LookUp(&newBufferTag, newHashCode);
    ASSERT_EQ(ret, buffers[0]);
    ASSERT_EQ(ret->bufTag, newBufferTag);
    buffers[0]->Unpin();
    FAULT_INJECTION_INACTIVE(DstoreBufMgrFI::BUFTABLE_INSERT_SYNC1, FI_GLOBAL);
    FAULT_INJECTION_INACTIVE(DstoreBufMgrFI::BUFTABLE_INSERT_SYNC2, FI_GLOBAL);
    FAULT_INJECTION_INACTIVE(DstoreBufMgrFI::BUFTABLE_INSERT_SYNC3, FI_GLOBAL);
    FAULT_INJECTION_INACTIVE(DstoreBufMgrFI::BUFTABLE_INSERT_SYNC4, FI_GLOBAL);
    FAULT_INJECTION_INACTIVE(DstoreBufMgrFI::BUFTABLE_INSERT_SYNC5, FI_GLOBAL);

    std::thread RemoveVisibleThread = std::thread(HashTableRemoveProc, table, oldBufferTag);
    FAULT_INJECTION_WAIT(DstoreBufMgrFI::BUFTABLE_REMOVE_SYNC1);
    ret = table->LookUp(&oldBufferTag, oldHashCode);
    ASSERT_EQ(ret, nullptr);
    FAULT_INJECTION_NOTIFY(DstoreBufMgrFI::BUFTABLE_REMOVE_SYNC2);
    ret = table->LookUp(&oldBufferTag, oldHashCode);
    ASSERT_EQ(ret, nullptr);
    RemoveVisibleThread.join();

    FAULT_INJECTION_INACTIVE(DstoreBufMgrFI::BUFTABLE_REMOVE_SYNC1, FI_GLOBAL);
    FAULT_INJECTION_INACTIVE(DstoreBufMgrFI::BUFTABLE_REMOVE_SYNC2, FI_GLOBAL);
    delete table;
    UtBufferUtils::free_buffer(buffers, 1);
}
