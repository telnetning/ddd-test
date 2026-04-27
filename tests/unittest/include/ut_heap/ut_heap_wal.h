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
#ifndef DSTORE_UT_HEAP_WAL_H
#define DSTORE_UT_HEAP_WAL_H

#include "fault_injection/fault_injection.h"
#include "common/fault_injection/dstore_undo_fault_injection.h"
#include "common/fault_injection/dstore_transaction_fault_injection.h"
#include "heap/dstore_heap_wal_struct.h"
#include "ut_utilities/ut_dstore_framework.h"
#include "ut_tablehandler/ut_table_handler.h"
#include "ut_wal/ut_wal_basic.h"
#include "ut_heap/ut_heap.h"

using namespace DSTORE;

class HeapWalTest : public WALBASICTEST, public UTHeap {
protected:
    void SetUp() override
    {
        DSTORETEST::SetUp();
        MockStorageInstance *instance = DstoreNew(m_ut_memory_context) MockStorageInstance();
        instance->Install(&DSTORETEST::m_guc, m_ut_memory_context);
        instance->Startup(&DSTORETEST::m_guc);
        m_utTableHandler = UTTableHandler::CreateTableHandler(g_defaultPdbId, m_ut_memory_context);
        m_walControlFile = g_storageInstance->GetPdb(g_defaultPdbId)->GetControlFile();
        m_walManager = g_storageInstance->GetPdb(g_defaultPdbId)->GetWalMgr();
        m_walStreamManager = const_cast<WalStreamManager *>(m_walManager->GetWalStreamManager());
        m_walWriter = thrd->m_walWriterContext;

        /* enable Wal flushing */
        PrepareControlFileContent();
        int ret = mkdir("dstore_wal", 0777);
        ASSERT_EQ(ret, 0);
        NodeId selfNode = 0; /* selfNode must be assigned a correct value */
        RetStatus retStatus = m_walStreamManager->Init(m_walControlFile);
        ASSERT_EQ(retStatus, DSTORE_SUCC);
        m_walStream = m_walStreamManager->GetWritingWalStream();
        WalReaderConf readerConf = {0, 0, m_walStream, nullptr,
                                    static_cast<uint64>(g_storageInstance->GetGuc()->walFileSize),
                                    DSTORE::WalReadSource::WAL_READ_FROM_DISK};
        retStatus = WalRecordReader::AllocateWalReader(readerConf, &m_walRecordReader, m_ut_memory_context);
        ASSERT_EQ(retStatus, DSTORE_SUCC);
        m_snapshotThread = nullptr;

#ifdef ENABLE_FAULT_INJECTION
        FaultInjectionEntry entries[] = {
            FAULT_INJECTION_ENTRY(DstoreUndoFI::SET_FREE_PAGE_NUM_ZERO, false, SetUndoZoneFreePageNumZero),
            FAULT_INJECTION_ENTRY(DstoreTransactionFI::EXTEND_UNDO_SPACE_FAIL, false, nullptr),
            FAULT_INJECTION_ENTRY(DstoreTransactionFI::INSERT_UNDO_RECORD_FAIL, false, nullptr)};
        ASSERT_EQ(RegisterFaultInjection(entries, sizeof(entries) / sizeof(entries[0]), FI_GLOBAL), ERROR_SYS_OK);
#endif
    }

    void TearDown() override
    {
        DestroySnapshotThrdIfNeed();
        UTTableHandler::Destroy(m_utTableHandler);
        m_utTableHandler = nullptr;
        MockStorageInstance *instance = (MockStorageInstance *)g_storageInstance;
        instance->Shutdown();
        delete instance;
        DSTORETEST::TearDown();
    }

    bool CmpTDInfo(TD* td1, TD* td2)
    {
        /* ignore m_lockerXid and m_commandId*/
        if (td1->m_xid != td2->m_xid) {
            return false;
        }
        if (td1->m_csnStatus == td2->m_csnStatus && td1->m_csn != td2->m_csn) {
            return false;
        }
        if (td1->m_undoRecPtr != td2->m_undoRecPtr) {
            return false;
        }
        if (td1->m_status != td2->m_status) {
            return false;
        }
        if (td1->m_pad != td2->m_pad) {
            return false;
        }
        return true;
    }

    void RedoHeapRecord(WalRecordRedoContext *redoCtx, WalRecordHeap *heapRecord, uint64 plsn)
    {
        BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
        BufferDesc *bufferDesc = bufMgr->Read(g_defaultPdbId,
            heapRecord->m_pageId, LW_EXCLUSIVE);
        redoCtx->recordEndPlsn = m_walStream->GetMaxAppendedPlsn();
        WalRecordHeap::RedoHeapRecord(redoCtx, heapRecord, bufferDesc);
        (void)bufMgr->MarkDirty(bufferDesc);
        bufMgr->UnlockAndRelease(bufferDesc);
    }

    void FetchPage(char* page, PageId pageId)
    {
        BufferDesc *bufferDesc = g_storageInstance->GetBufferMgr()->Read(g_defaultPdbId,
            pageId, DSTORE::LW_EXCLUSIVE);
        errno_t rc = memcpy_s(page, BLCKSZ, bufferDesc->GetPage(), BLCKSZ);
        storage_securec_check(rc, "\0", "\0");
        g_storageInstance->GetBufferMgr()->UnlockAndRelease(bufferDesc);
    }

    void RestorePage(char* page, PageId pageId)
    {
        BufferDesc *bufferDesc = g_storageInstance->GetBufferMgr()->Read(g_defaultPdbId,
            pageId, DSTORE::LW_EXCLUSIVE);
        errno_t rc = memcpy_s(bufferDesc->GetPage(), BLCKSZ, page, BLCKSZ);
        storage_securec_check(rc, "\0", "\0");
        g_storageInstance->GetBufferMgr()->UnlockAndRelease(bufferDesc);
    }

    void InitPage(PageId pageId)
    {
        BufferDesc *bufferDesc = g_storageInstance->GetBufferMgr()->Read(g_defaultPdbId,
            pageId, DSTORE::LW_EXCLUSIVE);
        HeapPage::InitHeapPage(bufferDesc, pageId, {{1,1}, 1});
        g_storageInstance->GetBufferMgr()->UnlockAndRelease(bufferDesc);
    }

    void WalDumpInfo(WalRecordHeap *record)
    {
        FILE *fp = fopen("waldump.txt", "w+");
        WalRecordHeap::DumpHeapRecord(record, fp);
        fclose(fp);
    }
};

#endif /* DSTORE_UT_HEAP_WAL_H */

