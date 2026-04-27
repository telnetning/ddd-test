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
#ifndef UT_UNDO_MGR_H
#define UT_UNDO_MGR_H

#include "ut_utilities/ut_dstore_framework.h"
#include "ut_wal/ut_wal_basic.h"
#include "ut_mock/ut_mock.h"
#include "undo/dstore_undo_wal.h"


class UndoWalTest : public WALBASICTEST {
protected:
    void SetUp() override
    {
        DSTORETEST::SetUp();
        WALBASICTEST::Prepare();

        m_bufferMgr = g_storageInstance->GetBufferMgr();
        m_tablespaceId = static_cast<TablespaceId>(TBS_ID::UNDO_TABLE_SPACE_ID);
        m_segment = (Segment *)SegmentInterface::AllocUndoSegment(g_defaultPdbId, m_tablespaceId,
            DSTORE::SegmentType::UNDO_SEGMENT_TYPE, m_bufferMgr);
        ASSERT_TRUE(SegmentIsValid(m_segment));

        /* enable Wal flushing */
        PrepareControlFileContent();
        NodeId selfNode = 0; /* todo selfNode must be assigned a correct value */
        RetStatus retStatus = m_walStreamManager->Init(m_walControlFile);
        ASSERT_EQ(retStatus, DSTORE_SUCC);
        m_walStream = m_walStreamManager->GetWritingWalStream();
        WalReaderConf readerConf = {0, 0, m_walStream, nullptr,
                                    static_cast<uint64>(g_storageInstance->GetGuc()->walFileSize),
                                    DSTORE::WalReadSource::WAL_READ_FROM_DISK};
        retStatus = WalRecordReader::AllocateWalReader(readerConf, &m_walRecordReader, m_ut_memory_context);
        ASSERT_EQ(retStatus, DSTORE_SUCC);
        m_waldump_fp = fopen("waldump.txt", "w+");

#ifdef ENABLE_FAULT_INJECTION
        FaultInjectionEntry entries[] = {
            FAULT_INJECTION_ENTRY(DstoreUndoFI::PERSIST_INIT_TRANSACTION_SLOT_FAIL, false, nullptr),
            FAULT_INJECTION_ENTRY(DstoreUndoFI::PERSIST_INIT_UNDO_RECORD_FAIL, false, nullptr)};
        ASSERT_EQ(RegisterFaultInjection(entries, sizeof(entries) / sizeof(entries[0]), FI_GLOBAL), ERROR_SYS_OK);
#endif
    }

    void TearDown() override
    {
        UtInstance *instance = (UtInstance *)g_storageInstance;
        instance->Shutdown();
        delete instance;
        fclose(m_waldump_fp);
        DSTORETEST::TearDown();
    }

    bool CmpUndoPageHdr(char *page1, char *page2)
    {
        return memcmp(page1 + sizeof(Page), page2 + sizeof(Page), sizeof(UndoRecordPageHeader)) == 0;
    }

    template<class T>
    bool CheckUndoRingPageHdr(const std::vector<WalRecordRedoInfo *> &records)
    {
        char redoPage[BLCKSZ];
        char afterPage[BLCKSZ];
        for (WalRecordRedoInfo *recordInfo : records) {
            const T *walRec = static_cast<T *>(&(recordInfo->walRecord));
            FetchPage(afterPage, walRec->GetPageId());
            ReadDataFileImage(walRec->GetPageId(), redoPage, BLCKSZ);
            RestorePage(redoPage, walRec->GetPageId());
            WalRecordRedoContext redoCtx = {INVALID_XID, 0, DSTORE::g_defaultPdbId};
            RedoUndoRecord(&redoCtx, walRec, m_walStream->GetMaxAppendedPlsn());
            FetchPage(redoPage, walRec->GetPageId());
            if (!CmpUndoPageHdr(redoPage, afterPage)) {
                return false;
            }
        }
        return true;
    }

    void RedoUndoRecord(WalRecordRedoContext *redoCtx, const WalRecordUndo *undoRecord, uint64 plsn)
    {
        BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
        BufferDesc *bufferDesc = bufMgr->Read(g_defaultPdbId,
            undoRecord->m_pageId, LW_EXCLUSIVE);
        STORAGE_CHECK_BUFFER_PANIC(bufferDesc, MODULE_WAL, undoRecord->m_pageId);
        redoCtx->recordEndPlsn = plsn;
        WalRecordUndo::RedoUndoRecord(redoCtx, undoRecord, bufferDesc);
        (void)bufMgr->MarkDirty(bufferDesc);
        bufMgr->UnlockAndRelease(bufferDesc);
    }

    void FetchPage(char* page, PageId pageId)
    {
        BufferDesc *bufferDesc = m_bufferMgr->
            Read(g_defaultPdbId, pageId, DSTORE::LW_EXCLUSIVE);
        errno_t rc = memcpy_s(page, BLCKSZ, bufferDesc->GetPage(), BLCKSZ);
        storage_securec_check(rc, "\0", "\0");
        m_bufferMgr->UnlockAndRelease(bufferDesc);
    }

    void RestorePage(char* page, PageId pageId)
    {
        BufferDesc *bufferDesc = m_bufferMgr->
            Read(g_defaultPdbId, pageId, DSTORE::LW_EXCLUSIVE);
        errno_t rc = memcpy_s(bufferDesc->GetPage(), BLCKSZ, page, BLCKSZ);
        storage_securec_check(rc, "\0", "\0");
        m_bufferMgr->UnlockAndRelease(bufferDesc);
    }

    FILE *m_waldump_fp;
    Segment *m_segment;
    TablespaceId m_tablespaceId;
    BufMgrInterface *m_bufferMgr;
};

#endif