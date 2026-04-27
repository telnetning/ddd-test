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
 *
 * ut_wal_basic.cpp
 *
 * Description:
 *
 * ---------------------------------------------------------------------------------------
 *
 */

#include "ut_wal/ut_wal_basic.h"
#include "ut_buffer/ut_buffer_fake_instance.h"
#include "vfs/vfs_interface.h"

namespace DSTORE {


void WALBASICTEST::SetUp()
{
    DSTORETEST::SetUp();
    UtInstance *instance = DstoreNew(m_ut_memory_context)UtInstance();
    instance->Install(&DSTORETEST::m_guc, m_ut_memory_context);
    instance->Startup(&DSTORETEST::m_guc);
    Prepare();
}

void WALBASICTEST::TearDown()
{
    Clear();
    DSTORETEST::TearDown();
}

void WALBASICTEST::Prepare(uint32 walFileSize) {
    if (g_storageInstance == nullptr) {
        DSTORETEST::m_guc.walFileSize = walFileSize;
        UtInstance *instance = DstoreNew(m_ut_memory_context)UtInstance();
        instance->Install(&DSTORETEST::m_guc, m_ut_memory_context);
        instance->Startup(&DSTORETEST::m_guc);
    }
    m_walControlFile = g_storageInstance->GetPdb(g_defaultPdbId)->GetControlFile();
    m_walManager = g_storageInstance->GetPdb(g_defaultPdbId)->GetWalMgr();
    m_walStreamManager = const_cast<WalStreamManager *>(m_walManager->GetWalStreamManager());
    m_walRedoManager =  &m_walManager->GetWalRedoManager();
    m_walWriter = thrd->m_walWriterContext;
    uint32 streamCount = 0;
    m_walStream = m_walStreamManager->GetWritingWalStream();
}

RetStatus WALBASICTEST::ReadRecordsAfterPlsn(uint64 plsn, WalType walType, std::vector<WalRecordRedoInfo *> &recordList)
{
    const WalRecordAtomicGroup *walGroup = nullptr;
    const WalRecord *walRecord;
    RetStatus retStatus;
    m_walRecordReader->SetReadStartPlsn(plsn);
    do {
        retStatus = m_walRecordReader->ReadNext(&walGroup);
        if (retStatus != DSTORE_SUCC) {
            return DSTORE_FAIL;
        }
        if (walGroup == nullptr) {
            break;
        }
        do {
            walRecord = m_walRecordReader->GetNextWalRecord();
            if (walRecord == nullptr) {
                break;
            }
            if (walRecord->m_type == walType || walType == WAL_TYPE_BUTTOM) {
                uint64 endPlsn = m_walRecordReader->GetCurRecordEndPlsn();
                BuffForDecompress buffForDecompress{nullptr, 0};
                const WalRecord *record = walRecord;
                if (WalRecovery::IsSupportCompress(walRecord->GetType())) {
                    buffForDecompress.bufferSize = INIT_TEMP_BUFF_SIZE;
                    buffForDecompress.buffer = static_cast<char *>(DstorePalloc0(INIT_TEMP_BUFF_SIZE));
                    record = WalRecovery::DecompressProc(walRecord, &buffForDecompress);
                }
                StorageAssert(record != nullptr);
                WalRecordRedoInfo *recordBuf = (WalRecordRedoInfo *)DstorePalloc0(record->GetSize() + sizeof(uint64));
                recordBuf->endPlsn = endPlsn;
                errno_t rc = memcpy_s(static_cast<char *>((char *)recordBuf + sizeof(uint64)), record->GetSize(),
                                      static_cast<const char *>(static_cast<const void *>(record)), record->GetSize());
                storage_securec_check(rc, "\0", "\0");
                DstorePfreeExt(buffForDecompress.buffer);
                recordList.push_back(reinterpret_cast<WalRecordRedoInfo *>(recordBuf));
            }
        } while (true);
    } while (true);
    return DSTORE_SUCC;
}

void WALBASICTEST::Clear()
{
    /* no need to delete m_walManager and m_walWriter */
    UtInstance *instance = (UtInstance *)g_storageInstance;
    instance->Shutdown();
    delete instance;

}

void WALBASICTEST::PrepareControlFileContent(NodeId selfNodeId)
{
    RetStatus retStatus;
    uint32 walFileCount = static_cast<uint32>(g_storageInstance->GetGuc()->walFileNumber);

    for (int i = 0; i < STREAM_COUNT; i++) {
        ControlWalStreamPageItemData *itemData =
            (ControlWalStreamPageItemData *) DstorePalloc0(sizeof(ControlWalStreamPageItemData));
        itemData->walId = (selfNodeId << NODE_ID_WAL_ID_FACTOR) + i;
        WalCheckPoint walCheckPoint = {0, 0};
        itemData->lastWalCheckpoint = walCheckPoint;
        itemData->createFilePara.flag = APPEND_WRITE_FILE;
        itemData->createFilePara.rangeSize = WAL_FILE_RANGE_SIZE;
        itemData->createFilePara.maxSize = static_cast<uint64>(g_storageInstance->GetGuc()->walFileSize);
        itemData->createFilePara.mode = FILE_READ_AND_WRITE_MODE;
        itemData->createFilePara.isReplayWrite = false;
        TenantConfig *tenantConfig = g_storageInstance->GetGuc()->tenantConfig;
        StorageAssert(tenantConfig);
        errno_t rc = strcpy_s(itemData->createFilePara.storeSpaceName, STORESPACE_NAME_MAX_LEN,
                            tenantConfig->storeSpaces[0].storeSpaceName);
        storage_securec_check(rc, "\0", "\0");
        itemData->initWalFileCount = walFileCount;
        retStatus = m_walControlFile->AddWalStream(*itemData);
        ASSERT_EQ(retStatus, DSTORE_SUCC);
        DstorePfreeExt(itemData);
    }

    /* check WalStreamWalFileInfo */
    for (int i = 0; i < STREAM_COUNT; i++) {
        WalId walId = static_cast<uint16>((selfNodeId << NODE_ID_WAL_ID_FACTOR) + i);
        ControlWalStreamPageItemData *itemData;
        retStatus = m_walControlFile->GetWalStreamInfo(walId, &itemData);
        ASSERT_EQ(retStatus, DSTORE_SUCC);
        ASSERT_EQ(itemData->walId, walId);
        ASSERT_EQ(itemData->initWalFileCount, walFileCount);
        m_walControlFile->FreeWalStreamsInfo(itemData);
    }
}

void WALBASICTEST::MarkBufferDirty(BufferDesc *bufferDesc)
{
    ASSERT_TRUE(bufferDesc != nullptr);
    bufferDesc->state |= Buffer::BUF_CONTENT_DIRTY;
    modifiedBuffers.push_back(bufferDesc);
}

void WALBASICTEST::UnmarkBufferDirty(BufferDesc *bufferDesc)
{
    ASSERT_TRUE(bufferDesc != nullptr);
    bufferDesc->state &= ~Buffer::BUF_CONTENT_DIRTY;
}

void WALBASICTEST::ClearDirtyFlags()
{
    for (BufferDesc *buffer : modifiedBuffers) {
        UnmarkBufferDirty(buffer);
    }
    modifiedBuffers.clear();
}

WalRecord *WALBASICTEST::BuildDDLWal(uint16 len)
{
    StorageAssert(len > sizeof(WalRecord));
    WalRecord *record = (WalRecord *)calloc(len, sizeof(char));
    char *record_data = reinterpret_cast<char *>(record);
    for (uint16 i = 0; i < len; i++) {
        record_data[i] = 'a' + i % 26;
    }
    record->SetSize(len);
    record->SetType(WalType::WAL_EMPTY_DDL_REDO);
    return record;
}

WalRecord *WALBASICTEST::BuildWal(uint16 len)
{
    StorageAssert(len > sizeof(WalRecord));
    WalRecord *record = (WalRecord *)calloc(len, sizeof(char));
    char *record_data = reinterpret_cast<char *>(record);
    for (uint16 i = 0; i < len; i++) {
        record_data[i] = 'a' + i % 26;
    }
    record->SetSize(len);
    record->SetType(WalType::WAL_EMPTY_REDO);
    return record;
}

WalRecordForPage *WALBASICTEST::BuildWalForPage(WalType type, uint16 len, uint64 fileVersion)
{
    WalRecordForPage *walDataPtr = (WalRecordForPage *)DstorePalloc0(len);
    WalPageHeaderContext pageHeader{type, len, PageId{2, 100}, 0, 100, 200, false, fileVersion};
    walDataPtr->SetWalPageHeader(pageHeader);
    return walDataPtr;
}

void WALBASICTEST::CheckWalRecordForPage(const WalRecordForPage *decompressRecord,
                                         const WalRecordForPage *orignalRecord)
{
    ASSERT_NE(decompressRecord, nullptr);
    ASSERT_EQ(decompressRecord->GetSize(), orignalRecord->GetSize());
    ASSERT_EQ(decompressRecord->GetType(), orignalRecord->GetType());
    ASSERT_EQ(decompressRecord->m_pageId, orignalRecord->m_pageId);
    ASSERT_EQ(decompressRecord->m_pagePreWalId, orignalRecord->m_pagePreWalId);
    ASSERT_EQ(decompressRecord->m_pagePrePlsn, orignalRecord->m_pagePrePlsn);
    ASSERT_EQ(decompressRecord->m_pagePreGlsn, orignalRecord->m_pagePreGlsn);
    ASSERT_EQ(decompressRecord->m_filePreVersion, orignalRecord->m_filePreVersion);
    ASSERT_EQ(decompressRecord->m_flags.m_flag.glsnChangeFlag, orignalRecord->m_flags.m_flag.glsnChangeFlag);
    ASSERT_EQ(
        decompressRecord->m_flags.m_flag.containFileVersionFlag, orignalRecord->m_flags.m_flag.containFileVersionFlag);
}

WalRecord *WALBASICTEST::CompressRecord(WalRecord *record)
{
    StorageAssert(record != nullptr);
    uint32 walDataSize = record->GetSize();
    const WalRecordCompressAndDecompressItem *walRecordItem = nullptr;
    if (record->GetType() >= WAL_UNDO_INIT_MAP_SEGMENT && record->GetType() <= WAL_TXN_ABORT) {
        walRecordItem = WalRecordUndo::GetWalRecordItem(record->GetType());
    }
    uint16 compressSize = MAX_WAL_RECORD_FOR_PAGE_COMPRESSED_SIZE;
    if (walRecordItem != nullptr) {
        compressSize = walRecordItem->getMaxCompressedSize(record);
    }
    walDataSize += compressSize;
    WalRecord *logRecord = static_cast<WalRecord *>(DstorePalloc0(walDataSize));
    m_walWriter->CompressProcess(record, logRecord, walDataSize, walRecordItem);
    return logRecord;
}
WalRecord *WALBASICTEST::DecompressRecord(const WalRecord *record)
{
    StorageAssert(record != nullptr);
    BuffForDecompress buffForDecompress;
    uint32 compressedSize = record->GetSize();
    buffForDecompress.bufferSize = compressedSize + MAX_WAL_RECORD_FOR_PAGE_DECOMPRESSED_SIZE;
    buffForDecompress.buffer =
        static_cast<char *>(DstorePalloc0(compressedSize + MAX_WAL_RECORD_FOR_PAGE_DECOMPRESSED_SIZE));
    return static_cast<WalRecord *>(WalRecovery::DecompressProc(record, &buffForDecompress));
}
}
