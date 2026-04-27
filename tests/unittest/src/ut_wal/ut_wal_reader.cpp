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
#include "ut_wal/ut_wal_reader.h"

/*#define DYNAMIC_LINK_VFS_LIB*/

constexpr uint32 UT_GROUP_HEADER_SIZE = sizeof(WalRecordAtomicGroup);

TEST_F (UTWalReaderTest, WalReadNextOverBlockOrWalFile)
{
    ASSERT_EQ(PrepareWalFile(), DSTORE_SUCC);
    WalGroupLsnInfo walRecordGroupPtr = {0, 0};
    /* Only first record need to consider start read plsn */
    m_record->SetSize(BLCKSZ - 100 - m_startReadPlsn);
    m_walWriter->AtomicInsertOneWal(m_record);
    m_record->SetSize(BLCKSZ - 200);
    m_walWriter->AtomicInsertOneWal(m_record);
    m_record->SetSize(BLCKSZ - sizeof(WalRecordAtomicGroup));
    m_walWriter->AtomicInsertOneWal(m_record);
    m_record->SetSize(BLCKSZ * 2);
    walRecordGroupPtr = m_walWriter->AtomicInsertOneWal(m_record);
    ASSERT_GT(walRecordGroupPtr.m_endPlsn, 0);
    m_walWriter->WaitTargetPlsnPersist(walRecordGroupPtr.m_walId, walRecordGroupPtr.m_endPlsn);

    WalReaderConf readerConf = {m_walStream->GetWalId(), m_startReadPlsn, m_walStream, nullptr, m_walFileSize, DSTORE::WalReadSource::WAL_READ_FROM_DISK};
    WalRecordReader *reader = DstoreNew(m_ut_memory_context) WalRecordReader(m_ut_memory_context, readerConf);
    reader->Init();
    ReadAndCheckWalRecordValid(4, reader, m_startReadPlsn, BLCKSZ - 100 - m_startReadPlsn, 1, DSTORE_SUCC);

    m_record->SetSize(m_walFileSize / 2);
    m_walWriter->AtomicInsertOneWal(m_record);
    m_record->SetSize( m_walFileSize - BLCKSZ);
    walRecordGroupPtr = m_walWriter->AtomicInsertOneWal(m_record);
    ASSERT_GT(walRecordGroupPtr.m_endPlsn, 0);
    m_walWriter->WaitTargetPlsnPersist(walRecordGroupPtr.m_walId, walRecordGroupPtr.m_endPlsn);

    ReadAndCheckWalRecordValid(2, reader, m_startReadPlsn, m_walFileSize / 2, 1, DSTORE_SUCC);

    m_record->SetSize(m_walFileSize);
    m_walWriter->AtomicInsertOneWal(m_record);
    m_record->SetSize(m_walFileSize * 2);
    walRecordGroupPtr = m_walWriter->AtomicInsertOneWal(m_record);
    ASSERT_GT(walRecordGroupPtr.m_endPlsn, 0);
    m_walWriter->WaitTargetPlsnPersist(walRecordGroupPtr.m_walId, walRecordGroupPtr.m_endPlsn);

    ReadAndCheckWalRecordValid(2, reader, m_startReadPlsn, m_walFileSize, 1, DSTORE_SUCC);
}

TEST_F (UTWalReaderTest, WalReadNextOverBlockOrWalFile2)
{
    ASSERT_EQ(PrepareWalFile(), DSTORE_SUCC);
    WalGroupLsnInfo walRecordGroupPtr = {0, 0};

    /* Only first record need to consider start read plsn */
    m_record->SetSize(BLCKSZ - m_startReadPlsn - 2);
    m_walWriter->AtomicInsertOneWal(m_record);
    m_record->SetSize(BLCKSZ + 1);
    m_walWriter->AtomicInsertOneWal(m_record);
    m_record->SetSize(BLCKSZ * 2);
    m_walWriter->AtomicInsertOneWal(m_record);
    m_record->SetSize(MAX_WAL_RECORD_SIZE - 1);
    walRecordGroupPtr = m_walWriter->AtomicInsertOneWal(m_record);
    ASSERT_GT(walRecordGroupPtr.m_endPlsn, 0);
    m_walWriter->WaitTargetPlsnPersist(walRecordGroupPtr.m_walId, walRecordGroupPtr.m_endPlsn);

    WalReaderConf readerConf = {m_walStream->GetWalId(), m_startReadPlsn, m_walStream, nullptr, m_walFileSize, DSTORE::WalReadSource::WAL_READ_FROM_DISK};
    WalRecordReader *reader = DstoreNew(m_ut_memory_context) WalRecordReader(m_ut_memory_context, readerConf);
    reader->Init();
    ReadAndCheckWalRecordValid(4, reader, m_startReadPlsn, BLCKSZ - 2 - m_startReadPlsn, 1, DSTORE_SUCC);

    m_record->SetSize(m_walFileSize);
    m_walWriter->AtomicInsertOneWal(m_record);
    m_record->SetSize(m_walFileSize * 2);
    walRecordGroupPtr = m_walWriter->AtomicInsertOneWal(m_record);
    ASSERT_GT(walRecordGroupPtr.m_endPlsn, 0);
    m_walWriter->WaitTargetPlsnPersist(walRecordGroupPtr.m_walId, walRecordGroupPtr.m_endPlsn);

    ReadAndCheckWalRecordValid(2, reader, m_startReadPlsn, BLCKSZ - WAL_FILE_HDR_SIZE - 2, 1, DSTORE_SUCC);
}

TEST_F (UTWalReaderTest, WalGroupLenCrossTwoBlockInOneFile)
{
    ASSERT_EQ(PrepareWalFile(2 * WAL_READ_BUFFER_BLOCK_SIZE), DSTORE_SUCC);
    /* The following groups are aligned by 8 KB. */
    uint16 groupSize = BLCKSZ;
    uint16 recordSize = groupSize - UT_GROUP_HEADER_SIZE;
    /* Only first record need to consider start read plsn. */
    m_record->SetSize(recordSize - m_startReadPlsn);
    m_walWriter->AtomicInsertOneWal(m_record);
    /* This group aligned to first read block size(16KB), and left 2 Bytes for next group's group len field. */
    m_record->SetSize(groupSize - UT_GROUP_HEADER_SIZE - 2);
    m_walWriter->AtomicInsertOneWal(m_record);

    /* This group's group len field cross two block. */
    m_record->SetSize(BLCKSZ - UT_GROUP_HEADER_SIZE);
    WalGroupLsnInfo walRecordGroupPtr = m_walWriter->AtomicInsertOneWal(m_record);

    ASSERT_GT(walRecordGroupPtr.m_endPlsn, 0);
    m_walWriter->WaitTargetPlsnPersist(walRecordGroupPtr.m_walId, walRecordGroupPtr.m_endPlsn);

    WalReaderConf readerConf = {m_walStream->GetWalId(), m_startReadPlsn, m_walStream, nullptr, m_walFileSize, DSTORE::WalReadSource::WAL_READ_FROM_DISK};
    WalRecordReader *reader = DstoreNew(m_ut_memory_context) WalRecordReader(m_ut_memory_context, readerConf);
    reader->Init();
    ReadAndCheckWalRecordValid(3, reader, m_startReadPlsn, recordSize - m_startReadPlsn, 1, DSTORE_SUCC);
}

TEST_F (UTWalReaderTest, WalGroupLenCrossTwoBlockInTwoFile)
{
    ASSERT_EQ(PrepareWalFile(2 * WAL_READ_BUFFER_BLOCK_SIZE), DSTORE_SUCC);
    /* The following groups are aligned by 8 KB. */
    uint16 groupSize = BLCKSZ;
    uint16 recordSize = groupSize - UT_GROUP_HEADER_SIZE;
    /* Only first record need to consider start read plsn. */
    m_record->SetSize(recordSize - m_startReadPlsn);
    m_walWriter->AtomicInsertOneWal(m_record);
    /* This group aligned to first wal file size(32KB), and left 2 Bytes for next group's group len field. */
    uint32 walFileRemainSize = m_walFileSize - BLCKSZ;
    m_record->SetSize(walFileRemainSize - UT_GROUP_HEADER_SIZE - 2);
    m_walWriter->AtomicInsertOneWal(m_record);

    /* This group's group len field cross two block. */
    m_record->SetSize(BLCKSZ - UT_GROUP_HEADER_SIZE);
    WalGroupLsnInfo walRecordGroupPtr = m_walWriter->AtomicInsertOneWal(m_record);

    ASSERT_GT(walRecordGroupPtr.m_endPlsn, 0);
    m_walWriter->WaitTargetPlsnPersist(walRecordGroupPtr.m_walId, walRecordGroupPtr.m_endPlsn);

    WalReaderConf readerConf = {m_walStream->GetWalId(), m_startReadPlsn, m_walStream, nullptr, m_walFileSize, DSTORE::WalReadSource::WAL_READ_FROM_DISK};
    WalRecordReader *reader = DstoreNew(m_ut_memory_context) WalRecordReader(m_ut_memory_context, readerConf);
    reader->Init();
    ReadAndCheckWalRecordValid(3, reader, m_startReadPlsn, recordSize - m_startReadPlsn, 1, DSTORE_SUCC);
}

TEST_F (UTWalReaderTest, WalReadStartPlsnTest)
{
    ASSERT_EQ(PrepareWalFile(), DSTORE_SUCC);
    WalGroupLsnInfo walRecordGroupPtr0 = {0, 0};
    WalGroupLsnInfo walRecordGroupPtr = {0, 0};
    /* Only first record need to consider start read plsn */
    m_record->SetSize(m_walFileSize / 2 - m_startReadPlsn);
    m_walWriter->AtomicInsertOneWal(m_record);
    m_record->SetSize(BLCKSZ - 200);
    walRecordGroupPtr0 = m_walWriter->AtomicInsertOneWal(m_record);
    m_record->SetSize(m_walFileSize - BLCKSZ);
    walRecordGroupPtr = m_walWriter->AtomicInsertOneWal(m_record);
    ASSERT_GT(walRecordGroupPtr.m_endPlsn, 0);
    m_walWriter->WaitTargetPlsnPersist(walRecordGroupPtr.m_walId, walRecordGroupPtr.m_endPlsn);

    ReadAndCheckWalRecordValid(3, nullptr, m_startReadPlsn, m_walFileSize / 2 - m_startReadPlsn, 1, DSTORE_SUCC);

    ReadAndCheckWalRecordValid(2, nullptr, walRecordGroupPtr0.m_startPlsn, BLCKSZ - 200, 1, DSTORE_SUCC);

    ReadAndCheckWalRecordValid(0, nullptr, walRecordGroupPtr.m_startPlsn + 10, 0, 1, DSTORE_SUCC);

    ReadAndCheckWalRecordValid(0, nullptr, walRecordGroupPtr.m_endPlsn, 0, 1, DSTORE_SUCC);

    ReadAndCheckWalRecordValid(0, nullptr, walRecordGroupPtr.m_endPlsn + 10, 0, 1, DSTORE_SUCC);
}

TEST_F (UTWalReaderTest, WalReadGetNextWalRecord)
{
    ASSERT_EQ(PrepareWalFile(), DSTORE_SUCC);
    uint32 recordNum = 3;
    /* Only first record need to consider start read plsn */
    uint64 eachRecordLen[3] = {m_walFileSize / 4 - m_startReadPlsn, m_walFileSize / 4 - 100, m_walFileSize / 4 + 100};
    WalGroupLsnInfo walRecordGroupPtr = {0, 0};
    m_walWriter->BeginAtomicWal(INVALID_XID);
    char *buf = reinterpret_cast<char *>(m_record);
    for (uint32 i = 0; i < recordNum; i++) {
        reinterpret_cast<WalRecord *>(buf)->SetSize(eachRecordLen[i]);
        reinterpret_cast<WalRecord *>(buf)->SetType(DSTORE::WAL_EMPTY_REDO);
        m_walWriter->PutNewWalRecord(reinterpret_cast<WalRecord *>(buf));
        buf += eachRecordLen[i];
    }
    walRecordGroupPtr = m_walWriter->EndAtomicWal();
    m_walWriter->WaitTargetPlsnPersist(walRecordGroupPtr.m_walId, walRecordGroupPtr.m_endPlsn);

    WalReaderConf readerConf = {m_walStream->GetWalId(), m_startReadPlsn, m_walStream, nullptr, m_walFileSize, DSTORE::WalReadSource::WAL_READ_FROM_DISK};
    WalRecordReader *reader = DstoreNew(m_ut_memory_context) WalRecordReader(m_ut_memory_context, readerConf);
    reader->Init();

    const WalRecordAtomicGroup *readGroup;
    RetStatus ret = reader->ReadNext(&readGroup);
    ASSERT_EQ(ret, DSTORE_SUCC);
    uint32 offset = offsetof(WalRecordAtomicGroup, crc) + sizeof((static_cast<WalRecordAtomicGroup*>(nullptr))->crc);
    uint32 checkSum =
        CompChecksum(reinterpret_cast<const uint8*>(readGroup) + offset, readGroup->groupLen - offset, CHECKSUM_CRC);
    ASSERT_EQ(checkSum, readGroup->crc);
    const WalRecord *curRecord = nullptr;
    int rc;
    buf = reinterpret_cast<char *>(m_record);
    for (uint32 i = 0; i < recordNum; i++) {
        curRecord = reader->GetNextWalRecord();
        ASSERT_EQ(curRecord->m_size, eachRecordLen[i]);
        rc = memcmp(curRecord, buf, eachRecordLen[i]);
        ASSERT_EQ(rc, 0);
        buf += eachRecordLen[i];
    }
    curRecord = reader->GetNextWalRecord();
    ASSERT_EQ(curRecord, nullptr);
    curRecord = reader->GetNextWalRecord();
    ASSERT_EQ(curRecord, nullptr);
}

#ifndef DYNAMIC_LINK_VFS_LIB
TEST_F (UTWalReaderTest, WalReadNextAllWalFilesFull)
{
    ASSERT_EQ(PrepareWalFile(), DSTORE_SUCC);
    WalGroupLsnInfo walRecordGroupPtr3 = {0, 0};
    WalGroupLsnInfo walRecordGroupPtr11 = {0, 0};

    /* Only first record need to consider start read plsn */
    m_record->SetSize(m_walFileSize - 200 - m_startReadPlsn);
    for (uint32 i = 0; i < 11; i++) {
        if (i == 2) {
            walRecordGroupPtr3 = m_walWriter->AtomicInsertOneWal(m_record);
            continue;
        } else if (i == 10) {
            walRecordGroupPtr11 = m_walWriter->AtomicInsertOneWal(m_record);
            continue;
        }
        m_walWriter->AtomicInsertOneWal(m_record);
        m_record->SetSize(m_walFileSize - 200);
    }

    ASSERT_GT(walRecordGroupPtr3.m_endPlsn, 0);
    ASSERT_GT(walRecordGroupPtr11.m_endPlsn, 0);
    m_walWriter->WaitTargetPlsnPersist(walRecordGroupPtr11.m_walId, walRecordGroupPtr11.m_endPlsn);

    ReadAndCheckWalRecordValid(11, nullptr, m_startReadPlsn, m_walFileSize - 200 - m_startReadPlsn, 1, DSTORE_SUCC);

    ReadAndCheckWalRecordValid(9, nullptr, walRecordGroupPtr3.m_startPlsn, m_walFileSize - 200, 1, DSTORE_SUCC);

    ReadAndCheckWalRecordValid(1, nullptr, walRecordGroupPtr11.m_startPlsn, m_walFileSize - 200, 1, DSTORE_SUCC);
}


TEST_F (UTWalReaderTest, WalReadNextExceptionCase1)
{
    ASSERT_EQ(PrepareWalFile(), DSTORE_SUCC);
    WalGroupLsnInfo walRecordGroupPtr = {0, 0};
    ReadAndCheckWalRecordValid(0, nullptr, m_startReadPlsn, 0, 1, DSTORE_SUCC);

    WalFile *walFile = m_walStream->GetWalFileManager()->GetWalFileByPlsn(m_startReadPlsn);
    ::ErrorCode errorCode;
    errorCode = Truncate(walFile->GetFileDescriptor(), 0);
    ASSERT_EQ(errorCode, 0);

    WalFileHeaderData *walFileHeaderData = (WalFileHeaderData *)m_record;
    walFileHeaderData->fileSize = m_walFileSize;
    walFileHeaderData->timelineId = 0;
    walFileHeaderData->startPlsn = m_startReadPlsn;
    walFileHeaderData->version = 0;
    walFileHeaderData->magicNum = WAL_FILE_HEAD_MAGIC;

    char *record_data = reinterpret_cast<char *>(m_record);
    uint32 checkSum = WalFileHeaderData::ComputeHdrCrc(reinterpret_cast<const uint8*>(walFileHeaderData));
    walFileHeaderData->crc = checkSum;
    PwriteAsync(walFile->GetFileDescriptor(), record_data, WAL_FILE_HDR_SIZE, 0, nullptr);

    ReadAndCheckWalRecordValid(0, nullptr, WAL_FILE_HDR_SIZE - 10, 0, 1, DSTORE_SUCC);
    ReadAndCheckWalRecordValid(0, nullptr, WAL_FILE_HDR_SIZE + 10, 0, 1, DSTORE_SUCC);
    ReadAndCheckWalRecordValid(0, nullptr, 0, 0, 1, DSTORE_SUCC);

    WalReaderConf readerConf = {m_walStream->GetWalId(), WAL_FILE_HDR_SIZE, m_walStream, nullptr, m_walFileSize,
                                DSTORE::WalReadSource::WAL_READ_FROM_DISK};
    WalRecordReader* reader = DstoreNew(m_ut_memory_context) WalRecordReader(m_ut_memory_context, readerConf);
    reader->Init();
    ReadAndCheckWalRecordValid(0, reader, WAL_FILE_HDR_SIZE, 0, 1, DSTORE_SUCC);
}

TEST_F (UTWalReaderTest, WalReadNextExceptionCase2)
{
    ASSERT_EQ(PrepareWalFile(), DSTORE_SUCC);
    WalGroupLsnInfo walRecordGroupPtr = {0, 0};
    ReadAndCheckWalRecordValid(0, nullptr, m_startReadPlsn, 0, 1, DSTORE_SUCC);
    WalFile *walFile = m_walStream->GetWalFileManager()->GetWalFileByPlsn(m_startReadPlsn);
    WalReaderConf readerConf = {m_walStream->GetWalId(), m_startReadPlsn, m_walStream, nullptr, m_walFileSize,
                                DSTORE::WalReadSource::WAL_READ_FROM_DISK};
    WalRecordReader* reader = DstoreNew(m_ut_memory_context) WalRecordReader(m_ut_memory_context, readerConf);
    reader->Init();

    m_record->SetType(WAL_HEAP_DELETE);
    /* Only first record need to consider start read plsn */
    m_record->SetSize(BLCKSZ - 100 - m_startReadPlsn);
    m_walWriter->AtomicInsertOneWal(m_record);
    m_record->SetSize(BLCKSZ - 200);
    m_walWriter->AtomicInsertOneWal(m_record);
    m_record->SetSize(BLCKSZ - sizeof(WalRecordAtomicGroup));
    m_walWriter->AtomicInsertOneWal(m_record);
    m_record->SetSize(BLCKSZ * 2);
    walRecordGroupPtr = m_walWriter->AtomicInsertOneWal(m_record);
    ASSERT_GT(walRecordGroupPtr.m_endPlsn, 0);
    m_walWriter->WaitTargetPlsnPersist(walRecordGroupPtr.m_walId, walRecordGroupPtr.m_endPlsn);

    WalRecordAtomicGroup *invalidGroup = (WalRecordAtomicGroup *)m_record;
    invalidGroup->groupLen = sizeof(WalRecordAtomicGroup) + 100;
    invalidGroup->crc = 100;
    invalidGroup->recordNum = 1;

    PwriteAsync(walFile->GetFileDescriptor(), m_record, invalidGroup->groupLen, walRecordGroupPtr.m_endPlsn, nullptr);
    ReadAndCheckWalRecordValid(4, reader, m_startReadPlsn, BLCKSZ - 100 - m_startReadPlsn, 1, DSTORE_SUCC);
}

TEST_F (UTWalReaderTest, DISABLED_WalReadNextInvalidSegHeaderTest1)
{
    ASSERT_EQ(PrepareWalFile(), DSTORE_SUCC);
    WalGroupLsnInfo walRecordGroupPtr = {0, 0};
    RetStatus ret;
    uint16 targetFileId = 1001;
    /* Only first record need to consider start read plsn */
    m_record->SetSize(m_walFileSize / 2 - m_startReadPlsn);
    m_walWriter->AtomicInsertOneWal(m_record);
    walRecordGroupPtr = m_walWriter->AtomicInsertOneWal(m_record);
    ASSERT_GT(walRecordGroupPtr.m_endPlsn, 0);
    m_walWriter->WaitTargetPlsnPersist(walRecordGroupPtr.m_walId, walRecordGroupPtr.m_endPlsn);

    PwriteAsync(m_vfs->GetFileDescriptor(targetFileId), m_record, WAL_FILE_HDR_SIZE, 0, nullptr);

    RestartWal();

    ReadAndCheckWalRecordValid(1, nullptr, m_startReadPlsn, m_walFileSize / 2 - m_startReadPlsn, 1, DSTORE_SUCC);
}

TEST_F (UTWalReaderTest, WalReadNextInvalidSegHeaderTest2)
{
    ASSERT_EQ(PrepareWalFile(), DSTORE_SUCC);
    WalGroupLsnInfo walRecordGroupPtr = {0, 0};
    RetStatus ret;
    uint16 targetFileId = 1001;
    uint64 startReadPlsn = m_startReadPlsn;
    m_record->SetSize(m_walFileSize - WAL_FILE_HDR_SIZE - sizeof(WalRecordAtomicGroup) - m_startReadPlsn);
    walRecordGroupPtr = m_walWriter->AtomicInsertOneWal(m_record);
    ASSERT_GT(walRecordGroupPtr.m_endPlsn, 0);
    m_walWriter->WaitTargetPlsnPersist(walRecordGroupPtr.m_walId, walRecordGroupPtr.m_endPlsn);

    PwriteAsync(m_vfs->GetFileDescriptor(targetFileId), m_record, WAL_FILE_HDR_SIZE, 0, nullptr);

    RestartWal();

    ReadAndCheckWalRecordValid(1, nullptr, startReadPlsn, m_walFileSize - WAL_FILE_HDR_SIZE -
            sizeof(WalRecordAtomicGroup) - startReadPlsn, 1, DSTORE_SUCC);
}
#endif
