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

/* #define DYNAMIC_LINK_VFS_LIB */
extern char g_utTopDir[MAXPGPATH];

constexpr static uint16 EACH_STREAM_PREPARED_WAL_GROUP_NUM = 10;
constexpr static uint16 EACH_GROUP_WAL_RECORD_NUM = 3;
constexpr static uint16 EACH_WAL_RECORD_LEN = 100;
constexpr static PageId TARGET_PAGE = {UINT16_MAX / 2, UINT32_MAX / 2};

class WalPageRecordReaderTest : public UTWalReaderTest {
protected:
    void SetUp() override
    {
        UTWalReaderTest::SetUp();
        ASSERT_EQ(UTWalReaderTest::PrepareWalFile(), DSTORE_SUCC);
    }

    void TearDown() override
    {
        if (m_recordForPageReader != nullptr) {
            delete m_recordForPageReader;
            m_recordForPageReader = nullptr;
        }
        UTWalReaderTest::TearDown();
    }

    void SetWalRecordTag(WalRecord *record, uint8 tag)
    {
        uint8 *recordData = reinterpret_cast<uint8 *>(record);
        recordData[sizeof(WalRecord)] = tag;
    }

    uint8 GetWalRecordTag(const WalRecord *record)
    {
        const uint8 *recordData = reinterpret_cast<const uint8 *>(record);
        return recordData[sizeof(WalRecord)];
    }

    void PrepareWal()
    {
        WalGroupLsnInfo result = {INVALID_WAL_ID, INVALID_PLSN, INVALID_PLSN};
        for (uint16 i = 0; i < EACH_STREAM_PREPARED_WAL_GROUP_NUM; i++) {
            Xid xid(0);
            m_walWriter->BeginAtomicWal(xid);
            m_record->SetSize(EACH_WAL_RECORD_LEN);
            for (uint16 j = 0; j < EACH_GROUP_WAL_RECORD_NUM; j++) {
                uint8 tag = static_cast<uint8>((i + 1) * (j + 1));
                SetWalRecordTag(m_record, tag);
                m_allRecordTag[i * EACH_GROUP_WAL_RECORD_NUM + j] = tag;
                m_walWriter->PutNewWalRecord(m_record);
            }
            result = m_walWriter->EndAtomicWal();
            ASSERT_NE(result.m_endPlsn, INVALID_PLSN);
            m_allGroupEndPlsn[i] = result.m_endPlsn;
        }
        if (result.m_endPlsn != INVALID_PLSN) {
            m_walWriter->WaitTargetPlsnPersist(result);
        }
    }

    WalReaderForPageConf *BuildConf(uint64 prevEndPlsn)
    {
        uint32 len = sizeof(WalReaderForPageConf) + STREAM_COUNT * sizeof(WalReaderConf);
        WalReaderForPageConf *conf = reinterpret_cast<WalReaderForPageConf *>(malloc(len));
        WalId walId = 0;
        WalRecordLsnInfo lsnInfo = {walId, prevEndPlsn, 0};

        conf->pageId = TARGET_PAGE;
        conf->prevRecord = lsnInfo;
        conf->walStreamNum = STREAM_COUNT;
        for (uint16 i = 0; i < STREAM_COUNT; i++) {
            conf->walStreams[i].walId = i;
            conf->walStreams[i].startPlsn = 0;
            conf->walStreams[i].walFileSize = m_walFileSize;
            conf->walStreams[i].fileReader = m_walStream;
            conf->walStreams[i].walReadSource = DSTORE::WalReadSource::WAL_READ_FROM_DISK;
        }
        return conf;
    }

    void CheckWalGroup(const WalRecordAtomicGroup *walGroup, uint16 walGroupIndex)
    {
        uint32 offset = offsetof(WalRecordAtomicGroup, crc) + sizeof((static_cast<WalRecordAtomicGroup*>(nullptr))->crc);
        uint32 checkSum = CompChecksum(
            reinterpret_cast<const uint8 *>(walGroup) + offset, walGroup->groupLen - offset, CHECKSUM_CRC);
        ASSERT_EQ(checkSum, walGroup->crc);
        const WalRecord *curRecord = nullptr;
        uint16 recordNum = 0;
        do {
            curRecord = m_recordForPageReader->GetNextWalRecord();
            if (curRecord) {
                ASSERT_EQ(
                    GetWalRecordTag(curRecord), m_allRecordTag[walGroupIndex * EACH_GROUP_WAL_RECORD_NUM + recordNum]);
                recordNum++;
            }
        } while (curRecord != nullptr);
        ASSERT_EQ(recordNum, EACH_GROUP_WAL_RECORD_NUM);
    }

    WalRecordForPageReader *m_recordForPageReader;
    uint64 m_allGroupEndPlsn[EACH_STREAM_PREPARED_WAL_GROUP_NUM];
    uint8 m_allRecordTag[EACH_STREAM_PREPARED_WAL_GROUP_NUM * EACH_GROUP_WAL_RECORD_NUM];
};

TEST_F (WalPageRecordReaderTest, Init_InitSuccess)
{
    uint16 targetWalIndex = EACH_STREAM_PREPARED_WAL_GROUP_NUM / 2;
    ASSERT_LT(targetWalIndex, EACH_STREAM_PREPARED_WAL_GROUP_NUM - 1);

    WalReaderForPageConf *conf = BuildConf(m_allGroupEndPlsn[targetWalIndex]);
    m_recordForPageReader = DstoreNew(m_ut_memory_context) WalRecordForPageReader(m_ut_memory_context);
    RetStatus ret = m_recordForPageReader->Init(*conf);
    EXPECT_EQ(ret, DSTORE_SUCC);
    free(conf);
}

TEST_F (WalPageRecordReaderTest, Init_RepeatInitSuccess)
{
    uint16 targetWalIndex = EACH_STREAM_PREPARED_WAL_GROUP_NUM / 2;
    ASSERT_LT(targetWalIndex, EACH_STREAM_PREPARED_WAL_GROUP_NUM - 1);

    WalReaderForPageConf *conf = BuildConf(m_allGroupEndPlsn[targetWalIndex]);
    m_recordForPageReader = DstoreNew(m_ut_memory_context) WalRecordForPageReader(m_ut_memory_context);
    RetStatus ret = m_recordForPageReader->Init(*conf);
    EXPECT_EQ(ret, DSTORE_SUCC);

    ret = m_recordForPageReader->Init(*conf);
    EXPECT_EQ(ret, DSTORE_SUCC);
    free(conf);
}

TEST_F (WalPageRecordReaderTest, Init_InvalidPageIdConf)
{
    uint16 targetWalIndex = EACH_STREAM_PREPARED_WAL_GROUP_NUM / 2;
    ASSERT_LT(targetWalIndex, EACH_STREAM_PREPARED_WAL_GROUP_NUM - 1);

    WalReaderForPageConf *conf = BuildConf(m_allGroupEndPlsn[targetWalIndex]);
    m_recordForPageReader = DstoreNew(m_ut_memory_context) WalRecordForPageReader(m_ut_memory_context);

    conf->pageId = INVALID_PAGE_ID;
    RetStatus ret = m_recordForPageReader->Init(*conf);
    EXPECT_EQ(ret, DSTORE_FAIL);
    free(conf);
}

TEST_F (WalPageRecordReaderTest, Init_InvalidWalStreamCountConf)
{
    uint16 targetWalIndex = EACH_STREAM_PREPARED_WAL_GROUP_NUM / 2;
    ASSERT_LT(targetWalIndex, EACH_STREAM_PREPARED_WAL_GROUP_NUM - 1);

    WalReaderForPageConf *conf = BuildConf(m_allGroupEndPlsn[targetWalIndex]);
    m_recordForPageReader = DstoreNew(m_ut_memory_context) WalRecordForPageReader(m_ut_memory_context);

    conf->walStreamNum = 0;
    RetStatus ret = m_recordForPageReader->Init(*conf);
    EXPECT_EQ(ret, DSTORE_FAIL);
    free(conf);
}

TEST_F (WalPageRecordReaderTest, Init_UnknownPreRecordWalId)
{
    uint16 targetWalIndex = EACH_STREAM_PREPARED_WAL_GROUP_NUM / 2;
    ASSERT_LT(targetWalIndex, EACH_STREAM_PREPARED_WAL_GROUP_NUM - 1);

    WalReaderForPageConf *conf = BuildConf(m_allGroupEndPlsn[targetWalIndex]);
    m_recordForPageReader = DstoreNew(m_ut_memory_context) WalRecordForPageReader(m_ut_memory_context);

    conf->prevRecord.walId = INVALID_WAL_ID;
    RetStatus ret = m_recordForPageReader->Init(*conf);
    EXPECT_EQ(ret, DSTORE_FAIL);
    free(conf);
}

TEST_F (WalPageRecordReaderTest, ReadPrev_ReadNotLastWalGroup)
{
    PrepareWal();
    uint16 targetWalIndex = EACH_STREAM_PREPARED_WAL_GROUP_NUM / 2;
    ASSERT_LT(targetWalIndex, EACH_STREAM_PREPARED_WAL_GROUP_NUM - 1);

    WalReaderForPageConf *conf = BuildConf(m_allGroupEndPlsn[targetWalIndex]);
    m_recordForPageReader = DstoreNew(m_ut_memory_context) WalRecordForPageReader(m_ut_memory_context);
    RetStatus ret = m_recordForPageReader->Init(*conf);
    EXPECT_EQ(ret, DSTORE_SUCC);
    free(conf);

    const WalRecordAtomicGroup *readResult;
    ret = m_recordForPageReader->ReadPrev(&readResult);
    ASSERT_EQ(ret, DSTORE_SUCC);
    CheckWalGroup(readResult, targetWalIndex);
}

TEST_F (WalPageRecordReaderTest, ReadPrev_ReadLastWalGroup)
{
    PrepareWal();
    uint16 targetWalIndex = EACH_STREAM_PREPARED_WAL_GROUP_NUM - 1;

    WalReaderForPageConf *conf = BuildConf(m_allGroupEndPlsn[targetWalIndex]);
    m_recordForPageReader = DstoreNew(m_ut_memory_context) WalRecordForPageReader(m_ut_memory_context);
    RetStatus ret = m_recordForPageReader->Init(*conf);
    EXPECT_EQ(ret, DSTORE_SUCC);
    free(conf);

    const WalRecordAtomicGroup *readResult;
    ret = m_recordForPageReader->ReadPrev(&readResult);
    ASSERT_EQ(ret, DSTORE_SUCC);
    CheckWalGroup(readResult, targetWalIndex);
}

TEST_F (WalPageRecordReaderTest, ReadPrev_ReadNotExistRecordAfterFinalRecord)
{
    PrepareWal();
    uint16 targetWalIndex = EACH_STREAM_PREPARED_WAL_GROUP_NUM - 1;

    WalReaderForPageConf *conf = BuildConf(m_allGroupEndPlsn[targetWalIndex] + 1);
    m_recordForPageReader = DstoreNew(m_ut_memory_context) WalRecordForPageReader(m_ut_memory_context);
    RetStatus ret = m_recordForPageReader->Init(*conf);
    EXPECT_EQ(ret, DSTORE_SUCC);
    free(conf);

    const WalRecordAtomicGroup *readResult;
    ret = m_recordForPageReader->ReadPrev(&readResult);
    ASSERT_EQ(ret, DSTORE_FAIL);
}

TEST_F (WalPageRecordReaderTest, ReadPrev_ReadNotExistRecordBeforeFirstRecord)
{
    PrepareWal();
    uint16 targetWalIndex = 0;

    WalReaderForPageConf *conf = BuildConf(m_allGroupEndPlsn[targetWalIndex] - 1);
    m_recordForPageReader = DstoreNew(m_ut_memory_context) WalRecordForPageReader(m_ut_memory_context);
    RetStatus ret = m_recordForPageReader->Init(*conf);
    EXPECT_EQ(ret, DSTORE_SUCC);
    free(conf);

    const WalRecordAtomicGroup *readResult;
    ret = m_recordForPageReader->ReadPrev(&readResult);
    ASSERT_EQ(ret, DSTORE_FAIL);
}

TEST_F (WalPageRecordReaderTest, ReadPrev_BeforeInit)
{
    uint16 targetWalIndex = EACH_STREAM_PREPARED_WAL_GROUP_NUM - 1;

    m_recordForPageReader = DstoreNew(m_ut_memory_context) WalRecordForPageReader(m_ut_memory_context);

    const WalRecordAtomicGroup *readResult;
    RetStatus ret = m_recordForPageReader->ReadPrev(&readResult);
    ASSERT_EQ(ret, DSTORE_FAIL);
}

TEST_F (WalPageRecordReaderTest, ReadPrev_RepeatReadTheSame)
{
    PrepareWal();
    uint16 targetWalIndex = EACH_STREAM_PREPARED_WAL_GROUP_NUM - 1;

    WalReaderForPageConf *conf = BuildConf(m_allGroupEndPlsn[targetWalIndex]);
    m_recordForPageReader = DstoreNew(m_ut_memory_context) WalRecordForPageReader(m_ut_memory_context);
    RetStatus ret = m_recordForPageReader->Init(*conf);
    EXPECT_EQ(ret, DSTORE_SUCC);
    free(conf);

    const WalRecordAtomicGroup *readResult;
    ret = m_recordForPageReader->ReadPrev(&readResult);
    ASSERT_EQ(ret, DSTORE_SUCC);

    ret = m_recordForPageReader->ReadPrev(&readResult);
    ASSERT_EQ(ret, DSTORE_FAIL);
}

TEST_F (WalPageRecordReaderTest, SetPrevRecordInfo_SetValidLsn)
{
    PrepareWal();
    uint16 targetWalIndex = 0;

    WalReaderForPageConf *conf = BuildConf(m_allGroupEndPlsn[targetWalIndex]);
    m_recordForPageReader = DstoreNew(m_ut_memory_context) WalRecordForPageReader(m_ut_memory_context);
    RetStatus ret = m_recordForPageReader->Init(*conf);
    EXPECT_EQ(ret, DSTORE_SUCC);
    free(conf);

    const WalRecordAtomicGroup *readResult;
    ret = m_recordForPageReader->ReadPrev(&readResult);
    ASSERT_EQ(ret, DSTORE_SUCC);
    CheckWalGroup(readResult, targetWalIndex);

    targetWalIndex = EACH_STREAM_PREPARED_WAL_GROUP_NUM / 2;
    WalRecordLsnInfo info {0, m_allGroupEndPlsn[targetWalIndex], 0};
    ret = m_recordForPageReader->SetPrevRecordInfo(info);
    ASSERT_EQ(ret, DSTORE_SUCC);

    ret = m_recordForPageReader->ReadPrev(&readResult);
    ASSERT_EQ(ret, DSTORE_SUCC);
    CheckWalGroup(readResult, targetWalIndex);
}

TEST_F (WalPageRecordReaderTest, SetPrevRecordInfo_SetInvalidWalId)
{
    PrepareWal();
    uint16 targetWalIndex = 0;

    WalReaderForPageConf *conf = BuildConf(m_allGroupEndPlsn[targetWalIndex]);
    m_recordForPageReader = DstoreNew(m_ut_memory_context) WalRecordForPageReader(m_ut_memory_context);
    RetStatus ret = m_recordForPageReader->Init(*conf);
    EXPECT_EQ(ret, DSTORE_SUCC);
    free(conf);

    const WalRecordAtomicGroup *readResult;
    ret = m_recordForPageReader->ReadPrev(&readResult);
    ASSERT_EQ(ret, DSTORE_SUCC);
    CheckWalGroup(readResult, targetWalIndex);

    targetWalIndex = EACH_STREAM_PREPARED_WAL_GROUP_NUM / 2;
    WalRecordLsnInfo info {INVALID_WAL_ID, m_allGroupEndPlsn[targetWalIndex], 0};
    ret = m_recordForPageReader->SetPrevRecordInfo(info);
    ASSERT_EQ(ret, DSTORE_FAIL);
}

TEST_F (WalPageRecordReaderTest, SetPrevRecordInfo_SetTheSameConfig)
{
    PrepareWal();
    uint16 targetWalIndex = EACH_STREAM_PREPARED_WAL_GROUP_NUM / 2;

    WalReaderForPageConf *conf = BuildConf(m_allGroupEndPlsn[targetWalIndex]);
    m_recordForPageReader = DstoreNew(m_ut_memory_context) WalRecordForPageReader(m_ut_memory_context);
    RetStatus ret = m_recordForPageReader->Init(*conf);
    EXPECT_EQ(ret, DSTORE_SUCC);
    free(conf);

    const WalRecordAtomicGroup *readResult;
    ret = m_recordForPageReader->ReadPrev(&readResult);
    ASSERT_EQ(ret, DSTORE_SUCC);
    CheckWalGroup(readResult, targetWalIndex);

    WalRecordLsnInfo info {0, m_allGroupEndPlsn[targetWalIndex], 0};
    for (uint16 i = 0; i < 10; i ++) {
        ret = m_recordForPageReader->SetPrevRecordInfo(info);
        ASSERT_EQ(ret, DSTORE_SUCC);

        ret = m_recordForPageReader->ReadPrev(&readResult);
        ASSERT_EQ(ret, DSTORE_SUCC);
        CheckWalGroup(readResult, targetWalIndex);
    }
}

TEST_F (WalPageRecordReaderTest, GetNextWalRecord_GetAllRecords)
{
    PrepareWal();
    uint16 targetWalIndex = EACH_STREAM_PREPARED_WAL_GROUP_NUM / 2;

    WalReaderForPageConf *conf = BuildConf(m_allGroupEndPlsn[targetWalIndex]);
    m_recordForPageReader = DstoreNew(m_ut_memory_context) WalRecordForPageReader(m_ut_memory_context);
    RetStatus ret = m_recordForPageReader->Init(*conf);
    EXPECT_EQ(ret, DSTORE_SUCC);
    free(conf);

    const WalRecordAtomicGroup *readResult;
    ret = m_recordForPageReader->ReadPrev(&readResult);
    ASSERT_EQ(ret, DSTORE_SUCC);

    const WalRecord *curRecord = nullptr;
    for (uint16 index = 0; index < readResult->recordNum; index++) {
        curRecord = m_recordForPageReader->GetNextWalRecord();
        ASSERT_NE(curRecord, nullptr);
        ASSERT_EQ(GetWalRecordTag(curRecord), m_allRecordTag[targetWalIndex * EACH_GROUP_WAL_RECORD_NUM + index]);
    }
}

TEST_F (WalPageRecordReaderTest, GetNextWalRecord_AfterGetAllRecords)
{
    PrepareWal();
    uint16 targetWalIndex = EACH_STREAM_PREPARED_WAL_GROUP_NUM / 2;

    WalReaderForPageConf *conf = BuildConf(m_allGroupEndPlsn[targetWalIndex]);
    m_recordForPageReader = DstoreNew(m_ut_memory_context) WalRecordForPageReader(m_ut_memory_context);
    RetStatus ret = m_recordForPageReader->Init(*conf);
    EXPECT_EQ(ret, DSTORE_SUCC);
    free(conf);

    const WalRecordAtomicGroup *readResult;
    ret = m_recordForPageReader->ReadPrev(&readResult);
    ASSERT_EQ(ret, DSTORE_SUCC);

    const WalRecord *curRecord = nullptr;
    for (uint16 i = 0; i < readResult->recordNum; i++) {
        curRecord = m_recordForPageReader->GetNextWalRecord();
        ASSERT_NE(curRecord, nullptr);
        ASSERT_EQ(GetWalRecordTag(curRecord), m_allRecordTag[targetWalIndex * EACH_GROUP_WAL_RECORD_NUM + i]);
    }

    for (uint16 i = 0; i < 10; i++) {
        curRecord = m_recordForPageReader->GetNextWalRecord();
        ASSERT_EQ(curRecord, nullptr);
    }
}

TEST_F (WalPageRecordReaderTest, GetNextWalRecord_BeforeReadPrev)
{
    uint16 targetWalIndex = EACH_STREAM_PREPARED_WAL_GROUP_NUM / 2;

    WalReaderForPageConf *conf = BuildConf(m_allGroupEndPlsn[targetWalIndex]);
    m_recordForPageReader = DstoreNew(m_ut_memory_context) WalRecordForPageReader(m_ut_memory_context);
    RetStatus ret = m_recordForPageReader->Init(*conf);
    EXPECT_EQ(ret, DSTORE_SUCC);
    free(conf);

    const WalRecord *curRecord = nullptr;
    for (uint16 i = 0; i < 10; i++) {
        curRecord = m_recordForPageReader->GetNextWalRecord();
        ASSERT_EQ(curRecord, nullptr);
    }
}

TEST_F (WalPageRecordReaderTest, GetNextWalRecord_AfterSetPrevRecordFail)
{
    PrepareWal();
    uint16 targetWalIndex = 0;

    WalReaderForPageConf *conf = BuildConf(m_allGroupEndPlsn[targetWalIndex]);
    m_recordForPageReader = DstoreNew(m_ut_memory_context) WalRecordForPageReader(m_ut_memory_context);
    RetStatus ret = m_recordForPageReader->Init(*conf);
    EXPECT_EQ(ret, DSTORE_SUCC);
    free(conf);

    const WalRecordAtomicGroup *readResult;
    ret = m_recordForPageReader->ReadPrev(&readResult);
    ASSERT_EQ(ret, DSTORE_SUCC);
    CheckWalGroup(readResult, targetWalIndex);

    targetWalIndex = EACH_STREAM_PREPARED_WAL_GROUP_NUM / 2;
    WalRecordLsnInfo info {INVALID_WAL_ID, m_allGroupEndPlsn[targetWalIndex], 0};
    ret = m_recordForPageReader->SetPrevRecordInfo(info);
    ASSERT_EQ(ret, DSTORE_FAIL);

    const WalRecord *curRecord = nullptr;
    for (uint16 i = 0; i < 10; i++) {
        curRecord = m_recordForPageReader->GetNextWalRecord();
        ASSERT_EQ(curRecord, nullptr);
    }
}
