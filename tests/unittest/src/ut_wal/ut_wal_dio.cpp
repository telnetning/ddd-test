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
#include <random>
#include <thread>

#include "securec.h"
#include "wal/dstore_wal_file_manager.h"
#include "ut_buffer/ut_buffer_fake_instance.h"
#include "ut_wal/ut_wal_basic.h"
#include "vfs/vfs_interface.h"

namespace DSTORE{

/* #define DYNAMIC_LINK_VFS_LIB */
static constexpr uint64 SEG_SIZE = 16 * 1024U;
static constexpr uint16 FILE_COUNT_INIT_VALUE = 8;
static constexpr uint16 FILE_COUNT_MAX_VALUE = 100;
static constexpr uint16 FREE_FILE_COUNT = 2;

void UtWalFileDioFlushCallback(::FileDescriptor *fd, off_t offset, ErrorCode errorCode, void *asyncContext)
{
    if (errorCode != 0 || asyncContext == nullptr) {
        return;
    }

    WalStream *stream = static_cast<WalStream *>(asyncContext);
    stream->HandleFileFlushResult(fd, offset);
}

void MockHandleFileDioFlushResult(WalStream *walStream, FileDescriptor *fd, off_t offset)
{
    StorageAssert(g_storageInstance != nullptr);
    (void)g_storageInstance->CreateThreadAndRegister();
    UtWalFileDioFlushCallback(fd, offset, 0, walStream);
    g_storageInstance->UnregisterThread();
}

class WalDioTest : public WALBASICTEST {
protected:
    void SetUp() override
    {
        SetDefaultPdbId(PDB_TEMPLATE1_ID);
        DSTORE::WALBASICTEST::SetUp();
#ifdef DYNAMIC_LINK_VFS_LIB
        /* Get the config file. */
        memset(m_configFilePath, 0, MAXPGPATH);
        char *execPath = m_configFilePath;
        int ret = readlink("/proc/self/exe", execPath, MAXPGPATH);
        ASSERT_GT(ret, 0);
        char *lastSlashPtr = strrchr(execPath, '/');
        ASSERT_NE(lastSlashPtr, nullptr);

        snprintf(lastSlashPtr + 1, MAXPGPATH / 2, "dynamic_vfs.conf.init");
        ASSERT_EQ(access(m_configFilePath, F_OK), 0); /* It must exist. */

        DSTORETEST::m_guc.vfsConfigPath = m_configFilePath;
#endif
        m_vfs = g_storageInstance->GetPdb(g_defaultPdbId)->GetVFS();
        m_walFileMgr = DstoreNew(m_ut_memory_context) WalFileManager(m_ut_memory_context);
        ASSERT_NE(m_walFileMgr, nullptr);
        m_walBuffer = DstoreNew(m_ut_memory_context) WalStreamBuffer(m_ut_memory_context, 4);
        ASSERT_NE(m_walBuffer, nullptr);
        m_walStream = DstoreNew(m_ut_memory_context)
            WalStream(m_ut_memory_context, m_walId, m_walFileMgr, SEG_SIZE, m_pdbId);
        ASSERT_NE(m_walStream, nullptr);
        m_walBuffer->Init(0, SEG_SIZE);
        m_walStream->Init(m_walBuffer);

        char *pdbWalPath = StorageInstance::GetPdbWalPath(g_storageInstance->GetGuc()->dataDir, g_defaultPdbId);
        FileParameter filePara;
        filePara.streamId = m_walId;
        filePara.flag = APPEND_WRITE_FILE;
        filePara.fileSubType = WAL_LOG_FILE_TYPE;
        filePara.rangeSize = WAL_FILE_RANGE_SIZE;
        filePara.maxSize = SEG_SIZE;
        filePara.recycleTtl = 0;
        filePara.mode = FILE_READ_AND_WRITE_MODE;
        filePara.isReplayWrite = false;
        TenantConfig *tenantConfig = g_storageInstance->GetGuc()->tenantConfig;
        StorageAssert(tenantConfig);
        errno_t rc = strcpy_s(
            filePara.storeSpaceName, STORESPACE_NAME_MAX_LEN, tenantConfig->storeSpaces[0].storeSpaceName);
        storage_securec_check(rc, "\0", "\0");
        InitWalFilesPara initPara = {
            {'0'}, m_vfs, m_pdbId, m_walId,
                                     FILE_COUNT_INIT_VALUE, FILE_COUNT_MAX_VALUE, FREE_FILE_COUNT,
            SEG_SIZE, 0, false, false, filePara, {UtWalFileDioFlushCallback, m_walStream}, {}
        };
        rc = strcpy_s(initPara.dir, MAXPGPATH, pdbWalPath);
        storage_securec_check(rc, "\0", "\0");
        m_retStatus = m_walFileMgr->InitWalFiles(initPara, true);
        ASSERT_EQ(m_retStatus, DSTORE_SUCC);
        m_resultCode = memset_s(m_recvBuf, SEG_SIZE, 0, SEG_SIZE);
        storage_securec_check(m_resultCode, "\0", "\0");
    }

    void TearDown() override
    {
        free(m_recvBuf);
        delete m_walStream;

        WALBASICTEST::TearDown();
    }

    uint16 m_walId = 1;
    WalStream *m_walStream;
    WalStreamBuffer *m_walBuffer;
    WalFileManager *m_walFileMgr;
    uint8 *m_recvBuf = (uint8 *)(calloc(1, SEG_SIZE));
    VFSAdapter *m_vfs = nullptr;
    RetStatus m_retStatus;
    int m_resultCode;
    char m_configFilePath[MAXPGPATH];
};

TEST_F (WalDioTest, DioWriteFileAlignedTest) {
    uint64 readSize;
    uint64 plsn = 0;
    uint64 dataLen = SEG_SIZE / 2; // aligned already
    uint8 *adaptedBuf = m_walBuffer->GetBufferBlock(plsn);
    m_resultCode = memset_s(adaptedBuf, SEG_SIZE, 'a', SEG_SIZE);
    storage_securec_check(m_resultCode, "\0", "\0");
    m_walStream->Write(plsn, adaptedBuf, dataLen);
    m_walStream->Flush(plsn, plsn + dataLen);
#ifdef DYNAMIC_LINK_VFS_LIB
    while (m_walStream->GetMaxFlushedPlsn() < plsn + dataLen) {}
#endif
    EXPECT_EQ(m_walStream->GetMaxFlushedPlsn(), plsn + dataLen);
    m_walStream->Read(plsn, m_recvBuf, dataLen, &readSize);
    ASSERT_EQ(readSize, dataLen);

    WalFileHeaderData *walFileHeaderData = (WalFileHeaderData *)m_recvBuf;
    EXPECT_EQ(walFileHeaderData->magicNum, WAL_FILE_HEAD_MAGIC);
    EXPECT_EQ(walFileHeaderData->startPlsn, 0);
    EXPECT_EQ(walFileHeaderData->crc, WalFileHeaderData::ComputeHdrCrc(m_recvBuf));
    EXPECT_EQ(memcmp(adaptedBuf, m_recvBuf, dataLen), 0);
}

TEST_F (WalDioTest, DioWriteFileNotAlignedTest) {
    uint64 readSize;
    uint64 plsn = 0;
    uint64 dataLen = SEG_SIZE / 2 - 1; // not aligned
    uint8 *adaptedBuf = m_walBuffer->GetBufferBlock(plsn);
    m_resultCode = memset_s(adaptedBuf, SEG_SIZE, 'a', SEG_SIZE);
    storage_securec_check(m_resultCode, "\0", "\0");
    m_walStream->Write(plsn, adaptedBuf, dataLen);
    m_walStream->Flush(plsn, plsn + dataLen);
#ifdef DYNAMIC_LINK_VFS_LIB
    while (m_walStream->GetMaxFlushedPlsn() < plsn + dataLen) {}
#endif
    EXPECT_EQ(m_walStream->GetMaxFlushedPlsn(), plsn + dataLen);
    m_walStream->Read(plsn, m_recvBuf, dataLen, &readSize);
    ASSERT_EQ(readSize, dataLen);

    WalFileHeaderData *walFileHeaderData = (WalFileHeaderData *)m_recvBuf;
    EXPECT_EQ(walFileHeaderData->magicNum, WAL_FILE_HEAD_MAGIC);
    EXPECT_EQ(walFileHeaderData->startPlsn, 0);
    EXPECT_EQ(walFileHeaderData->crc, WalFileHeaderData::ComputeHdrCrc(m_recvBuf));
    EXPECT_EQ(memcmp(adaptedBuf, m_recvBuf, dataLen), 0);
    // check last char is '\0'
    EXPECT_EQ(memcmp(m_recvBuf + readSize, "\0", SEG_SIZE / 2 - readSize), 0);
}

TEST_F (WalDioTest, DioWriteAndReadTest) {
    uint64 readSize;
    uint64 plsn = 0;
    uint64 dataLen = SEG_SIZE / 2 - 6; // not aligned
    uint8 *adaptedBuf = m_walBuffer->GetBufferBlock(plsn);
    m_resultCode = memset_s(adaptedBuf, SEG_SIZE, 'a', SEG_SIZE);
    storage_securec_check(m_resultCode, "\0", "\0");

    WalRecordAtomicGroup* group = (WalRecordAtomicGroup *)calloc(dataLen - sizeof(WalFileHeaderData), sizeof(char));
    group->groupLen = dataLen - sizeof(WalFileHeaderData);
    group->xid = INVALID_XID;
    group->crc = 0;
    group->recordNum = 1;
    WalRecord *record = (WalRecord*)((char*)group + sizeof(WalRecordAtomicGroup));
    record->SetSize(dataLen - sizeof(WalFileHeaderData) - sizeof(WalRecordAtomicGroup));
    record->SetType(WAL_EMPTY_REDO);
    uint32 offset = offsetof(WalRecordAtomicGroup, crc) + sizeof((static_cast<WalRecordAtomicGroup *>(nullptr))->crc);
    group->crc = CompChecksum(reinterpret_cast<const uint8*>(group) + offset, group->groupLen - offset, CHECKSUM_CRC);
    m_resultCode = memcpy_s(adaptedBuf + sizeof(WalFileHeaderData), SEG_SIZE - sizeof(WalFileHeaderData), group, group->groupLen);
    storage_securec_check(m_resultCode, "\0", "\0");
    free(group);
    /* Init file header remain len */
    WalFileHeaderData *header = reinterpret_cast<WalFileHeaderData *>(adaptedBuf);
    header->lastRecordRemainLen = 0;
    m_walStream->Write(plsn, adaptedBuf, dataLen);
    m_walStream->Flush(plsn, plsn + dataLen);
#ifdef DYNAMIC_LINK_VFS_LIB
    while (m_walStream->GetMaxFlushedPlsn() < plsn + dataLen) {}
#endif
    EXPECT_EQ(m_walStream->GetMaxFlushedPlsn(), plsn + dataLen);

    WalReaderConf readerConf = {0, 0, m_walStream, nullptr, SEG_SIZE, DSTORE::WalReadSource::WAL_READ_FROM_DISK};
    WalRecordReader *reader = DstoreNew(m_ut_memory_context) WalRecordReader(m_ut_memory_context, readerConf);
    reader->Init();
    const WalRecordAtomicGroup *readGroup;
    RetStatus ret = reader->ReadNext(&readGroup);
    ASSERT_EQ(ret, DSTORE_SUCC);
    ASSERT_NE(readGroup, nullptr);
    uint32 checkSum =
        CompChecksum(reinterpret_cast<const uint8*>(readGroup) + offset, readGroup->groupLen - offset, CHECKSUM_CRC);
    ASSERT_EQ(checkSum, readGroup->crc);

    const WalRecord *curRecord = nullptr;
    uint16 recordNum = 0;
    do {
        curRecord = reader->GetNextWalRecord();
        if (curRecord) {
            recordNum++;
        }
    } while (curRecord != nullptr);
    ASSERT_EQ(recordNum, 1);
    ASSERT_EQ(recordNum, readGroup->recordNum);

    ret = reader->ReadNext(&readGroup);
    ASSERT_EQ(ret, DSTORE_SUCC);
    ASSERT_EQ(readGroup, nullptr);

    delete reader;
}


}