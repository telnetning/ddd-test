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
static constexpr uint64 BUF_SIZE = 4 * SEG_SIZE;
static constexpr uint16 FILE_COUNT_INIT_VALUE = 8;
static constexpr uint16 FILE_COUNT_MAX_VALUE = 100;
static constexpr uint16 FREE_FILE_COUNT = 2;

uint16 SegNum = FILE_COUNT_INIT_VALUE;

void UtWalFileFlushCallback(::FileDescriptor *fd, off_t offset, ErrorCode errorCode, void *asyncContext)
{
    if (errorCode != 0 || asyncContext == nullptr) {
        return;
    }

    WalStream *stream = static_cast<WalStream *>(asyncContext);
    stream->HandleFileFlushResult(fd, offset);
}

off_t GetFileValidLen(FileDescriptor *fd)
{
    off_t fileSize = 0;
    GetSize(fd, &fileSize);
    return fileSize;
}

void MockHandleFileFlushResult(WalStream *walStream, FileDescriptor *fd, off_t offset)
{
    StorageAssert(g_storageInstance != nullptr);
    (void)g_storageInstance->CreateThreadAndRegister();
    UtWalFileFlushCallback(fd, offset, 0, walStream);
    g_storageInstance->UnregisterThread();
}

extern uint64 ComputeWaitPlsnSlotNo(uint64 plsn);

class VFSAdapterMock : public VFSAdapter {
public:
    VFSAdapterMock(PdbId pdbId, WalStream *walStream) : VFSAdapter(pdbId), m_walStream{walStream}
    {
    }

    void Destroy()
    {
        for (int i = 0; i < FILE_COUNT_MAX_VALUE; i++) {
            if (m_asyncWriteThreads[i] != nullptr) {
                m_asyncWriteThreads[i]->join();
                delete m_asyncWriteThreads[i];
            }
        }
    }
    void SetWalFile(WalStream *walStream)
    {
        m_walStream = walStream;
    }

    RetStatus Read(FileId fileId, void *buf, uint64 count, int64 offset, int64 *readSize)
    {
        *readSize = 0;
        return DSTORE_SUCC;
    }

    RetStatus PwriteAsync(FileId fileId, const void *buf, uint64_t count, int64_t offset,
                         const AsyncIoContext *aioContext) override
    {
        if (m_asyncWriteThreads[fileId % FILE_COUNT_MAX_VALUE] != nullptr) {
            m_asyncWriteThreads[fileId % FILE_COUNT_MAX_VALUE]->join();
            delete m_asyncWriteThreads[fileId % FILE_COUNT_MAX_VALUE];
            m_asyncWriteThreads[fileId % FILE_COUNT_MAX_VALUE] = nullptr;
        }
        m_asyncWriteThreads[fileId % FILE_COUNT_MAX_VALUE] = new std::thread(
            &MockHandleFileFlushResult, m_walStream, GetFileDescriptor(fileId), offset + count);
        GaussUsleep(10);
        return DSTORE_SUCC;
    }

    RetStatus Fsync(FileId fileId) override
    {
        return DSTORE_SUCC;
    }

private:
    std::thread *m_asyncWriteThreads[FILE_COUNT_MAX_VALUE] = {nullptr};
    WalStream *m_walStream;
};

class WalFileTest : public WALBASICTEST {
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
        m_walBuffer = DstoreNew(m_ut_memory_context) WalStreamBuffer(m_ut_memory_context, BUF_SIZE / WAL_BLCKSZ);
        ASSERT_NE(m_walBuffer, nullptr);
        EXPECT_EQ(m_walBuffer->Init(0, SEG_SIZE), DSTORE_SUCC);
        m_walStream = DstoreNew(m_ut_memory_context)
            WalStream(m_ut_memory_context, m_walId, m_walFileMgr, SEG_SIZE, m_pdbId);
        ASSERT_NE(m_walStream, nullptr);
        m_walStream->Init(m_walBuffer);
        m_bgWriter =
            DstoreNew(m_ut_memory_context) BgWalWriter(m_ut_memory_context, m_walStream, m_walStream->GetPdbId());
        m_walStream->m_bgWalWriter = m_bgWriter;

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
            SEG_SIZE, 0, false, false, filePara, {UtWalFileFlushCallback, m_walStream}, {}
        };
        rc = strcpy_s(initPara.dir, MAXPGPATH, pdbWalPath);
        storage_securec_check(rc, "\0", "\0");
        m_retStatus = m_walFileMgr->InitWalFiles(initPara, false);
        ASSERT_EQ(m_retStatus, DSTORE_SUCC);
        m_resultCode = memset_s(m_buf, BUF_SIZE, 'a', BUF_SIZE);
        storage_securec_check(m_resultCode, "\0", "\0");
        m_resultCode = memset_s(m_recvBuf, BUF_SIZE, 0, BUF_SIZE);
        storage_securec_check(m_resultCode, "\0", "\0");
    }

    void TearDown() override
    {
        free(m_recvBuf);
        delete m_walStream;
        WALBASICTEST::TearDown();
    }

    void ClearWalFileState(uint64 segSize)
    {
        delete m_walStream;
        m_walFileMgr = DstoreNew(m_ut_memory_context) WalFileManager(m_ut_memory_context);
        m_walBuffer = DstoreNew(m_ut_memory_context) WalStreamBuffer(m_ut_memory_context, 1);
        m_walStream = DstoreNew(m_ut_memory_context) WalStream(m_ut_memory_context, 0, m_walFileMgr, segSize, m_pdbId);
        m_walStream->Init(m_walBuffer);
        char *pdbWalPath = StorageInstance::GetPdbWalPath(g_storageInstance->GetGuc()->dataDir, g_defaultPdbId);
        FileParameter filePara;
        filePara.streamId = 0;
        filePara.flag = APPEND_WRITE_FILE;
        filePara.fileSubType = WAL_LOG_FILE_TYPE;
        filePara.rangeSize = WAL_FILE_RANGE_SIZE;
        filePara.maxSize = segSize;
        filePara.recycleTtl = 0;
        filePara.mode = FILE_READ_AND_WRITE_MODE;
        filePara.isReplayWrite = false;
        TenantConfig *tenantConfig = g_storageInstance->GetGuc()->tenantConfig;
        StorageAssert(tenantConfig);
        errno_t rc = strcpy_s(filePara.storeSpaceName, STORESPACE_NAME_MAX_LEN,
                            tenantConfig->storeSpaces[0].storeSpaceName);
        storage_securec_check(rc, "\0", "\0");
        InitWalFilesPara initPara = {
            {'0'}, m_vfs, m_pdbId, m_walId,
                                     FILE_COUNT_INIT_VALUE, FILE_COUNT_MAX_VALUE, FREE_FILE_COUNT,
            segSize, 0, false, false, filePara, {UtWalFileFlushCallback, m_walStream}, {}
        };
        rc = strcpy_s(initPara.dir, MAXPGPATH, pdbWalPath);
        storage_securec_check(rc, "\0", "\0");
        m_retStatus = m_walFileMgr->InitWalFiles(initPara, false);
    }

    uint16 m_walId = 1;
    WalStream *m_walStream;
    WalStreamBuffer *m_walBuffer;
    WalFileManager *m_walFileMgr;
    BgWalWriter* m_bgWriter;
    uint8 m_buf[BUF_SIZE];
    uint8 *m_recvBuf = (uint8 *)(calloc(1, BUF_SIZE));

    VFSAdapter *m_vfs = nullptr;
    RetStatus m_retStatus;
    int m_resultCode;
    char m_configFilePath[MAXPGPATH];
};

TEST_F (WalFileTest, LoadSegmentFilesTest) {
    char path[MAX_FILE_NAME_LEN];
    char *pdbWalPath = StorageInstance::GetPdbWalPath(g_storageInstance->GetGuc()->dataDir, g_defaultPdbId);
    bool fileExist;
    uint16 startFileId = 2000;
    for (uint16 i = 0; i < SegNum; i++) {
        m_resultCode =
            sprintf_s(path, MAX_FILE_NAME_LEN, "%s/%08hX_%08X_%016llX", pdbWalPath, m_walId, 0, i * SEG_SIZE);
        ASSERT_GT(m_resultCode, 0);
        fileExist = m_vfs->FileExists(startFileId + i, path);
        EXPECT_EQ(fileExist, true);
    }
}

TEST_F (WalFileTest, WritePartFileTest) {
    ssize_t readSize;
    uint64 startPlsn = 0;
    uint64 dataLen = SEG_SIZE / 2;
    WalRecordAtomicGroup *group = reinterpret_cast<WalRecordAtomicGroup *>(m_buf);
    group->groupLen = dataLen;
    void *bufPos = m_walBuffer->GetBufferBlock(startPlsn);
    int ret = memset_s(bufPos, WAL_FILE_HDR_SIZE, 0xFF, WAL_FILE_HDR_SIZE);
    storage_securec_check(ret, "\0", "\0");
    m_walStream->Append(m_buf, dataLen);
    m_walStream->Flush();
    while (m_walStream->GetMaxFlushedPlsn() < startPlsn + dataLen + WAL_FILE_HDR_SIZE) {}
    EXPECT_EQ(m_walStream->GetMaxFlushedPlsn(), startPlsn + dataLen + WAL_FILE_HDR_SIZE);
    FileDescriptor *fileDescriptor = m_walStream->GetWalFileDescriptor(startPlsn);
    Pread(fileDescriptor, m_recvBuf, dataLen + WAL_FILE_HDR_SIZE, startPlsn % SEG_SIZE, &readSize);
    ASSERT_EQ(readSize, dataLen + WAL_FILE_HDR_SIZE);

    WalFileHeaderData *walFileHeaderData = (WalFileHeaderData *)m_recvBuf;
    EXPECT_EQ(walFileHeaderData->fileSize, SEG_SIZE);
    EXPECT_EQ(walFileHeaderData->timelineId, 0);
    EXPECT_EQ(walFileHeaderData->startPlsn, startPlsn);
    EXPECT_EQ(walFileHeaderData->version, 0);
    EXPECT_EQ(walFileHeaderData->magicNum, WAL_FILE_HEAD_MAGIC);
    EXPECT_EQ(walFileHeaderData->lastRecordRemainLen, 0);
    EXPECT_EQ(walFileHeaderData->crc, WalFileHeaderData::ComputeHdrCrc(m_recvBuf));
    EXPECT_EQ(memcmp(m_buf, m_recvBuf + WAL_FILE_HDR_SIZE, dataLen), 0);
}

TEST_F (WalFileTest, WriteMiddleWalFileTest) {
    ssize_t readSize;
    uint64 startPlsn = SEG_SIZE;
    uint64 dataLen = SEG_SIZE / 2;
    WalRecordAtomicGroup *group = reinterpret_cast<WalRecordAtomicGroup *>(m_buf);
    group->groupLen = dataLen;
    void *bufPos = m_walBuffer->GetBufferBlock(startPlsn);
    int ret = memset_s(bufPos, WAL_FILE_HDR_SIZE, 0xFF, WAL_FILE_HDR_SIZE);
    storage_securec_check(ret, "\0", "\0");
    m_walStream->Append(m_buf, startPlsn - WAL_FILE_HDR_SIZE);
    m_walStream->Append(m_buf, dataLen);
    m_walStream->Flush();
    while (m_walStream->GetMaxFlushedPlsn() < startPlsn + dataLen + WAL_FILE_HDR_SIZE) {}
    EXPECT_EQ(m_walStream->GetMaxFlushedPlsn(), startPlsn + dataLen + WAL_FILE_HDR_SIZE);
    FileDescriptor *fileDescriptor = m_walStream->GetWalFileDescriptor(startPlsn);
    Pread(fileDescriptor, m_recvBuf, dataLen + WAL_FILE_HDR_SIZE, startPlsn % SEG_SIZE, &readSize);
    ASSERT_EQ(readSize, dataLen + WAL_FILE_HDR_SIZE);

    WalFileHeaderData *walFileHeaderData = (WalFileHeaderData *)m_recvBuf;
    EXPECT_EQ(walFileHeaderData->fileSize, SEG_SIZE);
    EXPECT_EQ(walFileHeaderData->timelineId, 0);
    EXPECT_EQ(walFileHeaderData->startPlsn, startPlsn);
    EXPECT_EQ(walFileHeaderData->version, 0);
    EXPECT_EQ(walFileHeaderData->magicNum, WAL_FILE_HEAD_MAGIC);
    EXPECT_EQ(walFileHeaderData->lastRecordRemainLen, 0);
    EXPECT_EQ(walFileHeaderData->crc, WalFileHeaderData::ComputeHdrCrc(m_recvBuf));
    EXPECT_EQ(memcmp(m_buf, m_recvBuf + WAL_FILE_HDR_SIZE, dataLen), 0);
}

TEST_F (WalFileTest, WriteLargeWalFileTest) {
#ifndef DYNAMIC_LINK_VFS_LIB
    uint64 largeFileSize = 4 * 1024 * 1024 * 1024UL - WAL_BLCKSZ;
    ClearWalFileState(largeFileSize);
    uint64 plsn = largeFileSize - SEG_SIZE / 2;
    uint64 dataLen = SEG_SIZE;

    m_walStream->Write(plsn, m_buf, dataLen);
    m_walStream->Flush(plsn, plsn + dataLen);
    EXPECT_EQ(m_walStream->GetMaxFlushedPlsn(), plsn + dataLen);
#endif
}

TEST_F (WalFileTest, WriteLenOverAllWalFileTest) {
    uint64 plsn = 0;
    uint16 overNum = 2;
    char *data = (char *)calloc((overNum + SegNum) * SEG_SIZE, sizeof(char));
    m_walStream->Write(plsn, reinterpret_cast<uint8 *>(data), (overNum + SegNum) * SEG_SIZE);
    m_walStream->Flush(plsn, (overNum + SegNum) * SEG_SIZE);
#ifdef DYNAMIC_LINK_VFS_LIB
    while (m_walStream->GetMaxFlushedPlsn() < (SegNum + overNum) * SEG_SIZE) {}
#endif
    ASSERT_EQ(m_walStream->GetMaxFlushedPlsn(), (SegNum + overNum) * SEG_SIZE);
    free(data);
}

TEST_F (WalFileTest, WriteOverOneFileTest) {
    uint64 plsn = 0;
    uint64 dataLen = 0;
    int writeTimes = 10;
    uint64 restSpace = 0;
    uint32 fileOffset = 0;
    uint64 curBytes = 0;
    ssize_t readSize = 0;
    std::mt19937 rng;
    rng.seed(std::random_device()());
    std::uniform_int_distribution<uint64> dist(0, 4 * SEG_SIZE);

    for (int i = 0; i < writeTimes; i++) {
        dataLen = dist(rng);
        m_walStream->Write(plsn, m_buf, dataLen);
        m_walStream->Flush(plsn, plsn + dataLen);
#ifdef DYNAMIC_LINK_VFS_LIB
        while (m_walStream->GetMaxFlushedPlsn() < plsn + dataLen) {}
#endif
        EXPECT_EQ(m_walStream->GetMaxFlushedPlsn(), plsn + dataLen);
        uint32 writeByte = 0;
        while (dataLen > 0) {
            fileOffset = plsn % SEG_SIZE;
            restSpace = SEG_SIZE - fileOffset;
            curBytes = restSpace < dataLen ? restSpace : dataLen;
            Pread(m_walStream->GetWalFileDescriptor(plsn),
                  m_recvBuf + writeByte, curBytes, fileOffset, &readSize);
            ASSERT_EQ(readSize, curBytes);
            if (fileOffset == 0 && curBytes >= WAL_FILE_HDR_SIZE) {
                WalFileHeaderData *walFileHeaderData = (WalFileHeaderData *)(m_recvBuf + writeByte);
                ASSERT_EQ(walFileHeaderData->magicNum, WAL_FILE_HEAD_MAGIC);
                EXPECT_EQ(walFileHeaderData->startPlsn, plsn);
                EXPECT_EQ(walFileHeaderData->crc, WalFileHeaderData::ComputeHdrCrc(m_recvBuf + writeByte));
            }
            dataLen -= curBytes;
            plsn += curBytes;
            writeByte += curBytes;
        }
        EXPECT_EQ(memcmp(m_buf, m_recvBuf, writeByte), 0);
    }
}

TEST_F (WalFileTest, DISABLED_WriteFileRecycleStateTest) {
    delete m_walFileMgr;
    VFSAdapterMock *vfsAdapterMock = DstoreNew(m_ut_memory_context) VFSAdapterMock(m_pdbId, nullptr);
    vfsAdapterMock->Bootstrap();
    m_walFileMgr = DstoreNew(m_ut_memory_context) WalFileManager(m_ut_memory_context);
    char *pdbWalPath = StorageInstance::GetPdbWalPath(g_storageInstance->GetGuc()->dataDir, g_defaultPdbId);
    FileParameter filePara;
    filePara.streamId = 0;
    filePara.flag = APPEND_WRITE_FILE;
    filePara.fileSubType = WAL_LOG_FILE_TYPE;
    filePara.rangeSize = WAL_FILE_RANGE_SIZE;
    filePara.maxSize = SEG_SIZE;
    filePara.recycleTtl = 0;
    filePara.mode = FILE_READ_AND_WRITE_MODE;
    filePara.isReplayWrite = false;
    TenantConfig *tenantConfig = g_storageInstance->GetGuc()->tenantConfig;
    StorageAssert(tenantConfig);
    errno_t rc = strcpy_s(filePara.storeSpaceName, STORESPACE_NAME_MAX_LEN,
                        tenantConfig->storeSpaces[0].storeSpaceName);
    storage_securec_check(rc, "\0", "\0");
    InitWalFilesPara initPara = {
        {'0'}, m_vfs, m_pdbId, m_walId,
                                 FILE_COUNT_INIT_VALUE, FILE_COUNT_MAX_VALUE, FREE_FILE_COUNT,
        SEG_SIZE, 0, false, false, filePara, {UtWalFileFlushCallback, m_walStream}, {}
    };
    rc = strcpy_s(initPara.dir, MAXPGPATH, pdbWalPath);
    storage_securec_check(rc, "\0", "\0");
    m_retStatus = m_walFileMgr->InitWalFiles(initPara, false);
    vfsAdapterMock->SetWalFile(m_walStream);

    uint64 plsn = 0;
    for (int i = 0; i < 2; i++) {
        for (int j = 0; j < SegNum; j++) {
            m_walStream->Write(plsn, m_buf, SEG_SIZE);
            plsn += SEG_SIZE;
        }
    }
    vfsAdapterMock->Destroy();
}

TEST_F (WalFileTest, SyncFileTest) {
#ifndef DYNAMIC_LINK_VFS_LIB
    uint64 plsn = 0;
    m_walStream->Write(plsn, m_buf, SEG_SIZE + 3);
    ASSERT_EQ(m_walStream->Flush(plsn, plsn + SEG_SIZE + 3), DSTORE_SUCC);
    EXPECT_EQ(m_walStream->GetMaxFlushedPlsn(), SEG_SIZE + 3);
    plsn += SEG_SIZE + 3;

    m_walStream->Write(plsn, m_buf, SEG_SIZE);
    ASSERT_EQ(m_walStream->Flush(plsn, plsn + SEG_SIZE), DSTORE_SUCC);
    EXPECT_EQ(m_walStream->GetMaxFlushedPlsn(), plsn + SEG_SIZE);
#endif
}

TEST_F (WalFileTest, DISABLED_TruncateFileTest) {
#ifndef DYNAMIC_LINK_VFS_LIB
    uint64 offset = 0;
    uint64 dataLen = SEG_SIZE;
    int writeTimes = SegNum * 3 / 2;
    for (int i = 0; i < writeTimes; i++) {
        m_walStream->Write(offset, m_buf, dataLen);
        m_walStream->Flush(offset, offset + dataLen);
        offset += dataLen;
    }
    ASSERT_EQ(m_walStream->GetMaxFlushedPlsn(), offset);
    EXPECT_EQ(m_walStream->Truncate(SEG_SIZE), DSTORE_SUCC);
    EXPECT_EQ(m_walStream->Truncate(offset + 1), DSTORE_SUCC);
    m_walStream->Truncate(offset / 2);
#endif
}

TEST_F (WalFileTest, WalFileReadFileTest) {
    uint64 startPlsn = 0;
    uint64 dataLen = SEG_SIZE;
    uint64 resultSize = 0;
    m_walStream->Write(startPlsn, m_buf, dataLen);
    m_walStream->Flush(startPlsn, startPlsn + dataLen);
#ifdef DYNAMIC_LINK_VFS_LIB
    while (m_walStream->GetMaxFlushedPlsn() < startPlsn + dataLen) {}
#endif
    EXPECT_EQ(m_walStream->GetMaxFlushedPlsn(), startPlsn + dataLen);
    m_walStream->Read(startPlsn, m_recvBuf, dataLen, &resultSize);
    ASSERT_EQ(dataLen, resultSize);
    EXPECT_EQ(memcmp(m_buf, m_recvBuf, dataLen), 0);
    startPlsn += dataLen;

    dataLen = SegNum * SEG_SIZE;
    uint8 tmpBuf[dataLen];
    uint8 *tmpRecvBuf = (uint8 *)(calloc(1, dataLen));
    m_walStream->Write(startPlsn, tmpBuf, dataLen);
    m_walStream->Flush(startPlsn, startPlsn + dataLen);
#ifdef DYNAMIC_LINK_VFS_LIB
    while (m_walStream->GetMaxFlushedPlsn() < startPlsn + dataLen) {}
#endif
    EXPECT_EQ(m_walStream->GetMaxFlushedPlsn(), startPlsn + dataLen);
    m_walStream->Read(startPlsn, tmpRecvBuf, dataLen * 2, &resultSize);
    EXPECT_EQ(memcmp(tmpBuf, tmpRecvBuf, dataLen), 0);
    EXPECT_EQ(resultSize, dataLen);
    free(tmpRecvBuf);
}

TEST_F (WalFileTest, WalFileReadInvalidParamTest) {
    uint64 startPlsn = 0;
    uint64 endPlsn = 0;
    uint64 readPlsn = 0;
    uint64 dataLen = 3 * SEG_SIZE;
    uint64 resultSize = 0;
    int writeTimes = 3;
    for (int i = 0; i < writeTimes; i++) {
        m_walStream->Write(startPlsn, m_buf, dataLen);
        startPlsn += dataLen;
    }
    m_walStream->Write(startPlsn, m_buf, SEG_SIZE - 1);
    endPlsn = startPlsn + SEG_SIZE - 1;
    m_walStream->Flush(0, endPlsn);
#ifdef DYNAMIC_LINK_VFS_LIB
    while (m_walStream->GetMaxFlushedPlsn() < endPlsn) {}
#endif
    EXPECT_EQ(m_walStream->GetMaxFlushedPlsn(), endPlsn);
    readPlsn = endPlsn + 1;
    m_retStatus = m_walStream->Read(readPlsn, m_recvBuf, dataLen, &resultSize);
    EXPECT_EQ(m_retStatus, DSTORE_SUCC);
    EXPECT_EQ(resultSize, 0);
}

TEST_F (WalFileTest, WalStreamWriteToFile) {
    uint64 startPlsn = 0;
    uint64 dataLen = SEG_SIZE;
    uint64 resultSize = 0;
    m_walStream->FlushWalBlock(startPlsn, m_buf, dataLen);
#ifdef DYNAMIC_LINK_VFS_LIB
    while (m_walStream->GetMaxFlushedPlsn() < startPlsn + dataLen) {}
#endif
    EXPECT_EQ(m_walStream->GetMaxFlushedPlsn(), startPlsn + dataLen);
    m_walStream->Read(startPlsn, m_recvBuf, dataLen, &resultSize);
    ASSERT_EQ(dataLen, resultSize);
    EXPECT_EQ(memcmp(m_buf, m_recvBuf, dataLen), 0);
}

TEST_F (WalFileTest, WaitTargetPlsnPersistSingleTest) {
    FaultInjectionEntry entries[] = {
        FAULT_INJECTION_ENTRY(DstoreWalFI::WAIT_PLSN_SLOT, false, nullptr),
    };
    ASSERT_EQ(RegisterFaultInjection(entries, sizeof(entries) / sizeof(entries[0]), FI_GLOBAL), ERROR_SYS_OK);
    uint64 startPlsn = 0;
    uint64 endPlsn = 0;
    uint64 dataLen = SEG_SIZE;
    m_walStream->Write(startPlsn, m_buf, dataLen);
    endPlsn = startPlsn + dataLen;
    uint64 slotNo = ComputeWaitPlsnSlotNo(endPlsn);
    FAULT_INJECTION_ACTIVE(DstoreWalFI::WAIT_PLSN_SLOT, FI_GLOBAL);
    std::thread t1 = std::thread([&endPlsn, this] {
        create_thread_and_register();
        m_walStream->WaitTargetPlsnPersist(endPlsn);
        unregister_thread();
    });
    EXPECT_EQ(m_walStream->GetMaxFlushedPlsn(), 0);
    FAULT_INJECTION_WAIT(DstoreWalFI::WAIT_PLSN_SLOT);
    EXPECT_EQ(m_walStream->m_plsnWaitSlot[slotNo].GetPlsnWaiterCount(), 1);
    m_walStream->Flush(0, endPlsn);
    EXPECT_EQ(m_walStream->GetMaxFlushedPlsn(), endPlsn);
    t1.join();
    EXPECT_EQ(m_walStream->m_plsnWaitSlot[slotNo].GetPlsnWaiterCount(), 0);

    FAULT_INJECTION_INACTIVE(DstoreWalFI::WAIT_PLSN_SLOT, FI_GLOBAL);
}

TEST_F (WalFileTest, WaitTargetPlsnPersistConcurrentTest) {
    FaultInjectionEntry entries[] = {
        FAULT_INJECTION_ENTRY(DstoreWalFI::WAIT_PLSN_SLOT, false, nullptr),
    };
    ASSERT_EQ(RegisterFaultInjection(entries, sizeof(entries) / sizeof(entries[0]), FI_GLOBAL), ERROR_SYS_OK);
    uint64 startPlsn = 0;
    uint64 endPlsn = 0;
    constexpr int numThreads = 4;
    int writeTimes = 3;
    uint64 dataLen = SEG_SIZE;
    uint64 resultSize = 0;
    m_walStream->Write(startPlsn, m_buf, dataLen);
    endPlsn = startPlsn + dataLen;
    pthread_barrier_t barrier;
    pthread_barrier_init(&barrier, nullptr, numThreads);
    uint64 slotNo = ComputeWaitPlsnSlotNo(endPlsn);
    FAULT_INJECTION_ACTIVE(DstoreWalFI::WAIT_PLSN_SLOT, FI_GLOBAL);
    std::thread t1 = std::thread([&endPlsn, this, &barrier] {
        create_thread_and_register();
        pthread_barrier_wait(&barrier);
        m_walStream->WaitTargetPlsnPersist(endPlsn);
        unregister_thread();
    });
    std::thread t2 = std::thread([&endPlsn, this, &barrier] {
        create_thread_and_register();
        pthread_barrier_wait(&barrier);
        m_walStream->WaitTargetPlsnPersist(endPlsn);
        unregister_thread();
    });
    std::thread t3 = std::thread([&endPlsn, this, &barrier] {
        create_thread_and_register();
        pthread_barrier_wait(&barrier);
        m_walStream->WaitTargetPlsnPersist(endPlsn);
        unregister_thread();
    });
    pthread_barrier_wait(&barrier);
    FAULT_INJECTION_WAIT(DstoreWalFI::WAIT_PLSN_SLOT);
    FAULT_INJECTION_WAIT(DstoreWalFI::WAIT_PLSN_SLOT);
    FAULT_INJECTION_WAIT(DstoreWalFI::WAIT_PLSN_SLOT);
    EXPECT_EQ(m_walStream->m_plsnWaitSlot[slotNo].GetPlsnWaiterCount(), 3);
    m_walStream->Flush(0, endPlsn);
    t1.join();
    t2.join();
    t3.join();
    EXPECT_EQ(m_walStream->m_plsnWaitSlot[slotNo].GetPlsnWaiterCount(), 0);
}

TEST_F (WalFileTest, DeleteWalFileTest) {
    WalId walId = m_walId + 1;
    WalFileManager *walFileMgr = DstoreNew(m_ut_memory_context) WalFileManager(m_ut_memory_context);
    ASSERT_NE(walFileMgr, nullptr);
    WalStreamBuffer *walBuffer = DstoreNew(m_ut_memory_context) WalStreamBuffer(m_ut_memory_context, 1);
    ASSERT_NE(walBuffer, nullptr);
    WalStream *walStream = DstoreNew(m_ut_memory_context)
        WalStream(m_ut_memory_context, walId, walFileMgr, SEG_SIZE, m_pdbId);
    ASSERT_NE(walStream, nullptr);
    walStream->Init(walBuffer);

    char *pdbWalPath = StorageInstance::GetPdbWalPath(g_storageInstance->GetGuc()->dataDir, m_pdbId);
    FileParameter filePara;
    filePara.streamId = walId;
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
    InitWalFilesPara initPara = {{'0'}, m_vfs, m_pdbId, walId, FILE_COUNT_INIT_VALUE, FILE_COUNT_MAX_VALUE,
        FREE_FILE_COUNT, SEG_SIZE, 0, false, false, filePara, {UtWalFileFlushCallback, walStream}, {}};
    rc = strcpy_s(initPara.dir, MAXPGPATH, pdbWalPath);
    storage_securec_check(rc, "\0", "\0");
    RetStatus retStatus = walFileMgr->InitWalFiles(initPara, false);
    ASSERT_EQ(retStatus, DSTORE_SUCC);
    bool fileExist;
    char path[MAXPGPATH] = {0};
    for (uint16 i = 0; i < SegNum; i++) {
        int resultCode =
            sprintf_s(path, MAX_FILE_NAME_LEN, "%s/%08hX_%08X_%016llX", pdbWalPath, walId, 0, i * SEG_SIZE);
        ASSERT_GT(resultCode, 0);
        fileExist = m_vfs->FileExists(path);
        EXPECT_EQ(fileExist, true);
    }

    delete walFileMgr;
    walFileMgr = DstoreNew(m_ut_memory_context) WalFileManager(m_ut_memory_context);
    ASSERT_NE(walFileMgr, nullptr);
    initPara.onlyForRead = true;
    initPara.startPlsn = SEG_SIZE * 2;
    retStatus = walFileMgr->InitWalFiles(initPara, false);
    ASSERT_EQ(retStatus, DSTORE_SUCC);
    walStream->m_walFileManager = walFileMgr;
    EXPECT_EQ(walFileMgr->GetAllWalFileCount(), SegNum);

    walFileMgr->StopRecycleWalFileWorker();
    walFileMgr->StartupRecycleWalFileWorker(true);
    while (!walFileMgr->IsRecoveryRecycleFinish()) {
        GaussUsleep(100);
    }
    for (uint16 i = 0; i < SegNum; i++) {
        int resultCode =
            sprintf_s(path, MAX_FILE_NAME_LEN, "%s/%08hX_%08X_%016llX", pdbWalPath, walId, 0, i * SEG_SIZE);
        ASSERT_GT(resultCode, 0);
        fileExist = m_vfs->FileExists(path);
        EXPECT_EQ(fileExist, false);
    }
    delete walStream;
}
}
