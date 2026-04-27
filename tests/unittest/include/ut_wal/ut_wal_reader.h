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
#include "ut_utilities/ut_dstore_framework.h"
#include "ut_buffer/ut_buffer_fake_instance.h"
#include "ut_wal/ut_wal_basic.h"
#include "vfs/vfs_interface.h"
#include "wal/dstore_wal_reader.h"
#include "common/algorithm/dstore_checksum_impl.h"

using namespace DSTORE;
extern char g_utTopDir[MAXPGPATH];

class UTWalReaderTest : public WALBASICTEST {
protected:
    void SetUp() override
    {
        SetDefaultPdbId(PDB_TEMPLATE1_ID);
        DSTORETEST::SetUp();
#ifdef DYNAMIC_LINK_VFS_LIB
        /* Get the config file. */
        memset(m_configFilePath, 0, MAXPGPATH);
        char *execPath = m_configFilePath;
        int readRet = readlink("/proc/self/exe", execPath, MAXPGPATH);
        ASSERT_GT(readRet, 0);
        char *lastSlashPtr = strrchr(execPath, '/');
        ASSERT_NE(lastSlashPtr, nullptr);

        snprintf(lastSlashPtr + 1, MAXPGPATH / 2, "dynamic_vfs.conf.init");
        ASSERT_EQ(access(m_configFilePath, F_OK), 0); /* It must exist. */

        DSTORETEST::m_guc.vfsConfigPath = m_configFilePath;
#endif
        uint32 length = MAX_WAL_RECORD_SIZE;
        m_record = (WalRecord *)calloc(length, sizeof(char));
        char *m_record_data = reinterpret_cast<char *>(m_record);
        for (uint32 i = 0; i < length; i++) {
            m_record_data[i] = 'a' + i % 26;
        }
        m_record->SetSize(length);
        m_record->SetType(WAL_EMPTY_REDO);
    }

    void TearDown() override
    {
        free(m_record);
        WALBASICTEST::TearDown();
    }

    RetStatus PrepareWalFile(uint64 walFileSize = WAL_READ_BUFFER_BLOCK_SIZE)
    {
        /* Read block size is min(WAL_READ_BUFFER_BLOCK_SIZE, walFileSize), so the wal file size need to be greater than
         * WAL_READ_BUFFER_BLOCK_SIZE */
        WALBASICTEST::Prepare(walFileSize);
        g_storageInstance->GetGuc()->walFileSize = walFileSize;
        PrepareControlFileContent();
        NodeId selfNode = 0;
        RetStatus retStatus = m_walStreamManager->Init(m_walControlFile);
        if (retStatus != DSTORE_SUCC) {
            return retStatus;
        }
        m_walStream = m_walStreamManager->GetWritingWalStream();
        m_startReadPlsn = m_walStream->GetMaxWrittenToFilePlsn();
#ifdef DYNAMIC_LINK_VFS_LIB
        m_walStream->StopRecycleThread();
#endif
        m_vfs = g_storageInstance->GetPdb(g_defaultPdbId)->GetVFS();
        m_walFileSize = g_storageInstance->GetGuc()->walFileSize;
        return DSTORE_SUCC;
    }

    void ReadAndCheckWalRecordValid(uint32 num, WalRecordReader *reader, uint64 readStartPlsn, uint64 minGroupLen,
                              uint32 expectRecordNum, RetStatus expectLastRet)
    {
        RetStatus ret;
        WalReaderConf readerConf = {m_walStream->GetWalId(), readStartPlsn, m_walStream, nullptr, m_walFileSize,
                                    DSTORE::WalReadSource::WAL_READ_FROM_DISK};
        bool readerAssignedByCaller = true;
        if (reader == nullptr) {
            readerAssignedByCaller = false;
            ret = WalRecordReader::AllocateWalReader(readerConf, &reader, m_ut_memory_context);
            ASSERT_EQ(ret, DSTORE_SUCC);
            ASSERT_NE(reader, nullptr);
        }
        const WalRecordAtomicGroup *readGroup;
        for (uint32 i = 0; i < num; i++) {
            ret = reader->ReadNext(&readGroup);
            ASSERT_EQ(ret, DSTORE_SUCC);
            ASSERT_NE(readGroup, nullptr);
            ASSERT_GE(readGroup->groupLen, minGroupLen);
            uint32 offset = offsetof(WalRecordAtomicGroup, crc) + sizeof((static_cast<WalRecordAtomicGroup*>(nullptr))->crc);
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
            ASSERT_EQ(recordNum, expectRecordNum);
            ASSERT_EQ(recordNum, readGroup->recordNum);
        }
        ret = reader->ReadNext(&readGroup);
        if (expectLastRet == DSTORE_SUCC) {
            ASSERT_EQ(ret, DSTORE_SUCC);
            ASSERT_EQ(readGroup, nullptr);
        }else {
            ASSERT_EQ(ret, DSTORE_FAIL);
        }
        if (!readerAssignedByCaller) {
            delete reader;
        }
    }

    void RestartWal()
    {
        int rc;
        char backupWalDir[MAXPGPATH] = {};
        char pdbWalPath[MAXPGPATH] = {};
        if (g_defaultPdbId == PDB_TEMPLATE1_ID) {
            rc = sprintf_s(pdbWalPath, MAXPGPATH, "%s/%s/%s/wal",  g_storageInstance->GetGuc()->dataDir, BASE_DIR, g_storageInstance->m_guc->tenantConfig->storageConfig.template1VfsName);
        } else {
            rc = sprintf_s(pdbWalPath, MAXPGPATH, "%s/%s/%s/wal", g_storageInstance->GetGuc()->dataDir, BASE_DIR, g_storageInstance->m_guc->tenantConfig->storageConfig.rootpdbVfsName);
        }
        storage_securec_check_ss(rc);
        rc = sprintf_s(backupWalDir, MAXPGPATH, "%s/wal_backup/", g_utTopDir);
        storage_securec_check_ss(rc);

        WALBASICTEST::TearDown();
        rc = rename(pdbWalPath, backupWalDir);
        ASSERT_EQ(rc, 0);

        DSTORETEST::SetUp();
        UtInstance *instance = DstoreNew(m_ut_memory_context)UtInstance();
        instance->Install(&DSTORETEST::m_guc, m_ut_memory_context);
        instance->Startup(&DSTORETEST::m_guc);
        Prepare(16 * 1024);
        char cmd[MAXPGPATH] = {};
        rc = sprintf_s(cmd, MAXPGPATH, "rm -r %s/*", pdbWalPath);
        storage_securec_check_ss(rc);
        rc = system(cmd);
        ASSERT_EQ(rc, 0);
        rc = rename(backupWalDir, pdbWalPath);
        ASSERT_EQ(rc, 0);

        m_walManager->Destroy();
        m_walManager->Init(m_walControlFile);

        m_walStreamManager = const_cast<WalStreamManager *>(m_walManager->GetWalStreamManager());
        delete thrd->m_walWriterContext;
        thrd->m_walWriterContext = DstoreNew(thrd->m_memoryMgr->GetRoot())
                AtomicWalWriterContext(thrd->m_memoryMgr->GetRoot(), m_pdbId,
                                       g_storageInstance->GetPdb(m_pdbId)->GetWalMgr());


        if (thrd->m_walWriterContext != nullptr) {
            UNUSE_PARAM RetStatus walWriterInitResult = thrd->m_walWriterContext->Init();
            StorageAssert(walWriterInitResult == DSTORE_SUCC);
        }
        m_walWriter = thrd->m_walWriterContext;

        uint32 streamCount = 0;
        m_walStream = m_walStreamManager->GetWritingWalStream();
        NodeId selfNode = 0;
        m_walStreamManager->InitWalStreamBgWriter(m_walStream);
    }
    WalRecord *m_record;
    VFSAdapter *m_vfs;
    WalStream *m_walStream;
    uint64 m_startReadPlsn;
    char m_configFilePath[MAXPGPATH];
    uint64 m_walFileSize;
};
