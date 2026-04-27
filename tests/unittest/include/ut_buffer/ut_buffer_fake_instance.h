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
#ifndef DSTORE_UT_BUFFER_FAKE_INSTANCE_H
#define DSTORE_UT_BUFFER_FAKE_INSTANCE_H
#include "ut_utilities/ut_dstore_framework.h"
#include "port/dstore_port.h"
#include "framework/dstore_instance.h"
#include "framework/dstore_thread.h"
#include "ut_mock/ut_mock.h"
#include "ut_mock/ut_wal_stream_mock.h"
#include "ut_wal/ut_wal_buffer.h"
#include "buffer/dstore_bg_page_writer_mgr.h"
#include "buffer/dstore_bg_disk_page_writer.h"
#include "framework/dstore_session_interface.h"

using namespace DSTORE;

class BufferMgrFakeStoragePdb : public StoragePdb {
public:
    explicit BufferMgrFakeStoragePdb(PdbId pdbId) : StoragePdb(pdbId) {}
    explicit BufferMgrFakeStoragePdb(PdbId pdbId, const char *pdbUuid) : StoragePdb(pdbId, pdbUuid) {}
    virtual ~BufferMgrFakeStoragePdb() {};

protected:

    /*
     * In buffer ut test, we skip BufMgrInit, because we will create BufMgr in UT test case.
     * so we should skip all steps which need buffer manager in StorageInstance initialize.
     *
     * In AttachUndoTablespace will init tablespace, so skip it.
     */
    void LoadUndoMapSegment() override {}
};

class BufferMgrFakeStorageInstance : public UtInstance {
public:
    BufferMgrFakeStorageInstance() : UtInstance() {}

    ~BufferMgrFakeStorageInstance() = default;

    void Startup(StorageGUC *guc) override
    {
        g_storageInstance = this;
        g_storageInstance->InitWorkingVersionNum(&GRAND_VERSION_NUM);
        (void)CreateMemMgr();
        thrd = dynamic_cast<ThreadContext*>(ThreadContextInterface::Create());
        (void)thrd->InitializeBasic();
        StorageSession *sc = CreateStorageSession(1ULL);
        thrd->AttachSessionToThread(sc);
        (void)StartupInstance(guc);

        (void)thrd->InitStorageContext(g_defaultPdbId);
        (void)thrd->InitTransactionRuntime(g_defaultPdbId, nullptr, nullptr);
    }

    void ThreadSetupAndRegister() override
    {
        StorageAssert(thrd == nullptr);
        StorageAssert(g_storageInstance != nullptr);

        thrd = DstoreNew(g_storageInstance->GetMemoryMgr()->GetGroupContext(DSTORE::MEMORY_CONTEXT_LONGLIVE))
            ThreadContext();
        (void)thrd->InitializeBasic();
        DSTORE::StorageSession *sc = DSTORE::CreateStorageSession(1LL);
        thrd->AttachSessionToThread(sc);
        (void)thrd->InitStorageContext(g_defaultPdbId);
        (void)thrd->InitTransactionRuntime(g_defaultPdbId, nullptr, nullptr);

        g_storageInstance->AddVisibleThread(thrd, g_defaultPdbId);
    }

protected:
    RetStatus BufMgrInit() override {
        m_bufMgr = DstoreNew(m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_BUFFER))BufMgr(m_guc->buffer, 1);
        m_bufMgr->Init();
        return DSTORE_SUCC;
    }

    void BufMgrDestroy() override {
        m_bufMgr->Destroy();
        delete m_bufMgr;
    }

    StoragePdb *CreatePdbObj(PdbId pdbId) override
    {
        m_pdb[StoragePdb::GetPdbIndex(pdbId)] =
            DstoreNew(m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LONGLIVE)) BufferMgrFakeStoragePdb(pdbId);
        return m_pdb[StoragePdb::GetPdbIndex(pdbId)];
    }

    StoragePdb *CreatePdbObj(PdbId pdbId, const char *pdbUuid) override
    {
        m_pdb[StoragePdb::GetPdbIndex(pdbId)] =
            DstoreNew(m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LONGLIVE)) BufferMgrFakeStoragePdb(pdbId, pdbUuid);
        return m_pdb[StoragePdb::GetPdbIndex(pdbId)];
    }
};

/* For BgPageWriter UT test.
 * (1) we mock the WalStream because it's not initialized in real code.
 * (2) we always initialize real BufMgr, the MockBufMgr can not work.
 */
class BgPageWriterFakeStorageInstance : public UtInstance {
public:
    BgPageWriterFakeStorageInstance() : UtInstance(), m_fakeWalStream{nullptr}
    {}
    ~BgPageWriterFakeStorageInstance() override = default;

    WalStream *GetWalStream()
    {
        return m_fakeWalStream;
    }

    void Startup(StorageGUC *guc) override
    {
        g_storageInstance = this;
        g_storageInstance->InitWorkingVersionNum(&GRAND_VERSION_NUM);
        (void)CreateMemMgr();
        thrd = DstoreNew(GetMemoryMgr()->GetGroupContext(DSTORE::MEMORY_CONTEXT_LONGLIVE)) ThreadContext();
        (void)thrd->InitializeBasic();
        auto *sc = CreateStorageSession(1ULL);
        thrd->AttachSessionToThread(sc);
        (void)StartupInstance(guc);

        bg_page_writer_thread_init();

        (void)thrd->InitStorageContext(g_defaultPdbId);
        (void)thrd->InitTransactionRuntime(g_defaultPdbId, nullptr, nullptr);
    }

    /* FIXME: add this in PDB */
    void bg_page_writer_thread_init()
    {
        BgPageWriterMgrInit();
        m_bgPageWriterMgr = DSTORE::g_storageInstance->GetPdb(DSTORE::g_defaultPdbId)->GetBgPageWriterMgr();
        m_walStreamBuffer = DstoreNew(m_mcxt) MockWalStreamBuffer(m_mcxt, 0);
        m_walFileMgr = DstoreNew(m_mcxt) WalFileManager(m_mcxt);

        m_fakeWalStream = DstoreNew(m_mcxt) MockWalStream(m_walId, m_mcxt, m_walFileMgr, g_defaultPdbId);
        m_fakeWalStream->Init(m_walStreamBuffer);

        int64 slotId;
        RetStatus ret = m_bgPageWriterMgr->CreateBgPageWriter(m_fakeWalStream, &slotId);
        StorageAssert(ret == DSTORE::DSTORE_SUCC);
        DSTORE::g_storageInstance->GetPdb(DSTORE::g_defaultPdbId)->SetBgWriterSlotId(slotId);

        BgPageWriterEntry *entry = m_bgPageWriterMgr->GetBgPageWriterEntry(slotId);
        entry->bgDiskThread = new std::thread(BgPageWriterMgr::BgPageWriterMain<BgDiskPageMasterWriter>,
                                              entry->bgDiskPageWriter, g_defaultPdbId, std::ref(ret));

        /* wait until BgPageWriter thread is ready */
        while (!entry->bgDiskPageWriter->IsReady()) {
            GaussUsleep(1000);
        }
    }

protected:
    WalStream *m_fakeWalStream;
    WalStreamBuffer *m_walStreamBuffer;
    WalFileManager *m_walFileMgr;
    uint32 m_walId = 1; /* Create a default wal stream */
    DstoreMemoryContext m_mcxt;
    BgPageWriterMgr *m_bgPageWriterMgr;

    void BgPageWriterMgrInit()
    {
        m_mcxt = DstoreAllocSetContextCreate(
            m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_BUFFER),
                "BgPageWriterMemoryContext",
                ALLOCSET_DEFAULT_MINSIZE,
                ALLOCSET_DEFAULT_INITSIZE,
                ALLOCSET_DEFAULT_INITSIZE,
                MemoryContextType::SHARED_CONTEXT);
        m_bgPageWriterMgr = DSTORE::g_storageInstance->GetPdb(DSTORE::g_defaultPdbId)->GetBgPageWriterMgr();
    }

    void BgPageWriterMgrDestroy()
    {
        m_bgPageWriterMgr->StopAllBgPageWriter();
        m_bgPageWriterMgr->Destroy();
        delete m_bgPageWriterMgr;
        delete m_fakeWalStream;
        delete m_walFileMgr;
        delete m_walStreamBuffer;
    }

    RetStatus BufMgrInit() override
    {
        m_bufMgr = DstoreNew(m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_BUFFER))BufMgr(m_guc->buffer, 1);
        m_bufMgr->Init();
        return DSTORE_SUCC;
    }

    void BufMgrDestroy() override
    {
        m_bufMgr->Destroy();
        delete m_bufMgr;
    }
};

#endif /* DSTORE_UT_BUFFER_FAKE_INSTANCE_H */
