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
 * dstore_control_file.cpp
 *
 *
 *
 * IDENTIFICATION
 *        storage/include/control/dstore_control_file.h
 *
 * ---------------------------------------------------------------------------------------
 */
#include "control/dstore_control_file.h"
#include "framework/dstore_instance.h"
namespace DSTORE {
const ControlPageTypeInfo CONTROL_PAGE_TYPE_INFOS[CONTROL_MAX_PAGE_TYPE] = {
    {
        CONTROL_PAGE_TYPE_INVALID,
        0,
        nullptr,
        nullptr,
        DSTORE_INVALID_BLOCK_NUMBER
    },
    {
        CONTROL_FILE_MATAPAGE_TYPE,
        0,
        nullptr,
        nullptr,
        DSTORE_INVALID_BLOCK_NUMBER
    },
    {
        CONTROL_TBS_METAPAGE_TYPE,
        0,
        nullptr,
        nullptr,
        DSTORE_INVALID_BLOCK_NUMBER
    },
    {
        CONTROL_TBS_TABLESPACE_DATAPAGE_TYPE,
        sizeof(ControlTablespacePageItemData),
        nullptr,
        ControlTablespacePageItemData::Dump,
        static_cast<BlockNumber>(DEFAULT_TABLESPACE_PAGE)
    },
    {
        CONTROL_TBS_DATAFILE_DATAPAGE_TYPE,
        sizeof(ControlDataFilePageItemData),
        nullptr,
        ControlDataFilePageItemData::Dump,
        static_cast<BlockNumber>(DEFAULT_DATAFILE_PAGE)
    },
    {
        CONTROL_WAL_STREAM_METAPAGE_TYPE,
        0,
        ControlWalInfoMeta::Dump,
        nullptr,
        DSTORE_INVALID_BLOCK_NUMBER
    },
    {
        CONTROL_WAL_STREAM_DATAPAGE_TYPE,
        sizeof(ControlWalStreamPageItemData),
        nullptr,
        ControlWalStreamPageItemData::Dump,
        static_cast<BlockNumber>(CONTROLFILE_PAGEMAP_WALSTREAM_START)
    },
    {
        CONTROL_CSN_METAPAGE_TYPE,
        0,
        ControlCsnPageData::Dump,
        nullptr,
        DSTORE_INVALID_BLOCK_NUMBER
    },
    {
        CONTROL_CSN_DATAPAGE_TYPE,
        0,
        nullptr,
        nullptr,
        static_cast<BlockNumber>(CONTROLFILE_PAGEMAP_CSN_START)
    },
    {
        CONTROL_RELMAP_METAPAGE_TYPE,
        0,
        ControlRelMapMeta::Dump,
        nullptr,
        DSTORE_INVALID_BLOCK_NUMBER
    },
    {
        CONTROL_RELMAP_SHARED_DATAPAGE_TYPE,
        sizeof(ControlSysTableItemData),
        nullptr,
        ControlSysTableItemData::Dump,
        static_cast<BlockNumber>(CONTROLFILE_PAGEMAP_RELMAP_START)
    },
    {
        CONTROL_RELMAP_LOCAL_DATAPAGE_TYPE,
        sizeof(ControlSysTableItemData),
        nullptr,
        ControlSysTableItemData::Dump,
        static_cast<BlockNumber>(CONTROLFILE_PAGEMAP_RELMAP_START + 1)
    },
    {
        CONTROL_PDBINFO_METAPAGE_TYPE,
        0,
        nullptr,
        nullptr,
        DSTORE_INVALID_BLOCK_NUMBER
    },
    {
        CONTROL_PDBINFO_DATAPAGE_TYPE,
        sizeof(ControlPdbInfoPageItemData),
        nullptr,
        ControlPdbInfoPageItemData::Dump,
        static_cast<BlockNumber>(CONTROLFILE_PAGEMAP_PDBINFO_START)
    },
    {
        CONTROL_LOGICALREP_METAPAGE_TYPE,
        0,
        nullptr,
        nullptr,
        DSTORE_INVALID_BLOCK_NUMBER
    },
    {
        CONTROL_LOGICALREP_DATAPAGE_TYPE,
        sizeof(ControlLogicalReplicationSlotPageItemData),
        nullptr,
        ControlLogicalReplicationSlotPageItemData::Dump,
        static_cast<BlockNumber>(CONTROLFILE_PAGEMAP_LOGICALREP_START)
    }
};

RetStatus ControlFile::Init(const char *dataDir)
{
    if (unlikely(m_initialized.load(std::memory_order_acquire))) {
        return DSTORE_SUCC;
    }
    RetStatus ret = InitFileMgrAndGroup(dataDir);
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Init control file manager and group failed."));
        return DSTORE_FAIL;
    }
    ret = m_controlFileMgr->OpenControlFiles();
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Open control file failed."));
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(m_controlFileMgr->LoadControlFile())) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Load controlFile failed when init controlFile"));
        return DSTORE_FAIL;
    }

    if (STORAGE_FUNC_FAIL(m_controlFileMgr->CheckCrcAndRecoveryForFileMeta()) ||
        STORAGE_FUNC_FAIL(m_controlPdbInfo->CheckCrcAndRecovery()) ||
        STORAGE_FUNC_FAIL(m_controlRelmap->CheckCrcAndRecovery()) ||
        STORAGE_FUNC_FAIL(m_controlCsnInfo->CheckCrcAndRecovery()) ||
        STORAGE_FUNC_FAIL(m_controlTablespace->CheckCrcAndRecovery()) ||
        STORAGE_FUNC_FAIL(m_controlWalInfo->CheckCrcAndRecovery()) ||
        STORAGE_FUNC_FAIL(m_controlLogicRep->CheckCrcAndRecovery())) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Recover control file failed when init controlFile"));
        return DSTORE_FAIL;
    }

    m_initialized.store(true, std::memory_order_release);
    return DSTORE_SUCC;
}

RetStatus ControlFile::InitFileMgrAndGroup(const char *dataDir)
{
    ErrLog(DSTORE_LOG, MODULE_CONTROL, ErrMsg("Start InitFileMgrAndGroup for pdb %u.", m_pdbId));
    m_ctx = DstoreAllocSetContextCreate(g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE),
                                        "ControlfileMemory", ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE,
                                        ALLOCSET_DEFAULT_MAXSIZE, MemoryContextType::SHARED_CONTEXT);
    if (m_ctx == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Create ControlfileMemory failed. Controlfile is unavailable."));
        return DSTORE_FAIL;
    }
    /* create filemgr instance */
    TenantConfig *tenantConfig = g_storageInstance->GetGuc()->tenantConfig;
    StorageAssert(tenantConfig);
    RetStatus ret = DSTORE_SUCC;
    do {
        m_controlFileMgr = DstoreNew(m_ctx) ControlFileMgr(m_pdbId, m_deployType, m_ctx);
        if (unlikely(m_controlFileMgr == nullptr)) {
            ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("New control file manager fail."));
            ret = DSTORE_FAIL;
            break;
        }
        m_controlFileMgr->SetEnableCachePage(m_enableCachePage.load(std::memory_order_acquire));
        if (STORAGE_FUNC_FAIL(m_controlFileMgr->Init(m_vfs, tenantConfig->storeSpaces[0].storeSpaceName, dataDir))) {
            ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Init control file manager fail."));
            ret = DSTORE_FAIL;
            break;
        }
        m_controlTablespace = DstoreNew(m_ctx) ControlTablespace(m_controlFileMgr, m_ctx, m_pdbId);
        if (unlikely(m_controlTablespace == nullptr)) {
            ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("New controltablespace fail."));
            ret = DSTORE_FAIL;
            break;
        }
        if (STORAGE_FUNC_FAIL(m_controlTablespace->Init(m_deployType))) {
            ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Init controltablespace fail."));
            ret = DSTORE_FAIL;
            break;
        }
        m_controlWalInfo = DstoreNew(m_ctx) ControlWalInfo(m_controlFileMgr, m_ctx, m_pdbId);
        if (unlikely(m_controlWalInfo == nullptr)) {
            ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("New controlwalinfo fail."));
            ret = DSTORE_FAIL;
            break;
        }
        if (STORAGE_FUNC_FAIL(m_controlWalInfo->Init(m_deployType))) {
            ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Init controlwalinfo fail."));
            ret = DSTORE_FAIL;
            break;
        }
        m_controlCsnInfo = DstoreNew(m_ctx) ControlCsnInfo(m_controlFileMgr, m_ctx, m_pdbId);
        if (unlikely(m_controlCsnInfo == nullptr)) {
            ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("New controlCsnInfo fail."));
            ret = DSTORE_FAIL;
            break;
        }
        if (STORAGE_FUNC_FAIL(m_controlCsnInfo->Init(m_deployType))) {
            ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Init controlcsninfo fail."));
            ret = DSTORE_FAIL;
            break;
        }
        m_controlRelmap = DstoreNew(m_ctx) ControlRelmap(m_controlFileMgr, m_ctx, m_pdbId);
        if (unlikely(m_controlRelmap == nullptr)) {
            ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("New controlRelmap fail."));
            ret = DSTORE_FAIL;
            break;
        }
        if (STORAGE_FUNC_FAIL(m_controlRelmap->Init(m_deployType))) {
            ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Init controlrelmap fail."));
            ret = DSTORE_FAIL;
            break;
        }
        m_controlPdbInfo = DstoreNew(m_ctx) ControlPdbInfo(m_controlFileMgr, m_ctx, m_pdbId);
        if (unlikely(m_controlPdbInfo == nullptr)) {
            ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("New controlPdbInfo fail."));
            ret = DSTORE_FAIL;
            break;
        }
        if (STORAGE_FUNC_FAIL(m_controlPdbInfo->Init(m_deployType))) {
            ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Init controlpdbinfo fail."));
            ret = DSTORE_FAIL;
            break;
        }
        m_controlLogicRep = DstoreNew(m_ctx) ControlLogicRep(m_controlFileMgr, m_ctx, m_pdbId);
        if (unlikely(m_controlLogicRep == nullptr)) {
            ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("New m_controlLogicRep fail."));
            ret = DSTORE_FAIL;
            break;
        }
        if (STORAGE_FUNC_FAIL(m_controlLogicRep->Init(m_deployType))) {
            ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Init controllogicrep fail."));
            ret = DSTORE_FAIL;
            break;
        }
    } while (0);
    if (unlikely(ret == DSTORE_FAIL)) {
        DestroyFileMgrAndGroupMgr();
        return ret;
    }
    ErrLog(DSTORE_LOG, MODULE_CONTROL, ErrMsg("Finish InitFileMgrAndGroup for pdb %u.", m_pdbId));
    return DSTORE_SUCC;
}

void ControlFile::DestroyFileMgrAndGroupMgr()
{
    ErrLog(DSTORE_LOG, MODULE_CONTROL, ErrMsg("Start DestroyFileMgrAndGroupMgr for pdb %u.", m_pdbId));
    if (m_controlFileMgr != nullptr) {
        m_controlFileMgr->CloseControlFiles();
        delete m_controlFileMgr;
        m_controlFileMgr = nullptr;
    }
    if (m_controlTablespace != nullptr) {
        m_controlTablespace->Destroy();
        delete m_controlTablespace;
        m_controlTablespace = nullptr;
    }
    if (m_controlWalInfo != nullptr) {
        m_controlWalInfo->Destroy();
        delete m_controlWalInfo;
        m_controlWalInfo = nullptr;
    }
    if (m_controlCsnInfo != nullptr) {
        m_controlCsnInfo->Destroy();
        delete m_controlCsnInfo;
        m_controlCsnInfo = nullptr;
    }
    if (m_controlRelmap != nullptr) {
        m_controlRelmap->Destroy();
        delete m_controlRelmap;
        m_controlRelmap = nullptr;
    }
    if (m_controlPdbInfo != nullptr) {
        m_controlPdbInfo->Destroy();
        delete m_controlPdbInfo;
        m_controlPdbInfo = nullptr;
    }
    if (m_controlLogicRep != nullptr) {
        m_controlLogicRep->Destroy();
        delete m_controlLogicRep;
        m_controlLogicRep = nullptr;
    }
    if (m_ctx != nullptr) {
        DstoreMemoryContextDelete(m_ctx);
        ErrLog(DSTORE_LOG, MODULE_CONTROL, ErrMsg("DstoreMemoryContextDelete for pdb %u.", m_pdbId));
        m_ctx = nullptr;
    }
    ErrLog(DSTORE_LOG, MODULE_CONTROL, ErrMsg("Finish DestroyFileMgrAndGroupMgr for pdb %u.", m_pdbId));
}

ControlFile::ControlFile(PdbId pdbId, VFSAdapter *vfs, DeployType deployType)
{
    StorageAssert(vfs != nullptr);
    m_initialized.store(false, std::memory_order_release);
    m_ctx = nullptr;
    m_pdbId = pdbId;
    m_vfs = vfs;
    m_controlFileMgr = nullptr;
    m_controlTablespace = nullptr;
    m_controlWalInfo = nullptr;
    m_controlCsnInfo = nullptr;
    m_controlRelmap = nullptr;
    m_controlPdbInfo = nullptr;
    m_controlLogicRep = nullptr;
    m_enableCachePage = true;
    if (deployType == DeployType::CONTROL_FILE_DEPLOYTYPE_INVALID) {
        m_deployType = (g_storageInstance->GetType() == StorageInstanceType::SINGLE) ? CONTROL_FILE_SINGLE_NODE
                                                                                    : CONTROL_FILE_DISTRIBUTE;
    } else {
        m_deployType = deployType;
    }
}

ControlFile::~ControlFile()
{
    DestroyFileMgrAndGroupMgr();
}

RetStatus ControlFile::Create(const char *dataDir)
{
    /* Step 1. Create file mgr and group. */
    ErrLog(DSTORE_LOG, MODULE_CONTROL, ErrMsg("Start create control file for pdb %u.", GetPdbId()));
    RetStatus ret = InitFileMgrAndGroup(dataDir);
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Init control file manager and group failed."));
        return DSTORE_FAIL;
    }
    /* Step 2. Create and open disk control file. */
    ErrLog(DSTORE_LOG, MODULE_CONTROL, ErrMsg("Init control file manager and group success."));
    ret = m_controlFileMgr->CreateControlFiles();
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Create control files failed."));
        return DSTORE_FAIL;
    }
    /* Step 3. Init group meta/info/data page. */
    ErrLog(DSTORE_LOG, MODULE_CONTROL, ErrMsg("Create control files success."));
    ret = m_controlTablespace->Create();
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Init table space related page failed."));
        return DSTORE_FAIL;
    }
    ErrLog(DSTORE_LOG, MODULE_CONTROL, ErrMsg("Init table space related page success."));
    ret = m_controlWalInfo->Create();
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Init wal stream related page failed."));
        return DSTORE_FAIL;
    }
    ErrLog(DSTORE_LOG, MODULE_CONTROL, ErrMsg("Init wal stream related page success."));
    ret = m_controlCsnInfo->Create();
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Init csn related page failed."));
        return DSTORE_FAIL;
    }
    ErrLog(DSTORE_LOG, MODULE_CONTROL, ErrMsg("Init csn related page success."));
    ret = m_controlRelmap->Create();
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Init relmap related page failed."));
        return DSTORE_FAIL;
    }
    ErrLog(DSTORE_LOG, MODULE_CONTROL, ErrMsg("Init relmap related page success."));
    ret = m_controlPdbInfo->Create();
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Init pdb info related page failed."));
        return DSTORE_FAIL;
    }
    ErrLog(DSTORE_LOG, MODULE_CONTROL, ErrMsg("Init pdb info related page success."));
    ret = m_controlLogicRep->Create();
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Init logic replication related page failed."));
        return DSTORE_FAIL;
    }
    ErrLog(DSTORE_LOG, MODULE_CONTROL, ErrMsg("Init logic replication related page success."));
    /* Step 4. Write, sync and close control file. */
    ret = m_controlFileMgr->WriteAndSyncFiles();
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Write and sync control files failed."));
        return DSTORE_FAIL;
    }
    ErrLog(DSTORE_LOG, MODULE_CONTROL, ErrMsg("Write and sync control files success."));
    /* Step 5. Close control file. */

    ret = m_controlFileMgr->CloseControlFiles();
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Close control files failed."));
        return DSTORE_FAIL;
    }
    ErrLog(DSTORE_LOG, MODULE_CONTROL, ErrMsg("End create control files for pdb %u.", GetPdbId()));
    return DSTORE_SUCC;
}

RetStatus ControlFile::CopyControlFile(ControlFile *srcControlFile)
{
    if (unlikely(srcControlFile == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("CopyControlFile srcControlFile is null."));
        return DSTORE_FAIL;
    }

    ControlFileMgr *srcControlFileMgr = srcControlFile->m_controlFileMgr;
    if (unlikely(srcControlFileMgr == nullptr || m_controlFileMgr == nullptr ||
        srcControlFile->m_controlWalInfo == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("CopyControlFile ControlFileMgr or ControlWalInfo is null."));
        return DSTORE_FAIL;
    }

    ControlDiskFile *srcfile1 = srcControlFileMgr->GetFile1();
    ControlDiskFile *file1 = m_controlFileMgr->GetFile1();
    ControlDiskFile *file2 = m_controlFileMgr->GetFile2();
    if (unlikely(srcfile1 == nullptr || file1 == nullptr || file2 == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("CopyControlFile  ControlDiskFile is null."));
        return DSTORE_FAIL;
    }

    // Add walstreaminfo control file read lock to avoid m_flag=1 when copying the control file.
    if (STORAGE_FUNC_FAIL(srcControlFile->m_controlWalInfo->LockGroup(CFLockMode::CF_SHARE))) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg(
            "Acquire control file lock fail, pbdId %u.", m_pdbId));
        return DSTORE_FAIL;
    }

    int64 realReadSize;
    int64 readBlock = CONTROL_WRITE_ONCE_BLOCK_COUNT;
    AutoMemCxtSwitch memCxtSwitch(m_ctx);
    char *buffer = static_cast<char *>(DstorePallocAligned(static_cast<Size>(readBlock * BLCKSZ), BLCKSZ));
    if (STORAGE_VAR_NULL(buffer)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Alloc memory failed."));
        srcControlFile->m_controlWalInfo->UnLockGroup(CFLockMode::CF_SHARE);
        return DSTORE_FAIL;
    }
    ErrLog(DSTORE_LOG, MODULE_CONTROL,
           ErrMsg("Start copy control file from pdb %u to pdb %u.", srcControlFile->GetPdbId(), GetPdbId()));
    BlockNumber blockCount = srcControlFile->GetBlockCount();
    for (uint32 i = 0; i < blockCount; i += CONTROL_WRITE_ONCE_BLOCK_COUNT) {
        if (i + CONTROL_WRITE_ONCE_BLOCK_COUNT <= blockCount) {
            readBlock = CONTROL_WRITE_ONCE_BLOCK_COUNT;
        } else {
            readBlock = (blockCount - i);
        }
        int64 offset = static_cast<int64>(i) * BLCKSZ;

        if (STORAGE_FUNC_FAIL(
                srcfile1->PreadSync(buffer, CONTROL_WRITE_ONCE_BLOCK_COUNT * BLCKSZ, offset, &realReadSize))) {
            DstorePfreeAligned(buffer);
            ErrLog(
                DSTORE_ERROR, MODULE_CONTROL,
                ErrMsg("Failed to pread control file from pdb %u to pdb %u.", srcControlFile->GetPdbId(), GetPdbId()));
            srcControlFile->m_controlWalInfo->UnLockGroup(CFLockMode::CF_SHARE);
            return DSTORE_FAIL;
        }
        StorageAssert(readBlock * BLCKSZ == realReadSize);
        RetStatus retStatus = file1->PwriteSync(buffer, realReadSize, offset);
        if (STORAGE_FUNC_FAIL(retStatus)) {
            DstorePfreeAligned(buffer);
            ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Failed to pwrite pdb %u controlfile1 failed.", GetPdbId()));
            srcControlFile->m_controlWalInfo->UnLockGroup(CFLockMode::CF_SHARE);
            return DSTORE_FAIL;
        }
        retStatus = file2->PwriteSync(buffer, realReadSize, offset);
        if (STORAGE_FUNC_FAIL(retStatus)) {
            DstorePfreeAligned(buffer);
            ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Failed to pwrite pdb %u controlfile1 failed.", GetPdbId()));
            srcControlFile->m_controlWalInfo->UnLockGroup(CFLockMode::CF_SHARE);
            return DSTORE_FAIL;
        }
    }
    DstorePfreeAligned(buffer);
    if (unlikely(file1->Fsync() != DSTORE_SUCC || file2->Fsync() != DSTORE_SUCC)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Failed to fsync pdb %u control file.", GetPdbId()));
        srcControlFile->m_controlWalInfo->UnLockGroup(CFLockMode::CF_SHARE);
        return DSTORE_FAIL;
    }
    ErrLog(DSTORE_LOG, MODULE_CONTROL,
           ErrMsg("End copy control file from pdb %u to pdb %u.", srcControlFile->GetPdbId(), GetPdbId()));
    srcControlFile->m_controlWalInfo->UnLockGroup(CFLockMode::CF_SHARE);

    return DSTORE_SUCC;
}

RetStatus ControlFile::OpenControlFile(char *dataDir)
{
    if (m_initialized.load(std::memory_order_acquire)) {
        return DSTORE_SUCC;
    }

    if (STORAGE_FUNC_FAIL(Init(dataDir))) {
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus ControlFile::LoadControlFile(bool reload)
{
    if (m_initialized.load(std::memory_order_acquire) && !reload) {
        return DSTORE_SUCC;
    }

    m_controlCsnInfo->Reload();
    m_controlTablespace->Reload();
    m_controlWalInfo->Reload();
    m_controlRelmap->Reload();
    m_controlPdbInfo->Reload();
    m_controlLogicRep->Reload();

    if (STORAGE_FUNC_FAIL(m_controlPdbInfo->BuildPdbHashIndex())) {
        return DSTORE_FAIL;
    }
    m_initialized.store(true, std::memory_order_release);
    return DSTORE_SUCC;
}
} // namespace DSTORE
