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
 * dstore_instance_interface.cpp
 *
 * IDENTIFICATION
 *        storage/src/framework/dstore_instance_interface.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "framework/dstore_instance_interface.h"
#include "framework/dstore_instance.h"
#include "systable/dstore_rootdb_utility.h"

namespace DSTORE {

static void *g_objectPtr = nullptr;

StorageInstanceInterface *StorageInstanceInterface::Create(StorageInstanceType type)
{
    size_t memSize = 0;
    if (type == StorageInstanceType::SINGLE) {
        memSize = sizeof(StorageInstance);
    } else {
        ErrLog(DSTORE_PANIC, MODULE_FRAMEWORK, ErrMsg("Unknow storage instance type."));
    }

    if (unlikely(g_objectPtr != nullptr)) {
        /* dstore instance is created twice, and the previous memory should be freed,
         * in nsql, it will only be created once. */
        free(g_objectPtr);
        g_objectPtr = nullptr;
    }
    g_objectPtr = malloc(memSize + sizeof(void *) + DSTORE_CACHELINE_SIZE);
    if (unlikely(g_objectPtr == nullptr)) {
        return nullptr;
    }

    void *alignedAddr = reinterpret_cast<void *>(TYPEALIGN(DSTORE_CACHELINE_SIZE, g_objectPtr));
    if (type == StorageInstanceType::SINGLE) {
        g_storageInstance = new (alignedAddr) StorageInstance();
    } else {
        ErrLog(DSTORE_PANIC, MODULE_FRAMEWORK, ErrMsg("Unknow storage instance type."));
    }
    RetStatus ret = g_storageInstance->CreateMemMgr();
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to create mem mgr."));
        g_storageInstance = nullptr;
        free(g_objectPtr);
        g_objectPtr = nullptr;
        return nullptr;
    }

    return static_cast<StorageInstanceInterface *>(g_storageInstance);
}

void StorageInstanceInterface::DestoryInstance()
{
    if (unlikely(g_storageInstance == nullptr)) {
        return;
    }
    static_cast<StorageInstanceInterface *>(g_storageInstance)->~StorageInstanceInterface();
    g_storageInstance = nullptr;
    thrd = nullptr;
    free(g_objectPtr);
    g_objectPtr = nullptr;
}

void DestroyObject(void **ptr)
{
    DstorePfreeExt(*ptr);
}

void InitSignalMask()
{
    sigset_t intMask;
    (void)sigfillset(&intMask);
    (void)sigdelset(&intMask, SIGPROF);
    (void)sigdelset(&intMask, SIGSEGV);
    (void)sigdelset(&intMask, SIGBUS);
    (void)sigdelset(&intMask, SIGFPE);
    (void)sigdelset(&intMask, SIGILL);
    (void)sigdelset(&intMask, SIGSYS);
    (void)pthread_sigmask(SIG_SETMASK, &intMask, nullptr);
}

void MemCallBackinitInterface(void *reserve, void *release)
{
    DstoreAllocSetContext::get_MemContextCallBack().InitMemCallBack(reserve, release);
}

void CreateTemplateTablespace(PdbId pdbId)
{
    StoragePdb *templatePdb = g_storageInstance->GetPdb(pdbId);
    templatePdb->CreateTemplateTablespace();
}

void CreateUndoMapSegment(PdbId pdbId)
{
    StoragePdb *pdb = g_storageInstance->GetPdb(pdbId);
    StorageReleasePanic(pdb == nullptr, MODULE_FRAMEWORK, ErrMsg("Pdb %u doesn't exist.", pdbId));
    pdb->CreateUndoMapSegment();
}

} /* namespace DSTORE */
