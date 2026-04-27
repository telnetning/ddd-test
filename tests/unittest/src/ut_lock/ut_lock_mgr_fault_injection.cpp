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

#ifndef ENABLE_FAULT_INJECTION
#error "The fault injection function is not enabled, some testcases may fail."
#endif

#include "ut_utilities/ut_dstore_framework.h"
#include "framework/dstore_instance.h"
#include "common/memory/dstore_mctx.h"
#include "lock/dstore_lock_mgr.h"
#include "lock/dstore_table_lock_mgr.h"
#include "lock/dstore_xact_lock_mgr.h"
#include "common/fault_injection/dstore_lock_mgr_fault_injection.h"
#include "fault_injection/fault_injection.h"
#include <thread>
#include <mutex>

using namespace DSTORE;

class UTLockMgrFaultInjection : public DSTORETEST {
protected:
    void SetUp() override
    {
        DSTORETEST::SetUp();
        UtInstance *instance = DstoreNew(m_ut_memory_context)UtInstance();
        instance->Install(&DSTORETEST::m_guc, m_ut_memory_context);
        instance->Startup(&DSTORETEST::m_guc);

        FaultInjectionEntry entries[] = {
            FAULT_INJECTION_ENTRY(DstoreLockMgrFI::LOCK_REQUEST_OOM_POINT, false, nullptr),
            FAULT_INJECTION_ENTRY(DstoreLockMgrFI::LOCAL_LOCK_OOM_POINT, false, nullptr),
            FAULT_INJECTION_ENTRY(DstoreLockMgrFI::LOCK_HASH_OOM_POINT, false, nullptr)};
        ASSERT_EQ(RegisterFaultInjection(entries, sizeof(entries) / sizeof(entries[0]), FI_GLOBAL), ERROR_SYS_OK);
    }

    void TearDown() override
    {
        UtInstance *instance = (UtInstance *)g_storageInstance;
        instance->Shutdown();
        delete instance;
        DSTORETEST::TearDown();

        DestroyFaultInjectionHash(FI_GLOBAL);
    }
};

TEST_F(UTLockMgrFaultInjection, LockWhenOOMTest_level0)
{
    LockTag tag;
    tag.SetTableLockTag(0, 0);
    LockMode mode = DSTORE_ACCESS_SHARE_LOCK;

    /* With the OOM injection point enabled, we expect lock mgr to return failure on new lock requests. */
    ASSERT_FALSE(g_storageInstance->GetLockMgr() == NULL);

    FAULT_INJECTION_ACTIVE(DstoreLockMgrFI::LOCAL_LOCK_OOM_POINT, FI_GLOBAL);
    RetStatus ret = g_storageInstance->GetLockMgr()->Lock(&tag, mode, LOCK_WAIT, nullptr);
    EXPECT_TRUE(ret == DSTORE_FAIL);
    EXPECT_TRUE(StorageGetErrorCode() == LOCK_ERROR_OUT_OF_MEMORY);
    FAULT_INJECTION_INACTIVE(DstoreLockMgrFI::LOCAL_LOCK_OOM_POINT, FI_GLOBAL);

    FAULT_INJECTION_ACTIVE(DstoreLockMgrFI::LOCK_HASH_OOM_POINT, FI_GLOBAL);
    ret = g_storageInstance->GetLockMgr()->Lock(&tag, mode, LOCK_WAIT, nullptr);
    EXPECT_TRUE(ret == DSTORE_FAIL);
    EXPECT_TRUE(StorageGetErrorCode() == LOCK_ERROR_OUT_OF_MEMORY);
    FAULT_INJECTION_INACTIVE(DstoreLockMgrFI::LOCK_HASH_OOM_POINT, FI_GLOBAL);

    FAULT_INJECTION_ACTIVE(DstoreLockMgrFI::LOCK_REQUEST_OOM_POINT, FI_GLOBAL);
    ret = g_storageInstance->GetLockMgr()->Lock(&tag, mode, LOCK_WAIT, nullptr);
    EXPECT_TRUE(ret == DSTORE_FAIL);
    EXPECT_TRUE(StorageGetErrorCode() == LOCK_ERROR_OUT_OF_MEMORY);
    FAULT_INJECTION_INACTIVE(DstoreLockMgrFI::LOCK_REQUEST_OOM_POINT, FI_GLOBAL);

    /*
     * With the OOM injection point enabled, we expect table lock mgr to return failure on new lock requests.
     * The exception is a weak lock that goes through the fast path, it doesn't need to allocate memory.
     */
    ASSERT_FALSE(g_storageInstance->GetTableLockMgr() == NULL);

    FAULT_INJECTION_ACTIVE(DstoreLockMgrFI::LOCAL_LOCK_OOM_POINT, FI_GLOBAL);
    ret = g_storageInstance->GetTableLockMgr()->Lock(&tag, mode, LOCK_WAIT, nullptr);
    EXPECT_TRUE(ret == DSTORE_FAIL);
    EXPECT_TRUE(StorageGetErrorCode() == LOCK_ERROR_OUT_OF_MEMORY);
    FAULT_INJECTION_INACTIVE(DstoreLockMgrFI::LOCAL_LOCK_OOM_POINT, FI_GLOBAL);

    FAULT_INJECTION_ACTIVE(DstoreLockMgrFI::LOCK_HASH_OOM_POINT, FI_GLOBAL);
    ret = g_storageInstance->GetTableLockMgr()->Lock(&tag, mode, LOCK_WAIT, nullptr);
    EXPECT_TRUE(ret == DSTORE_SUCC);
    g_storageInstance->GetTableLockMgr()->Unlock(&tag, mode);
    FAULT_INJECTION_INACTIVE(DstoreLockMgrFI::LOCK_HASH_OOM_POINT, FI_GLOBAL);

    FAULT_INJECTION_ACTIVE(DstoreLockMgrFI::LOCK_REQUEST_OOM_POINT, FI_GLOBAL);
    ret = g_storageInstance->GetTableLockMgr()->Lock(&tag, mode, LOCK_WAIT, nullptr);
    EXPECT_TRUE(ret == DSTORE_SUCC);
    g_storageInstance->GetTableLockMgr()->Unlock(&tag, mode);
    FAULT_INJECTION_INACTIVE(DstoreLockMgrFI::LOCK_REQUEST_OOM_POINT, FI_GLOBAL);

    mode = DSTORE_EXCLUSIVE_LOCK;

    FAULT_INJECTION_ACTIVE(DstoreLockMgrFI::LOCK_HASH_OOM_POINT, FI_GLOBAL);
    ret = g_storageInstance->GetTableLockMgr()->Lock(&tag, mode, LOCK_WAIT, nullptr);
    EXPECT_TRUE(ret == DSTORE_FAIL);
    EXPECT_TRUE(StorageGetErrorCode() == LOCK_ERROR_OUT_OF_MEMORY);
    FAULT_INJECTION_INACTIVE(DstoreLockMgrFI::LOCK_HASH_OOM_POINT, FI_GLOBAL);

    FAULT_INJECTION_ACTIVE(DstoreLockMgrFI::LOCK_REQUEST_OOM_POINT, FI_GLOBAL);
    ret = g_storageInstance->GetTableLockMgr()->Lock(&tag, mode, LOCK_WAIT, nullptr);
    EXPECT_TRUE(ret == DSTORE_FAIL);
    EXPECT_TRUE(StorageGetErrorCode() == LOCK_ERROR_OUT_OF_MEMORY);
    FAULT_INJECTION_INACTIVE(DstoreLockMgrFI::LOCK_REQUEST_OOM_POINT, FI_GLOBAL);

    /* With the OOM injection point enabled, we expect xact lock mgr to return failure on new lock requests. */
    ASSERT_FALSE(g_storageInstance->GetXactLockMgr() == NULL);
    Xid xid = INVALID_XID;

    FAULT_INJECTION_ACTIVE(DstoreLockMgrFI::LOCAL_LOCK_OOM_POINT, FI_GLOBAL);
    ret = g_storageInstance->GetXactLockMgr()->Lock(g_defaultPdbId, xid);
    EXPECT_TRUE(ret == DSTORE_FAIL);
    EXPECT_TRUE(StorageGetErrorCode() == LOCK_ERROR_OUT_OF_MEMORY);
    FAULT_INJECTION_INACTIVE(DstoreLockMgrFI::LOCAL_LOCK_OOM_POINT, FI_GLOBAL);

    FAULT_INJECTION_ACTIVE(DstoreLockMgrFI::LOCK_HASH_OOM_POINT, FI_GLOBAL);
    ret = g_storageInstance->GetXactLockMgr()->Lock(g_defaultPdbId, xid);
    EXPECT_TRUE(ret == DSTORE_FAIL);
    EXPECT_TRUE(StorageGetErrorCode() == LOCK_ERROR_OUT_OF_MEMORY);
    FAULT_INJECTION_INACTIVE(DstoreLockMgrFI::LOCK_HASH_OOM_POINT, FI_GLOBAL);

    FAULT_INJECTION_ACTIVE(DstoreLockMgrFI::LOCK_REQUEST_OOM_POINT, FI_GLOBAL);
    ret = g_storageInstance->GetXactLockMgr()->Lock(g_defaultPdbId, xid);
    EXPECT_TRUE(ret == DSTORE_FAIL);
    EXPECT_TRUE(StorageGetErrorCode() == LOCK_ERROR_OUT_OF_MEMORY);
    FAULT_INJECTION_INACTIVE(DstoreLockMgrFI::LOCK_REQUEST_OOM_POINT, FI_GLOBAL);
}
