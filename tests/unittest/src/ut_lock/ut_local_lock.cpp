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
#include "framework/dstore_instance.h"
#include "common/memory/dstore_mctx.h"
#include "lock/dstore_lock_mgr.h"
#include "lock/dstore_lock_thrd_local.h"
#include "lock/dstore_table_lock_mgr.h"
#include "lock/dstore_xact_lock_mgr.h"
#include <mutex>

using namespace DSTORE;

class UTLockMgrLocalLock : public DSTORETEST {
protected:
    void SetUp() override
    {
        DSTORETEST::SetUp();
        UtInstance *instance = DstoreNew(m_ut_memory_context)UtInstance();
        instance->Install(&DSTORETEST::m_guc, m_ut_memory_context);
        instance->Startup(&DSTORETEST::m_guc);
    }

    void TearDown() override
    {
        UtInstance *instance = (UtInstance *)g_storageInstance;
        instance->Shutdown();
        delete instance;
        DSTORETEST::TearDown();
    }

    bool IsHoldingLockInMode(const LockTag &tag, LockMode mode, int count);
};

bool UTLockMgrLocalLock::IsHoldingLockInMode(const LockTag &tag, LockMode mode, int count)
{
    ThreadLocalLock *localLock = thrd->GetLockCtx()->GetLocalLock();
    EXPECT_TRUE(localLock != nullptr);
    bool ret = false;
    LockMask mask = 0x1U << static_cast<uint8>(mode);

    const ThreadLocalLock::LocalLockEntry *entry;
    ThreadLocalLock::HoldLockIterator iter(localLock);
    while ((entry = iter.GetNextLock()) != nullptr) {
        if ((entry->tag == tag) && (entry->granted[mode] == count)) {
            EXPECT_TRUE((entry->GetMask() && mask) != 0);
            ret = true;
            break;
        }
    }
    return ret;
}

/*
 * Local Lock component test.
 */
TEST_F(UTLockMgrLocalLock, LocalLockTest_level0)
{
    ThreadLocalLock *localLock = thrd->GetLockCtx()->GetLocalLock();
    ASSERT_TRUE(localLock != nullptr);

    LockTag tag;
    tag.SetTableLockTag(0, 0);
    LockTag cmpTag;
    LockMode mode = DSTORE_EXCLUSIVE_LOCK;
    LockMode cmpMode;
    LockTagCache tagCache(&tag);
    RetStatus ret = localLock->GrantIfAlreadyHold(tagCache, mode, LOCK_MGR);
    EXPECT_FALSE(ret == DSTORE_SUCC);
    EXPECT_TRUE(localLock->IsWaitingLock(&cmpTag, &cmpMode));
    EXPECT_TRUE(cmpTag == tag);
    EXPECT_TRUE(cmpMode == mode);
    /* First time a particular lock is acquired, LOCK_INFO_NOT_AVAIL should be set. */
    EXPECT_TRUE(StorageGetErrorCode() == LOCK_INFO_NOT_AVAIL);

    /* After abort lock acquiring, local lock should be empty. */
    localLock->RecordLockResult(tagCache, mode, DSTORE_FAIL);
    EXPECT_TRUE(localLock->IsEmpty());
    EXPECT_FALSE(localLock->IsWaitingLock(&cmpTag, &cmpMode));

    /* Try again and grant this lock, local lock should not be empty. */
    ret = localLock->GrantIfAlreadyHold(tagCache, mode, LOCK_MGR);
    EXPECT_FALSE(ret == DSTORE_SUCC);
    EXPECT_TRUE(localLock->IsWaitingLock(&cmpTag, &cmpMode));
    EXPECT_TRUE(cmpTag == tag);
    EXPECT_TRUE(cmpMode == mode);
    localLock->RecordLockResult(tagCache, mode, DSTORE_SUCC);
    EXPECT_FALSE(localLock->IsEmpty());
    EXPECT_FALSE(localLock->IsWaitingLock(&cmpTag, &cmpMode));

    /* Ask for the same lock again, it should return succ. */
    ret = localLock->GrantIfAlreadyHold(tagCache, mode, LOCK_MGR);
    EXPECT_TRUE(ret == DSTORE_SUCC);
    EXPECT_FALSE(localLock->IsEmpty());
    EXPECT_FALSE(localLock->IsWaitingLock(&cmpTag, &cmpMode));
    EXPECT_TRUE(IsHoldingLockInMode(tag, mode, 2));

    /* Try release this lock from local lock. */
    ret = localLock->UngrantIfGrantedMultipleTimes(tagCache, mode);
    EXPECT_TRUE(ret == DSTORE_SUCC);
    EXPECT_FALSE(localLock->IsEmpty());
    EXPECT_FALSE(localLock->IsWaitingLock(&cmpTag, &cmpMode));

    /* Try release this lock again, it should fail because it is the only one for the same lock. */
    ret = localLock->UngrantIfGrantedMultipleTimes(tagCache, mode);
    EXPECT_FALSE(ret == DSTORE_SUCC);
    EXPECT_FALSE(localLock->IsEmpty());
    EXPECT_FALSE(localLock->IsWaitingLock(&cmpTag, &cmpMode));

    /* Release this lock, local lock should be empty. */
    localLock->RemoveLockRecord(tagCache, mode);
    EXPECT_TRUE(localLock->IsEmpty());
    EXPECT_FALSE(localLock->IsWaitingLock(&cmpTag, &cmpMode));
}
