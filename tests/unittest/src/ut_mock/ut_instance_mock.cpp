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
#include "ut_mock/ut_instance_mock.h"
#include "ut_utilities/ut_dstore_framework.h"
#include "framework/dstore_instance.h"
#include "framework/dstore_thread.h"
#include "ut_mock/ut_mock.h"

void UTStorageInstance::Initialize(StorageGUC *guc, bool bootStrap)
{
    thrd = DstoreNew(g_storageInstance->GetMemoryMgr()->GetGroupContext(DSTORE::MEMORY_CONTEXT_LONGLIVE))
            ThreadContext();
    (void)thrd->InitializeBasic();
    (void)GucInit(guc);
    (void)TypecacheMgrInit();
    (void)ThreadCoreMgrInit();
    (void)BufMgrInit();
    (void)InitAllLockMgrs();
    InitPdbLwLocks();
    (void)InitPdbSlots();
    InitPdbInfoCache();
    if (bootStrap) {
        CreateTemplatePdb();
    }
    InitAllPdb();
    m_instanceState = static_cast<uint8>(InstanceState::ACTIVE);

    create_thread_and_register();

    ut_init_transaction_runtime();
}

RetStatus UTStorageInstance::BufMgrInit()
{
    m_bufMgr = UtMockModule::UtInitBufMgr(m_guc->buffer, m_guc->bufferLruPartition);
    return DSTORE_SUCC;
}