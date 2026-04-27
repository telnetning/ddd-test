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
#include "framework/dstore_thread_interface.h"
#include "common/memory/dstore_mctx.h"
#include "framework/dstore_instance.h"
#include "framework/dstore_thread.h"
#include "transaction/dstore_csn_mgr.h"
#include "transaction/dstore_tac_snapshot.h"

namespace DSTORE {

ThreadContextInterface *ThreadContextInterface::Create()
{
    AutoMemCxtSwitch autoSwtich{
        g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE)};
    char *alignedMem = (char *)DstorePallocAligned(sizeof(ThreadContext), DSTORE_CACHELINE_SIZE);
    if (STORAGE_VAR_NULL(alignedMem)) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("Failed to create thread context for alloc alignedMem."));
        return nullptr;
    }
    thrd = DstoreNew(alignedMem) ThreadContext();
    if (STORAGE_VAR_NULL(thrd)) {
        DstorePfreeAligned(alignedMem);
        ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("Failed to create thread context."));
        return nullptr;
    }
    return thrd;
}

ThreadContextInterface *ThreadContextInterface::GetCurrentThreadContext()
{
    return thrd;
}

void ThreadContextInterface::DestroyCurrentThreadContext()
{
    thrd->Destroy();
    DstorePfreeAligned(thrd);
    thrd = nullptr;
}

bool ThreadContextInterface::IsTacReplayDisabled()
{
    return thrd->m_tacReplayDisabled;
}

void ThreadContextInterface::SetTacReplayDisabled(bool tacReplayDisabled)
{
    thrd->m_tacReplayDisabled = tacReplayDisabled;
}

TacSnapshotPayload ThreadContextInterface::GetTacSnapshotReceivedPayload()
{
    return thrd->m_tacSnapshotReceived;
}

void ThreadContextInterface::SetTacSnapshotReceivedPayload(CommitSeqNo csn)
{
    thrd->m_tacSnapshotReceived.snapshotcsn = csn;
}

TacSnapshotPayload ThreadContextInterface::GetTacSnapshotToSendPayload()
{
    return thrd->m_tacSnapshotToSend;
}

void ThreadContextInterface::SetTacSnapshotToSendPayload(CommitSeqNo csn)
{
    thrd->m_tacSnapshotToSend.snapshotcsn = csn;
}

void ThreadContextInterface::AddTacOrphanTrx()
{
    /* Check if firstStatementCsnmin is valid or not first */
    if (DstoreCsnIsValid(thrd->GetXact()->firstStatementCsnMin)) {
        g_storageInstance->GetCsnMgr()->AddCsnToTacTracker(thrd->GetXact()->firstStatementCsnMin);
        /* Reset the firstStatementCsnMin after we add them into the tracker */
        ClearTacFirstStatementCsnMin();
        return;
    }
}

void ThreadContextInterface::ApplyTacSnapshot()
{
    /* Check if we already have a received TAC snapshot from the client */
    if (thrd->m_tacSnapshotReceived.snapshotcsn != INVALID_CSN) {
        /* Apply (Overwrite) TAC snapshot if there is one received from the client. */
        thrd->SetSnapshotCsnForTac(thrd->m_tacSnapshotReceived.snapshotcsn);
    }
    /* Save the snapshot to be sent. */
    thrd->m_tacSnapshotToSend.snapshotcsn = thrd->GetSnapShotCsn();

    SetTacFirstStatementCsnMin(thrd->m_tacSnapshotToSend.snapshotcsn);
}

void ThreadContextInterface::SetTacFirstStatementCsnMin(CommitSeqNo csn)
{
    /* Save firstStatementCsnmin if it wasn't saved yet, given that the provided csn is valid */
    if (!DstoreCsnIsValid(thrd->GetXact()->firstStatementCsnMin) && DstoreCsnIsValid(csn)) {
        thrd->GetXact()->firstStatementCsnMin = csn;
        /* We don't want the firstStatementCsnmin to be considered as expired during a normal transaction */
        thrd->GetXact()->firstStatementCsnMinExpiryTimestamp = static_cast<uint64>(TIMESTAMPTZ_MAX);
    }
}

void ThreadContextInterface::StartTacFirstStatementCsnMinExpiryTimer()
{
    thrd->StartTacFirstStatementCsnMinExpiryTimer();
}

void ThreadContextInterface::ClearTacFirstStatementCsnMin()
{
    /* Clear out firstStatementCsnMin */
    thrd->GetXact()->firstStatementCsnMin = INVALID_CSN;
    /* Set expiry time for firstStatementCsnMin to be invalid */
    thrd->GetXact()->firstStatementCsnMinExpiryTimestamp = 0;
}

RetStatus ThreadContextInterface::GetAllThreadsInfo(ThreadStatsInfo **threadInfoArr, size_t *length)
{
    if (STORAGE_VAR_NULL(threadInfoArr) || STORAGE_VAR_NULL(length)) {
        return DSTORE_FAIL;
    }
    ThreadCoreMgr *coreManager = g_storageInstance->GetThreadCoreMgr();
    return coreManager->GetThreadsInfo(threadInfoArr, length);
}

/* FreeThreadInfoArr() - free the array of ThreadStatsInfo */
void ThreadContextInterface::FreeThreadInfoArr(ThreadStatsInfo **threadInfoArr, size_t length)
{
    if (STORAGE_VAR_NULL(threadInfoArr) || STORAGE_VAR_NULL(*threadInfoArr) || unlikely(length == 0)) {
        return;
    }
    size_t idx = 0;
    while (idx < length) {
        DstorePfreeExt((*threadInfoArr)[idx].threadName);
        idx++;
    }
    DstorePfreeExt(*threadInfoArr);
}

} /* namespace DSTORE */
