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
 * dstore_cursor.cpp
 *
 * IDENTIFICATION
 *        src/transaction/dstore_cursor.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "transaction/dstore_cursor.h"
#include "transaction/dstore_transaction_mgr.h"
#include "errorcode/dstore_transaction_error_code.h"
#include "framework/dstore_instance.h"
#include "common/log/dstore_log.h"

namespace DSTORE {

RetStatus CursorSnapshot::Init(const char *name, const SnapshotData &snapshot)
{
    AutoMemCxtSwitch memCxtSwitch(thrd->GetTransactionMemoryContext());
    /* initialize CursorSnapshot node before adding it to the CursorSnapshotList */
    UNUSE_PARAM Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("Transaction is null when init cursor snapshot."));
        return DSTORE_FAIL;
    }
    if (STORAGE_VAR_NULL(name)) {
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("Name is null when init cursor snapshot."));
        return DSTORE_FAIL;
    }
    StringInfoData string;
    if (unlikely(!string.init())) {
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("Malloc cursor name failed!"));
        return DSTORE_FAIL;
    }
    string.AppendString(name);
    /* initialize name */
    m_cursorName = string.data;
    /* initialize snapshot data */
    m_snapshot.SetCsn(snapshot.GetCsn());
    m_snapshot.SetCid(snapshot.GetCid());
    return DSTORE_SUCC;
}

bool CursorSnapshotList::CheckCursorSnapshotExists(const char *name)
{
    if (IsEmpty()) {
        return false;
    }

    dlist_mutable_iter iter;
    dlist_foreach_modify(iter, &m_cursorSnapshotList) {
        CursorSnapshot *cursorSnapshot = CursorSnapshot::GetCursorSnapshotFromNode(iter.cur);
        if (cursorSnapshot->HasSameName(name)) {
            return true;
        }
    }
    return false;
}

RetStatus CursorSnapshotList::AddCursorSnapshot(const char *name, const SnapshotData &snapshot)
{
    if (unlikely(CheckCursorSnapshotExists(name))) {
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
            ErrMsg("AddCursorSnapshot name %s, already exists", name));
        storage_set_error(TRANSACTION_ERROR_INVALID_STATE, name);
        return DSTORE_FAIL;
    }

    CursorSnapshot *cursorSnapshot = DstoreNew(thrd->GetTransactionMemoryContext()) CursorSnapshot();
    if (STORAGE_VAR_NULL(cursorSnapshot)) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        return DSTORE_FAIL;
    }

    StorageAssert(cursorSnapshot != nullptr);
    RetStatus ret = cursorSnapshot->Init(name, snapshot);
    if (STORAGE_FUNC_FAIL(ret)) {
        storage_set_error(COMMON_ERROR_UNDEFINED_ERROR);
        delete cursorSnapshot;
        return ret;
    }

    cursorSnapshot->PushSelfToListTail(&m_cursorSnapshotList);
    return DSTORE_SUCC;
}

RetStatus CursorSnapshotList::DeleteCursorSnapshot(const char *name)
{
    if (!IsEmpty()) {
        dlist_mutable_iter iter;
        dlist_foreach_modify(iter, &m_cursorSnapshotList) {
            CursorSnapshot *cursorSnapshot = CursorSnapshot::GetCursorSnapshotFromNode(iter.cur);

            if (cursorSnapshot->HasSameName(name)) {
                delete cursorSnapshot;
                break;
            }
        }
    }

    /* cursor could have been already deleted during commit or rollback */
    return DSTORE_SUCC;
}

void CursorSnapshotList::DeleteAll()
{
    if (IsEmpty()) {
        return;
    }
    dlist_mutable_iter iter;
    dlist_foreach_modify(iter, &m_cursorSnapshotList) {
        CursorSnapshot *cursorSnapshot = CursorSnapshot::GetCursorSnapshotFromNode(iter.cur);
        delete cursorSnapshot;
    }
}

CommitSeqNo CursorSnapshotList::GetCursorSnapshotMinCsn()
{
    /*
     * Head node will always have the lowest snapshot csn as snapshotcsn is always increasing
     * and new nodes are inserted at the tail node.
     */
    if (IsEmpty()) {
        return INVALID_CSN;
    }

    CursorSnapshot *cursorSnapshot = CursorSnapshot::GetCursorSnapshotFromNode(DListHeadNode(&m_cursorSnapshotList));
    return cursorSnapshot->GetCsn();
}

}