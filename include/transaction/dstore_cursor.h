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
 * dstore_cursor.h
 *     This file defines the building blocks that bg page writer needs.
 *
 * IDENTIFICATION
 *        include/transaction/dstore_cursor.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_CURSOR_H
#define DSTORE_CURSOR_H

#include "common/algorithm/dstore_ilist.h"
#include "undo/dstore_undo_types.h"
#include "transaction/dstore_transaction_types.h"
#include "transaction/dstore_resowner.h"

namespace DSTORE {

class CursorSnapshot : public BaseObject {
public:
    CursorSnapshot()
        : m_cursorName(nullptr)
    {
        m_snapshot.Init();
        DListNodeInit(&m_node);
    }

    ~CursorSnapshot()
    {
        DListDelete(&m_node);
        if (m_cursorName != nullptr) {
            DstorePfreeExt(m_cursorName);
            m_cursorName = nullptr;
        }
    }

    static CursorSnapshot *GetCursorSnapshotFromNode(dlist_node *node)
    {
        return static_cast<CursorSnapshot *>(dlist_container(CursorSnapshot, m_node, node));
    }

    void PushSelfToListTail(dlist_head *list)
    {
        DListPushTail(list, &m_node);
    }

    RetStatus Init(const char *name, const SnapshotData &snapshot);

    inline CommitSeqNo GetCsn() const
    {
        return m_snapshot.GetCsn();
    }

    bool HasSameName(const char *name) const
    {
        StorageAssert(name != nullptr);
        return strcmp(name, m_cursorName) == 0;
    }

    DISALLOW_COPY_AND_MOVE(CursorSnapshot);

private:
    dlist_node m_node;
    char *m_cursorName;
    SnapshotData m_snapshot;
};

class CursorSnapshotList : public BaseObject {
public:
    inline void Init()
    {
        DListInit(&m_cursorSnapshotList);
    }
    inline bool IsEmpty()
    {
        return DListIsEmpty(&m_cursorSnapshotList);
    }
    bool CheckCursorSnapshotExists(const char* name);
    CommitSeqNo GetCursorSnapshotMinCsn();
    RetStatus AddCursorSnapshot(const char* name, const SnapshotData &snapshot);
    RetStatus DeleteCursorSnapshot(const char* name);
    void DeleteAll();

private:
    dlist_head m_cursorSnapshotList{};
};

}
#endif
