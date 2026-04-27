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
 * dstore_savepoint.h
 *
 *
 * IDENTIFICATION
 *        storage/include/transaction/dstore_savepoint.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_SAVEPOINT_H
#define DSTORE_SAVEPOINT_H

#include "common/algorithm/dstore_ilist.h"
#include "undo/dstore_undo_types.h"
#include "transaction/dstore_transaction_types.h"
#include "transaction/dstore_resowner.h"

namespace DSTORE {

enum SavepointType : uint8 {
    USER_SAVEPOINT = 0,
    EXCEPTION_SAVEPOINT,
};

class Savepoint : public BaseObject {
public:
    Savepoint(PdbId pdbId);
    ~Savepoint() = default;
    DISALLOW_COPY_AND_MOVE(Savepoint);

    RetStatus Create(const char *name);
    RetStatus Rollback();
    void Release();

    PdbId GetPdbId();

    bool HasSameName(const char *name) const;
    inline bool HasSavepointName() const
    {
        /* only user defined savepoints have names */
        return m_name ? true : false;
    }

    char *GetSavepointName()
    {
        return m_name;
    }

    void* GetSavepointExtraResRtr() const
    {
        return m_extra_res;
    }

    void SetSavepointExtraResRtr(void* data)
    {
        m_extra_res = data;
    }

    void PushSelfToListHead(dlist_head *list);
    static Savepoint *GetSavepointFromNodeInList(dlist_node *node);

private:
    dlist_node m_nodeInList;
    char *m_name;
    UndoRecPtr m_lastUndoPtr;
    LockResource::SubLockResourceID m_lastLockPos;
    void *m_extra_res;
    PdbId m_pdbId;
};

/*
 * Savepoint List Behaviour:
 *  - List is a double linked list
 *  - Latest savepoints are added to the head of the list
 *  - Savepoints are parsed from latest to oldest on the list
 *  - Savepoints with m_name = nullptr are internal savepoints (created by exception)
 */
class SavepointList : public BaseObject {
public:
    SavepointList(PdbId pdbId) : m_savepointList{},
        m_nestLevel{},
        m_pdbId(pdbId)
    {}
    
    void Init();
    PdbId GetPdbId();

    RetStatus AddSavepoint(const char *name);
    RetStatus ReleaseSavepoint(const char *name, int16 *userSavepointCounter = nullptr,
                              int16 *exceptionSavepointCounter = nullptr);
    RetStatus RollbackToSavepoint(const char *name, int16 *userSavepointCounter = nullptr,
                                  int16 *exceptionSavepointCounter = nullptr);
    RetStatus SaveExtraResPtrToSavepoint(const char *name, void* data);
    void* GetExtraResPtrFromSavepoint(const char *name);
    void* GetExtraResPtrFromCurrentSavepoint();
    inline static int GetSavepointType(const char *name)
    {
        /* only user defined savepoints have names */
        return name ? static_cast<int>(SavepointType::USER_SAVEPOINT) :
            static_cast<int>(SavepointType::EXCEPTION_SAVEPOINT);
    }

    inline static const char *GetSavepointName(const char *name)
    {
        return name ? name : "Exception";
    }

    bool IsSavepointExist(const char *name);
    bool HasCurrentSavepointName();
    char *GetCurrentSavepointName();
    bool IsEmpty();
    void DeleteAll();
    inline int16 GetNestLevel() const
    {
        return m_nestLevel;
    }

private:
    dlist_head m_savepointList;
    int16 m_nestLevel;
    PdbId m_pdbId;
};

}
#endif
