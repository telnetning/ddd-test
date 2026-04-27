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
#ifndef DSTORE_UT_LOGICAL_REPLICATION_SLOT_H
#define DSTORE_UT_LOGICAL_REPLICATION_SLOT_H

#include "ut_logical_replication_base.h"

namespace DSTORE {
class LOGICALREPSLOTTEST : virtual public LOGICALREPBASETEST {
public:
    void SetUp() override;
    void TearDown() override;
    static void InsertTaskReadonlyTrx(LOGICALREPSLOTTEST *ptr)
    {
        ptr->TaskCreateReadonlyTransaction();
    }
    static void InsertTaskStartpoint(LOGICALREPSLOTTEST *ptr,
        const std::function<RetStatus(const CommitSeqNo)> syncCatalogCallBack)
    {
        ptr->TaskFindStartPoint(syncCatalogCallBack);
    }
    void TaskCreateReadonlyTransaction();
    void TaskFindStartPoint(const std::function<RetStatus(const CommitSeqNo)> syncCatalogCallBack);

protected:
    LogicalReplicationSlot *m_slot;
};
}
#endif