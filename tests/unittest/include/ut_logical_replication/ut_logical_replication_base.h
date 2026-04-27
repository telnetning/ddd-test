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
#ifndef DSTORE_UT_LOGICAL_REPLICATION_BASIC_H
#define DSTORE_UT_LOGICAL_REPLICATION_BASIC_H

#include "ut_wal/ut_wal_basic.h"
#include "logical_replication/dstore_logical_replication_struct.h"
#include "logical_replication/dstore_logical_replication_mgr.h"
#include "ut_utilities/ut_thread_pool.h"

namespace DSTORE {
class LOGICALREPBASETEST : virtual public WALBASICTEST {
public:
    void SetUp() override;
    void TearDown() override;
    static void TaskCreateAndDropSingleSlot(LOGICALREPBASETEST *basePtr, int i,
        const std::function<RetStatus(const CommitSeqNo)> syncCatalogCallBack)
    {
        basePtr->TaskCreateAndDropSingleRepSlot(i, syncCatalogCallBack);
    }
    static void InsertWriteTaskTrx(LOGICALREPBASETEST *ptr)
    {
        ptr->TaskWriteTransaction();
    }
    void TaskCreateAndDropSingleRepSlot(int i, const std::function<RetStatus(const CommitSeqNo)> syncCatalogCallBack);
    void TaskWriteTransaction();

protected:
    void MockDecodeOptions(bool includeXidsFlag = false, bool skipAttrNullsFlag = true, bool advanceSlotFlag = false,
        int parallelDecodeWorkerNum = 1);
    LogicalReplicaMgr *m_logicalReplicaMgr;
    DecodeOptions *m_decodeOptions;
    UTThreadPool m_pool;
};

static RetStatus MockSyncCatalog(CommitSeqNo syncCsn)
{
    return DSTORE_SUCC;
}
}
#endif
