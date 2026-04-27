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
#ifndef DSTORE_UT_LOGICAL_DECODE_MULTI_THREAD_H
#define DSTORE_UT_LOGICAL_DECODE_MULTI_THREAD_H

#include "ut_logical_replication_base.h"
#include "ut_heap/ut_heap_multi_thread.h"
#include "ut_tablehandler/ut_table_handler.h"

namespace DSTORE {

/* this ut aims to decode multi-thread dml, 
 * which may causes trx-abort and commit wal with misorder in walstream */
class LOGICALDECODEMULTI : virtual public LOGICALDECODE {
public:
    void SetUp() override;
    void TearDown() override;
    static void HeapInsertTask(LOGICALDECODEMULTI *ptr)
    {
        ptr->HeapInsertTaskWithWritingLogicalWal();
    }
    static void HeapUpdateTask(LOGICALDECODEMULTI *ptr)
    {
        ptr->HeapUpdateTaskWithWritingLogicalWal();
    }
    static void HeapUpdateTaskConflict(LOGICALDECODEMULTI *ptr)
    {
        ptr->HeapUpdateTaskConflictWithWritingLogicalWal();
    }
    static void HeapDeleteTask(LOGICALDECODEMULTI *ptr)
    {
        ptr->HeapDeleteTaskWithWritingLogicalWal();
    }
    static void HeapDeleteTaskConflict(LOGICALDECODEMULTI *ptr)
    {
        ptr->HeapDeleteTaskConflictWithWritingLogicalWal();
    }

    void HeapInsertTaskWithWritingLogicalWal();
    void HeapUpdateTaskWithWritingLogicalWal();
    void HeapUpdateTaskConflictWithWritingLogicalWal();
    void HeapDeleteTaskWithWritingLogicalWal();
    void HeapDeleteTaskConflictWithWritingLogicalWal();
protected:
    void BuildThreadLocalTableHandler();

    UTThreadPool m_pool;
    std::atomic<int> m_commitCounter{0};
    std::atomic<int> m_abortCounter{0};
    std::map<CommitSeqNo, Xid> m_csnXidMap;
    std::queue<ItemPointerData> m_ctids;
    LogicalDecodeHandler *m_decodeContext;
private:
    std::mutex m_mutex;
    PageId m_heapSegment = INVALID_PAGE_ID;
    PageId m_lobSegment = INVALID_PAGE_ID;  /* no use */
};

}
#endif
