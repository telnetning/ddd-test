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

#ifndef DSTORE_UT_BTREE_MULTI_THREAD_H
#define DSTORE_UT_BTREE_MULTI_THREAD_H

#include <gtest/gtest.h>
#include "heap/dstore_heap_update.h"
#include "ut_btree/ut_btree.h"
#include "ut_utilities/ut_thread_pool.h"
#include "transaction/dstore_transaction.h"
#include "ut_btree/ut_btree_build_ccindex.h"

class UTBtreeMultiThread : virtual public UTBtree {
public:
    void SetUp() override
    {
        UTBtree::SetUp();
        int threadNum = m_config.ReadInteger("UTBtreeMultiThread-ThreadNum");
        if (threadNum < 0) {
            threadNum = 64;
        }
        m_pool.Start(threadNum);
        m_itupCounter.store(0);
    }

    void TearDown() override
    {
        ResetTestData();
        m_pool.Shutdown();
        UTBtree::TearDown();
    }

    static void BtreeInsertTask(UTBtreeMultiThread *ptr, DefaultRowDef *insertRow, bool isUnique)
    {
        ptr->BtreeInsertTuple(insertRow, isUnique);
    }

    static void BtreeDeleteTask(UTBtreeMultiThread *ptr)
    {
        ptr->BtreeDeleteTuple();
    }

    static void BtreeScanTask(UTBtreeMultiThread *ptr, bool isUnique)
    {
        ptr->BtreeScanAndCheckWithSnapshot(isUnique);
    }

    void BtreeInsertTuple(DefaultRowDef *insertRow, bool isUnique);
    void BtreeScanAndCheckWithSnapshot(bool isUnique);
    void BtreeDeleteTuple();

    void BuildThreadLocalVar();
 
    void BuildIndexForUTTable()
    {
        /* Create index. it will be shared to all threads */
        m_keyAttrNum = 2;
        CreateIndexBySpecificData(m_keyAttrNum, &m_indexCols);
        m_heapSegment = m_utTableHandler->m_heapSegmentPageId;
        m_lobSegment = m_utTableHandler->m_lobSegmentPageId;
        m_indexSegment = m_utTableHandler->m_indexSegmentPageId;
    }

    void ResetTestData()
    {
        m_mutex.lock();
        m_snapshots.clear();
        while (!m_itups.empty()) {
            DstorePfreeExt(m_itups.front());
            m_itups.pop();
        }
        m_mutex.unlock();
    }

    void Lock()
    {
        m_mutex.lock();
    }

    void UnLock()
    {
        m_mutex.unlock();
    }

protected:
    UTThreadPool m_pool;
    std::mutex m_mutex;

    /* Index tuples and csn */
    std::atomic<int> m_itupCounter;
    std::queue<IndexTuple*> m_itups;
    std::map<CommitSeqNo, std::pair<ItemPointerData, std::pair<int16, int32>>> m_snapshots;

    /* Table and Btree index meta info */
    PageId m_heapSegment = INVALID_PAGE_ID;
    PageId m_lobSegment = INVALID_PAGE_ID;
    PageId m_indexSegment = INVALID_PAGE_ID;
    int m_keyAttrNum;
    int *m_indexCols = nullptr;
};

#endif //DSTORE_UT_BTREE_MULTI_THREAD_H