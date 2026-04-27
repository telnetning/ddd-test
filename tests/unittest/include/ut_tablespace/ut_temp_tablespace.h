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
#ifndef DSTORE_UT_TEMP_TABLESPACE_H
#define DSTORE_UT_TEMP_TABLESPACE_H

#include <queue>
#include "ut_tablehandler/ut_table_handler.h"
#include "heap/dstore_heap_update.h"
#include "heap/dstore_heap_interface.h"

using namespace DSTORE;
class UTTempTablespace : public virtual DSTORETEST {
public:
typedef struct {

} FuzzInput;
protected:
    void DestroySnapshotThrd();

    void SetUp() override
    {
        DSTORETEST::SetUp();
        MockStorageInstance *instance = DstoreNew(m_ut_memory_context) MockStorageInstance();
        instance->Install(&DSTORETEST::m_guc, m_ut_memory_context);
        instance->Startup(&DSTORETEST::m_guc);
        FaultInjectionEntry entries[] = {
        };
        ASSERT_EQ(RegisterFaultInjection(entries, sizeof(entries) / sizeof(entries[0]), FI_GLOBAL), ERROR_SYS_OK);

        bool useTempTablespace = true;
        m_utTableHandler = UTTableHandler::CreateTableHandler(g_defaultPdbId, m_ut_memory_context, true, useTempTablespace);
        m_snapshotThread = nullptr;
        m_ssMemCtx = nullptr;
    }

    void TearDown() override
    {
        DestroySnapshotThrd();
        UTTableHandler::Destroy(m_utTableHandler);
        m_utTableHandler = nullptr;
        MockStorageInstance *instance = (MockStorageInstance *)g_storageInstance;
        instance->Shutdown();
        delete instance;
        DSTORETEST::TearDown();
    }
    IndexTuple *InsertSpecificIndexTuples(DefaultRowDef *insertRow,
                                     bool *nullbitmap = DefaultNullBitMap,
                                     bool alreadyXactStart = false,
                                     UTTableHandler *utTableHandler = nullptr);
    std::queue<PageId> TestPivotTuple(TupleDesc indexTupleDesc, std::queue<PageId> &pivotPageIds, int level);
    bool IsTupleInExpectPosition(TupleDesc indexTupleDesc, std::queue<PageId> &leafPageIds, int position, IndexTuple *target);
    int CompareIndexTuples(IndexTuple *tuple1, IndexTuple *tuple2, TupleDesc indexTupleDesc);
    void TestLeafTuple(TupleDesc indexTupleDesc, std::queue<PageId> &leafPageIds, int rowNum);
    RetStatus DeleteIndexTuples(IndexTuple *indexTuple, bool alreadyXactStart = false,
                                        UTTableHandler *utTableHandler = nullptr);

    void TestBtreeRootPages(PageId rootPageId, uint32 rootLevel);
    bool CheckTupleDeleted(TupleDesc indexTupleDesc, std::queue<PageId> &leafPageIds,
                                 int offset, IndexTuple *target);
    PageId TestBtreeMetaPages(PageId metaPageId);
    ItemPointerData InsertSpecificHeapTuples(std::string data, SnapshotData *snapshot, bool alreadyStartXact);

    UTTableHandler *m_utTableHandler;

    class std::thread     *m_snapshotThread; /* transaction is in progress to prevent undo recycle */
    DstoreMemoryContext m_ssMemCtx;
    std::atomic<bool> m_isSnapshotThreadNeedStop;
};

#endif /* DSTORE_UT_TEMP_TABLESPACE_H */

