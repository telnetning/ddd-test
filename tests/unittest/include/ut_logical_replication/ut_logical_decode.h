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
#ifndef DSTORE_UT_LOGICAL_DECODE_H
#define DSTORE_UT_LOGICAL_DECODE_H

#include "ut_logical_replication_base.h"
#include "ut_heap/ut_heap.h"
#include "ut_tablehandler/ut_table_handler.h"

namespace DSTORE {

static Oid LOGICAL_DECODE_TEST_TABLE_OID = 1026;
static char LOGICAL_DECODE_TEST_TABLE_NAME[NAME_DATA_LEN] = "t1\0";
static char LOGICAL_DECODE_TEST_SLOT_NAME[NAME_DATA_LEN] = "TestSlot\0";
static char LOGICAL_DECODE_TEST_PLUGIN_NAME[NAME_DATA_LEN] = "Text_Plugin\0";

class LOGICALDECODE : virtual public WALBASICTEST, virtual public UTHeap {
public:
    void SetUp() override;
    void TearDown() override;
protected:
    void PrepareTableHandler();
    void PrepareDecodeDict();
    void PrepareWalWriteContext();
    void PrepareLogicalReplicationSlot();
    DecodeOptions* CreateDecodeOptions(CommitSeqNo upToCsn, int nChanges, int decodeWorkers);
    LogicalDecodeHandler *CreateDecodeHandler(DecodeOptions *decodeOption);
    void GenerateRandomHeapInsertWithLogicalWal(Xid &xid, CommitSeqNo &commitCsn, HeapTuple *&insertTup,
        ItemPointerData &ctid);
    void GenerateRandomHeapDeleteWithLogicalWal(ItemPointer ctid, Xid &xid, CommitSeqNo &commitCsn);
    void GenerateRandomHeapUpdateWithLogicalWal(HeapTuple *newTuple, ItemPointer oldCtid,
        ItemPointerData &newCtid, Xid &xid, CommitSeqNo &commitCsn);
    void GenerateMixLogicalWalInOneTrx(Xid &xid, CommitSeqNo &commitCsn);

    LogicalReplicationSlot *m_logicalSlot;
    UTTableHandler *m_utTableHandler;
    DecodeDict *m_decodeDict;
    TupleDesc m_fakeDesc;
};

}
#endif
