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
#include <gtest/gtest.h>
#include "catalog/dstore_typecache.h"
#include "heap/dstore_heap_interface.h"
#include "ut_mock/ut_mock.h"
#include "ut_heap/ut_heap.h"

TEST_F(UTHeap, LobFormTestOutOfLine_level0)
{
    /* Construct a tuple with lob */
    int textLen = 30000;
    std::string text = GenerateRandomString(textLen);
    HeapTuple *heapTuple = GenerateTupleWithLob(text, textLen);

    int status = memcmp(
        VarData4B((char*)heapTuple->GetDiskTuple() + heapTuple->GetDiskTupleSize()),
        (void*)text.c_str(),
        textLen
    );
    EXPECT_EQ(status, 0);

    bool isNull;
    void *tp = DatumGetPointer(
        heapTuple->GetAttr(CLOB_IDX + 1, m_utTableHandler->GetHeapTabSmgr()->GetTupleDesc(), &isNull, true));
    EXPECT_EQ(isNull, false);
    EXPECT_EQ(DstoreVarAttIsExternalDlob(tp), true);
    EXPECT_EQ(((VarattLobLocator *)VarData1BE(tp))->ctid, INVALID_ITEM_POINTER.m_placeHolder);

    DstorePfreeExt(heapTuple);
}

TEST_F(UTHeap, LobFormTestInline_level0)
{
    /* Construct a tuple with lob */
    int textLen = 1000;
    std::string text = GenerateRandomString(textLen);
    HeapTuple *heapTuple = GenerateTupleWithLob(text, textLen);

    bool isNull;
    void *tp = DatumGetPointer(
        heapTuple->GetAttr(CLOB_IDX + 1, m_utTableHandler->GetHeapTabSmgr()->GetTupleDesc(), &isNull, true));
    EXPECT_EQ(isNull, false);
    EXPECT_EQ(memcmp(VarData4B(tp), (void*)text.c_str(), textLen), 0);

    DstorePfreeExt(heapTuple);
}

TEST_F(UTHeap, LobInsertTestOutOfLine_level0)
{
    /* Construct a tuple with lob */
    int textLen = 30000;
    std::string text = GenerateRandomString(textLen);
    HeapTuple *heapTuple = GenerateTupleWithLob(text, textLen);

    /* Step 1: Insert the tuple */
    ItemPointerData ctid = INVALID_ITEM_POINTER;
    int status = InsertTupleWithLob(heapTuple, ctid);
    EXPECT_EQ(status, DSTORE_SUCC);
    EXPECT_NE(ctid, INVALID_ITEM_POINTER);

    DstorePfreeExt(heapTuple);
}

TEST_F(UTHeap, LobInsertTestInline_level0)
{
    /* Construct a tuple with lob */
    int textLen = 1000;
    std::string text = GenerateRandomString(textLen);
    HeapTuple *heapTuple = GenerateTupleWithLob(text, textLen);

    /* Step 1: Insert the tuple */
    ItemPointerData ctid = INVALID_ITEM_POINTER;
    int status = InsertTupleWithLob(heapTuple, ctid);
    EXPECT_EQ(status, DSTORE_SUCC);
    EXPECT_NE(ctid, INVALID_ITEM_POINTER);

    DstorePfreeExt(heapTuple);
}

TEST_F(UTHeap, LobFetchTestOutOfLine_TIER1_level0)
{
    /* Construct a tuple with lob */
    int textLen = 30000;
    std::string text = GenerateRandomString(textLen);
    HeapTuple *heapTuple = GenerateTupleWithLob(text, textLen);

    /* Step 1: Insert the tuple */
    ItemPointerData ctid = INVALID_ITEM_POINTER;
    InsertTupleWithLob(heapTuple, ctid);

    DstorePfreeExt(heapTuple);

    /* Step 2: Check whether insert OK */
    heapTuple = m_utTableHandler->FetchHeapTuple(&ctid);
    bool isNull;
    void *tp = DatumGetPointer(
        heapTuple->GetAttr(CLOB_IDX + 1, m_utTableHandler->GetHeapTabSmgr()->GetTupleDesc(), &isNull, true));
    EXPECT_EQ(isNull, false);
    varlena *lobValue = FetchLobValue(static_cast<varlena *>(tp));
    EXPECT_NE(lobValue, nullptr);
    EXPECT_EQ(memcmp(VarData4B(lobValue), (void*)text.c_str(), textLen), 0);

    DstorePfreeExt(lobValue);
    DstorePfreeExt(heapTuple);
}

TEST_F(UTHeap, LobFetchTestInline_TIER1_level0)
{
    /* Construct a tuple with lob */
    int textLen = 1000;
    std::string text = GenerateRandomString(textLen);
    HeapTuple *heapTuple = GenerateTupleWithLob(text, textLen);

    /* Step 1: Insert the tuple */
    ItemPointerData ctid = INVALID_ITEM_POINTER;
    InsertTupleWithLob(heapTuple, ctid);

    DstorePfreeExt(heapTuple);

    /* Step 2: Check whether insert OK */
    heapTuple = m_utTableHandler->FetchHeapTuple(&ctid);
    bool isNull;
    void *tp = DatumGetPointer(
        heapTuple->GetAttr(CLOB_IDX + 1, m_utTableHandler->GetHeapTabSmgr()->GetTupleDesc(), &isNull, true));
    EXPECT_EQ(isNull, false);
    varlena *lobValue = static_cast<varlena *>(tp);
    EXPECT_EQ(DstoreVarAttIsExternalDlob(lobValue), false);
    EXPECT_EQ(memcmp(VarData4B(lobValue), (void*)text.c_str(), textLen), 0);

    DstorePfreeExt(heapTuple);
}

TEST_F(UTHeap, LobDeleteTestOutOfLine_level0)
{
    /* Construct a tuple with lob */
    int textLen = 30000;
    std::string text = GenerateRandomString(textLen);
    HeapTuple *heapTuple = GenerateTupleWithLob(text, textLen);

    /* Step 1: Insert the tuple */
    ItemPointerData ctid = INVALID_ITEM_POINTER;
    InsertTupleWithLob(heapTuple, ctid);

    DstorePfreeExt(heapTuple);

    /* Step 2: delete tuple */
    HeapDeleteContext deleteContext;
    int status = DeleteTupleWithLob(deleteContext, ctid, true);
    EXPECT_EQ(status, DSTORE_SUCC);

    status = memcmp(
        VarData4B((char*)deleteContext.returnTup->GetDiskTuple() + deleteContext.returnTup->GetDiskTupleSize()),
        (void*)text.c_str(),
        textLen
    );
    EXPECT_EQ(status, 0);

    bool isNull;
    void *tp = DatumGetPointer(deleteContext.returnTup->GetAttr(
        CLOB_IDX + 1, m_utTableHandler->GetHeapTabSmgr()->GetTupleDesc(), &isNull, true));
    EXPECT_EQ(isNull, false);
    EXPECT_EQ(DstoreVarAttIsExternalDlob(tp), true);
    EXPECT_EQ(((VarattLobLocator *)VarData1BE(tp))->ctid, INVALID_ITEM_POINTER.m_placeHolder);

    DstorePfreeExt(deleteContext.returnTup);
}

TEST_F(UTHeap, LobDeleteTestInline_level0)
{
    /* Construct a tuple with lob */
    int textLen = 1000;
    std::string text = GenerateRandomString(textLen);
    HeapTuple *heapTuple = GenerateTupleWithLob(text, textLen);

    /* Step 1: Insert the tuple */
    ItemPointerData ctid = INVALID_ITEM_POINTER;
    InsertTupleWithLob(heapTuple, ctid);

    DstorePfreeExt(heapTuple);

    /* Step 2: delete tuple */
    HeapDeleteContext deleteContext;
    int status = DeleteTupleWithLob(deleteContext, ctid, true);
    EXPECT_EQ(status, DSTORE_SUCC);

    bool isNull;
    void *tp = DatumGetPointer(deleteContext.returnTup->GetAttr(
        CLOB_IDX + 1, m_utTableHandler->GetHeapTabSmgr()->GetTupleDesc(), &isNull, true));
    EXPECT_EQ(isNull, false);
    EXPECT_EQ(memcmp(VarData4B(tp), (void*)text.c_str(), textLen), 0);

    DstorePfreeExt(deleteContext.returnTup);
}

TEST_F(UTHeap, LobDeleteTestNoReturn_level0)
{
    /* Construct a tuple with lob */
    int textLen = 30000;
    std::string text = GenerateRandomString(textLen);
    HeapTuple *heapTuple = GenerateTupleWithLob(text, textLen);

    /* Step 1: Insert the tuple */
    ItemPointerData ctid = INVALID_ITEM_POINTER;
    InsertTupleWithLob(heapTuple, ctid);

    DstorePfreeExt(heapTuple);

    /* Step 2: delete tuple */
    HeapDeleteContext deleteContext;
    int status = DeleteTupleWithLob(deleteContext, ctid, false);
    EXPECT_EQ(status, DSTORE_SUCC);
    EXPECT_EQ(deleteContext.returnTup, nullptr);
}

TEST_F(UTHeap, LobUpdateTestOutOfLineBigger_TIER1_level0)
{
    /* Construct a tuple with lob */
    int textLen1 = 30000;
    std::string text1 = GenerateRandomString(textLen1);
    HeapTuple *heapTuple = GenerateTupleWithLob(text1, textLen1);

    /* Step 1: Insert the tuple */
    ItemPointerData ctid = INVALID_ITEM_POINTER;
    InsertTupleWithLob(heapTuple, ctid);

    DstorePfreeExt(heapTuple);

    /* Construct a tuple with lob */
    int textLen2 = 40000;
    std::string text2 = GenerateRandomString(textLen2);
    heapTuple = GenerateTupleWithLob(text2, textLen2);

    /* Step 2: Update tuple */
    HeapUpdateContext updateContext;
    int status = UpdateTupleWithLob(updateContext, ctid, heapTuple, true);

    EXPECT_EQ(status, DSTORE_SUCC);
    EXPECT_EQ(updateContext.oldCtid, updateContext.newCtid);

    status = memcmp(
        VarData4B((char*)updateContext.retOldTuple->GetDiskTuple() + updateContext.retOldTuple->GetDiskTupleSize()),
        (void*)text1.c_str(),
        textLen1
    );
    EXPECT_EQ(status, 0);

    bool isNull;
    void *tp = DatumGetPointer(
        updateContext.newTuple->GetAttr(CLOB_IDX + 1, m_utTableHandler->GetHeapTabSmgr()->GetTupleDesc(), &isNull, true));
    EXPECT_EQ(isNull, false);
    EXPECT_EQ(DstoreVarAttIsExternalDlob(tp), true);

    /* Step 3: Try fetch lob value */
    varlena *lobValue = FetchLobValue(static_cast<varlena *>(tp));
    EXPECT_EQ(memcmp(VarData4B(lobValue), (void*)text2.c_str(), textLen2), 0);
    DstorePfreeExt(lobValue);
    DstorePfreeExt(updateContext.newTuple);
    DstorePfreeExt(updateContext.retOldTuple);
}

TEST_F(UTHeap, LobUpdateTestOutOfLineSmaller_level0)
{
    /* Construct a tuple with lob */
    int textLen1 = 30000;
    std::string text1 = GenerateRandomString(textLen1);
    HeapTuple *heapTuple = GenerateTupleWithLob(text1, textLen1);

    /* Step 1: Insert the tuple */
    ItemPointerData ctid = INVALID_ITEM_POINTER;
    InsertTupleWithLob(heapTuple, ctid);

    DstorePfreeExt(heapTuple);

    /* Construct a tuple with lob */
    int textLen2 = 1000;
    std::string text2 = GenerateRandomString(textLen2);
    heapTuple = GenerateTupleWithLob(text2, textLen2);

    /* Step 2: Update tuple */
    HeapUpdateContext updateContext;
    int status = UpdateTupleWithLob(updateContext, ctid, heapTuple, true);

    EXPECT_EQ(status, DSTORE_SUCC);
    EXPECT_EQ(updateContext.oldCtid, updateContext.newCtid);

    status = memcmp(
        VarData4B((char*)updateContext.retOldTuple->GetDiskTuple() + updateContext.retOldTuple->GetDiskTupleSize()),
        (void*)text1.c_str(),
        textLen1
    );
    EXPECT_EQ(status, 0);

    bool isNull;
    void *tp = DatumGetPointer(updateContext.newTuple->GetAttr(
        CLOB_IDX + 1, m_utTableHandler->GetHeapTabSmgr()->GetTupleDesc(), &isNull, true));
    EXPECT_EQ(isNull, false);
    EXPECT_EQ(memcmp(VarData4B(tp), (void*)text2.c_str(), textLen2), 0);

    DstorePfreeExt(updateContext.newTuple);
    DstorePfreeExt(updateContext.retOldTuple);
}

TEST_F(UTHeap, LobUpdateTestInlineBigger_level0)
{
    /* Construct a tuple with lob */
    int textLen1 = 1000;
    std::string text1 = GenerateRandomString(textLen1);
    HeapTuple *heapTuple = GenerateTupleWithLob(text1, textLen1);

    /* Step 1: Insert the tuple */
    ItemPointerData ctid = INVALID_ITEM_POINTER;
    InsertTupleWithLob(heapTuple, ctid);

    DstorePfreeExt(heapTuple);

    /* Construct a tuple with lob */
    int textLen2 = 30000;
    std::string text2 = GenerateRandomString(textLen2);
    heapTuple = GenerateTupleWithLob(text2, textLen2);

    /* Step 2: Update tuple */
    HeapUpdateContext updateContext;
    int status = UpdateTupleWithLob(updateContext, ctid, heapTuple, true);

    EXPECT_EQ(status, DSTORE_SUCC);
    EXPECT_EQ(updateContext.oldCtid, updateContext.newCtid);

    bool isNull;
    void *tp = DatumGetPointer(updateContext.retOldTuple->GetAttr(
        CLOB_IDX + 1, m_utTableHandler->GetHeapTabSmgr()->GetTupleDesc(), &isNull, true));
    EXPECT_EQ(isNull, false);
    EXPECT_EQ(memcmp(VarData4B(tp), (void *)text1.c_str(), textLen1), 0);

    tp = DatumGetPointer(updateContext.newTuple->GetAttr(
        CLOB_IDX + 1, m_utTableHandler->GetHeapTabSmgr()->GetTupleDesc(), &isNull, true));
    EXPECT_EQ(isNull, false);
    EXPECT_EQ(DstoreVarAttIsExternalDlob(tp), true);

    /* Step 3: Try fetch lob value */
    varlena *lobValue = FetchLobValue(static_cast<varlena *>(tp));
    EXPECT_EQ(memcmp(VarData4B(lobValue), (void*)text2.c_str(), textLen2), 0);

    DstorePfreeExt(lobValue);
    DstorePfreeExt(updateContext.newTuple);
    DstorePfreeExt(updateContext.retOldTuple);
}

TEST_F(UTHeap, LobUpdateTestNoReturn_level0)
{
    /* Construct a tuple with lob */
    int textLen1 = 30000;
    std::string text1 = GenerateRandomString(textLen1);
    HeapTuple *heapTuple = GenerateTupleWithLob(text1, textLen1);

    /* Step 1: Insert the tuple */
    ItemPointerData ctid = INVALID_ITEM_POINTER;
    InsertTupleWithLob(heapTuple, ctid);

    DstorePfreeExt(heapTuple);

    /* Construct a tuple with lob */
    int textLen2 = 40000;
    std::string text2 = GenerateRandomString(textLen2);
    heapTuple = GenerateTupleWithLob(text2, textLen2);

    /* Step 2: Update tuple */
    HeapUpdateContext updateContext;
    int status = UpdateTupleWithLob(updateContext, ctid, heapTuple, false);

    EXPECT_EQ(status, DSTORE_SUCC);
    EXPECT_EQ(updateContext.oldCtid, updateContext.newCtid);
    EXPECT_EQ(updateContext.retOldTuple, nullptr);

    bool isNull;
    void *tp = DatumGetPointer(updateContext.newTuple->GetAttr(
        CLOB_IDX + 1, m_utTableHandler->GetHeapTabSmgr()->GetTupleDesc(), &isNull, true));
    EXPECT_EQ(isNull, false);
    EXPECT_EQ(DstoreVarAttIsExternalDlob(tp), true);

    /* Step 3: Try fetch lob value */
    varlena *lobValue = FetchLobValue(static_cast<varlena *>(tp));
    EXPECT_EQ(memcmp(VarData4B(lobValue), (void*)text2.c_str(), textLen2), 0);

    DstorePfreeExt(lobValue);
    DstorePfreeExt(updateContext.newTuple);
}

TEST_F(UTHeap, LobPruneTest_level0)
{
    std::vector<HeapTuple *> heapTupsForInsert;
    std::vector<ItemPointerData> heapTupCtids;

    /* Step 1: Create insert data. */
    constexpr int NUM_TUP = 26;
    constexpr int LEN_TUP = 8000;
    char x = 'a';
    Datum values[TYPE_CACHE_NUM];
    bool isNulls[TYPE_CACHE_NUM];
    for (int i = 0; i < TYPE_CACHE_NUM; i++) {
        values[i] = 0;
        isNulls[i] = true;
    }
    isNulls[CLOB_IDX] = false;
    for (int i = 0; i < NUM_TUP; ++i) {
        std::string text = std::string(LEN_TUP, x);
        Size lobValueSize = VARHDRSZ + LEN_TUP;
        varattrib_4b *lobValue = (varattrib_4b *) DstorePalloc0(lobValueSize);
        DstoreSetVarSize4B(lobValue, lobValueSize);
        EXPECT_EQ(memcpy_s(VarData4B(lobValue), LEN_TUP, (void*)text.c_str(), LEN_TUP), 0);
        values[CLOB_IDX] = PointerGetDatum(lobValue);
        HeapTuple *heapTuple = HeapTuple::FormTuple(m_utTableHandler->GetHeapTabSmgr()->GetTupleDesc(), values, isNulls);
        heapTupsForInsert.push_back(heapTuple);
        DstorePfreeExt(lobValue);
        x++;
    }

    /* Step 2: Insert the tuples, but delete tuple every other line to generate gaps between existing tuples. */
    RetStatus status;
    HeapDeleteContext deleteContext;
    deleteContext.needReturnTup = false;
    deleteContext.returnTup = nullptr;
    constexpr int INTERVAL = 2;
    StorageRelationData relation;
    relation.tableSmgr = m_utTableHandler->GetHeapTabSmgr();
    relation.lobTableSmgr = m_utTableHandler->GetLobTabSmgr();
    relation.m_pdbId = g_defaultPdbId;
    for (int i = 0; i < NUM_TUP; ++i) {
        thrd->GetActiveTransaction()->Start();
        thrd->GetActiveTransaction()->SetSnapshotCsn();
        status = HeapInterface::Insert(&relation, heapTupsForInsert[i], deleteContext.ctid,
                                       thrd->GetActiveTransaction()->GetCurCid());
        EXPECT_EQ(status, DSTORE_SUCC);
        heapTupCtids.push_back(deleteContext.ctid);
        thrd->GetActiveTransaction()->Commit();

        if (i % INTERVAL == 1) {
            thrd->GetActiveTransaction()->Start();
            thrd->GetActiveTransaction()->SetSnapshotCsn();
            deleteContext.snapshot = *thrd->GetActiveTransaction()->GetSnapshotData();
            deleteContext.cid = thrd->GetActiveTransaction()->GetCurCid();
            status = HeapInterface::Delete(&relation, &deleteContext);
            EXPECT_EQ(status, DSTORE_SUCC);
            thrd->GetActiveTransaction()->Commit();
        }
    }

    /* Step 3: Check existing tuples are still correct after vacuum. */
    thrd->GetActiveTransaction()->Start();
    thrd->GetActiveTransaction()->SetSnapshotCsn();
    HeapInterface::LazyVacuum(&relation);
    HeapScanHandler *heapScan = HeapInterface::CreateHeapScanHandler(&relation);
    HeapInterface::BeginScan(heapScan, thrd->GetActiveTransaction()->GetSnapshot());
    for (int i = 0; i < NUM_TUP; ++i) {
        if (i % INTERVAL == 1) {
            continue;
        }
        HeapTuple *tuple = HeapInterface::FetchTuple(heapScan, heapTupCtids[i]);
        EXPECT_EQ(tuple->GetDiskTupleSize(), heapTupsForInsert[i]->GetDiskTupleSize());
        EXPECT_EQ(memcmp(tuple->GetValues(),
                         heapTupsForInsert[i]->GetValues(),
                         tuple->GetDiskTupleSize() - tuple->GetValuesOffset()), 0);

        bool isNull;
        void *tp1 = DatumGetPointer(
            tuple->GetAttr(CLOB_IDX + 1, m_utTableHandler->GetHeapTabSmgr()->GetTupleDesc(), &isNull, true));
        EXPECT_EQ(isNull, false);
        EXPECT_EQ(DstoreVarAttIsExternalDlob(tp1), true);
        VarattLobLocator *lobLocator1 = STATIC_CAST_PTR_TYPE(VarData1BE(tp1), VarattLobLocator *);
        uint64_t ctid1 = lobLocator1->ctid;
        ItemPointerData lob_ctid1(lobLocator1->ctid);
        varlena *lobValue1 =
            HeapInterface::FetchLobValue(&relation, lob_ctid1, thrd->GetActiveTransaction()->GetSnapshot());

        void *tp2 = DatumGetPointer(heapTupsForInsert[i]->GetAttr(
            CLOB_IDX + 1, m_utTableHandler->GetHeapTabSmgr()->GetTupleDesc(), &isNull, true));
        EXPECT_EQ(isNull, false);
        EXPECT_EQ(DstoreVarAttIsExternalDlob(tp2), true);
        VarattLobLocator *lobLocator2 = STATIC_CAST_PTR_TYPE(VarData1BE(tp2), VarattLobLocator *);
        uint64_t ctid2 = lobLocator2->ctid;
        ItemPointerData lob_ctid2(lobLocator2->ctid);
        varlena *lobValue2 =
            HeapInterface::FetchLobValue(&relation, lob_ctid2, thrd->GetActiveTransaction()->GetSnapshot());

        EXPECT_EQ(ctid1, ctid2);
        EXPECT_EQ(memcmp(lobValue1, lobValue2, VARHDRSZ + LEN_TUP), 0);

        DstorePfreeExt(lobValue1);
        DstorePfreeExt(lobValue2);
        DstorePfreeExt(tuple);
    }
    HeapInterface::EndScan(heapScan);
    HeapInterface::DestroyHeapScanHandler(heapScan);
    thrd->GetActiveTransaction()->Commit();

    /* Step 4: Free memory. */
    for (int i = 0; i < NUM_TUP; ++i) {
        DstorePfreeExt(heapTupsForInsert[i]);
    }
}

TEST_F(UTHeap, LobVisibilityTest_level0)
{  
    /* Step 1: Insert the tuple */
    int textLen1 = 3000;
    std::string text1 = GenerateRandomString(textLen1);
    HeapTuple *heapTuple1 = GenerateTupleWithLob(text1, textLen1);
    ItemPointerData ctid1 = INVALID_ITEM_POINTER;
    int status = InsertTupleWithLob(heapTuple1, ctid1);
    EXPECT_EQ(status, DSTORE_SUCC);

    /* Step 2: save snapshot before update */
    thrd->GetActiveTransaction()->Start();
    thrd->GetActiveTransaction()->SetSnapshotCsn();
    SnapshotData snapshot = *(thrd->GetActiveTransaction()->GetSnapshot());
    EXPECT_EQ(snapshot.snapshotType, SnapshotType::SNAPSHOT_MVCC);
    EXPECT_NE(snapshot.snapshotCsn, INVALID_CSN);
    EXPECT_NE(snapshot.currentCid, INVALID_CID);
    thrd->GetActiveTransaction()->Commit();

    /* Step 3: Update tuple */
    int textLen2 = 4000;
    std::string text2 = GenerateRandomString(textLen2);
    HeapTuple *heapTuple2 = GenerateTupleWithLob(text2, textLen2);
    HeapUpdateContext updateContext;
    status = UpdateTupleWithLob(updateContext, ctid1, heapTuple2, false);
    EXPECT_EQ(status, DSTORE_SUCC);
    EXPECT_EQ(updateContext.oldCtid, ctid1);
    EXPECT_EQ(updateContext.newCtid, ctid1);

    /* Step 4: fetch tuple before update */
    HeapTuple *heapTuple = m_utTableHandler->FetchHeapTuple(&ctid1, &snapshot);
    bool isNull;
    void *tp = DatumGetPointer(
        heapTuple->GetAttr(CLOB_IDX + 1, m_utTableHandler->GetHeapTabSmgr()->GetTupleDesc(), &isNull));
    varlena *lobValue = FetchLobValue(static_cast<varlena *>(tp));
    EXPECT_NE(lobValue, nullptr);
    EXPECT_EQ(memcmp(VarData4B(lobValue), (void*)text1.c_str(), textLen1), 0);
    EXPECT_EQ(DstoreVarSize4B(lobValue), VARHDRSZ + textLen1);

    StorageRelationData relation;
    relation.tableSmgr = m_utTableHandler->GetHeapTabSmgr();
    relation.lobTableSmgr = m_utTableHandler->GetLobTabSmgr();
    relation.m_pdbId = g_defaultPdbId;
    HeapTuple *heapTupleWithLob = HeapInterface::FetchTupleWithLob(&relation, heapTuple, &snapshot);
    EXPECT_EQ(memcmp((char*)heapTupleWithLob->GetDiskTuple() + heapTupleWithLob->m_head.len,
        (char*)heapTuple1->GetDiskTuple() + heapTuple1->m_head.len, VARHDRSZ + textLen1), 0);
    
    DstorePfreeExt(lobValue);
    DstorePfreeExt(heapTuple);
    DstorePfreeExt(heapTupleWithLob);
    DstorePfreeExt(heapTuple1);
    DstorePfreeExt(heapTuple2);
}

TEST_F(UTHeap, LobVisibilityAfterPruneTest_level0)
{
    /* Step 1: Insert the tuple */
    int textLen1 = 3000;
    std::string text1 = GenerateRandomString(textLen1);
    HeapTuple *heapTuple1 = GenerateTupleWithLob(text1, textLen1);
    ItemPointerData ctid1 = INVALID_ITEM_POINTER;
    int status = InsertTupleWithLob(heapTuple1, ctid1);
    EXPECT_EQ(status, DSTORE_SUCC);

    /* Step 2: save snapshot before update */
    thrd->GetActiveTransaction()->Start();
    thrd->GetActiveTransaction()->SetSnapshotCsn();
    SnapshotData snapshot = *(thrd->GetActiveTransaction()->GetSnapshot());
    EXPECT_EQ(snapshot.snapshotType, SnapshotType::SNAPSHOT_MVCC);
    EXPECT_NE(snapshot.snapshotCsn, INVALID_CSN);
    EXPECT_NE(snapshot.currentCid, INVALID_CID);
    thrd->GetActiveTransaction()->Commit();

    /* Step 3: Update tuple */
    int textLen2 = 4000;
    std::string text2 = GenerateRandomString(textLen2);
    HeapTuple *heapTuple2 = GenerateTupleWithLob(text2, textLen2);
    HeapUpdateContext updateContext;
    status = UpdateTupleWithLob(updateContext, ctid1, heapTuple2, false);
    EXPECT_EQ(status, DSTORE_SUCC);

    /* Step 4: trigger prune */
    thrd->GetActiveTransaction()->Start();
    thrd->GetActiveTransaction()->SetSnapshotCsn();
    StorageRelationData relation;
    relation.tableSmgr = m_utTableHandler->GetHeapTabSmgr();
    relation.lobTableSmgr = m_utTableHandler->GetLobTabSmgr();
    relation.m_pdbId = g_defaultPdbId;
    HeapInterface::LazyVacuum(&relation);
    thrd->GetActiveTransaction()->Commit();

    int textLen3 = 3000;
    std::string text3 = GenerateRandomString(textLen3);
    HeapTuple *heapTuple3 = GenerateTupleWithLob(text3, textLen3);
    ItemPointerData ctid3 = INVALID_ITEM_POINTER;
    status = InsertTupleWithLob(heapTuple3, ctid3);
    EXPECT_EQ(status, DSTORE_SUCC);
    EXPECT_EQ(ctid3.val.m_pageid, ctid1.val.m_pageid);

    /* Step 5: fetch tuple before update */
    HeapTuple *heapTuple = m_utTableHandler->FetchHeapTuple(&ctid1, &snapshot);
    bool isNull;
    void *tp = DatumGetPointer(
        heapTuple->GetAttr(CLOB_IDX + 1, m_utTableHandler->GetHeapTabSmgr()->GetTupleDesc(), &isNull));
    varlena *lobValue = FetchLobValue(static_cast<varlena *>(tp));
    EXPECT_NE(lobValue, nullptr);
    EXPECT_EQ(memcmp(VarData4B(lobValue), (void*)text1.c_str(), textLen1), 0);
    EXPECT_EQ(DstoreVarSize4B(lobValue), VARHDRSZ + textLen1);

    HeapTuple *heapTupleWithLob = HeapInterface::FetchTupleWithLob(&relation, heapTuple, &snapshot);
    EXPECT_EQ(memcmp((char*)heapTupleWithLob->GetDiskTuple() + heapTupleWithLob->m_head.len,
        (char*)heapTuple1->GetDiskTuple() + heapTuple1->m_head.len, VARHDRSZ + textLen1), 0);

    DstorePfreeExt(lobValue);
    DstorePfreeExt(heapTuple);
    DstorePfreeExt(heapTupleWithLob);
    DstorePfreeExt(heapTuple1);
    DstorePfreeExt(heapTuple2);
}
