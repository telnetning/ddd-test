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
#include "ut_logical_replication/ut_logical_decode.h"
#include "ut_tablehandler/ut_table_handler.h"
#include "wal/dstore_wal.h"

using namespace DSTORE;

void LOGICALDECODE::SetUp()
{
    WALBASICTEST::SetUp();
    PrepareWalWriteContext();
    PrepareTableHandler();
    PrepareDecodeDict();
    PrepareLogicalReplicationSlot();
}

void LOGICALDECODE::TearDown()
{
    UTTableHandler::Destroy(m_utTableHandler);
    m_decodeDict->Destroy();
    delete m_decodeDict;
    m_decodeDict = nullptr;
    delete m_logicalSlot;
    WALBASICTEST::TearDown();
}

void LOGICALDECODE::PrepareTableHandler()
{
    m_utTableHandler = UTTableHandler::CreateTableHandler(g_defaultPdbId, m_ut_memory_context);
    TableStorageMgr *tableMgr = m_utTableHandler->GetHeapTabSmgr();
    tableMgr->SetTableOid(LOGICAL_DECODE_TEST_TABLE_OID);
    tableMgr->SetTupleDesc(m_utTableHandler->GenerateFakeLogicalDecodeTupDesc());
}

void LOGICALDECODE::PrepareDecodeDict()
{
    RetStatus rt;
    m_decodeDict = DstoreNew(m_ut_memory_context) DecodeDict(g_defaultPdbId, DstoreAllocSetContextCreate(
        m_ut_memory_context, "DecodeDict", ALLOCSET_DEFAULT_SIZES, MemoryContextType::SHARED_CONTEXT));
    rt = m_decodeDict->Init();
    ASSERT_EQ(rt, DSTORE_SUCC);
    m_fakeDesc = m_utTableHandler->GetHeapTabSmgr()->GetTupleDesc();

    CatalogInfo *rawInfo = static_cast<CatalogInfo *>(DstorePalloc(sizeof(CatalogInfo)));
    g_storageInstance->GetCsnMgr()->GetNextCsn(rawInfo->csn, true);
    rawInfo->tableOid = LOGICAL_DECODE_TEST_TABLE_OID;
    rawInfo->nspOid = 2200;
    rawInfo->natts = m_fakeDesc->natts;;
    rawInfo->sysAttr = static_cast<Form_pg_attribute *>(DstorePalloc(
        m_fakeDesc->natts * sizeof(DSTORE::Form_pg_attribute)));
    rawInfo->sysRel = static_cast<Form_pg_class>(DstorePalloc(sizeof(FormData_pg_class)));
    rawInfo->nspName = static_cast<char *>(DstorePalloc0(NAME_DATA_LEN));
    rawInfo->nspName[0] = 'p';
    rawInfo->nspName[1] = 'u';
    rawInfo->nspName[2] = 'b';
    rawInfo->nspName[3] = 'l';
    rawInfo->nspName[4] = 'i';
    rawInfo->nspName[5] = 'c';
    rawInfo->nspName[6] = '\0';
    errno_t rc;
    for (int i = 0; i < m_fakeDesc->natts; i++) {
        rawInfo->sysAttr[i] = static_cast<DSTORE::Form_pg_attribute>(DstorePalloc(MAXALIGN(ATTRIBUTE_FIXED_PART_SIZE)));
        rc = memcpy_s(rawInfo->sysAttr[i], MAXALIGN(ATTRIBUTE_FIXED_PART_SIZE),
                       m_fakeDesc->attrs[i], MAXALIGN(ATTRIBUTE_FIXED_PART_SIZE));
        storage_securec_check(rc, "\0", "\0");
    }
    rc = memcpy_s(rawInfo->sysRel->relname.data, NAME_DATA_LEN, LOGICAL_DECODE_TEST_TABLE_NAME, NAME_DATA_LEN);
    storage_securec_check(rc, "\0", "\0");
    rt = m_decodeDict->SynchronizeCatalog(rawInfo);
    ASSERT_EQ(rt, DSTORE_SUCC);
    DstorePfree(rawInfo->nspName);
    for (int i = 0; i < rawInfo->natts; i++) {
        DstorePfree(rawInfo->sysAttr[i]);
    }
    DstorePfree(rawInfo->sysAttr);
    DstorePfree(rawInfo->sysRel);
    DstorePfree(rawInfo);
    DecodeTableInfo *test =
        m_decodeDict->GetVisibleDecodeTableInfo(LOGICAL_DECODE_TEST_TABLE_OID, thrd->GetNextCsn());
    ASSERT_NE(test, nullptr);
}

void LOGICALDECODE::PrepareWalWriteContext()
{
    m_walControlFile = g_storageInstance->GetPdb(g_defaultPdbId)->GetControlFile();
    m_walManager = g_storageInstance->GetPdb(g_defaultPdbId)->GetWalMgr();
    m_walStreamManager = const_cast<WalStreamManager *>(m_walManager->GetWalStreamManager());
    m_walWriter = thrd->m_walWriterContext;
    g_storageInstance->GetGuc()->walLevel = static_cast<int>(WalLevel::WAL_LEVEL_LOGICAL);
    PrepareControlFileContent();
    int ret = mkdir("dstore_wal", 0777);
    ASSERT_EQ(ret, 0);
    RetStatus retStatus = m_walStreamManager->Init(m_walControlFile);
    ASSERT_EQ(retStatus, DSTORE_SUCC);
}

void LOGICALDECODE::PrepareLogicalReplicationSlot()
{
    m_logicalSlot = DstoreNew(m_ut_memory_context) LogicalReplicationSlot();
    m_logicalSlot->Init(g_defaultPdbId, LOGICAL_DECODE_TEST_SLOT_NAME, LOGICAL_DECODE_TEST_PLUGIN_NAME, m_walStream->GetWalId(), 0);
    m_logicalSlot->SerializeToDisk(true);
}

DecodeOptions* LOGICALDECODE::CreateDecodeOptions(CommitSeqNo upToCsn, int nChanges, int decodeWorkers)
{
    DecodeOptions *opt = static_cast<DecodeOptions *>(DstorePalloc(sizeof(DecodeOptions)));
    opt->includeXidsFlag = true;
    opt->includeTimeStampFlag = false;  /* not surpport now */
    opt->skipEmptyXactsFlag = true;
    opt->skipAttrNullsFlag = true;
    opt->advanceSlotFlag = false;
    opt->parallelDecodeWorkerNum = decodeWorkers;
    opt->uptoCSN = upToCsn;
    opt->uptoNchanges = nChanges;
    opt->outputWriteCb = nullptr;
    return opt;
}

LogicalDecodeHandler *LOGICALDECODE::CreateDecodeHandler(DecodeOptions *decodeOption)
{
    LogicalDecodeHandler *decodeHandler = DstoreNew(m_ut_memory_context) LogicalDecodeHandler(
        m_logicalSlot, decodeOption, m_decodeDict, m_ut_memory_context, g_defaultPdbId);
    RetStatus rt = decodeHandler->Init();
    StorageAssert(rt == DSTORE_SUCC);
    return decodeHandler;
}

void LOGICALDECODE::GenerateRandomHeapInsertWithLogicalWal(Xid &xid, CommitSeqNo &commitCsn,
    HeapTuple *&insertTup, ItemPointerData &ctid)
{
    commitCsn = INVALID_CSN;
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();
    HeapTuple *heapTuple = m_utTableHandler->GenerateRandomHeapTuple(m_fakeDesc);
    ctid = m_utTableHandler->InsertHeapTupAndCheckResult(heapTuple, true, txn->GetSnapshotData());
    xid = txn->GetCurrentXid();
    txn->Commit();
    insertTup = heapTuple;
    TransactionMgr *transactionMgr= g_storageInstance->GetPdb(g_defaultPdbId)->GetTransactionMgr();
    commitCsn = transactionMgr->GetCsnFromXid(xid);
}

void LOGICALDECODE::GenerateRandomHeapDeleteWithLogicalWal(ItemPointer ctid, Xid &xid,
    CommitSeqNo &commitCsn)
{
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();
    RetStatus rt = DeleteTuple(ctid, INVALID_SNAPSHOT, true, m_utTableHandler);
    xid = txn->GetCurrentXid();
    txn->Commit();
    TransactionMgr *transactionMgr= g_storageInstance->GetPdb(g_defaultPdbId)->GetTransactionMgr();
    commitCsn = transactionMgr->GetCsnFromXid(xid);
}

void LOGICALDECODE::GenerateRandomHeapUpdateWithLogicalWal(HeapTuple *newTuple, ItemPointer oldCtid,
    ItemPointerData &newCtid, Xid &xid, CommitSeqNo &commitCsn)
{
    commitCsn = INVALID_CSN;
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();
    newCtid = UpdateTuple(oldCtid, newTuple, INVALID_SNAPSHOT, true, m_utTableHandler);
    xid = txn->GetCurrentXid();
    if (newCtid == INVALID_ITEM_POINTER) {
        txn->Abort();
        return;
    }
    txn->Commit();
    TransactionMgr *transactionMgr= g_storageInstance->GetPdb(g_defaultPdbId)->GetTransactionMgr();
    commitCsn = transactionMgr->GetCsnFromXid(xid);
}

void LOGICALDECODE::GenerateMixLogicalWalInOneTrx(Xid &xid, CommitSeqNo &commitCsn)
{
    /* build multi op in one trx */
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();
    HeapTuple *oldTuple = m_utTableHandler->GenerateRandomHeapTuple(m_fakeDesc);
    ItemPointerData oldCtid = m_utTableHandler->InsertHeapTupAndCheckResult(oldTuple, true, txn->GetSnapshotData());
    ASSERT_NE(oldCtid, INVALID_ITEM_POINTER);
    HeapTuple *newTuple = m_utTableHandler->GenerateRandomHeapTuple(m_fakeDesc);
    ItemPointerData newCtid = UpdateTuple(&oldCtid, newTuple, INVALID_SNAPSHOT, true, m_utTableHandler);
    ASSERT_NE(newCtid, INVALID_ITEM_POINTER);
    RetStatus rt = DeleteTuple(&newCtid, INVALID_SNAPSHOT, true, m_utTableHandler);
    ASSERT_EQ(rt, DSTORE_SUCC);
    xid = txn->GetCurrentXid();
    txn->Commit();
    TransactionMgr *transactionMgr= g_storageInstance->GetPdb(g_defaultPdbId)->GetTransactionMgr();
    commitCsn = transactionMgr->GetCsnFromXid(xid);
}

TEST_F(LOGICALDECODE, DISABLED_DecodeInsertFromWal)
{
    std::vector<HeapTuple *>insertTups;
    std::vector<Xid> insertXids;
    std::vector<CommitSeqNo> insertCsns;

    uint64 beforePlsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), beforePlsn);
    m_logicalSlot->AdvanceRestartPlsn(beforePlsn);
    // std::cout << "beforePlsn:" << beforePlsn << std::endl;
    CommitSeqNo curCsn;
    g_storageInstance->GetCsnMgr()->GetNextCsn(curCsn, true);
    m_logicalSlot->AdvanceConfirmCsn(curCsn);
    // std::cout << "confirmCsn:" << m_logicalSlot->m_data.confirmedCsn << std::endl;
    static int insertCnt = 200;
    for (int i = 0; i < insertCnt; i++) {
        HeapTuple *insertTup;
        Xid insertXid;
        CommitSeqNo insertCsn;
        ItemPointerData ctid;
        GenerateRandomHeapInsertWithLogicalWal(insertXid, insertCsn, insertTup, ctid);
        insertTups.push_back(insertTup);
        insertXids.push_back(insertXid);
        insertCsns.push_back(insertCsn);
        // std::cout << "insertCsn:" << insertCsn << std::endl;
    }
    uint64 afterPlsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), afterPlsn);
    // std::cout << "afterPlsn:" << afterPlsn << std::endl;

    DecodeOptions *opts = CreateDecodeOptions(thrd->GetNextCsn() - 1, INVALID_NCHANGES, 1);
    LogicalDecodeHandler *decodeContext = CreateDecodeHandler(opts);
    decodeContext->StartUp();
    TrxLogicalLog *trxLog = nullptr;
    int logCnt = 0;
    while ((trxLog = decodeContext->GetNextTrxLogicalLog()) != nullptr) {
        ASSERT_EQ(trxLog->xid, insertXids[logCnt].m_placeHolder);
        ASSERT_EQ(trxLog->commitCsn, insertCsns[logCnt]);
        ASSERT_EQ(trxLog->nRows, 3);
        logCnt++;
    }
    ASSERT_EQ(insertCnt, logCnt);
    decodeContext->Stop();
    decodeContext->Destroy();
    delete decodeContext;
    decodeContext = nullptr;
    DstorePfree(opts);
}

TEST_F(LOGICALDECODE, DISABLED_DecodeDeleteFromWal)
{
    std::vector<ItemPointerData> insertCtids;
    uint64 curPlsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), curPlsn);

    static int insertCnt = 200;
    for (int i = 0; i < insertCnt; i++) {
        ItemPointerData ctid;
        HeapTuple *insertTup;
        Xid insertXid;
        CommitSeqNo insertCsn;
        GenerateRandomHeapInsertWithLogicalWal(insertXid, insertCsn, insertTup, ctid);
        insertCtids.push_back(ctid);
    }
    m_logicalSlot->AdvanceConfirmCsn(thrd->GetNextCsn() - 1);
    uint64 beforePlsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), beforePlsn);
    m_logicalSlot->AdvanceRestartPlsn(beforePlsn);

    /* now Delete */
    std::vector<Xid> deleteXids;
    std::vector<CommitSeqNo> deleteCsns;
    for (int i = 0; i < insertCnt; i++) {
        Xid deleteXid;
        CommitSeqNo deleteCsn;
        GenerateRandomHeapDeleteWithLogicalWal(&(insertCtids[i]), deleteXid, deleteCsn);
        deleteXids.push_back(deleteXid);
        deleteCsns.push_back(deleteCsn);
    }
    uint64 afterPlsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), afterPlsn);

    DecodeOptions *opts = CreateDecodeOptions(thrd->GetNextCsn() - 1, INVALID_NCHANGES, 1);
    LogicalDecodeHandler *decodeContext = CreateDecodeHandler(opts);
    decodeContext->StartUp();
    TrxLogicalLog *trxLog = nullptr;
    int logCnt = 0;
    while ((trxLog = decodeContext->GetNextTrxLogicalLog()) != nullptr) {
        ASSERT_EQ(trxLog->xid, deleteXids[logCnt].m_placeHolder);
        ASSERT_EQ(trxLog->commitCsn, deleteCsns[logCnt]);
        ASSERT_EQ(trxLog->nRows, 3);
        logCnt++;
    }
    ASSERT_EQ(insertCnt, logCnt);
    decodeContext->Stop();
    decodeContext->Destroy();
    delete decodeContext;
    decodeContext = nullptr;
    DstorePfree(opts);
}

TEST_F(LOGICALDECODE, DISABLED_DecodeUpdateFromWal)
{
    std::vector<ItemPointerData> insertCtids;
    uint64 curPlsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), curPlsn);

    static int insertCnt = 200;
    for (int i = 0; i < insertCnt; i++) {
        ItemPointerData ctid;
        HeapTuple *insertTup;
        Xid insertXid;
        CommitSeqNo insertCsn;
        GenerateRandomHeapInsertWithLogicalWal(insertXid, insertCsn, insertTup, ctid);
        insertCtids.push_back(ctid);
    }
    m_logicalSlot->AdvanceConfirmCsn(thrd->GetNextCsn() - 1);
    uint64 beforePlsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), beforePlsn);
    m_logicalSlot->AdvanceRestartPlsn(beforePlsn);

    /* now Update */
    std::vector<Xid> updateXids;
    std::vector<CommitSeqNo> updateCsns;
    for (int i = 0; i < insertCnt; i++) {
        Xid updateXid;
        CommitSeqNo updateCsn;
        ItemPointerData newCtid;
        HeapTuple *newTuple = m_utTableHandler->GenerateRandomHeapTuple(m_fakeDesc);
        GenerateRandomHeapUpdateWithLogicalWal(newTuple, &(insertCtids[i]), newCtid, updateXid, updateCsn);
        updateXids.push_back(updateXid);
        updateCsns.push_back(updateCsn);
    }
    uint64 afterPlsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), afterPlsn);

    DecodeOptions *opts = CreateDecodeOptions(thrd->GetNextCsn() - 1, INVALID_NCHANGES, 1);
    LogicalDecodeHandler *decodeContext = CreateDecodeHandler(opts);
    decodeContext->StartUp();
    TrxLogicalLog *trxLog = nullptr;
    int logCnt = 0;
    while ((trxLog = decodeContext->GetNextTrxLogicalLog()) != nullptr) {
        ASSERT_EQ(trxLog->xid, updateXids[logCnt].m_placeHolder);
        ASSERT_EQ(trxLog->commitCsn, updateCsns[logCnt]);
        ASSERT_EQ(trxLog->nRows, 3);
        logCnt++;
    }
    ASSERT_EQ(insertCnt, logCnt);
    decodeContext->Stop();
    decodeContext->Destroy();
    delete decodeContext;
    decodeContext = nullptr;
    DstorePfree(opts);
}

TEST_F(LOGICALDECODE, DISABLED_DecodeMixFromWal)
{
    uint64 beforePlsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), beforePlsn);
    m_logicalSlot->AdvanceRestartPlsn(beforePlsn);
    m_logicalSlot->AdvanceConfirmCsn(thrd->GetNextCsn() - 1);

    static int opCnt = 100;
    std::vector<Xid> opXids(opCnt * 3, INVALID_XID);
    std::vector<CommitSeqNo> opCsns(opCnt * 3, INVALID_CSN);
    for (int i = 0; i < opCnt * 3; i += 3) {
        ItemPointerData oldCtid;
        ItemPointerData newCtid;
        HeapTuple *insertTup;
        GenerateRandomHeapInsertWithLogicalWal(opXids[i], opCsns[i], insertTup, oldCtid);
        HeapTuple *newTuple = m_utTableHandler->GenerateRandomHeapTuple(m_fakeDesc);
        GenerateRandomHeapUpdateWithLogicalWal(newTuple, &oldCtid, newCtid, opXids[i + 1], opCsns[i + 1]);
        GenerateRandomHeapDeleteWithLogicalWal(&newCtid, opXids[i + 2], opCsns[i + 2]);
    }
    uint64 afterPlsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), afterPlsn);

    DecodeOptions *opts = CreateDecodeOptions(thrd->GetNextCsn() - 1, INVALID_NCHANGES, 1);
    LogicalDecodeHandler *decodeContext = CreateDecodeHandler(opts);
    decodeContext->StartUp();
    TrxLogicalLog *trxLog = nullptr;
    int logCnt = 0;
    while ((trxLog = decodeContext->GetNextTrxLogicalLog()) != nullptr) {
        ASSERT_EQ(trxLog->xid, opXids[logCnt].m_placeHolder);
        ASSERT_EQ(trxLog->commitCsn, opCsns[logCnt]);
        ASSERT_EQ(trxLog->nRows, 3); /* begin insert | update | delete commit */
        logCnt++;
    }
    ASSERT_EQ(opCnt * 3, logCnt);
    decodeContext->Stop();
    decodeContext->Destroy();
    delete decodeContext;
    decodeContext = nullptr;
    DstorePfree(opts);
}

TEST_F(LOGICALDECODE, DISABLED_WalReassembleByTrxUnit)
{
    uint64 beforePlsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), beforePlsn);
    m_logicalSlot->AdvanceRestartPlsn(beforePlsn);
    m_logicalSlot->AdvanceConfirmCsn(thrd->GetNextCsn() - 1);

    static int opCnt = 100;
    std::vector<Xid> opXids(opCnt, INVALID_XID);
    std::vector<CommitSeqNo> opCsns(opCnt, INVALID_CSN);
    for (int i = 0; i < opCnt; i++) {
        GenerateMixLogicalWalInOneTrx(opXids[i], opCsns[i]);
    }
    uint64 afterPlsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), afterPlsn);

    DecodeOptions *opts = CreateDecodeOptions(thrd->GetNextCsn() - 1, INVALID_NCHANGES, 1);
    LogicalDecodeHandler *decodeContext = CreateDecodeHandler(opts);
    decodeContext->StartUp();
    TrxLogicalLog *trxLog = nullptr;
    int logCnt = 0;
    while ((trxLog = decodeContext->GetNextTrxLogicalLog()) != nullptr) {
        ASSERT_EQ(trxLog->xid, opXids[logCnt].m_placeHolder);
        ASSERT_EQ(trxLog->commitCsn, opCsns[logCnt]);
        ASSERT_EQ(trxLog->nRows, 5);    /* begin insert update delete commit */
        logCnt++;
    }
    ASSERT_EQ(opCnt, logCnt);
    decodeContext->Stop();
    decodeContext->Destroy();
    delete decodeContext;
    decodeContext = nullptr;
    DstorePfree(opts);
}

TEST_F(LOGICALDECODE, DISABLED_DecodeWithParallelWorker)
{
    uint64 beforePlsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), beforePlsn);
    m_logicalSlot->AdvanceRestartPlsn(beforePlsn);
    m_logicalSlot->AdvanceConfirmCsn(thrd->GetNextCsn() - 1);

    static int opCnt = 100;
    std::vector<Xid> opXids(opCnt, INVALID_XID);
    std::vector<CommitSeqNo> opCsns(opCnt, INVALID_CSN);
    for (int i = 0; i < opCnt; i++) {
        GenerateMixLogicalWalInOneTrx(opXids[i], opCsns[i]);
    }
    uint64 afterPlsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), afterPlsn);

    /* create 5 parallel decode workers */
    static int parallelWorker = 5;
    DecodeOptions *opts = CreateDecodeOptions(thrd->GetNextCsn(), INVALID_NCHANGES, parallelWorker);
    LogicalDecodeHandler *decodeContext = CreateDecodeHandler(opts);
    decodeContext->StartUp();
    TrxLogicalLog *trxLog = nullptr;
    int curWorker = 0;
    int logCnt = 0;
    while ((trxLog = decodeContext->GetNextTrxLogicalLog()) != nullptr) {
        ASSERT_EQ(trxLog->xid, opXids[logCnt].m_placeHolder);
        ASSERT_EQ(trxLog->commitCsn, opCsns[logCnt]);
        ASSERT_EQ(trxLog->nRows, 5);    /* begin insert update delete commit */
        ASSERT_EQ(trxLog->workerId, curWorker);    /* check this logical log decoded by who */
        curWorker = (curWorker + parallelWorker + 1) % parallelWorker;
        logCnt++;
    }
    ASSERT_EQ(opCnt, logCnt);
    decodeContext->Stop();
    decodeContext->Destroy();
    delete decodeContext;
    decodeContext = nullptr;
    DstorePfree(opts);
}

TEST_F(LOGICALDECODE, DISABLED_DecodeWithAutoSlotAdvance)
{
    std::vector<HeapTuple *>insertTups;
    std::vector<Xid> insertXids;
    std::vector<CommitSeqNo> insertCsns;

    uint64 beforePlsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), beforePlsn);
    m_logicalSlot->AdvanceRestartPlsn(beforePlsn);
    CommitSeqNo curCsn;
    g_storageInstance->GetCsnMgr()->GetNextCsn(curCsn, true);
    m_logicalSlot->AdvanceConfirmCsn(curCsn);
    static int insertCnt = 200;
    for (int i = 0; i < insertCnt; i++) {
        HeapTuple *insertTup;
        Xid insertXid;
        CommitSeqNo insertCsn;
        ItemPointerData ctid;
        GenerateRandomHeapInsertWithLogicalWal(insertXid, insertCsn, insertTup, ctid);
    }
    uint64 afterPlsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), afterPlsn);
    DecodeOptions *opts = CreateDecodeOptions(thrd->GetNextCsn() - 1, INVALID_NCHANGES, 1);
    /* set advance true */
    opts->advanceSlotFlag = true;
    LogicalDecodeHandler *decodeContext = CreateDecodeHandler(opts);
    decodeContext->StartUp();
    TrxLogicalLog *trxLog = nullptr;
    TrxLogicalLog *lastTrxLog = nullptr;
    int logCnt = 0;
    while ((trxLog = decodeContext->GetNextTrxLogicalLog()) != nullptr) {
        /* mock client confirm */
        decodeContext->ConfirmTrxLogicalLog(trxLog);
        logCnt++;
        lastTrxLog = trxLog;
    }
    ASSERT_EQ(insertCnt, logCnt);
    decodeContext->Stop();
    /* check! */
    ASSERT_EQ(m_logicalSlot->GetConfirmCsn(), thrd->GetNextCsn() - 1);
    ASSERT_EQ(m_logicalSlot->GetRestartPlsn(), lastTrxLog->restartDecodingPlsn);
    decodeContext->Destroy();
    delete decodeContext;
    decodeContext = nullptr;
    DstorePfree(opts);
}
