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
 * ut_logical_replication_base.cpp
 *
 * Description:
 *
 * ---------------------------------------------------------------------------------------
 *
 */

#include "ut_logical_replication/ut_logical_replication_base.h"
#include "transaction/dstore_transaction_mgr.h"

namespace DSTORE {
void LOGICALREPBASETEST::SetUp()
{
    WALBASICTEST::SetUp();
    PrepareControlFileContent();
    int ret = mkdir("dstore_wal", 0777);
    StorageAssert(ret == 0);
    m_walManager->Init(m_walControlFile);
    RetStatus retStatus = m_walStreamManager->Init(m_walControlFile);
    ASSERT_EQ(retStatus, DSTORE_SUCC);
    m_logicalReplicaMgr = DstoreNew(m_ut_memory_context)LogicalReplicaMgr(g_defaultPdbId, m_ut_memory_context);
    m_logicalReplicaMgr->Init();
    MockDecodeOptions();
    int threadNum = m_config.ReadInteger("UTHeapMultiThread-ThreadNum");
    if (threadNum < 0) {
        threadNum = 64;
    }
    m_pool.Start(threadNum);
}

void LOGICALREPBASETEST::TearDown()
{
    m_pool.Shutdown();
    m_logicalReplicaMgr->Destroy();
    DstorePfreeExt(m_decodeOptions);
    WALBASICTEST::TearDown();
}

void LOGICALREPBASETEST::MockDecodeOptions(bool includeXidsFlag, bool skipAttrNullsFlag, bool advanceSlotFlag,
    int parallelDecodeWorkerNum)
{
    m_decodeOptions = static_cast<DecodeOptions *>(DstorePalloc(sizeof(DecodeOptions)));
    m_decodeOptions->includeXidsFlag = includeXidsFlag;
    m_decodeOptions->skipAttrNullsFlag = skipAttrNullsFlag;
    m_decodeOptions->includeTimeStampFlag = false; /* not surpport now */
    m_decodeOptions->advanceSlotFlag = false;
    m_decodeOptions->parallelDecodeWorkerNum = parallelDecodeWorkerNum;
    m_decodeOptions->outputWriteCb = nullptr;
}

void LOGICALREPBASETEST::TaskWriteTransaction()
{
    Transaction *trx = thrd->GetActiveTransaction();
    trx->Start();
    trx->AllocTransactionSlot();
    GaussUsleep(50000); /* 50ms */
    trx->Commit();
}

void LOGICALREPBASETEST::TaskCreateAndDropSingleRepSlot(int i,
    const std::function<RetStatus(const CommitSeqNo)> syncCatalogCallBack)
{
    Transaction *trx = thrd->GetActiveTransaction();
    trx->Start();
    char slotName[6] = "test0";
    char pluginName[20] = "test_decoding";
    slotName[4] = '0' + i;
    EXPECT_EQ(m_logicalReplicaMgr->CreateLogicalReplicationSlot(slotName, pluginName, MockSyncCatalog), DSTORE_SUCC);
    trx->Commit();
    EXPECT_EQ(m_logicalReplicaMgr->DropLogicalReplicationSlot(slotName), DSTORE_SUCC);
}

TEST_F(LOGICALREPBASETEST, CreateAndDropSlotTest)
{
    /* create 3 slot */
    int slotNum = 3;
    char slotName[6] = "test0";
    char plugin[20] = "test_decoding";
    for (int i = 0; i < slotNum; i++) {
        slotName[4] = '0' + i;
        EXPECT_EQ(m_logicalReplicaMgr->CreateLogicalReplicationSlot(slotName, plugin, MockSyncCatalog), DSTORE_SUCC);
    }
    /* recreate slot */
    EXPECT_EQ(m_logicalReplicaMgr->CreateLogicalReplicationSlot(slotName, plugin, MockSyncCatalog), DSTORE_FAIL);

    /* load from disk */
    m_logicalReplicaMgr->Destroy();
    m_logicalReplicaMgr = DstoreNew(m_ut_memory_context)LogicalReplicaMgr(g_defaultPdbId, m_ut_memory_context);
    m_logicalReplicaMgr->Init();
    slotName[4] = '0';
    for (int i = 0; i < slotNum; i++) {
        slotName[4] = '0' + i;
        EXPECT_NE(m_logicalReplicaMgr->AcquireLogicalReplicationSlot(slotName), nullptr);
    }

    /* create slot exceed MAX_LOGICAL_SLOT_NUM */
    slotName[4] = '0';
    for (int i = slotNum; i < MAX_LOGICAL_SLOT_NUM; i++) {
        slotName[4] = '0' + i;
        EXPECT_EQ(m_logicalReplicaMgr->CreateLogicalReplicationSlot(slotName, plugin, MockSyncCatalog), DSTORE_SUCC);
    }
    slotName[4] = '0' + MAX_LOGICAL_SLOT_NUM;
    EXPECT_EQ(m_logicalReplicaMgr->CreateLogicalReplicationSlot(slotName, plugin, MockSyncCatalog), DSTORE_FAIL);

    /* drop slot */
    slotName[4] = '0' + slotNum;
    EXPECT_EQ(m_logicalReplicaMgr->DropLogicalReplicationSlot(slotName), DSTORE_SUCC);
    EXPECT_EQ(m_logicalReplicaMgr->AcquireLogicalReplicationSlot(slotName), nullptr);
    /* slot does not exist, drop fail; slot is active, drop fail */
    EXPECT_EQ(m_logicalReplicaMgr->DropLogicalReplicationSlot(slotName), DSTORE_FAIL);
    slotName[4] = '0';
    EXPECT_EQ(m_logicalReplicaMgr->DropLogicalReplicationSlot(slotName), DSTORE_FAIL);
}

TEST_F(LOGICALREPBASETEST, CreateAndDropSlotMultiThreadTest)
{
    int writeThreadNum = 3;
    for (int i = 1; i <= writeThreadNum; i++) {
        m_pool.AddTask(TaskCreateAndDropSingleSlot, this, i, MockSyncCatalog);
    }
    m_pool.WaitAllTaskFinish();
}

TEST_F(LOGICALREPBASETEST, CreateAndDropSlotWithOtherTrxTest)
{
    int trxNum = 5;
    for (int i = 0; i < trxNum; i++) {
        m_pool.AddTask(InsertWriteTaskTrx, this);
    }
    m_pool.AddTask(TaskCreateAndDropSingleSlot, this, 0, MockSyncCatalog);
    m_pool.WaitAllTaskFinish();
}

TEST_F(LOGICALREPBASETEST, CreateAndDropSlotWithOtherTrxMultiThreadTest)
{
    int trxNum = 15;
    for (int i = 0; i < trxNum; i++) {
        m_pool.AddTask(InsertWriteTaskTrx, this);
    }
    int writeThreadNum = 3;
    for (int i = 1; i <= writeThreadNum; i++) {
        m_pool.AddTask(TaskCreateAndDropSingleSlot, this, i, MockSyncCatalog);
    }
    m_pool.WaitAllTaskFinish();
}
}
