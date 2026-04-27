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
#include "ut_logical_replication/ut_logical_replication_slot.h"
#include "transaction/dstore_transaction_mgr.h"

namespace DSTORE {
static char LOGICAL_REP_TEST_SLOT_NAME[NAME_DATA_LEN] = "TestSlot\0";
static char LOGICAL_REP_TEST_PLUGIN_NAME[NAME_DATA_LEN] = "Text_Plugin\0";

void LOGICALREPSLOTTEST::SetUp()
{
    LOGICALREPBASETEST::SetUp();
    m_slot = DstoreNew(m_ut_memory_context)LogicalReplicationSlot();
    m_slot->Init(g_defaultPdbId, LOGICAL_REP_TEST_SLOT_NAME, LOGICAL_REP_TEST_PLUGIN_NAME, 0, 0);
}

void LOGICALREPSLOTTEST::TearDown()
{
    m_slot->Destroy();
    delete m_slot;
    m_slot = nullptr;
    LOGICALREPBASETEST::TearDown();
}

void LOGICALREPSLOTTEST::TaskCreateReadonlyTransaction()
{
    Transaction *trx = thrd->GetActiveTransaction();
    trx->Start();
    GaussUsleep(1000000); /* 1s */
    trx->Commit();
}

void LOGICALREPSLOTTEST::TaskFindStartPoint(const std::function<RetStatus(const CommitSeqNo)> syncCatalogCallBack)
{
    Transaction *trx = thrd->GetActiveTransaction();
    trx->Start();
    GaussUsleep(50000); /* wait trx alloc xid */
    m_slot->FindStartPoint(syncCatalogCallBack);
    trx->Commit();
}

TEST_F(LOGICALREPSLOTTEST, FindStartPointTest)
{
    /* create 63 sql threads, each lasts 50 milliseconds */
    EXPECT_NE(thrd, nullptr);
    int trxNum = 5;
    for (int i = 0; i < trxNum; i++) {
        m_pool.AddTask(InsertWriteTaskTrx, this);
    }
    for (int i = 0; i < 3; i++) {
        m_pool.AddTask(InsertTaskReadonlyTrx, this);
    }
    m_pool.AddTask(InsertTaskStartpoint, this, MockSyncCatalog);
    m_pool.WaitAllTaskFinish();

    /* after finding the startpoint, check if the confirmedCsn is behind the last ended sql thread's */
    EXPECT_EQ(m_slot->GetConfirmCsn(), trxNum);
}
}