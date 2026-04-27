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
#include "ut_transaction/ut_csn_mgr.h"
#include "ut_utilities/ut_dstore_framework.h"
#include "framework/dstore_instance.h"
#include "diagnose/dstore_csn_mgr_diagnose.h"
#include <thread>
#include <vector>
#include <algorithm>

using namespace DSTORE;
static void UpdateLocalCsnMin(CommitSeqNo csnMin)
{
    thrd->threadCore.xact->csnMin = csnMin;
}

static void ThreadFunc(CommitSeqNo csnMin, pthread_barrier_t *barrier1, pthread_barrier_t *barrier2)
{
    create_thread_and_register();

    UpdateLocalCsnMin(csnMin);

    /* sync point after csnMin updated */
    pthread_barrier_wait(barrier1);

    /* need to wait on the second barrier to make sure thread does not unregister */
    pthread_barrier_wait(barrier2);

    unregister_thread();
}

TEST_F(CsnMgrTest, CsnMgrSingleThreadTest_level0)
{
    CsnMgr *csnMgr = g_storageInstance->GetCsnMgr();

    /* Without any valid csn, global_csn_min should be COMMITSEQNO_FIRST_NORMAL */
    EXPECT_EQ(csnMgr->GetRecycleCsnMin(INVALID_PDB_ID), COMMITSEQNO_FIRST_NORMAL);

    csnMgr->UpdateLocalCsnMin();
    EXPECT_EQ(csnMgr->GetRecycleCsnMin(INVALID_PDB_ID), COMMITSEQNO_FIRST_NORMAL);

    /* Make up a valid csn here */
    const CommitSeqNo expectedCsnMin = 10;
    UpdateLocalCsnMin(expectedCsnMin);
    GsAtomicFetchAddU64(&csnMgr->m_nextCsn, expectedCsnMin + 1);
    csnMgr->UpdateLocalCsnMin();

    /* Since only 1 thread, its csn min should be the global_csn_min */
    EXPECT_EQ(csnMgr->GetRecycleCsnMin(INVALID_PDB_ID), expectedCsnMin);
    
    /* Test flashback csn */
    const CommitSeqNo flashbackCsn = 5;
    csnMgr->SetFlashbackCsnMin(flashbackCsn);
    EXPECT_EQ(csnMgr->GetRecycleCsnMin(INVALID_PDB_ID), flashbackCsn);

    /* Test persist csn */
    CommitSeqNo newMaxReservedCsn;
    csnMgr->UpdateMaxReservedCsn(1000);
    g_storageInstance->GetPdb(PDB_TEMPLATE1_ID)->m_controlFile->GetMaxReservedCSN(newMaxReservedCsn);
    EXPECT_EQ(newMaxReservedCsn, 1000);
    CsnMgrDiagnose::DumpCsnMgr();
}

TEST_F(CsnMgrTest, CsnMgrMultipleThreadsTest_level0)
{
    CommitSeqNo nextCsn = 100;
    CsnMgr *csnMgr = g_storageInstance->GetCsnMgr();
    GsAtomicFetchAddU64(&csnMgr->m_nextCsn, nextCsn);

    /* one csn for each thread */
    const std::vector<CommitSeqNo> csns = {23UL, 25UL, 88UL, INVALID_CSN};
    const size_t NUM_THREADS = csns.size();

    /* barrier shared between all threads */
    pthread_barrier_t barrier1;
    pthread_barrier_init(&barrier1, nullptr, NUM_THREADS + 1);

    /* start the testing threads each with a second barrier */
    pthread_barrier_t thread_barriers[NUM_THREADS];
    std::thread threads[NUM_THREADS];
    for (auto i = 0; i < NUM_THREADS; ++i) {
        /* thread specific barrier, count is 1 + 1 = 2 (1 node thread + 1 testcase thread) */
        pthread_barrier_init(&thread_barriers[i], nullptr, 2);
        threads[i] = std::thread(ThreadFunc, csns[i], &barrier1, &thread_barriers[i]);
    }

    /* wait on barrier1 to make sure all threads are done updating csns */
    pthread_barrier_wait(&barrier1);

    csnMgr->UpdateLocalCsnMin();
    /* global_csn_min should be the minimum of all local csn_mins*/
    EXPECT_EQ(csnMgr->GetRecycleCsnMin(INVALID_PDB_ID), *(std::min_element(csns.begin(),csns.end() - 1)));

    /* remove each thread one by one (except for last thread) and recalculate global_csn_min */
    for (auto i = 0; i < NUM_THREADS - 2; ++i) {
        /* unblock barrier */
        pthread_barrier_wait(&thread_barriers[i]);
        threads[i].join();
        /* at this point, thread i should be unregistered */
        csnMgr->UpdateLocalCsnMin();
        /* global_csn_min should update to be the minimum of csns of threads still running */
        EXPECT_EQ(csnMgr->GetRecycleCsnMin(INVALID_PDB_ID), *(std::min_element(csns.begin() + i + 1, csns.end() - 1)));
    }

    /* remove second last thread */
    pthread_barrier_wait(&thread_barriers[NUM_THREADS - 2]);
    threads[NUM_THREADS - 2].join();
    csnMgr->UpdateLocalCsnMin();
    /* csn for the last thread is INVALID_CSN and without any other running threads, global_csn_min should be the m_nextCsn */
    EXPECT_EQ(csnMgr->GetRecycleCsnMin(INVALID_PDB_ID), nextCsn + 1);

    /* remove last thread */
    pthread_barrier_wait(&thread_barriers[NUM_THREADS - 1]);
    threads[NUM_THREADS - 1].join();
    csnMgr->UpdateLocalCsnMin();
    /* without any running threads, globalCsnMin should be back to m_nextCsn */
    EXPECT_EQ(csnMgr->GetRecycleCsnMin(INVALID_PDB_ID), nextCsn + 1);

    for (auto i = 0; i < NUM_THREADS; ++i) {
        pthread_barrier_destroy(&thread_barriers[i]);
    }

    pthread_barrier_destroy(&barrier1);
}
