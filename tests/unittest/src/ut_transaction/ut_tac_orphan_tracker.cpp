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
#include "ut_transaction/ut_tac_orphan_tracker.h"
#include "ut_utilities/ut_dstore_framework.h"
#include "framework/dstore_instance.h"
#include <thread>
#include <vector>
#include <algorithm>
#include <cmath>

struct TacOrphanTrxEntry {
    CommitSeqNo csn;
    TimestampTz expiryTimestamp;
};

static constexpr uint16 TAC_GRACE_PERIOD = 5;

static void UpdateLocalCsnMin(CommitSeqNo csnMin)
{
    thrd->threadCore.xact->firstStatementCsnMin = csnMin;
    /* Set expiry timer for the current thread's firstStatementCsnMin */
    thrd->GetXact()->firstStatementCsnMinExpiryTimestamp = GetCurrentTimestampInSecond() + TAC_GRACE_PERIOD;
}

static void ThreadFunc_UpdateLocalCsnMin(CommitSeqNo csnMin, pthread_barrier_t *barrier1, pthread_barrier_t *barrier2)
{
    create_thread_and_register();

    UpdateLocalCsnMin(csnMin);

    /* sync point after csnMin updated */
    pthread_barrier_wait(barrier1);

    /* need to wait on the second barrier to make sure thread does not unregister */
    pthread_barrier_wait(barrier2);

    unregister_thread();
}

static void AddTacOrphanTrx(CommitSeqNo csnMin)
{
    g_storageInstance->GetCsnMgr()->AddCsnToTacTracker(csnMin);
}

static void ThreadFunc(CommitSeqNo csnMin, pthread_barrier_t *barrier1, pthread_barrier_t *barrier2)
{
    create_thread_and_register();

    AddTacOrphanTrx(csnMin);

    /* sync point after csnMin updated */
    pthread_barrier_wait(barrier1);

    /* need to wait on the second barrier to make sure thread does not unregister */
    pthread_barrier_wait(barrier2);

    unregister_thread();
}

static void GetSmallestOrphanCsn()
{
    g_storageInstance->GetCsnMgr()->m_tacOrphanTrxTracker->GetSmallestOrphanCsn();
}

static void ThreadFunc2(pthread_barrier_t *barrier1, pthread_barrier_t *barrier2)
{
    create_thread_and_register();

    GetSmallestOrphanCsn();

    /* sync point after csnMin updated */
    pthread_barrier_wait(barrier1);

    /* need to wait on the second barrier to make sure thread does not unregister */
    pthread_barrier_wait(barrier2);

    unregister_thread();
}

TEST_F(TacOrphanTrxTrackerTest, DummyTrackerTest)
{
    class TacOrphanTrxTracker dummyTracker;
    dummyTracker.Init();
    ASSERT_TRUE(dummyTracker.m_allocator != nullptr);
    ASSERT_TRUE(dummyTracker.m_csnHeap != nullptr);
    ASSERT_EQ(dummyTracker.m_csnHeap->bh_space, 8);
    dummyTracker.Destroy();
    ASSERT_TRUE(dummyTracker.m_allocator == nullptr);
    ASSERT_TRUE(dummyTracker.m_csnHeap == nullptr);
}

TEST_F(TacOrphanTrxTrackerTest, SingleOrphanEntryTest)
{
    CsnMgr *csnMgr = g_storageInstance->GetCsnMgr();

    /* Without any valid csn, global_csn_min should be COMMITSEQNO_FIRST_NORMAL */
    EXPECT_EQ(csnMgr->GetRecycleCsnMin(INVALID_PDB_ID), COMMITSEQNO_FIRST_NORMAL);
    csnMgr->UpdateLocalCsnMin();
    EXPECT_EQ(csnMgr->GetRecycleCsnMin(INVALID_PDB_ID), COMMITSEQNO_FIRST_NORMAL);

    /* Make up a valid csn here */
    const CommitSeqNo expectedCsnMin = 30;

    g_storageInstance->GetCsnMgr()->AddCsnToTacTracker(expectedCsnMin);
    /* We need to increment nextcsn value for UpdateLocalCsn to skip nextcsn consideration */
    GsAtomicFetchAddU64(&csnMgr->m_nextCsn, expectedCsnMin + 1);
    csnMgr->UpdateLocalCsnMin();

    /* Csn min should be the one in the tracker */
    EXPECT_EQ(csnMgr->GetRecycleCsnMin(INVALID_PDB_ID), expectedCsnMin);
}

TEST_F(TacOrphanTrxTrackerTest, ExpiredOrphanEntryTest)
{
    CsnMgr *csnMgr = g_storageInstance->GetCsnMgr();

    /* Make up valid csn here */
    const CommitSeqNo orphanCsnMin1 = 100;
    const CommitSeqNo orphanCsnMin2 = 50;
    const CommitSeqNo orphanCsnMin3 = 150;
    
    g_storageInstance->GetCsnMgr()->AddCsnToTacTracker(orphanCsnMin1);
    g_storageInstance->GetCsnMgr()->AddCsnToTacTracker(orphanCsnMin2);
    g_storageInstance->GetCsnMgr()->AddCsnToTacTracker(orphanCsnMin3);

    GsAtomicFetchAddU64(&csnMgr->m_nextCsn, orphanCsnMin3 + 1);
    csnMgr->UpdateLocalCsnMin();

    /* Should be orphanCsnMin2 because it is the smallest element in the tracker */
    EXPECT_EQ(g_storageInstance->GetCsnMgr()->m_tacOrphanTrxTracker->GetSmallestOrphanCsn(), orphanCsnMin2);
    EXPECT_EQ(csnMgr->GetRecycleCsnMin(INVALID_PDB_ID), orphanCsnMin2);
    /* After two seconds they should all be expired */
    sleep(2);
    csnMgr->UpdateLocalCsnMin();
    /* The smallest orphan csn should return invalid */
    EXPECT_EQ(g_storageInstance->GetCsnMgr()->m_tacOrphanTrxTracker->GetSmallestOrphanCsn(), INVALID_CSN);
    /* The binary heap should be empty because we removed all entries in previous line */
    EXPECT_TRUE(binaryheap_empty(g_storageInstance->GetCsnMgr()->m_tacOrphanTrxTracker->m_csnHeap));
    /* The recycle csn should be normal again */
    csnMgr->UpdateLocalCsnMin();
    EXPECT_EQ(csnMgr->GetRecycleCsnMin(INVALID_PDB_ID), csnMgr->GetLocalCsnMin());
}

TEST_F(TacOrphanTrxTrackerTest, TrackerExpansionTest)
{
    /* The binary heap should be empty and has unexpanded capacity at first */
    EXPECT_EQ(g_storageInstance->GetCsnMgr()->m_tacOrphanTrxTracker->m_csnHeap->bh_space, 8);
    EXPECT_TRUE(binaryheap_empty(g_storageInstance->GetCsnMgr()->m_tacOrphanTrxTracker->m_csnHeap));

    CsnMgr *csnMgr = g_storageInstance->GetCsnMgr();

    /* The binary heap will expand three times */
    int numOfExpansions = 3;
    int baseCsnmin = 200;
    int numOfEntries = 8 * std::pow(2, numOfExpansions - 1) + 1;
    for (int i = numOfEntries - 1; i >= 0 ; i--) {
        /* Add entries into the tracker in decreasing order */
        g_storageInstance->GetCsnMgr()->AddCsnToTacTracker(i * 10 + baseCsnmin);
    }

    /* Add one expected entry into the tracker with decrement of 10 from basecsnmin */
    const CommitSeqNo expectedCsnMin = baseCsnmin - 10;
    g_storageInstance->GetCsnMgr()->AddCsnToTacTracker(expectedCsnMin);
    GsAtomicFetchAddU64(&csnMgr->m_nextCsn, (numOfEntries* 10 + baseCsnmin) + 1);
    csnMgr->UpdateLocalCsnMin();

    /* Should be expectedCsnMin because it is the smallest element in the tracker */
    EXPECT_EQ(csnMgr->GetRecycleCsnMin(INVALID_PDB_ID), expectedCsnMin);

    /* The expanded size should be reflected here, 8 is the initial capacity of the OrphanTrxTracker */
    EXPECT_EQ(g_storageInstance->GetCsnMgr()->m_tacOrphanTrxTracker->m_csnHeap->bh_space,
              8 * std::pow(2, numOfExpansions));
}

TEST_F(TacOrphanTrxTrackerTest, TrackerMultipleThreadsAddOrphanTest)
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

    /* remove each thread one by one and recalculate global_csn_min */
    for (auto i = 0; i < NUM_THREADS; ++i) {
        /* unblock barrier */
        pthread_barrier_wait(&thread_barriers[i]);
        threads[i].join();
        /* at this point, thread i should be unregistered */
        csnMgr->UpdateLocalCsnMin();
        /* global_csn_min should still be the minimum of all local csn_mins since they are retained in the tracker */
        EXPECT_EQ(csnMgr->GetRecycleCsnMin(INVALID_PDB_ID), *(std::min_element(csns.begin(), csns.end() - 1)));
    }

    for (auto i = 0; i < NUM_THREADS; ++i) {
        pthread_barrier_destroy(&thread_barriers[i]);
    }

    pthread_barrier_destroy(&barrier1);
}

TEST_F(TacOrphanTrxTrackerTest, TrackerMultipleThreadsGetOrphanTest)
{
    CommitSeqNo nextCsn = 100;
    CsnMgr *csnMgr = g_storageInstance->GetCsnMgr();

    int baseCsnmin = 1000;
    int numOfEntries = 100;

    for (int i = 0; i < numOfEntries; i++) {
        /* Add entries into the tracker with increment of 10 */
        g_storageInstance->GetCsnMgr()->AddCsnToTacTracker(i * 10 + baseCsnmin);
    }
    GsAtomicFetchAddU64(&csnMgr->m_nextCsn, (numOfEntries* 10 + baseCsnmin) + 1);
    csnMgr->UpdateLocalCsnMin();
    EXPECT_EQ(csnMgr->GetRecycleCsnMin(INVALID_PDB_ID), baseCsnmin);

    const size_t NUM_THREADS = 10;

    /* barrier shared between all threads */
    pthread_barrier_t barrier1;
    pthread_barrier_init(&barrier1, nullptr, NUM_THREADS + 1);

    /* start the testing threads each with a second barrier */
    pthread_barrier_t thread_barriers[NUM_THREADS];
    std::thread threads[NUM_THREADS];

    /* After two seconds, all the entries should be expired and we will try to remove them */
    sleep(2);

    /* Test for the removal of entires under multiple threads */
    for (auto i = 0; i < NUM_THREADS; ++i) {
        /* thread specific barrier, count is 1 + 1 = 2 (1 node thread + 1 testcase thread) */
        pthread_barrier_init(&thread_barriers[i], nullptr, 2);
        threads[i] = std::thread(ThreadFunc2, &barrier1, &thread_barriers[i]);
    }

    /* wait on barrier1 to make sure all threads are done updating csns */
    pthread_barrier_wait(&barrier1);

    csnMgr->UpdateLocalCsnMin();
    /* global_csn_min should be without anything from the tracker */
    EXPECT_EQ(csnMgr->GetRecycleCsnMin(INVALID_PDB_ID), csnMgr->GetLocalCsnMin());

    /* remove each thread one by one and recalculate global_csn_min */
    for (auto i = 0; i < NUM_THREADS; ++i) {
        /* unblock barrier */
        pthread_barrier_wait(&thread_barriers[i]);
        threads[i].join();
        /* at this point, thread i should be unregistered */
        csnMgr->UpdateLocalCsnMin();
        /* global_csn_min should be without anything from the tracker */
        EXPECT_EQ(csnMgr->GetRecycleCsnMin(INVALID_PDB_ID), csnMgr->GetLocalCsnMin());
    }

    for (auto i = 0; i < NUM_THREADS; ++i) {
        pthread_barrier_destroy(&thread_barriers[i]);
    }

    pthread_barrier_destroy(&barrier1);
}

TEST_F(TacOrphanTrxTrackerTest, TrackerBoundaryParametersTest)
{
    /* The tracker should not accept any entries that is INVALID_CSN */
    g_storageInstance->GetCsnMgr()->AddCsnToTacTracker(INVALID_CSN);
    /* Therefore, the tracker should be empty */
    EXPECT_TRUE(binaryheap_empty(g_storageInstance->GetCsnMgr()->m_tacOrphanTrxTracker->m_csnHeap));
    /* An empty tracker would return INVALID_CSN */
    EXPECT_EQ(g_storageInstance->GetCsnMgr()->m_tacOrphanTrxTracker->GetSmallestOrphanCsn(), INVALID_CSN);

    /* We forcefully add an entry to the tracker that has earlier csn and expires later than other entries */
    TacOrphanTrxEntry* entry = (TacOrphanTrxEntry*)DstorePalloc0(sizeof(TacOrphanTrxEntry));
    const CommitSeqNo orphanCsnMin = 100;
    entry->csn = orphanCsnMin;
    /* This entry will expire after 10 seconds, we have tacGracePeriod set as one second */
    entry->expiryTimestamp = GetCurrentTimestampInSecond() + TAC_GRACE_PERIOD;
    BinaryheapAdd(g_storageInstance->GetCsnMgr()->m_tacOrphanTrxTracker->m_csnHeap, PointerGetDatum(entry));
    /* Check if this entry has been successfully added to the tracker */
    EXPECT_EQ(g_storageInstance->GetCsnMgr()->m_tacOrphanTrxTracker->GetSmallestOrphanCsn(), orphanCsnMin);

    /* In this case, the tracker should not accept this entry since the top entry covers this new entry */
    const CommitSeqNo newOrphanCsnMin = 150;
    g_storageInstance->GetCsnMgr()->AddCsnToTacTracker(newOrphanCsnMin);
    EXPECT_EQ(g_storageInstance->GetCsnMgr()->m_tacOrphanTrxTracker->m_csnHeap->bh_size, 1);
}

TEST_F(TacOrphanTrxTrackerTest, UpdateLocalCsnMinMultipleThreadsWithTrackerEntryTest)
{
    CommitSeqNo nextCsn = 100;
    CsnMgr *csnMgr = g_storageInstance->GetCsnMgr();
    GsAtomicFetchAddU64(&csnMgr->m_nextCsn, nextCsn);

    /* one csn for each thread */
    const std::vector<CommitSeqNo> csns = {8UL, 9UL, 10UL, INVALID_CSN};
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
        threads[i] = std::thread(ThreadFunc_UpdateLocalCsnMin, csns[i], &barrier1, &thread_barriers[i]);
    }

    /* wait on barrier1 to make sure all threads are done updating csns */
    pthread_barrier_wait(&barrier1);

    csnMgr->UpdateLocalCsnMin();
    /* global_csn_min should be the minimum of all local csn_mins*/
    EXPECT_EQ(csnMgr->GetRecycleCsnMin(INVALID_PDB_ID), *(std::min_element(csns.begin(),csns.end() - 1)));

    /* At this point, we are removing the thread with the smallest csn to test orphan tracker */
    pthread_barrier_wait(&thread_barriers[0]);
    threads[0].join();
    /* Let's say this thread dies abruptly, which we will add an entry to the tracker with the csn of this thread */
    g_storageInstance->GetCsnMgr()->AddCsnToTacTracker(csns[0]);
    /* Update LocalCsnMin again, this time the tracker csn should kick in */
    csnMgr->UpdateLocalCsnMin();
    /* global_csn_min should update to be the one in the tracker */
    EXPECT_EQ(csnMgr->GetRecycleCsnMin(INVALID_PDB_ID), csns[0]);
    
    /* remove each thread one by one (except for last thread) and recalculate global_csn_min */
    for (auto i = 1; i < NUM_THREADS; ++i) {
        /* unblock barrier */
        pthread_barrier_wait(&thread_barriers[i]);
        threads[i].join();
        /* at this point, thread i should be unregistered */
        csnMgr->UpdateLocalCsnMin();
        /* global_csn_min should still to be the one in the tracker */
        EXPECT_EQ(csnMgr->GetRecycleCsnMin(INVALID_PDB_ID), csns[0]);
    }

    for (auto i = 0; i < NUM_THREADS; ++i) {
        pthread_barrier_destroy(&thread_barriers[i]);
    }

    pthread_barrier_destroy(&barrier1);
}

TEST_F(TacOrphanTrxTrackerTest, UpdateLocalCsnMinMultipleThreadsWithFirstStatementCsnMinExpired)
{
    CommitSeqNo nextCsn = 100;
    CsnMgr *csnMgr = g_storageInstance->GetCsnMgr();
    GsAtomicFetchAddU64(&csnMgr->m_nextCsn, nextCsn);

    /* one csn for each thread */
    const std::vector<CommitSeqNo> csns = {8UL, 9UL, 10UL, INVALID_CSN};
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
        threads[i] = std::thread(ThreadFunc_UpdateLocalCsnMin, csns[i], &barrier1, &thread_barriers[i]);
    }

    /* wait on barrier1 to make sure all threads are done updating csns */
    pthread_barrier_wait(&barrier1);

    /* Wait for TAC grace period to force expiry of firstStatementCsnMin */
    std::this_thread::sleep_for(std::chrono::seconds(TAC_GRACE_PERIOD));
    
    csnMgr->UpdateLocalCsnMin();

    /* global_csn_min should be nextCsn */
    nextCsn = GsAtomicReadU64(&csnMgr->m_nextCsn);
    EXPECT_EQ(csnMgr->GetRecycleCsnMin(INVALID_PDB_ID), nextCsn);

    /* remove each thread one by one and destroy its barrier */
    for (auto i = 0; i < NUM_THREADS; ++i) {
        /* unblock barrier */
        pthread_barrier_wait(&thread_barriers[i]);
        threads[i].join();
        pthread_barrier_destroy(&thread_barriers[i]);
    }

    pthread_barrier_destroy(&barrier1);
}