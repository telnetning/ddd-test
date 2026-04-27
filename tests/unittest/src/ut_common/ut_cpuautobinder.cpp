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
#include "ut_utilities/ut_dstore_framework.h"
#include "ut_utilities/ut_thread_pool.h"

#include "common/dstore_datatype.h"
#include "framework/dstore_thread_cpu_autobinder.h"

const static int UT_CPUAUTOBINDER_LOOP_COUNT = 10;

static std::mutex g_mutex;
static uint32 g_registerCount = 0;

static std::mutex g_bindMutex;
static uint32 g_bindCount = 0;

static std::mutex g_highBindMutex;
static uint32 g_highBindCount = 0;

static std::mutex g_lowBindMutex;
static uint32 g_lowBindCount = 0;

static std::mutex g_numaCpuBindMutex;
static uint32 g_numaCpuBindCount = 0;
static bool g_willUpdateRes = false;
static pthread_barrier_t g_afterRigsterBarrier;
static pthread_barrier_t g_beforeUnRigsterBarrier;
static pthread_barrier_t g_beforeUpdateCpuResBarrier;
static pthread_barrier_t g_afterUpdateCpuResBarrier;

struct BindResult {
    uint32 registerCount;
    uint32 bindCount;
    uint32 lowBindCount;
    uint32 highBindCount;
    uint32 numaCpuBindCount;
};

static void CheckResult(BindResult *result)
{
    EXPECT_EQ(g_bindCount, result->bindCount);
    EXPECT_EQ(g_highBindCount, result->highBindCount);
    EXPECT_EQ(g_lowBindCount, result->lowBindCount);
    EXPECT_EQ(g_registerCount, result->registerCount);
    EXPECT_EQ(g_numaCpuBindCount, result->numaCpuBindCount);
}

static void InitBarrier(uint32 threadNum)
{
    pthread_barrier_init(&g_afterRigsterBarrier, nullptr, threadNum);
    pthread_barrier_init(&g_beforeUnRigsterBarrier, nullptr, threadNum);
    if (g_willUpdateRes) {
        pthread_barrier_init(&g_beforeUpdateCpuResBarrier, nullptr, threadNum + 1);
        pthread_barrier_init(&g_afterUpdateCpuResBarrier, nullptr, threadNum + 1);
    }
}

static void DestroyBarrier()
{
    pthread_barrier_destroy(&g_afterRigsterBarrier);
    pthread_barrier_destroy(&g_beforeUnRigsterBarrier);
    if (g_willUpdateRes) {
        pthread_barrier_destroy(&g_beforeUpdateCpuResBarrier);
        pthread_barrier_destroy(&g_afterUpdateCpuResBarrier);
    }
}

static void InitGlobalCounter()
{
    g_bindCount = 0;
    g_registerCount = 0;
    g_highBindCount = 0;
    g_lowBindCount = 0;
    g_numaCpuBindCount = 0;
}

static inline int GetSysConfCoreNum()
{
    int numProcessors = sysconf(_SC_NPROCESSORS_CONF);
    return numProcessors;
}

bool IsCpuBindSucc(uint16 cpuId, cpu_set_t *getCpuSet)
{
    cpu_set_t expectCpuset;
    CPU_ZERO(&expectCpuset);
    CPU_SET(cpuId, &expectCpuset);
    return CPU_EQUAL(getCpuSet, &expectCpuset);
}

void ThreadBindCounter(uint32 bindCpuId, BindType bindType, CoreBindLevel level = CoreBindLevel::HIGH)
{
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);

    if (bindCpuId != INVALID_CPU_CORE) {
        /* Check if the instance has been attch to some specific CPUs. */
        int ret = pthread_getaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
        if (ret == 0) {
            EXPECT_TRUE(IsCpuBindSucc(bindCpuId, &cpuset));
        }
        g_bindMutex.lock();
        g_bindCount++;
        g_bindMutex.unlock();
        if (bindType == BindType::CPU_BIND) {
            if (level == CoreBindLevel::HIGH) {
                g_highBindMutex.lock();
                g_highBindCount++;
                g_highBindMutex.unlock();
            } else if (level == CoreBindLevel::LOW) {
                g_lowBindMutex.lock();
                g_lowBindCount++;
                g_lowBindMutex.unlock();
            }
        } else if (bindType == BindType::NUMA_CPU_BIND) {
            g_numaCpuBindMutex.lock();
            g_numaCpuBindCount++;
            g_numaCpuBindMutex.unlock();
        }
    }
}

void RegisterThreadToBindFunc(ThreadCpuAutoBinder *thrdBinder, CoreBindLevel level = CoreBindLevel::HIGH)
{
    ThreadId myTid = pthread_self();
    EXPECT_EQ(thrdBinder->RegisterThreadToCpuBind(myTid, level), DSTORE_SUCC);
    EXPECT_TRUE(thrdBinder->IsThreadRegistered(myTid));
    g_mutex.lock();
    g_registerCount++;
    g_mutex.unlock();

    pthread_barrier_wait(&g_afterRigsterBarrier);

    /* Count the number of bound threads after all threads are registered. */
    uint16 bindCpuId = thrdBinder->GetThreadBindCore(myTid);
    ThreadBindCounter(bindCpuId, BindType::CPU_BIND, level);

    /* Count the number of bound threads after cpu res updated. */
    if (g_willUpdateRes) {
        pthread_barrier_wait(&g_beforeUpdateCpuResBarrier);
        /* updating cpu resource....(if necessary) */
        pthread_barrier_wait(&g_afterUpdateCpuResBarrier);

        bindCpuId = thrdBinder->GetThreadBindCore(myTid);
        ThreadBindCounter(bindCpuId, BindType::CPU_BIND, level);
    }

    pthread_barrier_wait(&g_beforeUnRigsterBarrier);
    EXPECT_EQ(thrdBinder->UnRegisterThreadToCpuBind(myTid), DSTORE_SUCC);
    EXPECT_FALSE(thrdBinder->IsThreadRegistered(myTid));
}

void UpdateThreadBinderRes(ThreadCpuAutoBinder *thrdBinder, NumaInfo *updateNumaInfo)
{
    g_bindCount = 0;
    g_highBindCount = 0;
    g_lowBindCount = 0;
    g_numaCpuBindCount = 0;
    thrdBinder->UpdateCpuRes(0, 0, updateNumaInfo->numaCount, updateNumaInfo->numaCpuInfos);
}

class UTCpuAutoBinder : public DSTORETEST {
protected:
    void SetUp() override
    {
        DSTORETEST::SetUp();
        g_willUpdateRes = false;
        m_highLevelThrdNum = 0;
        m_lowLevelThrdNum = 0;
        UtInstance *instance = DstoreNew(m_ut_memory_context)UtInstance();
        instance->Install(&DSTORETEST::m_guc, m_ut_memory_context);
        instance->Startup(&DSTORETEST::m_guc);
        /* More steps may be added according to future needs. */
    }

    void TearDown() override
    {
        UtInstance *instance = (UtInstance *)g_storageInstance;
        instance->Shutdown();
        delete instance;
        DSTORETEST::TearDown();
    }

    void SetThreadNum(uint32 highThrdNum, uint32 lowThrdNum = 0)
    {
        m_highLevelThrdNum = highThrdNum;
        m_lowLevelThrdNum = lowThrdNum;
    }

    void InitThreadBinder(ThreadCpuAutoBinder *thrdBinder, NumaInfo *initNumaInfo)
    {
        thrdBinder->Initialize(true, initNumaInfo->numaCount, initNumaInfo->numaCpuInfos);
    }

    void InitThreadPool(UTThreadPool *threadPool, ThreadCpuAutoBinder *thrdBinder)
    {
        threadPool->Start(m_highLevelThrdNum + m_lowLevelThrdNum);

        for (uint32 i = 0; i < m_lowLevelThrdNum; i++) {
            threadPool->AddTask(RegisterThreadToBindFunc, thrdBinder, CoreBindLevel::LOW);
        }

        for (uint32 i = 0; i < m_highLevelThrdNum; i++) {
            threadPool->AddTask(RegisterThreadToBindFunc, thrdBinder, CoreBindLevel::HIGH);
        }
    }

    void AutoBinderProc(NumaInfo *initNumaInfo)
    {
        InitBarrier(m_highLevelThrdNum + m_lowLevelThrdNum);

        ThreadCpuAutoBinder thrdBinder;
        InitThreadBinder(&thrdBinder, initNumaInfo);

        UTThreadPool threadPool;
        InitThreadPool(&threadPool, &thrdBinder);

        threadPool.WaitAllTaskFinish();
        threadPool.Shutdown();

        EXPECT_EQ(thrdBinder.GetThreadNum(), 0);
        thrdBinder.Destroy();
        DestroyBarrier();
    }

    void UpdateCpuResProc(NumaInfo *initNumaInfo, NumaInfo *updateNumaInfo, BindResult *beforeUpdateResult)
    {
        InitBarrier(m_highLevelThrdNum + m_lowLevelThrdNum);

        ThreadCpuAutoBinder thrdBinder;
        InitThreadBinder(&thrdBinder, initNumaInfo);

        UTThreadPool threadPool;
        InitThreadPool(&threadPool, &thrdBinder);

        pthread_barrier_wait(&g_beforeUpdateCpuResBarrier);
        CheckResult(beforeUpdateResult);
        UpdateThreadBinderRes(&thrdBinder, updateNumaInfo);
        pthread_barrier_wait(&g_afterUpdateCpuResBarrier);

        threadPool.WaitAllTaskFinish();
        threadPool.Shutdown();
    
        EXPECT_EQ(thrdBinder.GetThreadNum(), 0);
        thrdBinder.Destroy();
        DestroyBarrier();
    }

    uint32 m_highLevelThrdNum;
    uint32 m_lowLevelThrdNum;
};


static void ThreadBindtoNumaCpu(ThreadCpuAutoBinder *thrdBinder, char *name)
{
    ThreadId myTid = pthread_self();
    EXPECT_EQ(thrdBinder->RegisterThreadToNumaCpuBind(myTid, name), DSTORE_SUCC);

    g_mutex.lock();
    g_registerCount++;
    g_mutex.unlock();

    pthread_barrier_wait(&g_afterRigsterBarrier);

    /* Count the number of bound threads after all threads are registered. */
    uint16 bindCpuId = thrdBinder->GetThreadBindCore(myTid, name);
    ThreadBindCounter(bindCpuId, BindType::NUMA_CPU_BIND);

    /* Count the number of bound threads after cpu res updated. */
    if (g_willUpdateRes) {
        pthread_barrier_wait(&g_beforeUpdateCpuResBarrier);
        /* updating cpu resource....(if necessary) */
        pthread_barrier_wait(&g_afterUpdateCpuResBarrier);

        bindCpuId = thrdBinder->GetThreadBindCore(myTid, name);
        ThreadBindCounter(bindCpuId, BindType::NUMA_CPU_BIND);
    }

    /* Count the number of bound threads after cpu res updated. */

    pthread_barrier_wait(&g_beforeUnRigsterBarrier);
    EXPECT_EQ(thrdBinder->UnRegisterThreadToNumaCpuBind(myTid, name), DSTORE_SUCC);
}

TEST_F(UTCpuAutoBinder, GetAllThreadInfoTest)
{
    size_t length = 0;
    DSTORE::ThreadStatsInfo *threadInfoArr = nullptr;
    auto ret = DSTORE::ThreadContextInterface::GetAllThreadsInfo(&threadInfoArr, &length);
    EXPECT_GT(length, 0);
    EXPECT_NE(threadInfoArr, nullptr);
    EXPECT_EQ(ret, DSTORE_SUCC);
    
    for (int i = 0; i < length && i < 10; i++) {
        ThreadStatsInfo cur_info = threadInfoArr[i];
        std::cout << "pid: " << cur_info.pid << " | " <<
            "startTime: " << cur_info.startTime << " | " <<
            "threadName: " << cur_info.threadName << " | " <<
            "lwpid: " << cur_info.lwpid << std::endl;
    }
    DSTORE::ThreadContextInterface::FreeThreadInfoArr(&threadInfoArr, length);
    EXPECT_EQ(threadInfoArr, nullptr);
}

TEST_F(UTCpuAutoBinder, TestThreadRegisterWhenCpuResSufficient)
{
    /* numa resouce : 1 numanode {5 cpu cores: 0~4}
     * thread num   : 3 HIGH
     * expect bind result : 3 HIGH
     */
    if (GetSysConfCoreNum() < 5) {
        EXPECT_EQ(true, true);
        return;
    }
    NumaInfo initNumaInfo;
    initNumaInfo.numaCount = 1;
    NumaCpuInfo numaCpuInfos[1] = {{0, "0-4"}};
    initNumaInfo.numaCpuInfos = &numaCpuInfos[0];
    uint32 highLevelThrdNum = 3;
    SetThreadNum(highLevelThrdNum);

    BindResult expectResult = {.registerCount = 3,
                               .bindCount     = 3,
                               .lowBindCount  = 0,
                               .highBindCount = 3,
                               .numaCpuBindCount = 0};

    for (uint32 i = 0; i < UT_CPUAUTOBINDER_LOOP_COUNT; i++) {
        InitGlobalCounter();
        AutoBinderProc(&initNumaInfo);

        CheckResult(&expectResult);
    }
}

TEST_F(UTCpuAutoBinder, TestThreadRegisterWhenCpuResInsufficient)
{
    /* numa resouce : 1 numanode {3 cpu cores: 0~2}
     * thread num   : 5 HIGH
     * expect bind result : 3 HIGH
     */
    if (GetSysConfCoreNum() < 3) {
        EXPECT_EQ(true, true);
        return;
    }
    NumaInfo initNumaInfo;
    initNumaInfo.numaCount = 1;
    NumaCpuInfo numaCpuInfos[1] = {{0, "0-2"}};
    initNumaInfo.numaCpuInfos = &numaCpuInfos[0];

    uint32 highLevelThrdNum = 5;
    SetThreadNum(highLevelThrdNum);

    BindResult expectResult = {.registerCount = 5,
                               .bindCount     = 3,
                               .lowBindCount  = 0,
                               .highBindCount = 3,
                               .numaCpuBindCount = 0};

    for (uint32 i = 0; i < UT_CPUAUTOBINDER_LOOP_COUNT; i++) {
        InitGlobalCounter();
        AutoBinderProc(&initNumaInfo);

        CheckResult(&expectResult);
    }
}

TEST_F(UTCpuAutoBinder, TestDefferentLevelThreadRegister)
{
    /* numa resouce       : 1 numanode {5 cpu cores: 0~4}
     * thread num         : 4 HIGH, 3 LOW
     * expect bind result : 2 HIGH, 1 LOW
     */
    if (GetSysConfCoreNum() < 5) {
        EXPECT_EQ(true, true);
        return;
    }
    NumaInfo initNumaInfo;
    initNumaInfo.numaCount = 1;
    NumaCpuInfo numaCpuInfos[1] = {{0, "0-4"}};
    initNumaInfo.numaCpuInfos = &numaCpuInfos[0];
    uint32 highLevelThrdNum = 4;
    uint32 lowLevelThrdNum = 3;
    SetThreadNum(highLevelThrdNum, lowLevelThrdNum);

    BindResult expectResult = {.registerCount = 7,
                               .bindCount     = 5,
                               .lowBindCount  = 1,
                               .highBindCount = 4,
                               .numaCpuBindCount = 0};

    for (uint32 i = 0; i < UT_CPUAUTOBINDER_LOOP_COUNT; i++) {
        InitGlobalCounter();
        AutoBinderProc(&initNumaInfo);

        CheckResult(&expectResult);
    }
}

TEST_F(UTCpuAutoBinder, TestUpdateCpuRes)
{
    /* numa resouce       : 1 numanode {5 cpu cores: 0~4}
     * thread num         : 4 HIGH, 3 LOW
     * expect bind result : 4 HIGH, 1 LOW
     */
    if (GetSysConfCoreNum() < 7) {
        EXPECT_EQ(true, true);
        return;
    }
    uint32 highLevelThrdNum = 4;
    uint32 lowLevelThrdNum = 3;
    SetThreadNum(highLevelThrdNum, lowLevelThrdNum);
    g_willUpdateRes = true;

    NumaInfo initNumaInfo;
    initNumaInfo.numaCount = 1;
    NumaCpuInfo numaCpuInfos[1] = {{0, "0-4"}};
    initNumaInfo.numaCpuInfos = &numaCpuInfos[0];
    BindResult beforeUpdatResult = {.registerCount = 7,
                                    .bindCount     = 5,
                                    .lowBindCount  = 1,
                                    .highBindCount = 4,
                                    .numaCpuBindCount = 0};
    /* update numa resouce: 1 numanode {7 cpu cores: 0~6}
     * expect bind result : 4 HIGH, 3 LOW
     */
    NumaInfo updateNumaInfo;
    updateNumaInfo.numaCount = 1;
    NumaCpuInfo updateNumaCpuInfos[1] = {{0, "0-6"}};
    updateNumaInfo.numaCpuInfos = &updateNumaCpuInfos[0];
    BindResult expectResult = {.registerCount = 7,
                               .bindCount     = 7,
                               .lowBindCount  = 3,
                               .highBindCount = 4,
                               .numaCpuBindCount = 0};

    for (uint32 i = 0; i < UT_CPUAUTOBINDER_LOOP_COUNT; i++) {
        InitGlobalCounter();
        UpdateCpuResProc(&initNumaInfo, &updateNumaInfo, &beforeUpdatResult);

        CheckResult(&expectResult);
    }
}

TEST_F(UTCpuAutoBinder, TestThreadGroupBind)
{
    if (GetSysConfCoreNum() < 13) {
        EXPECT_EQ(true, true);
        return;
    }
    /* numa resouce       : 2 numanode {6 cpu cores: 0~4,9; 6 cpu cores: 5~7,10~12}
     * thread num         : 1 HIGH, 2 LOW
     * thread group1      : 3threads
     * thread group2      : 3threads
     * expect bind result : 2 HIGH, 1 LOW, 6 NUMA_CPU
     */
    NumaInfo initNumaInfo;
    initNumaInfo.numaCount = 2;
    NumaCpuInfo numaCpuInfos[2] = {{0, "0-4,9"}, {1, "5-7, 10-12"}};
    initNumaInfo.numaCpuInfos = &numaCpuInfos[0];
    uint32 highLevelThrdNum = 1;
    uint32 lowLevelThrdNum = 2;
    uint32 thrdCntInGroup1 = 3;
    uint32 thrdCntInGroup2 = 3;
    SetThreadNum(highLevelThrdNum, lowLevelThrdNum);
    InitGlobalCounter();
    BindResult expectResult = {.registerCount = 9,
                               .bindCount     = 9,
                               .lowBindCount  = 2,
                               .highBindCount = 1,
                               .numaCpuBindCount = 6};
    ThreadCpuAutoBinder thrdBinder;
    InitThreadBinder(&thrdBinder, &initNumaInfo);
    InitBarrier(thrdCntInGroup1 + thrdCntInGroup2 + highLevelThrdNum + lowLevelThrdNum);

    /* init group1 */
    UTThreadPool threadGroup1;
    threadGroup1.Start(thrdCntInGroup1);
    char name1[128] = "group1";
    for (uint32 i = 0; i < thrdCntInGroup1; i++) {
        threadGroup1.AddTask(ThreadBindtoNumaCpu, &thrdBinder, name1);
    }

    /* init group2 */
    UTThreadPool threadGroup2;
    threadGroup2.Start(thrdCntInGroup2);
    char name2[128] = "group2";
    for (uint32 i = 0; i < thrdCntInGroup2; i++) {
        threadGroup2.AddTask(ThreadBindtoNumaCpu, &thrdBinder, name2);
    }

    /* init threadpool */
    UTThreadPool threadPool;
    InitThreadPool(&threadPool, &thrdBinder);

    threadGroup1.WaitAllTaskFinish();
    threadGroup1.Shutdown();
    threadGroup2.WaitAllTaskFinish();
    threadGroup2.Shutdown();
    threadPool.WaitAllTaskFinish();
    threadPool.Shutdown();
    thrdBinder.Destroy();
    DestroyBarrier();
    CheckResult(&expectResult);
}

TEST_F(UTCpuAutoBinder, TestThreadGroupBindWhenUpdateCpuRes)
{
    if (GetSysConfCoreNum() < 10) {
        EXPECT_EQ(true, true);
        return;
    }
    /* numa resouce       : 2 numanode {5 cpu cores: 0~4; 5 cpu cores: 5~9}
     * thread num         : 1 HIGH, 2 LOW
     * thread group1      : 3threads
     * thread group2      : 3threads
     * expect bind result : 2 HIGH, 1 LOW, 6 NUMA_CPU
     */
    NumaInfo initNumaInfo;
    initNumaInfo.numaCount = 2;
    NumaCpuInfo numaCpuInfos[2] = {{0, "0-4"}, {1, "5-9"}};
    initNumaInfo.numaCpuInfos = &numaCpuInfos[0];
    g_willUpdateRes = true;
    uint32 highLevelThrdNum = 1;
    uint32 lowLevelThrdNum = 1;
    uint32 thrdCntInGroup1 = 3;
    uint32 thrdCntInGroup2 = 3;
    SetThreadNum(highLevelThrdNum, lowLevelThrdNum);
    InitGlobalCounter();
    BindResult expectResult = {.registerCount = 8,
                               .bindCount     = 8,
                               .lowBindCount  = 1,
                               .highBindCount = 1,
                               .numaCpuBindCount = 6};
    ThreadCpuAutoBinder thrdBinder;
    InitThreadBinder(&thrdBinder, &initNumaInfo);
    InitBarrier(thrdCntInGroup1 + thrdCntInGroup2 + highLevelThrdNum + lowLevelThrdNum);

    /* init group1 */
    UTThreadPool threadGroup1;
    threadGroup1.Start(thrdCntInGroup1);
    char name1[128] = "group1";
    for (uint32 i = 0; i < thrdCntInGroup1; i++) {
        threadGroup1.AddTask(ThreadBindtoNumaCpu, &thrdBinder, name1);
    }

    /* init group2 */
    UTThreadPool threadGroup2;
    threadGroup2.Start(thrdCntInGroup2);
    char name2[128] = "group2";
    for (uint32 i = 0; i < thrdCntInGroup2; i++) {
        threadGroup2.AddTask(ThreadBindtoNumaCpu, &thrdBinder, name2);
    }

    /* init threadpool */
    UTThreadPool threadPool;
    InitThreadPool(&threadPool, &thrdBinder);

    NumaInfo updateNumaInfo;
    updateNumaInfo.numaCount = 1;
    NumaCpuInfo updateNumaCpuInfos[1] = {{0, "0-4"}};
    updateNumaInfo.numaCpuInfos = &updateNumaCpuInfos[0];
    pthread_barrier_wait(&g_beforeUpdateCpuResBarrier);
    CheckResult(&expectResult);
    UpdateThreadBinderRes(&thrdBinder, &updateNumaInfo);
    pthread_barrier_wait(&g_afterUpdateCpuResBarrier);

    BindResult afterUpdateResult = {.registerCount = 8,
                                    .bindCount     = 5,
                                    .lowBindCount  = 1,
                                    .highBindCount = 1,
                                    .numaCpuBindCount = 3};
    threadGroup1.WaitAllTaskFinish();
    threadGroup1.Shutdown();
    threadGroup2.WaitAllTaskFinish();
    threadGroup2.Shutdown();
    threadPool.WaitAllTaskFinish();
    threadPool.Shutdown();
    thrdBinder.Destroy();
    DestroyBarrier();
    CheckResult(&afterUpdateResult);
}

