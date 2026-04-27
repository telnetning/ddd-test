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
 * ---------------------------------------------------------------------------------
 *
 * ut_event_loop.cpp
 * unit tests of event-loop
 *
 * ---------------------------------------------------------------------------------
 */

#if !defined(WINDOWS_PLATFORM)
#include <unistd.h>
#endif

#include <gtest/gtest.h>
#include <thread>
#include <iostream>
#include <fstream>
#include <unistd.h>
#include <sys/syscall.h>
#include "event/loop_base.h"

constexpr uint32_t JIFFY_TIME_USEC = TIMER_WHEEL_JIFFY * USEC_PER_MSEC;

using namespace std;
class EventLoopTest : public testing::Test {
public:
    static void SetUpTestSuite()
    {
        ErrorCode errorCode = StartLogger();
        EXPECT_EQ(errorCode, ERROR_SYS_OK);
        errorCode = OpenLogger();
        EXPECT_EQ(errorCode, ERROR_SYS_OK);
    }

    static void TearDownTestSuite()
    {
        usleep(100);
        /* default init  */
        CloseLogger();
        StopLogger();
    }

    void SetUp() override
    {
        m_loop = NULL;
        GSDB_ATOMIC32_SET(&m_threadRunning, 0);
        m_runLoopExitedErrCode = ERROR_SYS_OK;
        GSDB_ATOMIC32_SET(&m_args1, 0);
        GSDB_ATOMIC32_SET(&m_args2, 0);
        m_evObj1 = NULL;
        return;
    };

    void TearDown() override
    {
        ASSERT_TRUE(m_loop == NULL);
        ASSERT_TRUE(m_loopExited);
        ResetTotalEventObjectCount();
        ResetTotalLoopCount();
    };

    static void *LoopThread(void *arg)
    {
        EventLoopTest *self = (EventLoopTest *)arg;
        GSDB_ATOMIC32_SET(&self->m_threadRunning, 1);
        self->osTid = syscall(SYS_gettid);
        self->m_runLoopExitedErrCode = RunEventLoop(self->m_loop);
        self->m_loopExited = true;
        CloseLogger();
        return NULL;
    }

    void CreateLoop()
    {
        m_loop = CreateEventLoop(NULL);
        ASSERT_TRUE(m_loop != NULL);
    }

    void RunLoopInOtherThread()
    {
        GSDB_ATOMIC32_SET(&m_threadRunning, 0);
        m_loopExited = false;
        ErrorCode errCode = ThreadCreate(&m_threadId, LoopThread, this);
        ASSERT_TRUE(errCode == ERROR_SYS_OK);
        while (GSDB_ATOMIC32_GET(&m_threadRunning) != 1) {
            Usleep(1000);
        }
    }

    void RunLoopDirectly()
    {
        ErrorCode errCode = RunEventLoop(m_loop);
        ASSERT_TRUE(errCode == ERROR_SYS_OK);
    }

    void QuiteLoopInOtherThread()
    {
       ErrorCode errCode = QuitEventLoop(m_loop);
       ASSERT_TRUE(errCode == ERROR_SYS_OK);
       ThreadJoin(m_threadId, NULL);
       ASSERT_TRUE(m_loopExited);
       ASSERT_TRUE(m_runLoopExitedErrCode == ERROR_SYS_OK);
    }

    void DestroyLoopInOtherThread()
    {
       DestroyEventLoop(m_loop);
       m_loop = NULL;
       ThreadJoin(m_threadId, NULL);
       ASSERT_TRUE(m_loopExited);
       ASSERT_TRUE(m_runLoopExitedErrCode == ERROR_SYS_OK);
    }

    void DestroyLoop()
    {
        DestroyEventLoop(m_loop);
        m_loop = NULL;
    }

    EventLoop *GetLoopPointer()
    {
        ASSERT(m_loop != NULL);
        return m_loop;
    }

    static uint64_t UtGetCurrentTimeInMs()
    {
        TimeValue curTv = GetCurrentTimeValue();
        return (uint64_t)curTv.seconds * 1000 + curTv.useconds / 1000;
    }

/* Data Section */
    EventLoop *m_loop = NULL;
    Tid m_threadId;
    bool m_loopExited = true;
    Atomic32 m_threadRunning = 0;
    ErrorCode m_runLoopExitedErrCode = ERROR_SYS_OK;
    Atomic32 m_args1 = 0;
    Atomic32 m_args2 = 0;
    Event *m_evObj1 = NULL;
    pid_t osTid = -1;
};

/**
 * @tc.name: CreateLoopObjectTest001_Level0
 * @tc.desc: Test whether a LOOP object can be created correctly.
 * @tc.type: FUNC
 */
TEST_F(EventLoopTest, CreateLoopObjectTest001_Level0)
{
    ErrorCode errCode;
    EventLoop *loop = NULL;

    loop = CreateEventLoop(NULL);
    ASSERT_TRUE(loop != NULL);
    ASSERT_EQ(GetTotalLoopCount(), 1);
    /*Destroy the LOOP without QuiteLoop() invoked.*/
    DestroyEventLoop(loop);
    ASSERT_EQ(GetTotalLoopCount(), 0);

    loop = CreateEventLoop(&errCode);
    ASSERT_TRUE(loop != NULL);
    ASSERT_TRUE(errCode == ERROR_SYS_OK);
    ASSERT_EQ(GetTotalLoopCount(), 1);
    /*Destroy the LOOP, QuiteLoop() is not necessary.*/
    errCode = QuitEventLoop(loop);
    DestroyEventLoop(loop);
    ASSERT_EQ(GetTotalLoopCount(), 0);
}

/**
 * @tc.name: CreateEventObjectTest001_Level0
 * @tc.desc: Test whether an event object can be created and destroyed correctly.
 * @tc.type: FUNC
 */
TEST_F(EventLoopTest, CreateEventObjectTest001_Level0)
{
    Event *evObj1, *evObj2 = NULL;
    EventLoop *loop = NULL;

    loop = CreateEventLoop(NULL);
    ASSERT_TRUE(loop != NULL);

    evObj1 = CreateEvent(loop, 1, EVENT_WRITE, 0, NULL);
    ASSERT_TRUE(evObj1 != NULL);

    evObj2 = CreateTimerEvent(loop, 1, NULL);
    ASSERT_TRUE(evObj2 != NULL);

    ASSERT_EQ(GetTotalLoopCount(), 1);
    ASSERT_EQ(GetTotalEventObjectCount(), 2);

    DestroyEvent(evObj1);
    ASSERT_EQ(GetTotalLoopCount(), 1);
    ASSERT_EQ(GetTotalEventObjectCount(), 1);

    DestroyEventLoop(loop);
    /* LOOP is refed by evObj2 */
    ASSERT_EQ(GetTotalLoopCount(), 1);
    ASSERT_EQ(GetTotalEventObjectCount(), 1);

    DestroyEvent(evObj2);
    ASSERT_EQ(GetTotalLoopCount(), 0);
    ASSERT_EQ(GetTotalEventObjectCount(), 0);
}

/**
 * @tc.name: FreeLoopCpuUsageTest001_Level0
 * @tc.desc: Run the loop, and test it's cpu usage when it is free status(no any time or event)
 * @tc.type: FUNC
 */
TEST_F(EventLoopTest, FreeLoopCpuUsageTest001_Level0)
{
    CreateLoop();
    RunLoopInOtherThread();

#define TOTAL_TIME (100 * 1000)  // 100ms
    usleep(TOTAL_TIME);
    ASSERT_NE(osTid, -1);

    /* count loop thread cpu usage */
    ifstream istrm(string("/proc/") + to_string(getpid()) + "/task/" + to_string(osTid) + "/schedstat");
    time_t runTime = 0;
    time_t waitTime = 0;;
    istrm >> runTime >> waitTime;
    EXPECT_NE(runTime, 0);
    /* cpu usage = (runTime+waitTime)(ns)/(total)(ms), and sure less then 8%, because total time less then TOTAL_TIME
     * a little, so the result usage < 1% */
    ASSERT_LT((double)(runTime + waitTime) / 1000 / TOTAL_TIME  * 100, 1);

    QuiteLoopInOtherThread();
    DestroyLoop();
    ASSERT_EQ(GetTotalLoopCount(), 0);
}

/**
 * @tc.name: LoopRunningTest001_Level0
 * @tc.desc: Run the loop test and add the timer before starting the thread.
 * @tc.type: FUNC
 */
static EventCbType OnTimerRun001(Event *self, unsigned eventType, void *context)
{
    EventLoopTest *testCase = (EventLoopTest*)context;
    ASSERT(eventType == EVENT_TIMEOUT);

    GSDB_ATOMIC32_ADD(&testCase->m_args1, 1);
    return EV_CB_CONTINUE;
}

static void OnTimerFinalize001(Event *self, void *context)
{
    EventLoopTest *testCase = (EventLoopTest*)context;
    GSDB_ATOMIC32_ADD(&testCase->m_args2, 1);
}

TEST_F(EventLoopTest, LoopRunningTest001_Level0)
{
    CreateLoop();
    Event *evObj = CreateTimerEvent(GetLoopPointer(), TIMER_WHEEL_JIFFY, NULL);
    ASSERT_TRUE(evObj != NULL);

    SetEventCallbacks(evObj, OnTimerRun001, OnTimerFinalize001, this);
    ErrorCode errCode = AddEventToLoop(evObj);
    ASSERT_EQ(errCode, ERROR_SYS_OK);

    RunLoopInOtherThread();

    /* Expected: timer removed by 'return EV_CB_EXIT' */
    while (GSDB_ATOMIC32_GET(&m_args1) == 0) {
        Usleep(JIFFY_TIME_USEC);
    }

    DestroyEvent(evObj);
    while (GetTotalEventObjectCount() != 0) {
        Usleep(JIFFY_TIME_USEC);
    }

    ASSERT_EQ(GSDB_ATOMIC32_GET(&m_args2), 1);
    ASSERT_EQ(GetTotalLoopCount(), 1);

    QuiteLoopInOtherThread();
    DestroyLoop();
    ASSERT_EQ(GetTotalLoopCount(), 0);

    /* Create event loop and add events, quit event loop without running */
    CreateLoop();
    evObj = CreateTimerEvent(GetLoopPointer(), TIMER_WHEEL_JIFFY, NULL);
    ASSERT_TRUE(evObj != NULL);
    SetEventCallbacks(evObj, OnTimerRun001, OnTimerFinalize001, this);
    ASSERT_EQ(AddEventToLoop(evObj), ERROR_SYS_OK);
    DestroyLoop();
    DestroyEvent(evObj);
    ASSERT_EQ(GSDB_ATOMIC32_GET(&m_args2), 2);
}

/**
 * @tc.name: LoopRunningTest002_Level0
 * @tc.desc: Loop running test. Add and delete the timer after the thread is started.
 * @tc.type: FUNC
 */
static EventCbType OnTimerRun002(Event *self, unsigned eventType, void *context)
{
    EventLoopTest *testCase = (EventLoopTest*)context;
    ASSERT(eventType == EVENT_TIMEOUT);

    GSDB_ATOMIC32_ADD(&testCase->m_args1, 1);
    return EV_CB_CONTINUE;
}

static void OnTimerFinalize002(Event *self, void *context)
{
    EventLoopTest *testCase = (EventLoopTest*)context;
    GSDB_ATOMIC32_ADD(&testCase->m_args2, 1);
}

TEST_F(EventLoopTest, LoopRunningTest002_Level0)
{
    CreateLoop();
    RunLoopInOtherThread();

    Event *evObj = CreateTimerEvent(GetLoopPointer(), TIMER_WHEEL_JIFFY, NULL);
    ASSERT_TRUE(evObj != NULL);
    SetEventCallbacks(evObj, OnTimerRun002, OnTimerFinalize002, this);

    ErrorCode errCode = AddEventToLoop(evObj);
    ASSERT_EQ(errCode, ERROR_SYS_OK);

    /* Expected: timer removed by 'return EV_CB_EXIT' */
    while (GSDB_ATOMIC32_GET(&m_args1) == 0) {
        Usleep(JIFFY_TIME_USEC);
    }

    RemoveEventFromLoop(evObj);
    DestroyEvent(evObj);

    while (GetTotalEventObjectCount() != 0) {
        Usleep(JIFFY_TIME_USEC);
    }

    ASSERT_EQ(GSDB_ATOMIC32_GET(&m_args2), 1);
    ASSERT_EQ(GetTotalLoopCount(), 1);

    DestroyLoopInOtherThread();
    ASSERT_EQ(GetTotalEventObjectCount(), 0);
    ASSERT_EQ(GetTotalLoopCount(), 0);
}

/**
 * @tc.name: LoopRunningTest003_Level0
 * @tc.desc: Test whether a timer can be automatically removed itself.
 * @tc.type: FUNC
 */
static EventCbType OnTimerRun003(Event *self, unsigned eventType, void *context)
{
    EventLoopTest *testCase = (EventLoopTest*)context;
    ASSERT(eventType == EVENT_TIMEOUT);

    GSDB_ATOMIC32_ADD(&testCase->m_args1, 1);
    return EV_CB_EXIT;
}

static void OnTimerFinalize003(Event *self, void *context)
{
    EventLoopTest *testCase = (EventLoopTest*)context;
    GSDB_ATOMIC32_ADD(&testCase->m_args2, 1);
}

TEST_F(EventLoopTest, LoopRunningTest003_Level0)
{
    CreateLoop();
    RunLoopInOtherThread();

    Event *evObj = CreateTimerEvent(GetLoopPointer(), TIMER_WHEEL_JIFFY, NULL);
    ASSERT_TRUE(evObj != NULL);
    SetEventCallbacks(evObj, OnTimerRun003, OnTimerFinalize003, this);

    ErrorCode errCode = AddEventToLoop(evObj);
    ASSERT_EQ(errCode, ERROR_SYS_OK);

    /* Expected: timer removed by 'return EV_CB_EXIT' */
    while (GSDB_ATOMIC32_GET(&m_args1) == 0) {
        Usleep(JIFFY_TIME_USEC);
    }

    Usleep(3 * JIFFY_TIME_USEC);
    ASSERT_EQ(GSDB_ATOMIC32_GET(&m_args1), 1);

    DestroyEvent(evObj);
    DestroyLoopInOtherThread();
    ASSERT_EQ(GSDB_ATOMIC32_GET(&m_args2), 1);
    ASSERT_EQ(GetTotalEventObjectCount(), 0);
    ASSERT_EQ(GetTotalLoopCount(), 0);
}

/**
 * @tc.name: LoopRunningTest004_Level0
 * @tc.desc: Test whether a timer' timeout value can be change during loop running.
 * @tc.type: FUNC
 */
static EventCbType OnTimerRun004(Event *self, unsigned eventType, void *context)
{
    EventLoopTest *testCase = (EventLoopTest*)context;
    ASSERT(eventType == EVENT_TIMEOUT);

    GSDB_ATOMIC32_ADD(&testCase->m_args1, 1);
    return EV_CB_EXIT;
}

static void OnTimerFinalize004(Event *self, void *context)
{
    EventLoopTest *testCase = (EventLoopTest*)context;
    GSDB_ATOMIC32_ADD(&testCase->m_args2, 1);
}

TEST_F(EventLoopTest, LoopRunningTest004_Level0)
{
    CreateLoop();
    RunLoopInOtherThread();

    Event *evObj = CreateTimerEvent(GetLoopPointer(), 0, NULL);
    ASSERT_TRUE(evObj != NULL);
    SetEventCallbacks(evObj, OnTimerRun004, OnTimerFinalize004, this);

    ErrorCode errCode = AddEventToLoop(evObj);
    ASSERT_EQ(errCode, ERROR_SYS_OK);

    int waitRetry = 3;
    bool timerIsRunning = false;
    while (--waitRetry >= 0) {
        Usleep(JIFFY_TIME_USEC);
        if (GSDB_ATOMIC32_GET(&m_args1) == 0) {
            continue;
        }
        timerIsRunning = true;
        break;
    }

    ASSERT_TRUE(!timerIsRunning);
    ASSERT_EQ(GSDB_ATOMIC32_GET(&m_args1), 0);

    ResetEventTimeout(evObj, TIMER_WHEEL_JIFFY);
    waitRetry = 10;
    timerIsRunning = false;
    while (--waitRetry >= 0) {
        Usleep(JIFFY_TIME_USEC);
        if (GSDB_ATOMIC32_GET(&m_args1) == 0) {
            continue;
        }
        timerIsRunning = true;
        break;
    }

    ASSERT_TRUE(timerIsRunning);
    ASSERT_TRUE(GSDB_ATOMIC32_GET(&m_args1) > 0);

    DestroyEvent(evObj);
    DestroyLoopInOtherThread();
    ASSERT_EQ(GSDB_ATOMIC32_GET(&m_args2), 1);
    ASSERT_EQ(GetTotalEventObjectCount(), 0);
    ASSERT_EQ(GetTotalLoopCount(), 0);
}

/**
 * @tc.name: LoopRunningTest005_Level0
 * @tc.desc: Test of adding a timer multiple times during loop running.
 * @tc.type: FUNC
 */
static EventCbType OnTimerRun005(Event *self, unsigned eventType, void *context)
{
    EventLoopTest *testCase = (EventLoopTest*)context;
    ASSERT(eventType == EVENT_TIMEOUT);
    GSDB_ATOMIC32_ADD(&testCase->m_args1, 1);
    return EV_CB_CONTINUE;
}

static void OnTimerFinalize005(Event *self, void *context)
{
    EventLoopTest *testCase = (EventLoopTest*)context;
    GSDB_ATOMIC32_ADD(&testCase->m_args2, 1);
}

TEST_F(EventLoopTest, LoopRunningTest005_Level0)
{
    CreateLoop();
    RunLoopInOtherThread();

    Event *evObj = CreateTimerEvent(GetLoopPointer(), TIMER_WHEEL_JIFFY, NULL);
    SetEventCallbacks(evObj, OnTimerRun005, OnTimerFinalize005, this);
    ErrorCode errCode = AddEventToLoop(evObj);
    ASSERT_EQ(errCode, ERROR_SYS_OK);

    errCode = AddEventToLoop(evObj);
    ASSERT_EQ(errCode, ERROR_SYS_OK);   /* will output warning message */

    errCode = AddEventToLoop(evObj);
    ASSERT_EQ(errCode, ERROR_SYS_OK);

    while (GSDB_ATOMIC32_GET(&m_args1) == 0) {
        Usleep(JIFFY_TIME_USEC);
    }

    errCode = RemoveEventFromLoop(evObj);
    ASSERT_EQ(errCode, ERROR_SYS_OK);
    DestroyEvent(evObj);

    DestroyLoopInOtherThread();
    ASSERT_EQ(GetTotalEventObjectCount(), 0);
    ASSERT_EQ(GetTotalLoopCount(), 0);
}

/**
 * @tc.name: LoopRunningTest006_Level0
 * @tc.desc: Test of removing a timer multiple times during loop running.
 * @tc.type: FUNC
 */
static EventCbType OnTimerRun006(Event *self, unsigned eventType, void *context)
{
    EventLoopTest *testCase = (EventLoopTest*)context;
    ASSERT(eventType == EVENT_TIMEOUT);
    GSDB_ATOMIC32_ADD(&testCase->m_args1, 1);
    return EV_CB_CONTINUE;
}

static void OnTimerFinalize006(Event *self, void *context)
{
    EventLoopTest *testCase = (EventLoopTest*)context;
    GSDB_ATOMIC32_ADD(&testCase->m_args2, 1);
}

TEST_F(EventLoopTest, LoopRunningTest006_Level0)
{
    CreateLoop();
    RunLoopInOtherThread();

    Event *evObj = CreateTimerEvent(GetLoopPointer(), TIMER_WHEEL_JIFFY, NULL);
    SetEventCallbacks(evObj, OnTimerRun006, OnTimerFinalize006, this);
    ErrorCode errCode = AddEventToLoop(evObj);
    ASSERT_EQ(errCode, ERROR_SYS_OK);

    while (GSDB_ATOMIC32_GET(&m_args1) == 0) {
        Usleep(1000);
    }

    errCode = RemoveEventFromLoop(evObj);
    ASSERT_EQ(errCode, ERROR_SYS_OK);

    errCode = RemoveEventFromLoop(evObj);
    ASSERT_EQ(errCode, ERROR_SYS_OK);

    errCode = RemoveEventFromLoop(evObj);
    ASSERT_EQ(errCode, ERROR_SYS_OK);

    DestroyEvent(evObj);
    DestroyLoopInOtherThread();
    ASSERT_EQ(GSDB_ATOMIC32_GET(&m_args2), 1);
    ASSERT_EQ(GetTotalEventObjectCount(), 0);
    ASSERT_EQ(GetTotalLoopCount(), 0);
}

/**
 * @tc.name: LoopRunningTest007_Level0
 * @tc.desc: Test of adding and removing the timers multiple times during loop running.
 * @tc.type: FUNC
 */
static EventCbType OnTimerRun007(Event *self, unsigned eventType, void *context)
{
    EventLoopTest *testCase = (EventLoopTest*)context;
    ASSERT(eventType == EVENT_TIMEOUT);
    GSDB_ATOMIC32_ADD(&testCase->m_args1, 1);
    return EV_CB_CONTINUE;
}

static void OnTimerFinalize007(Event *self, void *context)
{
    EventLoopTest *testCase = (EventLoopTest*)context;
    GSDB_ATOMIC32_ADD(&testCase->m_args2, 1);
}

#define MAX_LOOP_TEST_TIMER_COUNT2 512

TEST_F(EventLoopTest, LoopRunningTest007_Level0)
{
    CreateLoop();
    Event *evArr[MAX_LOOP_TEST_TIMER_COUNT2];
    for (int index = 0; index < MAX_LOOP_TEST_TIMER_COUNT2; ++index) {
        unsigned timeoutInMsec = (index % 3 + 1) * TIMER_WHEEL_JIFFY;
        evArr[index] = CreateTimerEvent(GetLoopPointer(), timeoutInMsec, NULL);
        ASSERT_TRUE(evArr[index] != NULL);
        SetEventCallbacks(evArr[index], OnTimerRun007, OnTimerFinalize007, this);
        ErrorCode errCode = AddEventToLoop(evArr[index]);
        ASSERT_EQ(errCode, ERROR_SYS_OK);
    }

    RunLoopInOtherThread();

    Usleep(10 * JIFFY_TIME_USEC);

    for (int index = 0; index < MAX_LOOP_TEST_TIMER_COUNT2; ++index) {
        ErrorCode errCode = RemoveEventFromLoop(evArr[index]);
        ASSERT_EQ(errCode, ERROR_SYS_OK);
        errCode = AddEventToLoop(evArr[index]);
        ASSERT_EQ(errCode, ERROR_SYS_OK);
    }

    Usleep(10 * JIFFY_TIME_USEC);

    for (int index = 0; index < MAX_LOOP_TEST_TIMER_COUNT2; ++index) {
        ErrorCode errCode = RemoveEventFromLoop(evArr[index]);
        ASSERT_EQ(errCode, ERROR_SYS_OK);
        DestroyEvent(evArr[index]);
    }

    int waitRetry = 100;
    bool allTimerDestroyed = false;
    while (--waitRetry >= 0) { /* wait 1s */
        Usleep(JIFFY_TIME_USEC);
        if (GSDB_ATOMIC32_GET(&m_args2) != MAX_LOOP_TEST_TIMER_COUNT2) {
            continue;
        }
        allTimerDestroyed = true;
        break;
    }

    ASSERT_TRUE(GSDB_ATOMIC32_GET(&m_args1) > 0);
    ASSERT_TRUE(allTimerDestroyed);

    DestroyLoopInOtherThread();
    ASSERT_EQ(GetTotalLoopCount(), 0);
    ASSERT_EQ(GetTotalEventObjectCount(), 0);
}

#if !defined(WINDOWS_PLATFORM)

/**
 * @tc.name: LoopRunningTest008_Level0
 * @tc.desc: Test whether the loop can detect a readable event.
 * @tc.type: FUNC
 */
static EventCbType OnEventRun008(Event *self, unsigned eventType, void *context)
{
    EventLoopTest *testCase = (EventLoopTest*)context;
    ASSERT(eventType == EVENT_READ);
    GSDB_ATOMIC32_ADD(&testCase->m_args1, 1);
    return EV_CB_EXIT;
}

TEST_F(EventLoopTest, LoopRunningTest008_Level0)
{
    CreateLoop();
    RunLoopInOtherThread();

    int fd[2];
    ASSERT_EQ(pipe(fd), 0);

    Event *evObj = CreateEvent(GetLoopPointer(), fd[0], 0, 0, NULL);
    ASSERT_TRUE(evObj != NULL);

    SetEventCallbacks(evObj, OnEventRun008, NULL, this);
    ErrorCode errCode = AddEventToLoop(evObj);
    ASSERT_EQ(errCode, ERROR_SYS_OK);

    char a = 'a';
    ASSERT_EQ(write(fd[1], &a, sizeof(a)), 1);

    int waitRetry = 3;
    bool readable = false;
    while (--waitRetry >= 0) {
        Usleep(JIFFY_TIME_USEC);
        if (GSDB_ATOMIC32_GET(&m_args1) == 0) {
            continue;
        }
        readable = true;
        break;
    }
    ASSERT_TRUE(!readable);

    errCode = AddEventType(evObj, EVENT_READ);
    ASSERT_EQ(errCode, ERROR_SYS_OK);

    waitRetry = 10;
    readable = false;
    while (--waitRetry >= 0) {
        Usleep(JIFFY_TIME_USEC);
        if (GSDB_ATOMIC32_GET(&m_args1) == 0) {
            continue;
        }
        readable = true;
        break;
    }
    ASSERT_TRUE(readable);

    DestroyEvent(evObj);
    DestroyLoopInOtherThread();
    ASSERT_EQ(GetTotalLoopCount(), 0);
    ASSERT_EQ(GetTotalEventObjectCount(), 0);

    close(fd[0]);
    close(fd[1]);
}

/**
 * @tc.name: LoopRunningTest009_Level0
 * @tc.desc: Test whether the loop can detect a writable event.
 * @tc.type: FUNC
 */
static EventCbType OnEventRun009(Event *self, unsigned eventType, void *context)
{
    EventLoopTest *testCase = (EventLoopTest*)context;
    ASSERT(eventType == EVENT_WRITE);
    GSDB_ATOMIC32_ADD(&testCase->m_args1, 1);
    return EV_CB_EXIT;
}

TEST_F(EventLoopTest, LoopRunningTest009_Level0)
{
    CreateLoop();
    RunLoopInOtherThread();

    int fd[2];
    ASSERT_EQ(pipe(fd), 0);

    Event *evObj = CreateEvent(GetLoopPointer(), fd[1], EVENT_WRITE, 0, NULL);
    ASSERT_TRUE(evObj != NULL);

    SetEventCallbacks(evObj, OnEventRun009, NULL, this);
    ErrorCode errCode = AddEventToLoop(evObj);
    ASSERT_EQ(errCode, ERROR_SYS_OK);

    int waitRetry = 10;
    bool writable = false;
    while (--waitRetry >= 0) {
        Usleep(JIFFY_TIME_USEC);
        if (GSDB_ATOMIC32_GET(&m_args1) == 0) {
            continue;
        }
        writable = true;
        break;
    }
    ASSERT_TRUE(writable);

    DestroyEvent(evObj);
    DestroyLoopInOtherThread();
    ASSERT_EQ(GetTotalLoopCount(), 0);
    ASSERT_EQ(GetTotalEventObjectCount(), 0);

    close(fd[0]);
    close(fd[1]);
}

/**
 * @tc.name: LoopRunningTest010_Level0
 * @tc.desc: Test whether the loop can remove 'EVENT_WRITE' type on running.
 * @tc.type: FUNC
 */
static EventCbType OnEventRun010(Event *self, unsigned eventType, void *context)
{
    EventLoopTest *testCase = (EventLoopTest*)context;
    ASSERT(eventType == EVENT_WRITE);
    GSDB_ATOMIC32_ADD(&testCase->m_args1, 1);
    Usleep(100);
    return EV_CB_CONTINUE;
}

TEST_F(EventLoopTest, LoopRunningTest010_Level0)
{
    CreateLoop();
    RunLoopInOtherThread();

    int fd[2];
    ASSERT_EQ(pipe(fd), 0);

    Event *evObj = CreateEvent(GetLoopPointer(), fd[1], EVENT_WRITE, 0, NULL);
    ASSERT_TRUE(evObj != NULL);

    SetEventCallbacks(evObj, OnEventRun010, NULL, this);
    ErrorCode errCode = AddEventToLoop(evObj);
    ASSERT_EQ(errCode, ERROR_SYS_OK);

    while (GSDB_ATOMIC32_GET(&m_args1) == 0) {
        Usleep(JIFFY_TIME_USEC);
    }

    errCode = RemoveEventType(evObj, EVENT_WRITE);
    ASSERT_EQ(errCode, ERROR_SYS_OK);
    Usleep(2 * JIFFY_TIME_USEC);
    int oldValue = GSDB_ATOMIC32_GET(&m_args1);
    Usleep(JIFFY_TIME_USEC);
    int newValue = GSDB_ATOMIC32_GET(&m_args1);

    ASSERT_EQ(oldValue, newValue);
    DestroyEvent(evObj);
    DestroyLoopInOtherThread();
    ASSERT_EQ(GetTotalLoopCount(), 0);
    ASSERT_EQ(GetTotalEventObjectCount(), 0);

    close(fd[0]);
    close(fd[1]);
}

#endif

/**
 * @tc.name: LoopRunningTest011_Level0
 * @tc.desc: Test whether the loop can be quited in ev callbacks while running.
 * @tc.type: FUNC
 */
static EventCbType OnTimerRun011(Event *self, unsigned eventType, void *context)
{
    EventLoopTest *testCase = (EventLoopTest*)context;
    ErrorCode errCode = QuitEventLoop(testCase->m_loop);
    ASSERT(errCode == ERROR_SYS_OK);
    GSDB_ATOMIC32_ADD(&testCase->m_args1, 1);
    return EV_CB_CONTINUE;
}

TEST_F(EventLoopTest, LoopRunningTest011_Level0)
{
    CreateLoop();

    Event *evObj = CreateTimerEvent(GetLoopPointer(), TIMER_WHEEL_JIFFY, NULL);
    SetEventCallbacks(evObj, OnTimerRun011, NULL, this);
    ErrorCode errCode = AddEventToLoop(evObj);
    ASSERT_EQ(errCode, ERROR_SYS_OK);

    RunLoopDirectly();
    ASSERT_TRUE(true);

    errCode = RemoveEventFromLoop(evObj);
    ASSERT_EQ(errCode, ERROR_SYS_OK);

    DestroyEvent(evObj);
    DestroyLoop();

    ASSERT_EQ(GSDB_ATOMIC32_GET(&m_args1), 1);
    ASSERT_EQ(GetTotalEventObjectCount(), 0);
    ASSERT_EQ(GetTotalLoopCount(), 0);
}

/**
 * @tc.name: LoopRunningTest012_Level0
 * @tc.desc: Test Destroy/Removing a event object from another event callback.
 * @tc.type: FUNC
 */
static EventCbType OnTimerRun012Fast(Event *self, unsigned eventType, void *context)
{
    EventLoopTest *testCase = (EventLoopTest*)context;
    GSDB_ATOMIC32_ADD(&testCase->m_args1, 1);
    return EV_CB_CONTINUE;
}

static EventCbType OnTimerRun012Slow(Event *self, unsigned eventType, void *context)
{
    EventLoopTest *testCase = (EventLoopTest*)context;
    DestroyEvent(testCase->m_evObj1);
    GSDB_ATOMIC32_ADD(&testCase->m_args2, 1);
    return EV_CB_EXIT;
}

TEST_F(EventLoopTest, LoopRunningTest012_Level0)
{
    CreateLoop();

    Event *timerFast = CreateTimerEvent(GetLoopPointer(), TIMER_WHEEL_JIFFY, NULL);
    SetEventCallbacks(timerFast, OnTimerRun012Fast, NULL, this);
    ErrorCode errCode = AddEventToLoop(timerFast);
    ASSERT_EQ(errCode, ERROR_SYS_OK);
    m_evObj1 = timerFast;

    Event *timerSlow = CreateTimerEvent(GetLoopPointer(), 10 * TIMER_WHEEL_JIFFY, NULL);
    SetEventCallbacks(timerSlow, OnTimerRun012Slow, NULL, this);
    errCode = AddEventToLoop(timerSlow);
    ASSERT_EQ(errCode, ERROR_SYS_OK);
    RefObjectDecRef(UP_TYPE_CAST(timerSlow, RefObject));

    RunLoopInOtherThread();

    Usleep(20 * JIFFY_TIME_USEC);

    DestroyLoopInOtherThread();

    if(GSDB_ATOMIC32_GET(&m_args2) > 0) {
        ASSERT_EQ(GetTotalEventObjectCount(), 0);
        ASSERT_EQ(GetTotalLoopCount(), 0);
    }
}

/**
 * @tc.name: LoopRunningTest013_Level0
 * @tc.desc: Test quit reference count in event loop
 * @tc.type: FUNC
 */
TEST_F(EventLoopTest, LoopRunningTest013_Level0)
{
    CreateLoop();
    RunLoopInOtherThread();
    Usleep(JIFFY_TIME_USEC);
    ASSERT_EQ(m_loop->state, RUNNING);
    ASSERT_EQ(QuitEventLoop(m_loop), ERROR_SYS_OK);
    Usleep(JIFFY_TIME_USEC);
    ASSERT_EQ(m_loop->state, QUITED);
    /* Test quit operation faster than begin, loop should not run */
    ASSERT_EQ(QuitEventLoop(m_loop), ERROR_SYS_OK);
    RunLoopInOtherThread();
    Usleep(JIFFY_TIME_USEC);
    ASSERT_EQ(m_loop->state, QUITED);
    /* Next begin operation, loop should run */
    RunLoopInOtherThread();
    Usleep(JIFFY_TIME_USEC);
    ASSERT_EQ(m_loop->state, RUNNING);
    DestroyLoopInOtherThread();
}

/**
 * @tc.name: LoopRunningTest014_Level0
 * @tc.desc: Test of loop quit and destroy.
 * @tc.type: FUNC
 */
static EventCbType OnTimerRun014(Event *self, unsigned eventType, void *context)
{
    EventLoopTest *testCase = (EventLoopTest*)context;
    ASSERT(eventType == EVENT_TIMEOUT);
    GSDB_ATOMIC32_ADD(&testCase->m_args1, 1);
    return EV_CB_CONTINUE;
}

TEST_F(EventLoopTest, LoopRunningTest014_Level0)
{
    CreateLoop();
    RunLoopInOtherThread();

    int evCount = 512;
    while (--evCount >= 0) {
        Event *timer = CreateTimerEvent(GetLoopPointer(), TIMER_WHEEL_JIFFY, NULL);
        SetEventCallbacks(timer, OnTimerRun014, NULL, this);
        ErrorCode errCode = AddEventToLoop(timer);
        ASSERT_EQ(errCode, ERROR_SYS_OK);
        RefObjectDecRef(UP_TYPE_CAST(timer, RefObject));
    }

    Usleep(JIFFY_TIME_USEC);
    QuiteLoopInOtherThread();
    DestroyLoopInOtherThread();
    ASSERT_EQ(GetTotalEventObjectCount(), 0);
    ASSERT_EQ(GetTotalLoopCount(), 0);
}

/**
 * @tc.name: LoopRunningTest015_Level0
 * @tc.desc: Prepare 10W events, test of adding and removing the timers multiple times during loop running.
 * @tc.type: FUNC
 */
struct TestEventTimeContext
{
    int eventId;
    EventTime expectTimeout;
    uint64_t addEventTime;
    bool isFirstRunning;
};

constexpr int EVENT_TOTAL_COUNT = 10000; /* 1W events in loop */

static uint64_t GetTimeGap(uint64_t expectTime, uint64_t actualTime)
{
    if (expectTime > actualTime) {
        return expectTime - actualTime;
    } else {
        return actualTime - expectTime;
    }
}

static EventCbType OnTimerRun015(Event *self, unsigned eventType, void *context)
{
    ASSERT(eventType == EVENT_TIMEOUT);
    TestEventTimeContext *curContext = (TestEventTimeContext *)context;
    uint64_t curTime = EventLoopTest::UtGetCurrentTimeInMs();
    if (curContext->isFirstRunning) {
        curContext->isFirstRunning = false;
        /* Use fix timeout to balance pressure in loop */
        ResetEventTimeout(self, 4000);
        curContext->expectTimeout = 4000;
        curContext->addEventTime = curTime;
        return EV_CB_CONTINUE;
    }
    uint64_t timeGap = GetTimeGap(curContext->expectTimeout, curTime - curContext->addEventTime);
    static const int MAX_TIME_GAP = 100; /* 100 ms */
    if (timeGap > MAX_TIME_GAP) {
        printf("Event id %d waiting time exceed %dms, expect waiting time: %lu, actual waiting time: %lu\n",
               curContext->eventId, MAX_TIME_GAP, curContext->expectTimeout, curTime - curContext->addEventTime);
        ASSERT(0);
    }
    curContext->addEventTime = curTime;
    return EV_CB_CONTINUE;
}

static void OnTimerFinalize015(Event *self, void *context)
{
    TestEventTimeContext *curContext = (TestEventTimeContext *)context;
    curContext->isFirstRunning = true;
}

TEST_F(EventLoopTest, LoopRunningTest015_Level0)
{
    ErrorCode errorCode;
    CreateLoop();
    Event *evArr[EVENT_TOTAL_COUNT];
    TestEventTimeContext eventTimeContext[EVENT_TOTAL_COUNT];
    for (int i = 0; i < EVENT_TOTAL_COUNT; ++i) {
        /* Each event timeout vary in [1.0, 1.1, 1.2, ..., 4.9]s */
        unsigned timeoutInMs = (i % 40 + 10) * 10 * TIMER_WHEEL_JIFFY;
        evArr[i] = CreateTimerEvent(GetLoopPointer(), timeoutInMs, &errorCode);
        eventTimeContext[i].eventId = i;
        eventTimeContext[i].expectTimeout = timeoutInMs;
        eventTimeContext[i].isFirstRunning = true;
        ASSERT_NE(evArr[i], nullptr);
        ASSERT_EQ(errorCode, 0);
        SetEventCallbacks(evArr[i], OnTimerRun015, OnTimerFinalize015, &eventTimeContext[i]);
    }
    /* Add events to loop */
    for (auto &event : evArr) {
        ASSERT_EQ(AddEventToLoop(event), 0);
    }
    /* Run event loop */
    RunLoopInOtherThread();
    Usleep((long)30 * MSEC_PER_SEC * USECS_PER_MSEC);
    /* Remove and destroy all events */
    for (auto &event : evArr) {
        ErrorCode errCode = RemoveEventFromLoop(event);
        ASSERT_EQ(errCode, ERROR_SYS_OK);
        DestroyEvent(event);
    }

    DestroyLoopInOtherThread();
    ASSERT_EQ(GetTotalLoopCount(), 0);
    ASSERT_EQ(GetTotalEventObjectCount(), 0);
}
