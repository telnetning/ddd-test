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
 * ut_fault_injection.cpp
 *
 * ---------------------------------------------------------------------------------
 */
#include <thread>
#include "securec.h"
#include "gtest/gtest.h"
#include "port/platform_port.h"
#include "fault_injection/fault_injection.h"

#define TEST_THREAD_COUNT 3

const long ONE_MILLISECOND = 1000;

enum FaultInjectionPoint { FAULT_INJECT_STRING_COPY_POINT, FAULT_INJECT_DELAY_EXECUTE_POINT,
                           FAULT_INJECT_SYNC_POINT_1, FAULT_INJECT_SYNC_POINT_2,
                           GLOBAL_FAULT_INJECT_SYNC_POINT_1, GLOBAL_FAULT_INJECT_SYNC_POINT_2,
                           FAULT_INJECT_SKIP_EXPECT_TRIGGER_POINT };

int copyStrForReturnTest(char *dest, const char *source, int destSize)
{
    FAULT_INJECTION_RETURN(FAULT_INJECT_STRING_COPY_POINT, -1);
    errno_t ret = strcpy_s(dest, destSize, source);
    return ret;
}

int copyStrForActionTest(char *dest, const char *source, int destSize)
{
    FAULT_INJECTION_ACTION(FAULT_INJECT_DELAY_EXECUTE_POINT, Usleep(ONE_MILLISECOND));
    errno_t ret = strcpy_s(dest, destSize, source);

    FAULT_INJECTION_ACTION(FAULT_INJECT_STRING_COPY_POINT, ret = strcpy_s(dest, destSize, "abcde"));
    return ret;
}

int copyStrForReplaceTest(char *dest, const char *source, int destSize)
{
    errno_t ret = EOK;

    FAULT_INJECTION_CALL_REPLACE(FAULT_INJECT_STRING_COPY_POINT, dest, destSize)
    ret = strcpy_s(dest, destSize, source);
    FAULT_INJECTION_CALL_REPLACE_END;

    return ret;
}

int copyStrForCallTest(char *dest, const char *source, int destSize)
{
    errno_t ret = strcpy_s(dest, destSize, source);

    FAULT_INJECTION_CALL(FAULT_INJECT_STRING_COPY_POINT, dest, destSize);
    return ret;
}

#ifdef ENABLE_FAULT_INJECTION

void setString(const FaultInjectionEntry *entry, char *dest, int destSize)
{
    switch (entry->exceptionMode) {
        case 1:
            (void)strcpy_s(dest, destSize, "!@#$%");
            break;
        default:
            (void)strcpy_s(dest, destSize, "abcde");
    }
}

void addCount(const FaultInjectionEntry *entry, int *count)
{
    (*count) = (*count) + 1;
}

class FaultInjectionTest : public testing::Test {
public:
    static void SetUpTestSuite()
    {}

    static void TearDownTestSuite()
    {}

    void SetUp() override
    {}

    void TearDown() override
    {}

};

TEST_F(FaultInjectionTest, fiReturnTest)
{
    FaultInjectionEntry entries[] = {
        FAULT_INJECTION_ENTRY(FAULT_INJECT_STRING_COPY_POINT, false, setString),
        FAULT_INJECTION_ENTRY(FAULT_INJECT_DELAY_EXECUTE_POINT, false, nullptr),
    };

    ASSERT_EQ(RegisterFaultInjection(entries, sizeof(entries) / sizeof(entries[0]), FI_GLOBAL), ERROR_SYS_OK);

    const char *source = "12345";
    char *dest = new char[10]{};
    int ret = copyStrForReturnTest(dest, source, 10);
    ASSERT_EQ(ret, 0);
    ASSERT_STREQ(dest, source);
    delete[] dest;

    FAULT_INJECTION_ACTIVE(FAULT_INJECT_STRING_COPY_POINT, FI_GLOBAL);

    dest = new char[10]{};
    ret = copyStrForReturnTest(dest, source, 10);
    ASSERT_EQ(ret, -1);
    ASSERT_STREQ(dest, "");
    delete[] dest;

    FAULT_INJECTION_INACTIVE(FAULT_INJECT_STRING_COPY_POINT, FI_GLOBAL);

    DestroyFaultInjectionHash(FI_GLOBAL);
}

TEST_F(FaultInjectionTest, fiActionTest)
{
    FaultInjectionEntry entries[] = {
        FAULT_INJECTION_ENTRY(FAULT_INJECT_STRING_COPY_POINT, false, setString),
        FAULT_INJECTION_ENTRY(FAULT_INJECT_DELAY_EXECUTE_POINT, false, nullptr),
    };

    ASSERT_EQ(RegisterFaultInjection(entries, sizeof(entries) / sizeof(entries[0]), FI_GLOBAL), ERROR_SYS_OK);

    const char *source = "12345";
    char *dest = new char[10]{};
    int ret = copyStrForActionTest(dest, source, 10);
    ASSERT_EQ(ret, 0);
    ASSERT_STREQ(dest, "12345");

    FAULT_INJECTION_ACTIVE(FAULT_INJECT_DELAY_EXECUTE_POINT, FI_GLOBAL);
    FAULT_INJECTION_ACTIVE(FAULT_INJECT_STRING_COPY_POINT, FI_GLOBAL);

    ret = copyStrForActionTest(dest, source, 10);
    ASSERT_EQ(ret, 0);
    ASSERT_STREQ(dest, "abcde");
    delete[] dest;

    FAULT_INJECTION_INACTIVE(FAULT_INJECT_STRING_COPY_POINT, FI_GLOBAL);
    FAULT_INJECTION_INACTIVE(FAULT_INJECT_DELAY_EXECUTE_POINT, FI_GLOBAL);

    DestroyFaultInjectionHash(FI_GLOBAL);
}

TEST_F(FaultInjectionTest, fiReplaceTest)
{
    FaultInjectionEntry entries[] = {
        FAULT_INJECTION_ENTRY(FAULT_INJECT_STRING_COPY_POINT, false, setString),
        FAULT_INJECTION_ENTRY(FAULT_INJECT_DELAY_EXECUTE_POINT, false, nullptr)
    };

    ASSERT_EQ(RegisterFaultInjection(entries, sizeof(entries) / sizeof(entries[0]), FI_GLOBAL), ERROR_SYS_OK);

    const char *source = "12345";
    char *dest = new char[10]{};
    int ret = copyStrForReplaceTest(dest, source, 10);
    ASSERT_EQ(ret, 0);
    ASSERT_STREQ(dest, source);
    delete[] dest;

    FAULT_INJECTION_ACTIVE(FAULT_INJECT_STRING_COPY_POINT, FI_GLOBAL);

    dest = new char[10]{};
    ret = copyStrForReplaceTest(dest, source, 10);
    ASSERT_EQ(ret, 0);
    ASSERT_STREQ(dest, "abcde");
    delete[] dest;

    FAULT_INJECTION_ACTIVE_MODE_LEVEL(FAULT_INJECT_STRING_COPY_POINT, 1, FI_GLOBAL, 0, INT_MAX);
    dest = new char[10]{};
    ret = copyStrForReplaceTest(dest, source, 10);
    ASSERT_EQ(ret, 0);
    ASSERT_STREQ(dest, "!@#$%");
    delete[] dest;

    FAULT_INJECTION_INACTIVE(FAULT_INJECT_STRING_COPY_POINT, FI_GLOBAL);

    DestroyFaultInjectionHash(FI_GLOBAL);
}

TEST_F(FaultInjectionTest, fiCallTest)
{
    FaultInjectionEntry entries[] = {
        FAULT_INJECTION_ENTRY(FAULT_INJECT_STRING_COPY_POINT, false, setString),
        FAULT_INJECTION_ENTRY(FAULT_INJECT_DELAY_EXECUTE_POINT, false, nullptr),
    };

    ASSERT_EQ(RegisterFaultInjection(entries, sizeof(entries) / sizeof(entries[0]), FI_GLOBAL), ERROR_SYS_OK);

    const char *source = "12345";
    char *dest = new char[10]{};
    int ret = copyStrForCallTest(dest, source, 10);
    ASSERT_EQ(ret, 0);
    ASSERT_STREQ(dest, source);
    delete[] dest;

    FAULT_INJECTION_ACTIVE(FAULT_INJECT_STRING_COPY_POINT, FI_GLOBAL);

    dest = new char[10]{};
    ret = copyStrForCallTest(dest, source, 10);
    ASSERT_EQ(ret, 0);
    ASSERT_STREQ(dest, "abcde");
    delete[] dest;

    FAULT_INJECTION_INACTIVE(FAULT_INJECT_STRING_COPY_POINT, FI_GLOBAL);

    DestroyFaultInjectionHash(FI_GLOBAL);
}

static void GlobalLevelWaitNotifyThread1(int *counter1, int *counter2)
{
    *counter1 = 33;
    FAULT_INJECTION_NOTIFY(FAULT_INJECT_SYNC_POINT_2);

    /* wait thread 2 the value of counter1 */
    FAULT_INJECTION_WAIT(FAULT_INJECT_SYNC_POINT_1);
    *counter1 = *counter2;
    ASSERT_EQ(*counter1, 44);
}

static void GlobalLevelWaitNotifyThread2(int *counter1, int *counter2)
{
    FAULT_INJECTION_WAIT(FAULT_INJECT_SYNC_POINT_2);
    *counter2 = *counter1;
    ASSERT_EQ(*counter2, 33);
    *counter2 = 44;
    FAULT_INJECTION_NOTIFY(FAULT_INJECT_SYNC_POINT_1);
}

TEST_F(FaultInjectionTest, fiWaitNotifyTest)
{
    FaultInjectionEntry entries[] = {
        FAULT_INJECTION_ENTRY(FAULT_INJECT_SYNC_POINT_1, false, nullptr),
        FAULT_INJECTION_ENTRY(FAULT_INJECT_SYNC_POINT_2, false, nullptr),
        FAULT_INJECTION_ENTRY(FAULT_INJECT_SKIP_EXPECT_TRIGGER_POINT, false, addCount)
    };

    ASSERT_EQ(RegisterFaultInjection(entries, sizeof(entries) / sizeof(entries[0]), FI_GLOBAL), ERROR_SYS_OK);

    FAULT_INJECTION_ACTIVE(FAULT_INJECT_SYNC_POINT_1, FI_GLOBAL);
    FAULT_INJECTION_ACTIVE(FAULT_INJECT_SYNC_POINT_2, FI_GLOBAL);

    int counter1 = 0;
    int counter2 = 0;

    std::thread t1 = std::thread(GlobalLevelWaitNotifyThread1, &counter1, &counter2);
    std::thread t2 = std::thread(GlobalLevelWaitNotifyThread2, &counter1, &counter2);

    t1.join();
    t2.join();

    FAULT_INJECTION_INACTIVE(FAULT_INJECT_SYNC_POINT_1, FI_GLOBAL);
    FAULT_INJECTION_INACTIVE(FAULT_INJECT_SYNC_POINT_2, FI_GLOBAL);

    DestroyFaultInjectionHash(FI_GLOBAL);
}

TEST_F(FaultInjectionTest, fiSkipAndExpectTriggerTest)
{
    FaultInjectionEntry entries[] = {
        FAULT_INJECTION_ENTRY(FAULT_INJECT_SYNC_POINT_1, false, nullptr),
        FAULT_INJECTION_ENTRY(FAULT_INJECT_SYNC_POINT_2, false, nullptr),
        FAULT_INJECTION_ENTRY(FAULT_INJECT_SKIP_EXPECT_TRIGGER_POINT, false, addCount)
    };

    ASSERT_EQ(RegisterFaultInjection(entries, sizeof(entries) / sizeof(entries[0]), FI_GLOBAL), ERROR_SYS_OK);

    FAULT_INJECTION_ACTIVE_MODE_LEVEL(FAULT_INJECT_SKIP_EXPECT_TRIGGER_POINT, 0, FI_GLOBAL, 5, 10);

    int count = 0;
    for (int i = 0; i < 20; i++) {
        FAULT_INJECTION_CALL(FAULT_INJECT_SKIP_EXPECT_TRIGGER_POINT, &count);
        if (i < 5) {
            ASSERT_EQ(count, 0);
        } else if (i < 15) {
            ASSERT_EQ(count, (i + 1) - 5);
        } else {
            ASSERT_EQ(count, 10);
        }
    }

    FAULT_INJECTION_INACTIVE(FAULT_INJECT_SKIP_EXPECT_TRIGGER_POINT, FI_GLOBAL);

    DestroyFaultInjectionHash(FI_GLOBAL);
}

TEST_F(FaultInjectionTest, faultInjectionCallConcurrencyTest)
{
#define THREAD_NUM 1000
    FaultInjectionEntry entries[] = {FAULT_INJECTION_ENTRY(FAULT_INJECT_SKIP_EXPECT_TRIGGER_POINT, false, NULL)};

    ASSERT_EQ(RegisterFaultInjection(entries, sizeof(entries) / sizeof(entries[0]), FI_GLOBAL), ERROR_SYS_OK);

    FAULT_INJECTION_ACTIVE_MODE_LEVEL(FAULT_INJECT_SKIP_EXPECT_TRIGGER_POINT, 0, FI_GLOBAL, THREAD_NUM / 4,
                                      THREAD_NUM / 2);

    std::thread threads[THREAD_NUM];
    for (int i = 0; i < THREAD_NUM; i++) {
        threads[i] = std::thread([]() {
            for (int i = 0; i < 200; i++) {
                FAULT_INJECTION_CALL(FAULT_INJECT_SKIP_EXPECT_TRIGGER_POINT, NULL);
            }
        });
    }
    for (int i = 0; i < THREAD_NUM; i++) {
        threads[i].join();
    }
    FaultInjectionEntry *entry = FindFaultInjectionEntry("FAULT_INJECT_SKIP_EXPECT_TRIGGER_POINT", FI_GLOBAL);
    ASSERT_NE(entry, nullptr);
    ASSERT_EQ(entry->skipCount, 0);
    ASSERT_EQ(entry->calledCount, THREAD_NUM / 2);
}

TEST_F(FaultInjectionTest, faultInjectionActionConcurrencyTest)
{
#define THREAD_NUM 1000
    FaultInjectionEntry entries[] = {FAULT_INJECTION_ENTRY(FAULT_INJECT_SKIP_EXPECT_TRIGGER_POINT, false, NULL)};

    ASSERT_EQ(RegisterFaultInjection(entries, sizeof(entries) / sizeof(entries[0]), FI_GLOBAL), ERROR_SYS_OK);

    FAULT_INJECTION_ACTIVE_MODE_LEVEL(FAULT_INJECT_SKIP_EXPECT_TRIGGER_POINT, 0, FI_GLOBAL, THREAD_NUM / 4,
                                      THREAD_NUM / 2);

    std::thread threads[THREAD_NUM];
    for (int i = 0; i < THREAD_NUM; i++) {
        threads[i] = std::thread([]() {
            for (int i = 0; i < 200; i++) {
                FAULT_INJECTION_ACTION(FAULT_INJECT_SKIP_EXPECT_TRIGGER_POINT, ASSERT_EQ(true, true));
            }
        });
    }
    for (int i = 0; i < THREAD_NUM; i++) {
        threads[i].join();
    }
    FaultInjectionEntry *entry = FindFaultInjectionEntry("FAULT_INJECT_SKIP_EXPECT_TRIGGER_POINT", FI_GLOBAL);
    ASSERT_NE(entry, nullptr);
    ASSERT_EQ(entry->skipCount, 0);
    ASSERT_EQ(entry->calledCount, THREAD_NUM / 2);
}

TEST_F(FaultInjectionTest, faultInjectionWaitConcurrencyTest)
{
#define THREAD_NUM 1000
    FaultInjectionEntry entries[] = {FAULT_INJECTION_ENTRY(FAULT_INJECT_SKIP_EXPECT_TRIGGER_POINT, false, NULL)};

    ASSERT_EQ(RegisterFaultInjection(entries, sizeof(entries) / sizeof(entries[0]), FI_GLOBAL), ERROR_SYS_OK);

    FAULT_INJECTION_ACTIVE_MODE_LEVEL(FAULT_INJECT_SKIP_EXPECT_TRIGGER_POINT, 0, FI_GLOBAL, THREAD_NUM / 4,
                                      THREAD_NUM / 2);

    std::thread threads[THREAD_NUM];
    for (int i = 0; i < THREAD_NUM / 2; i++) {
        threads[i] = std::thread([]() { FAULT_INJECTION_WAIT(FAULT_INJECT_SKIP_EXPECT_TRIGGER_POINT); });
    }
    for (int i = THREAD_NUM / 2; i < THREAD_NUM; i++) {
        threads[i] = std::thread([]() { FAULT_INJECTION_NOTIFY(FAULT_INJECT_SKIP_EXPECT_TRIGGER_POINT); });
    }
    for (int i = 0; i < THREAD_NUM; i++) {
        threads[i].join();
    }
    FaultInjectionEntry *entry = FindFaultInjectionEntry("FAULT_INJECT_SKIP_EXPECT_TRIGGER_POINT", FI_GLOBAL);
    ASSERT_NE(entry, nullptr);
    ASSERT_EQ(entry->skipCount, 0);
    ASSERT_EQ(entry->calledCount, THREAD_NUM / 4);
}

/* Thread local FaultInjection point test */
static void FaultInjectionThreadLevelActiveThread()
{
    FaultInjectionEntry entries[] = {
        FAULT_INJECTION_ENTRY(FAULT_INJECT_STRING_COPY_POINT, false, setString),
        FAULT_INJECTION_ENTRY(FAULT_INJECT_DELAY_EXECUTE_POINT, false, nullptr),
    };

    ASSERT_EQ(RegisterFaultInjection(entries, sizeof(entries) / sizeof(entries[0]), FI_THREAD), ERROR_SYS_OK);

    /* test FAULT_INJECTION_RETURN */
    const char *source = "12345";
    char *dest = new char[10]{};
    int ret = copyStrForReturnTest(dest, source, 10);
    ASSERT_EQ(ret, 0);
    ASSERT_STREQ(dest, source);
    memset_s(dest, 10, 0, 10);

    FAULT_INJECTION_ACTIVE(FAULT_INJECT_STRING_COPY_POINT, FI_THREAD);

    ret = copyStrForReturnTest(dest, source, 10);
    ASSERT_EQ(ret, -1);
    ASSERT_STREQ(dest, "");
    memset_s(dest, 10, 0, 10);

    FAULT_INJECTION_INACTIVE(FAULT_INJECT_STRING_COPY_POINT, FI_THREAD);

    /* test FAULT_INJECTION_ACTION */
    ret = copyStrForActionTest(dest, source, 10);
    ASSERT_EQ(ret, 0);
    ASSERT_STREQ(dest, "12345");
    memset_s(dest, 10, 0, 10);

    FAULT_INJECTION_ACTIVE(FAULT_INJECT_DELAY_EXECUTE_POINT, FI_THREAD);
    FAULT_INJECTION_ACTIVE(FAULT_INJECT_STRING_COPY_POINT, FI_THREAD);

    ret = copyStrForActionTest(dest, source, 10);
    ASSERT_EQ(ret, 0);
    ASSERT_STREQ(dest, "abcde");
    memset_s(dest, 10, 0, 10);

    FAULT_INJECTION_INACTIVE(FAULT_INJECT_STRING_COPY_POINT, FI_THREAD);
    FAULT_INJECTION_INACTIVE(FAULT_INJECT_DELAY_EXECUTE_POINT, FI_THREAD);

    /* test FAULT_INJECTION_CALL_REPLACE */
    ret = copyStrForReplaceTest(dest, source, 10);
    ASSERT_EQ(ret, 0);
    ASSERT_STREQ(dest, source);
    memset_s(dest, 10, 0, 10);

    FAULT_INJECTION_ACTIVE(FAULT_INJECT_STRING_COPY_POINT, FI_THREAD);

    ret = copyStrForReplaceTest(dest, source, 10);
    ASSERT_EQ(ret, 0);
    ASSERT_STREQ(dest, "abcde");
    memset_s(dest, 10, 0, 10);

    FAULT_INJECTION_ACTIVE_MODE_LEVEL(FAULT_INJECT_STRING_COPY_POINT, 1, FI_THREAD, 0, INT_MAX);
    ret = copyStrForReplaceTest(dest, source, 10);
    ASSERT_EQ(ret, 0);
    ASSERT_STREQ(dest, "!@#$%");
    memset_s(dest, 10, 0, 10);

    FAULT_INJECTION_INACTIVE(FAULT_INJECT_STRING_COPY_POINT, FI_THREAD);

    /* test FAULT_INJECTION_CALL */
    ret = copyStrForCallTest(dest, source, 10);
    ASSERT_EQ(ret, 0);
    ASSERT_STREQ(dest, source);
    memset_s(dest, 10, 0, 10);

    FAULT_INJECTION_ACTIVE(FAULT_INJECT_STRING_COPY_POINT, FI_THREAD);

    ret = copyStrForCallTest(dest, source, 10);
    ASSERT_EQ(ret, 0);
    ASSERT_STREQ(dest, "abcde");
    delete[] dest;

    FAULT_INJECTION_INACTIVE(FAULT_INJECT_STRING_COPY_POINT, FI_THREAD);

    /* Destroy the thread local FaultInjection hash table */
    DestroyFaultInjectionHash(FI_THREAD);
}

/* Thread local Action FaultInjection point test which has a different action */
static void FaultInjectionThreadLevelInactiveThread()
{
    FaultInjectionEntry entries[] = {
        FAULT_INJECTION_ENTRY(FAULT_INJECT_STRING_COPY_POINT, false, setString),
        FAULT_INJECTION_ENTRY(FAULT_INJECT_DELAY_EXECUTE_POINT, false, nullptr),
    };

    ASSERT_EQ(RegisterFaultInjection(entries, sizeof(entries) / sizeof(entries[0]), FI_THREAD), ERROR_SYS_OK);

    /* test FAULT_INJECTION_RETURN */
    const char *source = "12345";
    char *dest = new char[10]{};
    int ret = copyStrForReturnTest(dest, source, 10);
    ASSERT_EQ(ret, 0);
    ASSERT_STREQ(dest, source);
    memset_s(dest, 10, 0, 10);

    /* test FAULT_INJECTION_ACTION */
    ret = copyStrForActionTest(dest, source, 10);
    ASSERT_EQ(ret, 0);
    ASSERT_STREQ(dest, "12345");
    memset_s(dest, 10, 0, 10);

    /* test FAULT_INJECTION_CALL_REPLACE */
    ret = copyStrForReplaceTest(dest, source, 10);
    ASSERT_EQ(ret, 0);
    ASSERT_STREQ(dest, source);
    memset_s(dest, 10, 0, 10);

    /* test FAULT_INJECTION_CALL */
    ret = copyStrForCallTest(dest, source, 10);
    ASSERT_EQ(ret, 0);
    ASSERT_STREQ(dest, source);
    memset_s(dest, 10, 0, 10);

    delete[] dest;

    /* Destroy the thread local FaultInjection hash table */
    DestroyFaultInjectionHash(FI_THREAD);
}

/*
 * Create three threads, each thread registered a thread local FaultInjection(FI)
 * hash table, execute the same FI Action Test on each thread.
 */
TEST_F(FaultInjectionTest, fiThreadTest001)
{
    std::thread threads[TEST_THREAD_COUNT];

    for (auto &th : threads) {
        th = std::thread(FaultInjectionThreadLevelActiveThread);
    }
    for (auto &th : threads) {
        th.join();
    }
}

/*
 * Create two threads, each thread registered a thread local FaultInjection(FI)
 * hash table, execute different FI Action Test on each thread.
 */
TEST_F(FaultInjectionTest, fiThreadTest002)
{
    std::thread th1;
    std::thread th2;

    th1 = std::thread(FaultInjectionThreadLevelActiveThread);
    th2 = std::thread(FaultInjectionThreadLevelInactiveThread);

    th1.join();
    th2.join();
}

static Tid threadTids[2];

static void ThreadLevelWaitNotifyThread1(int *counter1, int *counter2)
{
    FaultInjectionEntry entries[] = {
        FAULT_INJECTION_ENTRY(FAULT_INJECT_SYNC_POINT_1, false, nullptr),
        FAULT_INJECTION_ENTRY(FAULT_INJECT_SYNC_POINT_2, false, nullptr),
    };

    ASSERT_EQ(RegisterFaultInjection(entries, sizeof(entries) / sizeof(entries[0]), FI_THREAD), ERROR_SYS_OK);

    FAULT_INJECTION_ACTIVE(FAULT_INJECT_SYNC_POINT_1, FI_THREAD);

    Tid selfTid = GetCurrentTid();
    threadTids[0] = selfTid;
    FAULT_INJECTION_NOTIFY(GLOBAL_FAULT_INJECT_SYNC_POINT_1);
    FAULT_INJECTION_THREAD_LEVEL_WAIT(selfTid, FAULT_INJECT_SYNC_POINT_1);

    *counter1 = 33;
    FAULT_INJECTION_THREAD_LEVEL_NOTIFY(threadTids[1], FAULT_INJECT_SYNC_POINT_2);

    /* wait thread 2 the value of counter1 */
    FAULT_INJECTION_THREAD_LEVEL_WAIT(selfTid, FAULT_INJECT_SYNC_POINT_1);
    *counter1 = *counter2;
    ASSERT_EQ(*counter1, 44);

    FAULT_INJECTION_INACTIVE(FAULT_INJECT_SYNC_POINT_1, FI_THREAD);
    DestroyFaultInjectionHash(FI_THREAD);
}

static void ThreadLevelWaitNotifyThread2(int *counter1, int *counter2)
{
    FaultInjectionEntry entries[] = {
        FAULT_INJECTION_ENTRY(FAULT_INJECT_SYNC_POINT_1, false, nullptr),
        FAULT_INJECTION_ENTRY(FAULT_INJECT_SYNC_POINT_2, false, nullptr),
    };

    ASSERT_EQ(RegisterFaultInjection(entries, sizeof(entries) / sizeof(entries[0]), FI_THREAD), ERROR_SYS_OK);

    FAULT_INJECTION_ACTIVE(FAULT_INJECT_SYNC_POINT_2, FI_THREAD);

    Tid selfTid = GetCurrentTid();
    threadTids[1] = selfTid;
    FAULT_INJECTION_NOTIFY(GLOBAL_FAULT_INJECT_SYNC_POINT_2);
    FAULT_INJECTION_THREAD_LEVEL_WAIT(selfTid, FAULT_INJECT_SYNC_POINT_2);

    FAULT_INJECTION_THREAD_LEVEL_WAIT(selfTid, FAULT_INJECT_SYNC_POINT_2);
    *counter2 = *counter1;
    ASSERT_EQ(*counter2, 33);
    *counter2 = 44;
    FAULT_INJECTION_THREAD_LEVEL_NOTIFY(threadTids[0], FAULT_INJECT_SYNC_POINT_1);

    FAULT_INJECTION_INACTIVE(FAULT_INJECT_SYNC_POINT_2, FI_THREAD);
    DestroyFaultInjectionHash(FI_THREAD);
}

TEST_F(FaultInjectionTest, fiThreadLevelWaitAndNotifyTest)
{
    FaultInjectionEntry entries[] = {
        FAULT_INJECTION_ENTRY(GLOBAL_FAULT_INJECT_SYNC_POINT_1, false, nullptr),
        FAULT_INJECTION_ENTRY(GLOBAL_FAULT_INJECT_SYNC_POINT_2, false, nullptr),
    };

    ASSERT_EQ(RegisterFaultInjection(entries, sizeof(entries) / sizeof(entries[0]), FI_GLOBAL), ERROR_SYS_OK);

    FAULT_INJECTION_ACTIVE(GLOBAL_FAULT_INJECT_SYNC_POINT_1, FI_GLOBAL);
    FAULT_INJECTION_ACTIVE(GLOBAL_FAULT_INJECT_SYNC_POINT_2, FI_GLOBAL);

    std::thread th1;
    std::thread th2;

    int counter1 = 0;
    int counter2 = 0;

    th1 = std::thread(ThreadLevelWaitNotifyThread1, &counter1, &counter2);
    th2 = std::thread(ThreadLevelWaitNotifyThread2, &counter1, &counter2);

    FAULT_INJECTION_WAIT(GLOBAL_FAULT_INJECT_SYNC_POINT_1);
    FAULT_INJECTION_WAIT(GLOBAL_FAULT_INJECT_SYNC_POINT_2);
    FAULT_INJECTION_THREAD_LEVEL_NOTIFY(threadTids[0], FAULT_INJECT_SYNC_POINT_1);
    FAULT_INJECTION_THREAD_LEVEL_NOTIFY(threadTids[1], FAULT_INJECT_SYNC_POINT_2);

    th1.join();
    th2.join();

    FAULT_INJECTION_INACTIVE(GLOBAL_FAULT_INJECT_SYNC_POINT_1, FI_GLOBAL);
    FAULT_INJECTION_INACTIVE(GLOBAL_FAULT_INJECT_SYNC_POINT_2, FI_GLOBAL);

    DestroyFaultInjectionHash(FI_GLOBAL);
}

static void ThreadLevelSkipAndExpectThread(int skip, int expect)
{
    FaultInjectionEntry entries[] = {
        FAULT_INJECTION_ENTRY(FAULT_INJECT_SYNC_POINT_1, false, nullptr),
        FAULT_INJECTION_ENTRY(FAULT_INJECT_SYNC_POINT_2, false, nullptr),
        FAULT_INJECTION_ENTRY(FAULT_INJECT_SKIP_EXPECT_TRIGGER_POINT, false, addCount)
    };

    ASSERT_EQ(RegisterFaultInjection(entries, sizeof(entries) / sizeof(entries[0]), FI_THREAD), ERROR_SYS_OK);

    FAULT_INJECTION_ACTIVE_MODE_LEVEL(FAULT_INJECT_SKIP_EXPECT_TRIGGER_POINT, 0, FI_THREAD, skip, expect);

    int loop = 2 * (skip + expect);
    int count = 0;
    for (int i = 0; i < loop; i++) {
        FAULT_INJECTION_CALL(FAULT_INJECT_SKIP_EXPECT_TRIGGER_POINT, &count);
        if (i < skip) {
            ASSERT_EQ(count, 0);
        } else if (i < (skip + expect)) {
            ASSERT_EQ(count, (i + 1) - skip);
        } else {
            ASSERT_EQ(count, expect);
        }
    }

    FAULT_INJECTION_INACTIVE(FAULT_INJECT_SKIP_EXPECT_TRIGGER_POINT, FI_THREAD);

    DestroyFaultInjectionHash(FI_THREAD);
}

TEST_F(FaultInjectionTest, fiThreadLevelSkipAndExpectTest)
{
    std::thread th1;
    std::thread th2;

    th1 = std::thread(ThreadLevelSkipAndExpectThread, 5, 10);
    th2 = std::thread(ThreadLevelSkipAndExpectThread, 50, 100);

    th1.join();
    th2.join();
}

#endif
