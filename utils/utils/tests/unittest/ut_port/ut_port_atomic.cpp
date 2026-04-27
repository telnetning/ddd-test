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

#include <thread>
#include "gtest/gtest.h"
#include "port/platform_port.h"

namespace {

constexpr uint32_t TEST_LOOP_COUNT = 100;
constexpr uint32_t TEST_THREAD_COUNT = 50;
MemoryOrder allMemoryList[] = {MEMORY_ORDER_RELAXED, MEMORY_ORDER_CONSUME, MEMORY_ORDER_ACQUIRE,
                               MEMORY_ORDER_RELEASE, MEMORY_ORDER_ACQ_REL, MEMORY_ORDER_SEQ_CST};
MemoryOrder supportMultiThreadMemoryList[] = {MEMORY_ORDER_CONSUME, MEMORY_ORDER_ACQUIRE, MEMORY_ORDER_RELEASE,
                                              MEMORY_ORDER_ACQ_REL, MEMORY_ORDER_SEQ_CST};

constexpr uint32_t BIT_32 = 32;

}

struct TestValueRange {
    union {
        uint32_t u32Min;
        uint64_t u64Min;
    } minVal;
    union {
        uint32_t u32Max;
        uint64_t u64Max;
    } maxVal;
};

class PortAtomicTest : public ::testing::Test {
protected:
    void SetUp() override
    {}

    void TearDown() override
    {}
};

TEST_F(PortAtomicTest, Uint32AddSingleThreadTest001)
{
    for (auto &memoryOrder : allMemoryList) {
        AtomicU32 u32Value;
        uint32_t initValue = 0;
        for (uint32_t loop = 0; loop < TEST_LOOP_COUNT; ++loop) {
            AtomicU32Set(&u32Value, initValue, memoryOrder);
            uint32_t testRepeatTime = 10000;
            uint32_t addValue = 1;
            uint32_t expectRes = initValue + addValue * testRepeatTime;
            TestValueRange valueRange = {};
            valueRange.minVal.u32Min = initValue;
            valueRange.maxVal.u32Max = expectRes;
            for (uint32_t i = 0; i < testRepeatTime; ++i) {
                uint32_t curRes = AtomicU32FetchAdd(&u32Value, addValue, memoryOrder);
                ASSERT_GE(curRes, valueRange.minVal.u32Min);
                ASSERT_LE(curRes, valueRange.maxVal.u32Max);
            }
            uint32_t result = AtomicU32Get(&u32Value, memoryOrder);
            ASSERT_EQ(result, expectRes);
        }
    }
}

TEST_F(PortAtomicTest, Uint32SubSingleThreadTest001)
{
    for (auto &memoryOrder : allMemoryList) {
        AtomicU32 u32Value;
        uint32_t initValue = 10000;
        for (uint32_t loop = 0; loop < TEST_LOOP_COUNT; ++loop) {
            AtomicU32Set(&u32Value, initValue, memoryOrder);
            uint32_t testRepeatTime = 10000;
            uint32_t subValue = 1;
            uint32_t expectRes = initValue - subValue * testRepeatTime;
            TestValueRange valueRange = {};
            valueRange.minVal.u32Min = expectRes;
            valueRange.maxVal.u32Max = initValue;
            for (uint32_t i = 0; i < testRepeatTime; ++i) {
                uint32_t curRes = AtomicU32FetchSub(&u32Value, subValue, memoryOrder);
                ASSERT_GE(curRes, valueRange.minVal.u32Min);
                ASSERT_LE(curRes, valueRange.maxVal.u32Max);
            }
            uint32_t result = AtomicU32Get(&u32Value, memoryOrder);
            ASSERT_EQ(result, expectRes);
        }
    }
}

TEST_F(PortAtomicTest, Uint32LogicSingleThreadTest001)
{
    for (auto &memoryOrder : allMemoryList) {
        AtomicU32 u32Value;
        for (uint32_t loop = 0; loop < TEST_LOOP_COUNT; ++loop) {
            AtomicU32Set(&u32Value, 0, memoryOrder);
            ASSERT_EQ(AtomicU32Get(&u32Value, memoryOrder), 0);
            AtomicU32FetchOr(&u32Value, 1, memoryOrder);
            ASSERT_EQ(AtomicU32Get(&u32Value, memoryOrder), 1);
            AtomicU32FetchXor(&u32Value, 1, memoryOrder);
            ASSERT_EQ(AtomicU32Get(&u32Value, memoryOrder), 0);
            AtomicU32FetchXor(&u32Value, 1, memoryOrder);
            ASSERT_EQ(AtomicU32Get(&u32Value, memoryOrder), 1);
            AtomicU32FetchAnd(&u32Value, 0, memoryOrder);
            ASSERT_EQ(AtomicU32Get(&u32Value, memoryOrder), 0);
        }
    }
}

TEST_F(PortAtomicTest, Uint32ExchangeSingleThreadTest001)
{
    for (auto &memoryOrder : allMemoryList) {
        AtomicU32 u32Value;
        for (uint32_t loop = 0; loop < TEST_LOOP_COUNT; ++loop) {
            AtomicU32Set(&u32Value, 0, memoryOrder);
            ASSERT_EQ(AtomicU32Get(&u32Value, memoryOrder), 0);
            ASSERT_EQ(AtomicU32Exchange(&u32Value, 1, memoryOrder), 0);
            ASSERT_EQ(AtomicU32Get(&u32Value, memoryOrder), 1);
            ASSERT_EQ(AtomicU32Exchange(&u32Value, 0, memoryOrder), 1);
            ASSERT_EQ(AtomicU32Get(&u32Value, memoryOrder), 0);
        }
    }
}

void TestAtomicU32AddFunc(AtomicU32 *value, uint32_t add, uint32_t repeat, MemoryOrder memory, TestValueRange range)
{
    for (uint32_t i = 0; i < repeat; ++i) {
        uint32_t curRes = AtomicU32FetchAdd(value, add, memory);
        ASSERT_GE(curRes, range.minVal.u32Min);
        ASSERT_LE(curRes, range.maxVal.u32Max);
    }
}

void TestAtomicU32SubFunc(AtomicU32 *value, uint32_t sub, uint32_t repeat, MemoryOrder memory, TestValueRange range)
{
    for (uint32_t i = 0; i < repeat; ++i) {
        uint32_t curRes = AtomicU32FetchSub(value, sub, memory);
        ASSERT_GE(curRes, range.minVal.u32Min);
        ASSERT_LE(curRes, range.maxVal.u32Max);
    }
}

TEST_F(PortAtomicTest, Uint32AddMultiThreadTest001)
{
    for (auto &memoryOrder : supportMultiThreadMemoryList) {
        AtomicU32 u32Value;
        uint32_t initValue = 0;
        AtomicU32Set(&u32Value, initValue, memoryOrder);
        uint32_t testRepeatTime = 10000;
        uint32_t addValue = 1;
        uint32_t expectRes = initValue + addValue * testRepeatTime * TEST_THREAD_COUNT;
        TestValueRange valueRange = {};
        valueRange.minVal.u32Min = initValue;
        valueRange.maxVal.u32Max = expectRes;
        std::thread threads[TEST_THREAD_COUNT];
        for (auto &thread : threads) {
            thread = std::thread(TestAtomicU32AddFunc, &u32Value, addValue, testRepeatTime, memoryOrder, valueRange);
        }
        for (auto &thread : threads) {
            thread.join();
        }
        uint32_t result = AtomicU32Get(&u32Value, memoryOrder);
        ASSERT_EQ(result, expectRes);
    }
}

TEST_F(PortAtomicTest, Uint32SubMultiThreadTest001)
{
    for (auto &memoryOrder : supportMultiThreadMemoryList) {
        AtomicU32 u32Value;
        uint32_t initValue = UINT32_MAX;
        AtomicU32Set(&u32Value, initValue, memoryOrder);
        uint32_t testRepeatTime = 10000;
        uint32_t subValue = 8;
        uint32_t expectRes = initValue - subValue * testRepeatTime * TEST_THREAD_COUNT;
        TestValueRange valueRange = {};
        valueRange.minVal.u32Min = expectRes;
        valueRange.maxVal.u32Max = initValue;
        std::thread threads[TEST_THREAD_COUNT];
        for (auto &thread : threads) {
            thread = std::thread(TestAtomicU32SubFunc, &u32Value, subValue, testRepeatTime, memoryOrder, valueRange);
        }
        for (auto &thread : threads) {
            thread.join();
        }
        uint32_t result = AtomicU32Get(&u32Value, memoryOrder);
        ASSERT_EQ(result, expectRes);
    }
}

void TestAtomicU32AndFunc(AtomicU32 *value, uint32_t target, MemoryOrder memoryOrder)
{
    AtomicU32FetchAnd(value, target, memoryOrder);
}

void TestAtomicU32OrFunc(AtomicU32 *value, uint32_t target, MemoryOrder memoryOrder)
{
    AtomicU32FetchOr(value, target, memoryOrder);
}

void TestAtomicU32XorFunc(AtomicU32 *value, uint32_t target, MemoryOrder memoryOrder)
{
    AtomicU32FetchXor(value, target, memoryOrder);
}

TEST_F(PortAtomicTest, Uint32LogicMultiThreadTest001)
{
    AtomicU32 u32Value;

    uint32_t opsValList[BIT_32];
    for (uint32_t i = 0; i < BIT_32; ++i) {
        opsValList[i] = (1 << i);
    }
    for (auto &memoryOrder : supportMultiThreadMemoryList) {
        AtomicU32Set(&u32Value, 0, memoryOrder);
        ASSERT_EQ(AtomicU32Get(&u32Value, memoryOrder), 0);
        std::thread threads[BIT_32];
        for (uint32_t i = 0; i < BIT_32; ++i) {
            threads[i] = std::thread(TestAtomicU32OrFunc, &u32Value, opsValList[i], memoryOrder);
        }
        for (auto &thread : threads) {
            thread.join();
        }
        ASSERT_EQ(AtomicU32Get(&u32Value, memoryOrder), ~0);

        for (uint32_t i = 0; i < BIT_32; ++i) {
            threads[i] = std::thread(TestAtomicU32AndFunc, &u32Value, ~opsValList[i], memoryOrder);
        }
        for (auto &thread : threads) {
            thread.join();
        }
        ASSERT_EQ(AtomicU32Get(&u32Value, memoryOrder), 0);

        for (uint32_t i = 0; i < BIT_32; ++i) {
            threads[i] = std::thread(TestAtomicU32XorFunc, &u32Value, opsValList[i], memoryOrder);
        }
        for (auto &thread : threads) {
            thread.join();
        }
        ASSERT_EQ(AtomicU32Get(&u32Value, memoryOrder), ~0);

        for (uint32_t i = 0; i < BIT_32; ++i) {
            threads[i] = std::thread(TestAtomicU32XorFunc, &u32Value, opsValList[i], memoryOrder);
        }
        for (auto &thread : threads) {
            thread.join();
        }
        ASSERT_EQ(AtomicU32Get(&u32Value, memoryOrder), 0);
    }
}

void TestAtomicU32CompareExchangeStrongFunc(AtomicU32 *value, uint32_t compareValue, uint32_t writeValue, bool *result,
    MemoryOrder successMemoryOrder)
{
    uint32_t expectValue = compareValue;
    *result = AtomicU32CompareExchangeStrong(value, &expectValue, writeValue, successMemoryOrder,
        MEMORY_ORDER_RELAXED);
    uint32_t curValue = AtomicU32Get(value, successMemoryOrder);
    if (*result) {
        ASSERT_EQ(writeValue, curValue);
    } else {
        ASSERT_EQ(expectValue, curValue);
    }
}

TEST_F(PortAtomicTest, Uint32CompareExchangeStrongMultiThreadTest001)
{
    MemoryOrder successMemoryOrderList[] = {MEMORY_ORDER_ACQ_REL, MEMORY_ORDER_SEQ_CST};
    for (auto &memoryOrder : successMemoryOrderList) {
        AtomicU32 u32Value;
        AtomicU32Set(&u32Value, 0, memoryOrder);
        ASSERT_EQ(AtomicU32Get(&u32Value, memoryOrder), 0);
        bool resList[TEST_THREAD_COUNT];
        memset_s(resList, sizeof(resList), 0, sizeof(resList));
        std::thread threads[TEST_THREAD_COUNT];
        for (uint32_t i = 0; i < TEST_THREAD_COUNT; ++i) {
            threads[i] = std::thread(TestAtomicU32CompareExchangeStrongFunc, &u32Value, 0, 1, &resList[i],
                memoryOrder);
        }
        for (auto &thread : threads) {
            thread.join();
        }
        uint32_t trueCount = 0;
        for (auto res : resList) {
            if (res) {
                trueCount++;
            }
        }
        ASSERT_EQ(trueCount, 1);
        ASSERT_EQ(AtomicU32Get(&u32Value, memoryOrder), 1);
    }
}

void TestAtomicU32CompareExchangeWeakFunc(AtomicU32 *value, uint32_t compareValue, uint32_t writeValue, bool *result,
    MemoryOrder successMemoryOrder)
{
    uint32_t expectValue = compareValue;
    *result = AtomicU32CompareExchangeStrong(value, &expectValue, writeValue, successMemoryOrder,
        MEMORY_ORDER_RELAXED);
    uint32_t curValue = AtomicU32Get(value, successMemoryOrder);
    if (*result) {
        ASSERT_EQ(writeValue, curValue);
    }
}

TEST_F(PortAtomicTest, Uint32CompareExchangeWeakMultiThreadTest001)
{
    MemoryOrder successMemoryOrderList[] = {MEMORY_ORDER_ACQ_REL, MEMORY_ORDER_SEQ_CST};
    for (auto &memoryOrder : successMemoryOrderList) {
        AtomicU32 u32Value;
        AtomicU32Set(&u32Value, 0, memoryOrder);
        ASSERT_EQ(AtomicU32Get(&u32Value, memoryOrder), 0);
        bool resList[TEST_THREAD_COUNT];
        memset_s(resList, sizeof(resList), 0, sizeof(resList));
        std::thread threads[TEST_THREAD_COUNT];
        for (uint32_t i = 0; i < TEST_THREAD_COUNT; ++i) {
            threads[i] = std::thread(TestAtomicU32CompareExchangeWeakFunc, &u32Value, 0, 1, &resList[i],
                memoryOrder);
        }
        for (auto &thread : threads) {
            thread.join();
        }
        uint32_t trueCount = 0;
        for (auto res : resList) {
            if (res) {
                trueCount++;
            }
        }
        ASSERT_EQ(trueCount, 1);
        ASSERT_EQ(AtomicU32Get(&u32Value, memoryOrder), 1);
    }
}

void TestAtomicU64AddFunc(AtomicU64 *value, uint64_t add, uint32_t repeat, MemoryOrder memory, TestValueRange range)
{
    for (uint32_t i = 0; i < repeat; ++i) {
        uint64_t curRes = AtomicU64FetchAdd(value, add, memory);
        ASSERT_GE(curRes, range.minVal.u64Min);
        ASSERT_LE(curRes, range.maxVal.u64Max);
    }
}

void TestAtomicU64SubFunc(AtomicU64 *value, uint64_t sub, uint32_t repeat, MemoryOrder memory, TestValueRange range)
{
    for (uint32_t i = 0; i < repeat; ++i) {
        uint64_t curRes = AtomicU64FetchSub(value, sub, memory);
        ASSERT_GE(curRes, range.minVal.u64Min);
        ASSERT_LE(curRes, range.maxVal.u64Max);
    }
}

TEST_F(PortAtomicTest, Uint64AddMultiThreadTest001)
{
    for (auto &memoryOrder : supportMultiThreadMemoryList) {
        AtomicU64 u64Value;
        uint64_t initValue = UINT32_MAX;
        AtomicU64Set(&u64Value, initValue, memoryOrder);
        uint32_t testRepeatTime = 10000;
        uint64_t addValue = 60;
        uint64_t expectRes = initValue + addValue * testRepeatTime * TEST_THREAD_COUNT;
        TestValueRange valueRange = {};
        valueRange.minVal.u64Min = initValue;
        valueRange.maxVal.u64Max = expectRes;
        std::thread threads[TEST_THREAD_COUNT];
        for (auto &thread : threads) {
            thread = std::thread(TestAtomicU64AddFunc, &u64Value, addValue, testRepeatTime, memoryOrder, valueRange);
        }
        for (auto &thread : threads) {
            thread.join();
        }
        uint64_t result = AtomicU64Get(&u64Value, memoryOrder);
        ASSERT_EQ(result, expectRes);
    }
}

TEST_F(PortAtomicTest, Uint64SubMultiThreadTest001)
{
    for (auto &memoryOrder : supportMultiThreadMemoryList) {
        AtomicU64 u64Value;
        uint64_t initValue = (uint64_t)1000 + UINT32_MAX;
        AtomicU64Set(&u64Value, initValue, memoryOrder);
        uint32_t testRepeatTime = 10000;
        uint64_t subValue = 80;
        uint64_t expectRes = initValue - subValue * testRepeatTime * TEST_THREAD_COUNT;
        TestValueRange valueRange = {};
        valueRange.minVal.u64Min = expectRes;
        valueRange.maxVal.u64Max = initValue;
        std::thread threads[TEST_THREAD_COUNT];
        for (auto &thread : threads) {
            thread = std::thread(TestAtomicU64SubFunc, &u64Value, subValue, testRepeatTime, memoryOrder, valueRange);
        }
        for (auto &thread : threads) {
            thread.join();
        }
        uint64_t result = AtomicU64Get(&u64Value, memoryOrder);
        ASSERT_EQ(result, expectRes);
    }
}

void ReleaseAtomicU32(AtomicU32 *u32Value, uint32_t setValue, uint32_t *data, uint32_t dataSetValue)
{
    *data = dataSetValue;
    /* All other thread will get *data = dataSetValue if u32Value equal to setValue */
    AtomicU32Set(u32Value, setValue, MEMORY_ORDER_RELEASE);
}

void AcquireAtomicU32(AtomicU32 *u32Value, uint32_t setValue, uint32_t *data, uint32_t dataExpectValue)
{
    uint32_t retryMaxTime = 100000;
    uint32_t curRetryTime = 0;
    for (;;) {
        uint32_t curValue = AtomicU32Get(u32Value, MEMORY_ORDER_ACQUIRE);
        if (curValue == setValue) {
            break;
        }
        if (curRetryTime < retryMaxTime) {
            curRetryTime++;
            Usleep(100);
        } else {
            ASSERT_TRUE(false);
        }
    }
    ASSERT_EQ(*data, dataExpectValue);
}

TEST_F(PortAtomicTest, Uint32AcqRelTest001)
{
    AtomicU32 atomicU32;
    uint32_t data;
    for (uint32_t i = 0; i < TEST_LOOP_COUNT; ++i) {
        data = 0;
        AtomicU32Set(&atomicU32, 0, MEMORY_ORDER_RELEASE);
        std::thread loadThread = std::thread(AcquireAtomicU32, &atomicU32, 1, &data, 1);
        std::thread storeThread = std::thread(ReleaseAtomicU32, &atomicU32, 1, &data, 1);
        loadThread.join();
        storeThread.join();
    }
}

void TestWriteAtomicBool(AtomicBool *boolVal, bool writeVal, MemoryOrder memoryOrder)
{
    AtomicBoolSet(boolVal, writeVal, memoryOrder);
}

void TestReadAndAdd(AtomicBool *readVal, AtomicBool *checkVal, AtomicU32 *resVal, MemoryOrder memoryOrder)
{
    while (!AtomicBoolGet(readVal, memoryOrder)) {
    }
    if (AtomicBoolGet(checkVal, memoryOrder)) {
        AtomicU32FetchAdd(resVal, 1, memoryOrder);
    }
}

TEST_F(PortAtomicTest, MultiAtomicTest001)
{
    MemoryOrder memoryOrder = MEMORY_ORDER_SEQ_CST;
    AtomicBool boolVal1, boolVal2;
    AtomicU32 u32Value;
    for (uint32_t i = 0; i < TEST_LOOP_COUNT; ++i) {
        AtomicBoolSet(&boolVal1, false, memoryOrder);
        AtomicBoolSet(&boolVal2, false, memoryOrder);
        AtomicU32Set(&u32Value, 0, memoryOrder);
        std::thread writeThread1 = std::thread(TestWriteAtomicBool, &boolVal1, true, memoryOrder);
        std::thread writeThread2 = std::thread(TestWriteAtomicBool, &boolVal2, true, memoryOrder);
        std::thread readWriteThread1 = std::thread(TestReadAndAdd, &boolVal1, &boolVal2, &u32Value, memoryOrder);
        std::thread readWriteThread2 = std::thread(TestReadAndAdd, &boolVal2, &boolVal1, &u32Value, memoryOrder);
        writeThread1.join();
        writeThread2.join();
        readWriteThread1.join();
        readWriteThread2.join();
        ASSERT_GT(AtomicU32Get(&u32Value, memoryOrder), 0);
    }
}

Atomic8 g_initializedOnlyOnceSuccessTestVariable = ATOMIC_INIT(0);
ErrorCode InitializedOnlyOnceSuccessCallbackFunction(void *arg)
{
    int8_t oldValue;
    oldValue = Atomic8FetchAdd(&g_initializedOnlyOnceSuccessTestVariable, 1, MEMORY_ORDER_SEQ_CST);
    return ERROR_SYS_OK;
}
AtomicU8 g_initialOnlyOnceSuccessTestAtomicU8 = ATOMIC_INIT(ATOMICU8_INSTANCE_INITIAL_VALUE);
AtomicBool g_threadExecutesSuccessBarrierAtomicBool = ATOMIC_INIT(false);
void *InitializedOnlyOnceSuccessTestThread(void *arg)
{
    while (!AtomicBoolGet(&g_threadExecutesSuccessBarrierAtomicBool, MEMORY_ORDER_ACQUIRE)) {
        /* Do nothing, wait g_threadExecutesBarrierAtomicBool is true.
         * Ensure that multiple threads perform post-processing at the same time.*/
    }
    ErrorCode errorCode =
            InitializedOnlyOnce(&g_initialOnlyOnceSuccessTestAtomicU8, InitializedOnlyOnceSuccessCallbackFunction, arg);
    *(ErrorCode *)arg = errorCode;
    return arg;
}

TEST_F(PortAtomicTest, InitializedOnlyOnceSuccessTest001)
{
#define INITIALIZED_ONLY_ONCE_TEST_THREAD_COUNT 3
    Tid tid[INITIALIZED_ONLY_ONCE_TEST_THREAD_COUNT];
    ErrorCode threadErrorCode[INITIALIZED_ONLY_ONCE_TEST_THREAD_COUNT];
    ErrorCode errorCode[INITIALIZED_ONLY_ONCE_TEST_THREAD_COUNT];
    ErrorCode *threadErrorCodePointer[INITIALIZED_ONLY_ONCE_TEST_THREAD_COUNT];
    int i;
    for (i = 0; i < INITIALIZED_ONLY_ONCE_TEST_THREAD_COUNT; i++) {
        errorCode[i] = ThreadCreate(tid + i, InitializedOnlyOnceSuccessTestThread, threadErrorCode + i);
        ASSERT_EQ(errorCode[i], ERROR_SYS_OK);
    }
    /* Barrier synchronization and let the thread run. */
    AtomicBoolSet(&g_threadExecutesSuccessBarrierAtomicBool, true, MEMORY_ORDER_RELEASE);
    for (i = 0; i < INITIALIZED_ONLY_ONCE_TEST_THREAD_COUNT; i++) {
        threadErrorCodePointer[i] = threadErrorCode + i;
        errorCode[i] = ThreadJoin(tid[i], (void **)&(threadErrorCodePointer[i]));
        ASSERT_EQ(errorCode[i], ERROR_SYS_OK);
        ASSERT_EQ(threadErrorCode[i], ERROR_SYS_OK);
    }
    /* Verify that only one thread is initialized and the atomic variable is incremented by 1. */
    int8_t currentValue = Atomic8Get(&g_initializedOnlyOnceSuccessTestVariable, MEMORY_ORDER_SEQ_CST);
    ASSERT_EQ(currentValue, 1);
}

Atomic8 g_initializedOnlyOnceFailedTestVariable = ATOMIC_INIT(0);
ErrorCode InitializedOnlyOnceFailedCallbackFunction(void *arg)
{
    int8_t oldValue;
    oldValue = Atomic8FetchAdd(&g_initializedOnlyOnceFailedTestVariable, 1, MEMORY_ORDER_SEQ_CST);
    return ERROR_UTILS_COMMON_FAILED;
}
AtomicU8 g_initialOnlyOnceFailedTestAtomicU8 = ATOMIC_INIT(ATOMICU8_INSTANCE_INITIAL_VALUE);
AtomicBool g_threadExecutesFailedBarrierAtomicBool = ATOMIC_INIT(false);
void *InitializedOnlyOnceFailedTestThread(void *arg)
{
    while (!AtomicBoolGet(&g_threadExecutesFailedBarrierAtomicBool, MEMORY_ORDER_ACQUIRE)) {
        /* Do nothing, wait g_threadExecutesBarrierAtomicBool is true.
         * Ensure that multiple threads perform post-processing at the same time.*/
    }
    ErrorCode errorCode =
            InitializedOnlyOnce(&g_initialOnlyOnceFailedTestAtomicU8, InitializedOnlyOnceFailedCallbackFunction, arg);
    *(ErrorCode *)arg = errorCode;
    return arg;
}

TEST_F(PortAtomicTest, InitializedOnlyOnceFailedTest001)
{
#define INITIALIZED_ONLY_ONCE_TEST_THREAD_COUNT 3
    Tid tid[INITIALIZED_ONLY_ONCE_TEST_THREAD_COUNT];
    ErrorCode threadErrorCode[INITIALIZED_ONLY_ONCE_TEST_THREAD_COUNT];
    ErrorCode errorCode[INITIALIZED_ONLY_ONCE_TEST_THREAD_COUNT];
    ErrorCode *threadErrorCodePointer[INITIALIZED_ONLY_ONCE_TEST_THREAD_COUNT];
    int i;
    for (i = 0; i < INITIALIZED_ONLY_ONCE_TEST_THREAD_COUNT; i++) {
        errorCode[i] = ThreadCreate(tid + i, InitializedOnlyOnceFailedTestThread, threadErrorCode + i);
        ASSERT_EQ(errorCode[i], ERROR_SYS_OK);
    }
    /* Barrier synchronization and let the thread run. */
    AtomicBoolSet(&g_threadExecutesFailedBarrierAtomicBool, true, MEMORY_ORDER_RELEASE);
    for (i = 0; i < INITIALIZED_ONLY_ONCE_TEST_THREAD_COUNT; i++) {
        threadErrorCodePointer[i] = threadErrorCode + i;
        errorCode[i] = ThreadJoin(tid[i], (void **)&(threadErrorCodePointer[i]));
        ASSERT_EQ(errorCode[i], ERROR_SYS_OK);
        /* All threads return initialization failure. */
        ASSERT_EQ(threadErrorCode[i], ERROR_UTILS_COMMON_FAILED);
    }
    /* The failure is just a simulation failure, and the variable should have been successfully increased by 1. */
    int8_t currentValue = Atomic8Get(&g_initializedOnlyOnceFailedTestVariable, MEMORY_ORDER_SEQ_CST);
    ASSERT_EQ(currentValue, 1);
}