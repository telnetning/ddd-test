
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
 * Description: test mpmc queue
 */

#include "gtest/gtest.h"
#include <thread>
#include <atomic>
#include <chrono>
#include <iostream>
#include <condition_variable>

#include "container/mpmc_queue.h"

using namespace std;

#undef ENABLE_DEBUG_PRINT

#define ARRAY_SIZE(a) sizeof(a)/sizeof(a[0])

static void MpmcQueueDumpStatus(const MpmcQueue *queue, const char *title)
{
    ASSERT_TRUE(queue != NULL);
    ASSERT_TRUE(title != NULL);

    MpmcQueueStat stat;
    MpmcQueueGetStatus(queue, &stat);

#ifdef ENABLE_DEBUG_PRINT
    printf("--------------------------------------\n");
    printf("dump detail of queue(%s, %p) for %s\n", stat.name, queue, title);
    printf(" capacity:%lu\n", stat.capacity + 1);
    printf(" push count:%lu\n", stat.pushCount);
    printf(" pop  count:%lu\n", stat.popCount);
    printf(" element size:%u\n", stat.elementSize);
#endif

    for (uint64_t i = 0; i < stat.capacity; i++) {
        uint8_t data[stat.elementSize];
        MQElementType popData = &data;
        if (MpmcQueueGetElement(queue, i, &popData)){
#ifdef ENABLE_DEBUG_PRINT
            printf("slots[%lu]: data:", i);
            for (uint32_t i = 0; i < stat.elementSize; i++) {
                printf("0x%02x ", data[i]);
            }
            printf("\n");
#endif
        }
    }
#ifdef ENABLE_DEBUG_PRINT
    printf("dump finish\n");
#endif
}

class MPMCQueueTest: public testing::Test {
public:
    static void SetUpTestSuite()
    {
    }

    static void TearDownTestSuite()
    {
    }

    void SetUp() override
    {
    };

    void TearDown() override
    {
    };
};

TEST_F(MPMCQueueTest, TestWhenQueueHandleIsNULLThenFail)
{
    bool ret = MpmcQueuePush(NULL, (MQElementType)0x1234);
    ASSERT_FALSE(ret);

    MQElementType popData;
    ret = MpmcQueuePop(NULL, &popData);
    ASSERT_FALSE(ret);

    ret = MpmcQueueIsEmpty(NULL);
    ASSERT_FALSE(ret);

    ret = MpmcQueueIsFull(NULL);
    ASSERT_FALSE(ret);

    ret = MpmcQueueGetCapacity(NULL);
    ASSERT_FALSE(ret);

    // expect function execution complete
    MpmcQueueStat stat;
    MpmcQueueGetStatus(NULL, &stat);
    MpmcQueueGetStatus(NULL, NULL);

    MQElementType data;
    ret = MpmcQueueGetElement(NULL, 0, &data);
    ASSERT_FALSE(ret);
}

TEST_F(MPMCQueueTest, TestCreateWithInvalidName)
{
    MpmcQueue *queue;

    // create success when name is null pointer
    queue = MpmcQueueCreate(NULL, 8);
    ASSERT_TRUE(queue != NULL);
    MpmcQueueDestroy(queue);

    // create success when name is empty string
    queue = MpmcQueueCreate("", 8);
    ASSERT_TRUE(queue != NULL);
    MpmcQueueDestroy(queue);

    // create success when the length of name is valid
    char nameOK[MPMC_QUEUE_NAME_SIZE] = {0};
    memset(nameOK, 'a', sizeof(nameOK) - 1);

    queue = MpmcQueueCreate(nameOK, 8);
    ASSERT_TRUE(queue != NULL);
    MpmcQueueDestroy(queue);

    // create success when the length of name is invalid
    char nameInvalid[MPMC_QUEUE_NAME_SIZE + 1] = {0};
    memset(nameInvalid, 'a', sizeof(nameInvalid) - 1);

    queue = MpmcQueueCreate(nameInvalid, 8);
    ASSERT_TRUE(queue == NULL);
}

TEST_F(MPMCQueueTest, TestCreateWithDiffSize)
{
    // we expect the capacity of queue is always the power of two
    // 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024,
    // 2048, 4096, 8192, 16384, 32768, 65536, 131072
    // 262144, 524288, 1048576, 2097152, 4194304
    struct TestSizeAndCapacity {
        uint64_t allocateSize;
        uint64_t expectCapacity;
    } tests[] = {
        {0, MPMC_QUEUE_MIN_CAPACITY},
        {1, 1},
        {2, 2},
        {3, 4}, {4, 4},
        {5, 8}, {8, 8},
        {100, 128}, {128, 128},
        {200, 256},
        {500, 512},
        {800, 1024},
        {1111, 2048},
        {3000, 4096},
        {5000, 8192},
        {1000*1000, 1048576}, // Support at least 1 million
        {MPMC_QUEUE_MAX_CAPACITY-1, MPMC_QUEUE_MAX_CAPACITY},
        // reach max size
        {MPMC_QUEUE_MAX_CAPACITY,   MPMC_QUEUE_MAX_CAPACITY},
        {MPMC_QUEUE_MAX_CAPACITY+1, MPMC_QUEUE_MAX_CAPACITY},
        {-1, MPMC_QUEUE_MAX_CAPACITY},
    };

    for (int i = 0; i < sizeof(tests)/sizeof(tests[0]); i++) {
        MpmcQueue *queue = MpmcQueueCreate(NULL, tests[i].allocateSize);
        ASSERT_TRUE(queue != NULL);

        uint64_t capacity = MpmcQueueGetCapacity(queue);
        ASSERT_EQ(capacity, tests[i].expectCapacity);
        MpmcQueueDestroy(queue);
    }
}

// Test case:
// When queue is empty then MpmcQueueIsEmpty return true
// When queue is not empty then MpmcQueueIsEmpty return false
TEST_F(MPMCQueueTest, TestIsEmpty)
{
    MpmcQueue *queue = MpmcQueueCreate(NULL, 8);
    ASSERT_TRUE(queue != NULL);

    // queue is empty when begin
    bool isEmpty = MpmcQueueIsEmpty(queue);
    ASSERT_TRUE(isEmpty);

    MQElementType data = (MQElementType)0x1234;
    bool ret = MpmcQueuePush(queue, data);
    ASSERT_TRUE(ret);

    // queue is not empty after push
    isEmpty = MpmcQueueIsEmpty(queue);
    ASSERT_FALSE(isEmpty);

    MQElementType popData;
    ret = MpmcQueuePop(queue, &popData);
    ASSERT_TRUE(ret);
    ASSERT_TRUE(popData == data);

    // queue is not empty after pop
    isEmpty = MpmcQueueIsEmpty(queue);
    ASSERT_TRUE(isEmpty);

    MpmcQueueDestroy(queue);
}

TEST_F(MPMCQueueTest, TestFull)
{
    uint64_t size = 4;
    MpmcQueue *queue = MpmcQueueCreate(NULL, size);
    ASSERT_TRUE(queue != NULL);

    // queue is not full when begin
    bool isFull = MpmcQueueIsFull(queue);
    ASSERT_FALSE(isFull);

    for (uint64_t i=0; i<size; i++) {
        // queue is not full
        isFull = MpmcQueueIsFull(queue);
        ASSERT_FALSE(isFull);

        MQElementType data = (MQElementType)(i + 0x1);
        bool ret = MpmcQueuePush(queue, data);
        ASSERT_TRUE(ret);
    }

    // queue is full after push finish
    isFull = MpmcQueueIsFull(queue);
    ASSERT_TRUE(isFull);

    MQElementType popData;
    bool ret = MpmcQueuePop(queue, &popData);
    ASSERT_TRUE(ret);

    // queue is not full after pop
    isFull = MpmcQueueIsFull(queue);
    ASSERT_FALSE(isFull);

    MpmcQueueDestroy(queue);
}

TEST_F(MPMCQueueTest, TestDumpStatusWhenPushPop)
{
    uint64_t size = 4;
    MpmcQueue *queue = MpmcQueueCreate(NULL, size);
    ASSERT_TRUE(queue != NULL);

    MpmcQueueDumpStatus(queue, "after create queue");

    for (uint64_t i=0; i<size; i++) {
        MQElementType data = (MQElementType)(i + 0x1);
        bool ret = MpmcQueuePush(queue, data);
        ASSERT_TRUE(ret);
    }

    MpmcQueueDumpStatus(queue, "after push 4 elements");

    // fail to push because the queue is full
    bool ret = MpmcQueuePush(queue, (MQElementType)(0x0));
    ASSERT_FALSE(ret);

    MQElementType popData;
    for (uint64_t i=0; i<size; i++) {
        ret = MpmcQueuePop(queue, &popData);
        ASSERT_TRUE(ret);
    }

    MpmcQueueDumpStatus(queue, "after pop 4 elements");

    // fail to pop because the queue is empty
    ret = MpmcQueuePop(queue, &popData);
    ASSERT_FALSE(ret);

    for (uint64_t i=0; i<size; i++) {
        MQElementType data = (MQElementType)(i + 0x41);
        bool ret = MpmcQueuePush(queue, data);
        ASSERT_TRUE(ret);
    }

    MpmcQueueDumpStatus(queue, "after push 4 elements again");

    MpmcQueueDestroy(queue);
}

static void pushByCount(uint64_t queueSize, uint64_t pushCount)
{
    MpmcQueue *queue = MpmcQueueCreate(NULL, queueSize);
    ASSERT_TRUE(queue != NULL);

    for (uint64_t i = 1; i <= pushCount; i++) {
        MQElementType data = (MQElementType)i;
        if (!MpmcQueuePush(queue, data)) {
            // if push fail, then pop all data and try again
            for (;;) {
                MQElementType popData;
                if (MpmcQueuePop(queue, &popData)) {
                    break;
                }
            }
            bool ret = MpmcQueuePush(queue, data);
            ASSERT_TRUE(ret);
        }
    }

    string strCount = to_string(pushCount);
    string strTitle = "test push " + strCount;

    MpmcQueueDumpStatus(queue, strTitle.c_str());

    MQElementType datas[pushCount];
    uint32_t popCount = pushCount;
    bool ret = MpmcQueuePopN(queue, datas, &popCount);

    MpmcQueueDestroy(queue);
}

TEST_F(MPMCQueueTest, TestViewTheDetailOfQueue)
{
    pushByCount(10, 10);
    pushByCount(10, 16);
}

TEST_F(MPMCQueueTest, TestPushWhenQueueIsFullThenFail)
{
    uint64_t size = 4;
    MpmcQueue *queue = MpmcQueueCreate(NULL, size);
    ASSERT_TRUE(queue != NULL);

    for (uint64_t i=0; i<size; i++) {
        MQElementType data = (MQElementType)(i + 0x1);
        bool ret = MpmcQueuePush(queue, data);
        ASSERT_TRUE(ret);
    }

    bool isFull = MpmcQueueIsFull(queue);
    ASSERT_TRUE(isFull);

    // fail to push because the queue is full
    bool ret = MpmcQueuePush(queue, (MQElementType)(0x0));
    ASSERT_FALSE(ret);

    MpmcQueueDestroy(queue);
}

TEST_F(MPMCQueueTest, TestPopWhenQueueIsEmptyThenFail)
{
    uint64_t size = 4;
    MpmcQueue *queue = MpmcQueueCreate(NULL, size);
    ASSERT_TRUE(queue != NULL);

    bool ret = MpmcQueuePush(queue, (MQElementType)0x1);
    ASSERT_TRUE(ret);

    bool isEmpty = MpmcQueueIsEmpty(queue);
    ASSERT_FALSE(isEmpty);

    MQElementType popData;
    ret = MpmcQueuePop(queue, &popData);
    ASSERT_TRUE(ret);

    isEmpty = MpmcQueueIsEmpty(queue);
    ASSERT_TRUE(isEmpty);

    // fail to pop because the queue is empty
    ret = MpmcQueuePop(queue, &popData);
    ASSERT_FALSE(ret);

    MpmcQueueDestroy(queue);
}

static void testComplexData(uint32_t queueSize, const uint8_t *data, uint32_t dataSize)
{
    MpmcQueue *queue = MpmcQueueCreateEx(NULL, queueSize, dataSize);
    if (dataSize <= sizeof(MQElementType)) {
        ASSERT_TRUE(queue == NULL);
        return;
    }

    ASSERT_TRUE(queue != NULL);

    bool ret = MpmcQueuePush(queue, (MQElementType)data);
    ASSERT_TRUE(ret);

    uint8_t output[dataSize];
    MQElementType popData = &output;
    ret = MpmcQueuePop(queue, &popData);
    ASSERT_TRUE(ret);

    ASSERT_EQ(0, memcmp(output, data, dataSize));

    MpmcQueueDestroy(queue);
}

TEST_F(MPMCQueueTest, TestPopWithComplexData)
{
    /**
     * @tc.steps: step1. Test MpmcQueueCreateEx with dataSize (<= 8)
     * @tc.expected: step1. Create queue failure
     */
    char tc[9] = {0x11, 0x22, 0x33, 0x44, 0x1F, 0x2F, 0x3F, 0x4F, 0xAA};
    for (uint32_t i = 1; i <= 8; i++) {
        testComplexData(4, (uint8_t*)tc, i);
    }

    /**
     * @tc.steps: step2. Test MpmcQueueCreateEx with dataSize = 9
     * @tc.expected: step2. Create queue success, and push&pop success
     */
    testComplexData(4, (uint8_t*)tc, sizeof(tc));

    /**
     * @tc.steps: step3. Test MpmcQueueCreateEx with complex data structures
     * @tc.expected: step3. Create queue success, and push&pop success
     */
    typedef struct myData {
        int a;
        int b;
        int *p;
        char c[20];
    } myData;

    myData input = {1, 2, (int*)0x100, "this is test"};

    testComplexData(4, (uint8_t*)&input, sizeof(myData));
}

TEST_F(MPMCQueueTest, TestPopNWithComplexData)
{
    /**
     * @tc.steps: step1. Test popN with complex data
     * @tc.expected: step1. popN success
     */
    typedef struct myData {
        int a; int b; int *p; char c[20];
    } myData;

    uint64_t size = 4;
    MpmcQueue *queue = MpmcQueueCreateEx(NULL, size, sizeof(myData));
    ASSERT_TRUE(queue != NULL);

    const int N = 2;
    myData inputs[N] = {
        {1, 2, (int*)0x100, "this is A test"},
        {8, 9, (int*)0xabc, "this is B test"},
    };

    for (int i = 0; i < ARRAY_SIZE(inputs); i++) {
        bool ret = MpmcQueuePush(queue, (MQElementType)&inputs[i]);
        ASSERT_TRUE(ret);
    }

    MpmcQueueDumpStatus(queue, "after push for complex data");

    MpmcQueueStat stat;
    bool ret = MpmcQueueGetStatus(queue, &stat);
    ASSERT_TRUE(ret);

    bool isEmpty = MpmcQueueIsEmpty(queue);
    ASSERT_FALSE(isEmpty);

    myData outputs[N] = {0};
    uint32_t popCount = N;
    ret = MpmcQueuePopN(queue, (MQElementType*)&outputs, &popCount);
    ASSERT_EQ(popCount, N);
    ASSERT_TRUE(ret);

    for (int i = 0; i < ARRAY_SIZE(inputs); i++) {
        ASSERT_EQ(0, memcmp(&outputs[i], &inputs[i], sizeof(myData)));
    }

    MpmcQueueDestroy(queue);
}

//////////////////////////////////////////////////////////////////////
// Test for multiple thread

condition_variable g_cv;
mutex g_cv_m;
bool g_startTest = false;

atomic_int64_t g_pushCount = {0};
static void ProducerThreadFunc(MpmcQueue *queue, uint32_t maxPushCount)
{
    {
        unique_lock<mutex> lk(g_cv_m);
        g_cv.wait(lk, []{return g_startTest;});
    }

    int64_t count = 0;
    for (;;) {
        MQElementType data = (MQElementType)0x1234;
        if (MpmcQueuePush(queue, data)) {
            g_pushCount++;
            count++;
        }
        if (count >= maxPushCount) {
            break;
        }
    }

#ifdef ENABLE_DEBUG_PRINT
    {
        thread::id this_id = this_thread::get_id();
        unique_lock<mutex> lk(g_cv_m); // Use locks to avoid cluttered printing
        cout << "thread:" << this_id << ", produce count " << count << " when g_pushCount " << g_pushCount << endl;
    }
#endif
}

atomic_int64_t g_popCount = {0};
static void ConsumerThreadFunc(MpmcQueue *queue, uint32_t maxPopCount)
{
    {
        unique_lock<mutex> lk(g_cv_m);
        g_cv.wait(lk, []{return g_startTest;});
    }

    int64_t count = 0;

    for (;;) {
        MQElementType popData;
        if (MpmcQueuePop(queue, &popData)) {
            g_popCount++;
            count++;
        }
        if (g_popCount >= maxPopCount) {
            break;
        }
    }

#ifdef ENABLE_DEBUG_PRINT
    {
        thread::id this_id = this_thread::get_id();
        unique_lock<mutex> lk(g_cv_m); // Use locks to avoid cluttered printing
        cout << "thread:" << this_id << ", consume count " << count << " when g_popCount " << g_popCount << endl;
    }
#endif
}

static void doPushPopCountTest(uint64_t queueSize, uint64_t pushCount, uint64_t prodNum, uint64_t consNum)
{
    // We need to make sure that the sum of the average amount of data produced by each producer
    // is just equal to the total amount of data, otherwise the consumer code can not finish.
    //static_assert(pushCount/prodNum*prodNum == pushCount);
    ASSERT_EQ(pushCount/prodNum*prodNum, pushCount);

    MpmcQueue *queue = MpmcQueueCreate(NULL, queueSize);
    ASSERT_TRUE(queue != NULL);

    // clear
    g_pushCount = 0;
    g_popCount = 0;
    g_startTest = false;

    thread threadProd[prodNum];
    for (int i = 0; i < prodNum; i++) {
        threadProd[i] = thread(ProducerThreadFunc, queue, pushCount/prodNum);
    }

    thread threadCons[consNum];
    for (int i = 0; i < consNum; i++) {
        threadCons[i] = thread(ConsumerThreadFunc, queue, pushCount);
    }

    // OK, let's start produce-consume test
    {
        lock_guard<mutex> lk(g_cv_m);
        g_startTest = true;
    }
    g_cv.notify_all();

    // wait all thread finish
    for (auto i = 0; i < prodNum; i++) {
        threadProd[i].join();
    }

    for (auto i = 0; i < consNum; i++) {
        threadCons[i].join();
    }

    MpmcQueueDestroy(queue);

    ASSERT_EQ(g_pushCount, pushCount);
    EXPECT_EQ(g_pushCount, g_popCount);
}

TEST_F(MPMCQueueTest, TestPopPushCountForMutipleThreads)
{
    doPushPopCountTest(3000, 10000, 1, 1);
    doPushPopCountTest(3000, 10000, 1, 2);
    doPushPopCountTest(3000, 10000, 2, 1);
    doPushPopCountTest(3000, 10000, 2, 2);
    doPushPopCountTest(3000, 30000, 3, 3);
}

TEST_F(MPMCQueueTest, TestPerf)
{
    const uint64_t QUEUE_SIZE = 3000;
    const uint64_t totalPush = 1000000;
    const uint64_t PROD_NUM = 10;
    const uint64_t CONS_NUM = 10;

    auto start = chrono::high_resolution_clock::now();

    doPushPopCountTest(QUEUE_SIZE, totalPush, PROD_NUM, CONS_NUM);

    auto stop = chrono::high_resolution_clock::now();
    auto duration = chrono::duration_cast<chrono::microseconds>(stop - start);
    cout << "elapsed time " << duration.count() << " microseconds" <<
        ", pushCount:" << g_pushCount << ", popCount:" << g_popCount << endl;

    ASSERT_EQ(g_pushCount, totalPush);
    ASSERT_EQ(g_popCount, g_pushCount);
}

// Sequential push data between 'start' and 'end'
static void SeqPushFunc(MpmcQueue *queue, uint32_t start, uint32_t end)
{
    {
        unique_lock<mutex> lk(g_cv_m);
        g_cv.wait(lk, []{return g_startTest;});
#ifdef ENABLE_DEBUG_PRINT
        cout << "enter:" << __FUNCTION__
            << ",start=" << start
            << ",end=" << end
            << endl;
#endif
    }

    int64_t count = 0;
    for (uint32_t i = start; i <= end; ) {
        MQElementType data = (MQElementType)i;
        if (MpmcQueuePush(queue, data)) {
            i++;
            g_pushCount++;
            count++;
        }
    }

    {
#ifdef ENABLE_DEBUG_PRINT
        thread::id this_id = this_thread::get_id();
        unique_lock<mutex> lk(g_cv_m); // Use locks to avoid cluttered printing
        cout << "thread:" << this_id << ", produce count " << count << " when g_pushCount " << g_pushCount << endl;
        //MpmcQueueDumpStatus(queue, "popN producer");
#endif
    }
}

atomic_uint64_t g_popSum = {0};
static void ConsumerCalcSumFunc(MpmcQueue *queue, uint32_t maxPopCount)
{
    {
        unique_lock<mutex> lk(g_cv_m);
        g_cv.wait(lk, []{return g_startTest;});
    }

    int64_t count = 0;

    for (;;) {
        MQElementType popData;
        if (MpmcQueuePop(queue, &popData)) {
            g_popCount++;
            count++;
            g_popSum += (uint64_t)popData; // calculate sum
        }
        if (g_popCount >= maxPopCount) {
            break;
        }
    }

    {
#ifdef ENABLE_DEBUG_PRINT
        thread::id this_id = this_thread::get_id();
        unique_lock<mutex> lk(g_cv_m); // Use locks to avoid cluttered printing
        cout << "thread:" << this_id << ", consume count " << count << " when g_popCount " << g_popCount << endl;
#endif
    }
}

static void doSumTest(uint64_t queueSize, uint64_t prodNum, uint64_t consNum, uint64_t maxValue)
{
    ASSERT_EQ(maxValue%prodNum, 0);
    ASSERT_TRUE(prodNum > 0);
    ASSERT_TRUE(consNum > 0);

    // clear
    g_pushCount = 0;
    g_popCount = 0;
    g_startTest = false;
    g_popSum = 0;

    MpmcQueue *queue = MpmcQueueCreate(NULL, queueSize);
    ASSERT_TRUE(queue != NULL);

    // Data is pushed in sequence, starting from 1 to the maximum value maxValue.
    thread threadProd[prodNum];
    uint64_t step = maxValue / prodNum;
    uint64_t start = 1;
    uint64_t end = start + step - 1;
    for (int i = 0; i < prodNum; i++) {
        threadProd[i] = thread(SeqPushFunc, queue, start, end);
        start = end + 1;
        end = start + step - 1;
    }

    thread threadCons[consNum];
    for (int i = 0; i < consNum; i++) {
        threadCons[i] = thread(ConsumerCalcSumFunc, queue, maxValue);
    }

    // OK, let's start produce-consume test
    {
        lock_guard<mutex> lk(g_cv_m);
        g_startTest = true;
    }
    g_cv.notify_all();

    // wait all thread finish
    for (auto i = 0; i < prodNum; i++) {
        threadProd[i].join();
    }

    for (auto i = 0; i < consNum; i++) {
        threadCons[i].join();
    }

    MpmcQueueDestroy(queue);

    // check, we expect the sum of the pushed data is equal to the sum of pop up data
    uint64_t pushSum = (1 + maxValue) * maxValue / 2;

    ASSERT_EQ(pushSum, g_popSum);
#ifdef ENABLE_DEBUG_PRINT
    cout << "push sum:" << pushSum << endl;
    cout << "pop sum:" << g_popSum << endl;
#endif

    // clear again
    g_pushCount = 0;
    g_popCount = 0;
    g_startTest = false;
    g_popSum = 0;
}

TEST_F(MPMCQueueTest, TestCorrect)
{
    // The queue size is larger than the amount of data to be pushed.
    doSumTest(1000, 1, 1, 300); // queue size is 1000, count of pushed is 300
    doSumTest(1000, 2, 1, 300);
    doSumTest(1000, 1, 2, 300);
    doSumTest(1000, 2, 2, 300);
    doSumTest(1000, 3, 3, 300);

    // The queue size is less than the amount of data to be pushed.
    doSumTest(100, 1, 1, 3000);
    doSumTest(100, 1, 3, 3000);
    doSumTest(100, 3, 1, 3000);
    doSumTest(100, 3, 3, 3000);
    doSumTest(1000, 3, 3, 30000);
    doSumTest(1000, 10, 10, 300000);
}

TEST_F(MPMCQueueTest, TestBatchPopWhenQueueIsEmptyThenFail)
{
    uint64_t size = 4;
    MpmcQueue *queue = MpmcQueueCreate(NULL, size);
    ASSERT_TRUE(queue != NULL);

    bool ret;
    MQElementType popData;

    MQElementType datas[4];
    uint32_t popCount = 4;

    //   case 1, after create queue
    {
        ret = MpmcQueuePopN(queue, datas, &popCount);
        ASSERT_FALSE(ret);
    }

    //   case 2, after push one and pop all
    {
        ret = MpmcQueuePush(queue, (MQElementType)0x1);
        ASSERT_TRUE(ret);

        ret = MpmcQueuePop(queue, &popData);
        ASSERT_TRUE(ret);

        ret = MpmcQueuePopN(queue, datas, &popCount);
        ASSERT_FALSE(ret);
    }

    //   case 3, after push half of capacity and pop all
    {
        for (int i = 0; i < 2; i++) {
            ret = MpmcQueuePush(queue, (MQElementType)0x1);
            ASSERT_TRUE(ret);
        }

        for (int i = 0; i < 2; i++) {
            ret = MpmcQueuePop(queue, &popData);
            ASSERT_TRUE(ret);
        }

        ret = MpmcQueuePopN(queue, datas, &popCount);
        ASSERT_FALSE(ret);
    }

    //   case 4, after push capacity and pop all
    {
        for (int i = 0; i < 4; i++) {
            ret = MpmcQueuePush(queue, (MQElementType)0x1);
            ASSERT_TRUE(ret);
        }

        for (int i = 0; i < 4; i++) {
            ret = MpmcQueuePop(queue, &popData);
            ASSERT_TRUE(ret);
        }

        ret = MpmcQueuePopN(queue, datas, &popCount);
        ASSERT_FALSE(ret);
    }

    MpmcQueueDestroy(queue);
}

TEST_F(MPMCQueueTest, TestBatchPopWhenInvaliParamThenFail)
{
    MpmcQueue *queue = MpmcQueueCreate(NULL, 4);

    bool ret;
    MQElementType datas[4];
    uint32_t popCount = 4;

    ret = MpmcQueuePopN(NULL, datas, &popCount);
    ASSERT_FALSE(ret);

    ret = MpmcQueuePopN(queue, NULL, &popCount);
    ASSERT_FALSE(ret);

    ret = MpmcQueuePopN(queue, datas, NULL);
    ASSERT_FALSE(ret);

    popCount = 0;
    ret = MpmcQueuePopN(queue, datas, &popCount);
    ASSERT_FALSE(ret);

    MpmcQueueDestroy(queue);
}

TEST_F(MPMCQueueTest, TestBatchPopWhenNLargeThanCapacityThenSuccess)
{
    uint64_t size = 4;
    MpmcQueue *queue = MpmcQueueCreate(NULL, size);
    ASSERT_TRUE(queue != NULL);

    bool ret;
    MQElementType popData;

    MQElementType datas[4];

    // case 1, push 1, pop 10000
    {
        ret = MpmcQueuePush(queue, (MQElementType)0x1);
        ASSERT_TRUE(ret);

        uint32_t popCount = 10000;
        ret = MpmcQueuePopN(queue, datas, &popCount);
        ASSERT_TRUE(ret);
        ASSERT_EQ(popCount, 1);
    }

    // case 2, push 2, pop 10000
    {
        ret = MpmcQueuePush(queue, (MQElementType)0x1);
        ASSERT_TRUE(ret);
        ret = MpmcQueuePush(queue, (MQElementType)0x1);
        ASSERT_TRUE(ret);

        uint32_t popCount = 10000;
        ret = MpmcQueuePopN(queue, datas, &popCount);
        ASSERT_TRUE(ret);
        ASSERT_EQ(popCount, 2);
    }

    MpmcQueueDestroy(queue);
}

TEST_F(MPMCQueueTest, TestBatchPopWhenCountLessThanNThenSusscces)
{
    uint64_t size = 4;
    MpmcQueue *queue = MpmcQueueCreate(NULL, size);
    ASSERT_TRUE(queue != NULL);

    bool ret;
    MQElementType popData;

    MQElementType datas[4];

    // case 1, push 1, pop 4
    {
        ret = MpmcQueuePush(queue, (MQElementType)0x1);
        ASSERT_TRUE(ret);

        uint32_t popCount = 4;
        ret = MpmcQueuePopN(queue, datas, &popCount);
        ASSERT_TRUE(ret);
        ASSERT_EQ(popCount, 1);
    }

    // case 2, push 2, pop 3
    {
        ret = MpmcQueuePush(queue, (MQElementType)0x11);
        ASSERT_TRUE(ret);
        ret = MpmcQueuePush(queue, (MQElementType)0x22);
        ASSERT_TRUE(ret);

        uint32_t popCount = 3;
        ret = MpmcQueuePopN(queue, datas, &popCount);
        ASSERT_TRUE(ret);
        ASSERT_EQ(popCount, 2);
        ASSERT_EQ(datas[0], (MQElementType)0x11);
        ASSERT_EQ(datas[1], (MQElementType)0x22);
    }

    // case 3, push 3, pop 4
    {
        ret = MpmcQueuePush(queue, (MQElementType)0x101);
        ASSERT_TRUE(ret);
        ret = MpmcQueuePush(queue, (MQElementType)0x202);
        ASSERT_TRUE(ret);
        ret = MpmcQueuePush(queue, (MQElementType)0x303);
        ASSERT_TRUE(ret);

        uint32_t popCount = 4;
        ret = MpmcQueuePopN(queue, datas, &popCount);
        ASSERT_TRUE(ret);
        ASSERT_EQ(popCount, 3);
        ASSERT_EQ(datas[0], (MQElementType)0x101);
        ASSERT_EQ(datas[1], (MQElementType)0x202);
        ASSERT_EQ(datas[2], (MQElementType)0x303);
    }

    MpmcQueueDestroy(queue);
}

TEST_F(MPMCQueueTest, TestBatchPopWhenCountLargeThanNThenSusscces)
{
    // popN success when the count(real number of elements) if larger than N
    uint64_t size = 4;
    MpmcQueue *queue = MpmcQueueCreate(NULL, size);
    ASSERT_TRUE(queue != NULL);

    bool ret;
    MQElementType popData;

    MQElementType datas[4];

    // case 1, push 4, pop 3
    {
        ret = MpmcQueuePush(queue, (MQElementType)0x1);
        ASSERT_TRUE(ret);

        uint32_t popCount = 4;
        ret = MpmcQueuePopN(queue, datas, &popCount);
        ASSERT_TRUE(ret);
        ASSERT_EQ(popCount, 1);
    }

    // case 2, push 2, pop 3
    {
        ret = MpmcQueuePush(queue, (MQElementType)0x11);
        ASSERT_TRUE(ret);
        ret = MpmcQueuePush(queue, (MQElementType)0x22);
        ASSERT_TRUE(ret);

        uint32_t popCount = 3;
        ret = MpmcQueuePopN(queue, datas, &popCount);
        ASSERT_TRUE(ret);
        ASSERT_EQ(popCount, 2);
        ASSERT_EQ(datas[0], (MQElementType)0x11);
        ASSERT_EQ(datas[1], (MQElementType)0x22);
    }

    // case 3, push 3, pop 4
    {
        ret = MpmcQueuePush(queue, (MQElementType)0x101);
        ASSERT_TRUE(ret);
        ret = MpmcQueuePush(queue, (MQElementType)0x202);
        ASSERT_TRUE(ret);
        ret = MpmcQueuePush(queue, (MQElementType)0x303);
        ASSERT_TRUE(ret);

        uint32_t popCount = 4;
        ret = MpmcQueuePopN(queue, datas, &popCount);
        ASSERT_TRUE(ret);
        ASSERT_EQ(popCount, 3);
        ASSERT_EQ(datas[0], (MQElementType)0x101);
        ASSERT_EQ(datas[1], (MQElementType)0x202);
        ASSERT_EQ(datas[2], (MQElementType)0x303);
    }

    MpmcQueueDestroy(queue);

}

static void ConsumerCalcSumFuncForPopN(MpmcQueue *queue, uint32_t popCount, uint32_t maxPopCount)
{
    {
        unique_lock<mutex> lk(g_cv_m);
        g_cv.wait(lk, []{return g_startTest;});
#ifdef ENABLE_DEBUG_PRINT
        cout << "enter:" << __FUNCTION__
            << ",popCount=" << popCount
            << ",maxPopCount=" << maxPopCount
            << endl;
#endif
    }

    int64_t count = 0;
    MQElementType popDatas[popCount];

    for (;;) {
        uint32_t realPopCount = popCount;
        if (MpmcQueuePopN(queue, popDatas, &realPopCount)) {
            count++;
            for (uint32_t i = 0; i < realPopCount; i++) {
                uint32_t index = g_popCount + i;
                g_popSum += (uint64_t)popDatas[i]; // calculate sum
            }
            g_popCount += realPopCount;
        }
        if (g_popCount >= maxPopCount) {
            break;
        }
    }

    {
#ifdef ENABLE_DEBUG_PRINT
        thread::id this_id = this_thread::get_id();
        unique_lock<mutex> lk(g_cv_m); // Use locks to avoid cluttered printing
        cout << "thread:" << this_id << ", popN consume count " << count << " when g_popCount " << g_popCount << endl;
        //MpmcQueueDumpStatus(queue, "popN consumer");
#endif
    }
}

static void doSumTestForPopN(uint64_t queueSize, uint64_t prodNum, uint64_t consNum, uint64_t maxValue, uint32_t popCount)
{
#ifdef ENABLE_DEBUG_PRINT
    cout << "enter:" << __FUNCTION__
        << ",queueSize=" << queueSize
        << ",prodNum=" << prodNum
        << ",consNum=" << consNum
        << ",maxValue=" << maxValue
        << ",popCount=" << popCount
        << endl;
#endif
    ASSERT_EQ(maxValue%prodNum, 0);
    ASSERT_TRUE(prodNum > 0);
    ASSERT_TRUE(consNum > 0);

    // clear
    g_pushCount = 0;
    g_popCount = 0;
    g_startTest = false;
    g_popSum = 0;

    MpmcQueue *queue = MpmcQueueCreate(NULL, queueSize);
    ASSERT_TRUE(queue != NULL);

    // Data is pushed in sequence, starting from 1 to the maximum value maxValue.
    thread threadProd[prodNum];
    uint64_t step = maxValue / prodNum;
    uint64_t start = 1;
    uint64_t end = start + step - 1;
    for (int i = 0; i < prodNum; i++) {
        threadProd[i] = thread(SeqPushFunc, queue, start, end);
        start = end + 1;
        end = start + step - 1;
    }

    thread threadCons[consNum];
    for (int i = 0; i < consNum; i++) {
        threadCons[i] = thread(ConsumerCalcSumFuncForPopN, queue, popCount, maxValue);
    }

    // OK, let's start produce-consume test
    {
        lock_guard<mutex> lk(g_cv_m);
        g_startTest = true;
    }
    g_cv.notify_all();

    // wait all thread finish
    for (auto i = 0; i < prodNum; i++) {
        threadProd[i].join();
    }

    for (auto i = 0; i < consNum; i++) {
        threadCons[i].join();
    }

    // check, we expect the sum of the pushed data is equal to the sum of pop up data
    uint64_t pushSum = (1 + maxValue) * maxValue / 2;

#ifdef ENABLE_DEBUG_PRINT
    if (pushSum != g_popSum) {
        MpmcQueueDumpStatus(queue, "test popN");
    }
    cout << "push sum:" << pushSum << endl;
    cout << "pop sum:" << g_popSum << endl;
#endif

    MpmcQueueDestroy(queue);

    ASSERT_EQ(pushSum, g_popSum);

    // clear again
    g_pushCount = 0;
    g_popCount = 0;
    g_startTest = false;
    g_popSum = 0;
}

// --gtest_repeat=1000
TEST_F(MPMCQueueTest, TestBatchPopForMutipleThreads)
{
    // The queue size is less than the amount of data to be pushed.
    doSumTestForPopN(10, 1, 1, 30, 3);
    doSumTestForPopN(10, 1, 1, 300, 10);
    doSumTestForPopN(100, 1, 1, 3000, 10);
    doSumTestForPopN(10, 3, 1, 300, 10);
    doSumTestForPopN(10, 1, 3, 300, 10);
    doSumTestForPopN(100, 1, 3, 3000, 10);
    doSumTestForPopN(100, 3, 1, 3000, 10);
    doSumTestForPopN(100, 3, 3, 3000, 10);
    doSumTestForPopN(1000, 3, 3, 30000, 10);
    doSumTestForPopN(1000, 10, 10, 30000, 10);
    doSumTestForPopN(1000, 10, 10, 300000, 10);
}
