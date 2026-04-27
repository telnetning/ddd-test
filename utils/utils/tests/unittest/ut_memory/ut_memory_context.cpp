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
 * Description:
 * 1. unit tests of memory_context.c
 *    all the test need under enable asan check.
 *
 * ---------------------------------------------------------------------------------
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <map>
#include <tuple>
#include <atomic>
#include <thread>
#include <condition_variable>
#include <time.h>
#include <stdlib.h>
#include <unistd.h>
#include <signal.h>
#include "securec.h"
#include "memory/memory_ctx.h"
#include "syslog/err_log.h"
#include "syslog/err_log_internal.h"

#define TEST_MCTX_NAME "ut-tmp_memory_context"
#define KB(x) ((x) * 1024)
#define MB(x) ((x) * 1024 * 1024)

using namespace std;

static condition_variable cv;
static mutex mu;
static bool ready = false;
static string g_errlogDir;
static bool g_removeTestArtifact = true;
static vector<sighandler_t> g_oldHandler(SIGRTMAX);

class MemoryContextTest : public testing::Test {
public:
    static void SetUpTestSuite() {
        g_errlogDir = "./memctx_log/";
        system(("rm -rf " + g_errlogDir).c_str());
        SetErrLogDirectory(g_errlogDir.c_str());
        ASSERT_EQ(StartLogger(), ERROR_SYS_OK);

        auto signalHandler = [](int signum) {
            exit(signum);
        };
        /* register all signal to my signalHandler */
        for (int signum = 0; signum < SIGRTMAX; signum++) {
            /* ignore registe SIGCHLD and SIGUSR2 */
            g_oldHandler[signum] = ((signum != SIGCHLD && signum != SIGUSR1) ? signal(signum, signalHandler) : SIG_DFL);
        }
    }

    static void TearDownTestSuite() {
        usleep(100);
        CloseLogger();
        StopLogger();
        if (g_removeTestArtifact) {
            system(("rm -rf " + g_errlogDir).c_str());
        }
        /* restore all signal handler */
        for (int i = 0; i < SIGRTMAX; i++) {
            if (g_oldHandler[i] != SIG_ERR) {
                (void)signal(i, g_oldHandler[i]);
            }
        }
    }

    void SetUp() override {
        // init default parameter memory context
        g_rootShareMctx = MemoryContextCreate(nullptr, MEM_CXT_TYPE_SHARE, "ut-mctx-share",
                                              MCTX_UNUSED, MCTX_UNUSED, DEFAULT_UNLIMITED_SIZE);
        g_rootSimpleShareMctx = MemoryContextCreate(nullptr, MEM_CXT_TYPE_SIMPLE_SHARE, "ut-mctx-simple",
                                                    MCTX_UNUSED, MCTX_UNUSED, DEFAULT_UNLIMITED_SIZE);
        g_rootStackMctx = MemoryContextCreate(nullptr, MEM_CXT_TYPE_STACK, "ut-mctx-stack",
                                              MCTX_UNUSED, MCTX_UNUSED, MB(1000)); // 1000MB stack memory context
        g_rootGenericMctx = MemoryContextCreate(nullptr, MEM_CXT_TYPE_GENERIC, "ut-mctx-generic",
                                                0, DEFAULT_UNLIMITED_SIZE, DEFAULT_UNLIMITED_SIZE);
        MemoryContextSwitchTo(g_rootGenericMctx);
        ::testing::GTEST_FLAG(death_test_style) = "threadsafe";
    }

    void TearDown() override {
        ASSERT_TRUE(MemoryContextIsValid(g_rootShareMctx));
        ASSERT_TRUE(MemoryContextIsValid(g_rootSimpleShareMctx));
        ASSERT_TRUE(MemoryContextIsValid(g_rootStackMctx));
        ASSERT_TRUE(MemoryContextIsValid(g_rootGenericMctx));
        MemoryContextDelete(g_rootShareMctx);
        MemoryContextDelete(g_rootSimpleShareMctx);
        MemoryContextDelete(g_rootStackMctx);
        MemoryContextDelete(g_rootGenericMctx);
        ready = false;
        g_removeTestArtifact = !::testing::Test::HasFailure(); /* if run test fail, leave the test log */
    }

    MemoryContext g_rootShareMctx = nullptr;
    MemoryContext g_rootSimpleShareMctx = nullptr;
    static thread_local MemoryContext g_rootStackMctx;
    static thread_local MemoryContext g_rootGenericMctx;
};

thread_local MemoryContext MemoryContextTest::g_rootStackMctx = nullptr;
thread_local MemoryContext MemoryContextTest::g_rootGenericMctx = nullptr;

TEST_F(MemoryContextTest, CreateDeleteContextWithFail)
{
    // create temp root generic memctx by below genParameters
    // generic memory context create fail
    ASSERT_EQ(MemoryContextCreate(nullptr, MEM_CXT_TYPE_GENERIC, TEST_MCTX_NAME, KB(8), KB(8), KB(3)), nullptr);
    ASSERT_EQ(MemoryContextCreate(nullptr, MEM_CXT_TYPE_GENERIC, TEST_MCTX_NAME, KB(8), KB(3), KB(8)), nullptr);
    // stack memory context max size need less then 4G
    ASSERT_EQ(MemoryContextCreate(nullptr, MEM_CXT_TYPE_STACK, TEST_MCTX_NAME, MCTX_UNUSED, MCTX_UNUSED,
                                  MB((size_t)4096) + 1), nullptr);
}

TEST_F(MemoryContextTest, CreateDeleteContextWithSuccess)
{
    MemoryContext tmpctx;
    // generic memory context create success
    tmpctx = MemoryContextCreate(nullptr, MEM_CXT_TYPE_GENERIC, TEST_MCTX_NAME, KB(8), KB(8), KB(8));
    ASSERT_NE(tmpctx, nullptr);
    MemoryContextDelete(tmpctx);

    tmpctx = MemoryContextCreate(nullptr, MEM_CXT_TYPE_GENERIC, TEST_MCTX_NAME, KB(32), KB(512), MB(2));
    ASSERT_NE(tmpctx, nullptr);
    MemoryContextDelete(tmpctx);

    // create temp root share/simple_share memctx success
    // create success but can't alloc memory on it, this memory context is invaild
    tmpctx = MemoryContextCreate(nullptr, MEM_CXT_TYPE_SHARE, TEST_MCTX_NAME, MCTX_UNUSED, MCTX_UNUSED, 0);
    ASSERT_EQ(tmpctx, nullptr);
    MemoryContextDelete(tmpctx);

    tmpctx = MemoryContextCreate(nullptr, MEM_CXT_TYPE_SHARE, TEST_MCTX_NAME, MCTX_UNUSED, MCTX_UNUSED, KB(8));
    ASSERT_NE(tmpctx, nullptr);
    MemoryContextDelete(tmpctx);

    tmpctx = MemoryContextCreate(nullptr, MEM_CXT_TYPE_SHARE, TEST_MCTX_NAME, MCTX_UNUSED, MCTX_UNUSED,
                                 DEFAULT_UNLIMITED_SIZE);
    ASSERT_NE(tmpctx, nullptr);
    MemoryContextDelete(tmpctx);

    tmpctx = MemoryContextCreate(nullptr, MEM_CXT_TYPE_SIMPLE_SHARE, TEST_MCTX_NAME, MCTX_UNUSED, MCTX_UNUSED, KB(8));
    ASSERT_NE(tmpctx, nullptr);
    MemoryContextDelete(tmpctx);

    // create temp root stack memctx success
    tmpctx = MemoryContextCreate(nullptr, MEM_CXT_TYPE_STACK, TEST_MCTX_NAME, MCTX_UNUSED, MCTX_UNUSED, 0);
    ASSERT_EQ(tmpctx, nullptr);
    MemoryContextDelete(tmpctx);

    tmpctx = MemoryContextCreate(nullptr, MEM_CXT_TYPE_STACK, TEST_MCTX_NAME, MCTX_UNUSED, MCTX_UNUSED, MB(2));
    ASSERT_NE(tmpctx, nullptr);
    MemoryContextDelete(tmpctx);
}

/* using MemoryContextIsEmpty to decide wether memory leak */
TEST_F(MemoryContextTest, MemoryContextLeakCheckBaseTest)
{
    /* below two function will used in Pagestore memleak detect feature, so first test those two API */
    ASSERT_EQ(GetMemoryContextParent(nullptr), nullptr);
    ASSERT_EQ(GetMemoryContextType(nullptr), MEM_CXT_TYPE_NULL);
    ASSERT_EQ(MemGetSpace(nullptr), 0);

    auto ctx = MemoryContextCreate(nullptr, MEM_CXT_TYPE_SIMPLE_SHARE, TEST_MCTX_NAME,
                                   MCTX_UNUSED, MCTX_UNUSED, DEFAULT_MAXSIZE_8M);
    ASSERT_NE(ctx, nullptr);
    ASSERT_EQ(MemoryContextIsEmpty(ctx), true);
    ASSERT_EQ(GetMemoryContextParent(ctx), nullptr);
    ASSERT_EQ(GetMemoryContextType(ctx), MEM_CXT_TYPE_SIMPLE_SHARE);
    auto child = MemoryContextCreate(ctx, MEM_CXT_TYPE_SIMPLE_SHARE, TEST_MCTX_NAME,
                                     MCTX_UNUSED, MCTX_UNUSED, DEFAULT_MAXSIZE_8M);
    ASSERT_EQ(MemoryContextIsEmpty(child), true);
    ASSERT_EQ(GetMemoryContextParent(child), ctx);
    ASSERT_EQ(GetMemoryContextType(child), MEM_CXT_TYPE_SIMPLE_SHARE);
    auto ptr = MemoryContextAlloc(child, KB(1));
    ASSERT_GE(MemGetSpace(ptr), KB(1)); /* make sure get space greater than request size */
    ASSERT_EQ(MemoryContextIsEmpty(child), false);
    ASSERT_EQ(MemoryContextIsEmpty(ctx), false);
    MemFree(ptr);
    ASSERT_EQ(MemoryContextIsEmpty(child), true);
    ASSERT_EQ(MemoryContextIsEmpty(ctx), false);
    MemoryContextDelete(child);
    ASSERT_EQ(MemoryContextIsEmpty(ctx), true);
    MemoryContextDelete(ctx);
}

static bool CheckFileContent(string fileName, string string)
{
    int status = system(("grep -q \'" + string + "\' " + fileName).c_str());
    return (WEXITSTATUS(status) == 0);
}

static string getFileContentResult(string cmd)
{
    string output(1024, 0);
    FILE *fp = popen(cmd.c_str(), "r");  //  output length is 1024, check '\0' will truncate
    fgets((char *)output.c_str(), output.capacity(), fp);  /* get line */
    pclose(fp);
    return move(output);
}

TEST_F(MemoryContextTest, MemoryContextSetSilentTest01)
{
    MemoryContext testMemoryCtx[] = {g_rootGenericMctx, g_rootShareMctx, g_rootStackMctx, g_rootSimpleShareMctx};
    for (int i = 0; i < sizeof(testMemoryCtx) / sizeof(testMemoryCtx[0]) / 2; i++) {
        string name = "leakTest" + to_string(i);
        string fileContent = name + ", " + __FILE__ + ":";
        auto ctx = MemoryContextCreate(testMemoryCtx[i], GetMemoryContextType(testMemoryCtx[i]), name.c_str(),
                                       DEFAULT_INIT_BLOCKSIZE_8K, MB(30), MB(30));

        MemoryContextSetSilent(ctx, true);  // close memory context print errlog when memory can't alloc
        void *ptr = nullptr;
        auto checkLine = (ptr = MemoryContextAlloc(ctx, UINT32_MAX), __LINE__); // allocate UINT32_MAX is over the context limit
        ASSERT_EQ(ptr, nullptr);
        FlushLogger();   // flush errlog
        ASSERT_FALSE(CheckFileContent(g_errlogDir + "/*log", fileContent + to_string(checkLine))) << "[----------] "
            << "\033[1;33;46m" << "grep -q \'" + fileContent + to_string(checkLine) + "\' " + g_errlogDir + "/*log" << "\033[0m";

        MemoryContextSetSilent(ctx, false);  // open memory context print errlog when memory can't alloc
        checkLine = (ptr = MemoryContextAlloc(ctx, UINT32_MAX), __LINE__); // allocate UINT32_MAX is over the context limit
        ASSERT_EQ(ptr, nullptr);
        FlushLogger();
        ASSERT_TRUE(CheckFileContent(g_errlogDir + "/*log", fileContent + to_string(checkLine))) << "[----------] "
            << "\033[1;33;46m" << "grep -q \'" + fileContent + to_string(checkLine) + "\' " + g_errlogDir + "/*log" << "\033[0m";
    }
    if (::testing::Test::HasFailure()) { /* if run test fail, leave the test log */
        g_removeTestArtifact = false;
    }
}

TEST_F(MemoryContextTest, MemoryContextSetSilentTest02)
{
    MemoryContext testMemoryCtx[] = {g_rootGenericMctx, g_rootShareMctx, g_rootStackMctx, g_rootSimpleShareMctx};
    for (int i = sizeof(testMemoryCtx) / sizeof(testMemoryCtx[0]) / 2; i < sizeof(testMemoryCtx) / sizeof(testMemoryCtx[0]); i++) {
        string name = "leakTest" + to_string(i);
        string fileContent = name + ", " + __FILE__ + ":";
        auto ctx = MemoryContextCreate(testMemoryCtx[i], GetMemoryContextType(testMemoryCtx[i]), name.c_str(),
                                       DEFAULT_INIT_BLOCKSIZE_8K, MB(30), MB(30));

        MemoryContextSetSilent(ctx, true);  // close memory context print errlog when memory can't alloc
        void *ptr = nullptr;
        auto checkLine = (ptr = MemoryContextAlloc(ctx, UINT32_MAX), __LINE__); // allocate UINT32_MAX is over the context limit
        ASSERT_EQ(ptr, nullptr);
        FlushLogger();   // flush errlog
        ASSERT_FALSE(CheckFileContent(g_errlogDir + "/*log", fileContent + to_string(checkLine))) << "[----------] "
            << "\033[1;33;46m" << "grep -q \'" + fileContent + to_string(checkLine) + "\' " + g_errlogDir + "/*log" << "\033[0m";

        MemoryContextSetSilent(ctx, false);  // open memory context print errlog when memory can't alloc
        checkLine = (ptr = MemoryContextAlloc(ctx, UINT32_MAX), __LINE__); // allocate UINT32_MAX is over the context limit
        ASSERT_EQ(ptr, nullptr);
        FlushLogger();
        ASSERT_TRUE(CheckFileContent(g_errlogDir + "/*log", fileContent + to_string(checkLine))) << "[----------] "
            << "\033[1;33;46m" << "grep -q \'" + fileContent + to_string(checkLine) + "\' " + g_errlogDir + "/*log" << "\033[0m";
    }
    if (::testing::Test::HasFailure()) { /* if run test fail, leave the test log */
        g_removeTestArtifact = false;
    }
}

TEST_F(MemoryContextTest, SupportMemleakDfxTest)
{
    int checkLine = 0;
    MemoryContext testMemoryCtx[] = {g_rootGenericMctx, g_rootShareMctx, g_rootStackMctx};
    size_t testAllocSize[] = {1, 2, 20, 200, 500, KB(2), KB(20), KB(200), MB(1), MB(2)};

    for (int i = 0; i < sizeof(testMemoryCtx) / sizeof(testMemoryCtx[0]); i++) {
        size_t total = 0;
        string name = "leakTest" + to_string(i);
        auto testctx = MemoryContextCreate(testMemoryCtx[i], GetMemoryContextType(testMemoryCtx[i]),
            name.c_str(), DEFAULT_INIT_BLOCKSIZE_8K, MB(30), MB(30));
        auto oldCtx = MemoryContextSwitchTo(testctx);
        MemoryContextSetPrintSummary(testctx, true);
        /* firstly, random alloc [1, 3M] */
        srand(time(NULL));
        ASSERT_NE(MemAllocEx(rand() % MB(3) + 1, MCTX_ALLOC_FLAG_OOM), nullptr);
        ASSERT_NE(MemAllocEx(rand() % MB(3) + 1, MCTX_ALLOC_FLAG_OOM | MCTX_ALLOC_FLAG_ZERO), nullptr);
        for (uintptr_t j = 0, ptr = 1; ptr != 0; j++, j %= (sizeof(testAllocSize) / sizeof(testAllocSize[0]))) {
            MemAlloc(testAllocSize[j]);
            MemAlloc0(testAllocSize[j]);
            MemRealloc(MemAlloc0(testAllocSize[j] / 2), testAllocSize[j]);
            MemRealloc(MemAlloc0(testAllocSize[j]), testAllocSize[j] * 2);
            MemAllocEx(testAllocSize[j], MCTX_ALLOC_FLAG_DEFAULT);
            MemAllocEx(testAllocSize[j], MCTX_ALLOC_FLAG_ZERO);
            checkLine = (ptr = (uintptr_t)MemAlloc(testAllocSize[j]), __LINE__);
            total += (ptr != 0 ? testAllocSize[j] : 0);
        }
        FlushLogger();
        /* check log */
        string fileContent = name + ", " + __FILE__ + ":" + to_string(checkLine) + " total alloc ";
        ASSERT_TRUE(CheckFileContent(g_errlogDir + "/*log", fileContent)) << "[----------] " <<
            "CheckFileContent is result of execute: " << endl << "[----------] " << "\033[1;33;46m" <<
            "grep -q \'" + fileContent + "\' " + g_errlogDir + "/*log" << "\033[0m";
        string cmd = "sed -n 's#.*" + name +", " + __FILE__ + ":" + to_string(checkLine) +
            " total alloc ##p' " + g_errlogDir + "/*log";
        ASSERT_EQ(stoll(getFileContentResult(cmd)), total) << "[----------] " <<
            "getFileContentResult is result of execute: " << endl << "[----------] " << "\033[1;33;46m" << cmd << "\033[0m";
        MemoryContextDelete(testctx);
        (void)MemoryContextSwitchTo(oldCtx);
    }
    /* simple share memory context test alone */
    vector<void *> vPtr;
    auto oldCtx = MemoryContextSwitchTo(MemoryContextCreate(nullptr, MEM_CXT_TYPE_SIMPLE_SHARE, "shareleakTest",
                                                            DEFAULT_INIT_BLOCKSIZE_8K, MB(30), MB(30)));
    for (uintptr_t j = 0, ptr = 1; ptr != 0; j++, j %= (sizeof(testAllocSize) / sizeof(testAllocSize[0]))) {
        vPtr.push_back(MemAlloc(testAllocSize[j]));
        vPtr.push_back(MemAlloc0(testAllocSize[j]));
        vPtr.push_back(MemRealloc(MemAlloc0(testAllocSize[j] / 2), testAllocSize[j]));
        vPtr.push_back(MemRealloc(MemAlloc0(testAllocSize[j]), testAllocSize[j] * 2));
        vPtr.push_back(MemAllocEx(testAllocSize[j], MCTX_ALLOC_FLAG_DEFAULT));
        vPtr.push_back(MemAllocEx(testAllocSize[j], MCTX_ALLOC_FLAG_ZERO));
        checkLine = (ptr = (uintptr_t)MemAlloc(testAllocSize[j]), __LINE__);
        vPtr.push_back((void *)ptr);
        /* simple share memory context have no summary print log */
    }
    for (auto &ptr : vPtr) { /* free all memory */
        MemFree(ptr);
    }
    FlushLogger();
    /* check log */
    string fileContent = "shareleakTest, ut_memory_context.cpp:" + to_string(checkLine) + ".*statistics";
    ASSERT_TRUE(CheckFileContent(g_errlogDir + "/*log", fileContent)) << "[----------] " <<
        "CheckFileContent is result of execute: " << endl << "[----------] " << "\033[1;33;46m" <<
        "grep -q \'" + fileContent + "\' " + g_errlogDir + "/*log" << "\033[0m";
    MemoryContextDelete(MemoryContextSwitchTo(oldCtx));
    if (::testing::Test::HasFailure()) { /* if run test fail, leave the test log */
        g_removeTestArtifact = false;
    }
}

TEST_F(MemoryContextTest, Create3x3ContextTreeWithDeleteNotMemleak)
{
    struct TestParam {
        MemoryContext root;
        size_t        initBlockSize;
        size_t        maxBlockSize;
        size_t        maxSize;
    };
    // create level 3 memctx of every memctx has 3 children
#define TEST_LEVEL 3
    map<MemoryContextType, TestParam> testMaps = {
        {MEM_CXT_TYPE_GENERIC, {g_rootGenericMctx, 0, DEFAULT_UNLIMITED_SIZE, DEFAULT_UNLIMITED_SIZE}},
        {MEM_CXT_TYPE_SHARE, {g_rootShareMctx, MCTX_UNUSED, MCTX_UNUSED, DEFAULT_UNLIMITED_SIZE}},
        {MEM_CXT_TYPE_SIMPLE_SHARE, {g_rootSimpleShareMctx, MCTX_UNUSED, MCTX_UNUSED, DEFAULT_UNLIMITED_SIZE}},
        {MEM_CXT_TYPE_STACK, {g_rootStackMctx, MCTX_UNUSED, MCTX_UNUSED, MB(200)}},
    };
    for (auto &test : testMaps) {
        MemoryContext mctxLevelOne[TEST_LEVEL];
        MemoryContext mctxLevelTwo[TEST_LEVEL][TEST_LEVEL];

        auto type = test.first;
        auto param = test.second;
        for (auto i = 0; i < TEST_LEVEL; i++) { // level 1
            mctxLevelOne[i] = MemoryContextCreate(param.root, type, TEST_MCTX_NAME, param.initBlockSize,
                                                  param.maxBlockSize, param.maxSize);
            ASSERT_NE(mctxLevelOne[i], nullptr);
            ASSERT_TRUE(MemoryContextIsValid(mctxLevelOne[i]));
            for (auto j = 0; j < TEST_LEVEL; j++) {
                mctxLevelTwo[i][j] = MemoryContextCreate(mctxLevelOne[i], type, TEST_MCTX_NAME, param.initBlockSize,
                                                         param.maxBlockSize, param.maxSize);
                ASSERT_NE(mctxLevelTwo[i][j], nullptr);
                ASSERT_TRUE(MemoryContextIsValid(mctxLevelTwo[i][j]));
                for (auto k = 0; k < TEST_LEVEL; k++) {
                    ASSERT_NE(MemoryContextCreate(mctxLevelTwo[i][j], type, TEST_MCTX_NAME, param.initBlockSize,
                                                  param.maxBlockSize, param.maxSize), nullptr);
                }
            }
            ASSERT_EQ(MemoryContextNums(mctxLevelOne[i]),  1 + 3 + 9);
        }
        ASSERT_NE(MemoryContextNums(param.root),  1);
        // reset level 2, index 0,1 memctx
        MemoryContextReset(mctxLevelTwo[0][1]);
        // delete level 2, index 1,1 memctx
        MemoryContextDelete(mctxLevelTwo[1][1]);
        for (auto i = 0 ; i < TEST_LEVEL; i++) {
            ASSERT_TRUE(MemoryContextIsValid(mctxLevelOne[i]));
            MemoryContextDelete(mctxLevelOne[i]);
        }
        // check root memctx only one in tree
        ASSERT_EQ(MemoryContextNums(param.root),  1);
    }
}

// generic memory context alloc
TEST_F(MemoryContextTest, AllocMemoryByGenericContext)
{
    MemoryContext mctx = MemoryContextCreate(g_rootGenericMctx, MEM_CXT_TYPE_GENERIC, TEST_MCTX_NAME,
                                             KB(32), KB(512), MB(2));
    ASSERT_NE(mctx, nullptr);

    // root memory context alloc
    ASSERT_NE(MemoryContextAlloc(g_rootGenericMctx, 100), nullptr);
    ASSERT_NE(MemoryContextAlloc(g_rootGenericMctx, KB(100)), nullptr);
    ASSERT_NE(MemoryContextAlloc(g_rootGenericMctx, MB(100)), nullptr);
    ASSERT_NE(MemoryContextAllocEx(g_rootGenericMctx, 100, MCTX_ALLOC_FLAG_ZERO), nullptr);
    ASSERT_NE(MemoryContextAllocEx(g_rootGenericMctx, KB(100), MCTX_ALLOC_FLAG_ZERO), nullptr);
    ASSERT_NE(MemoryContextAllocEx(g_rootGenericMctx, MB(100), MCTX_ALLOC_FLAG_ZERO), nullptr);
    // mctx alloc
    ASSERT_EQ(MemoryContextAlloc(mctx, 0), nullptr);
    ASSERT_EQ(MemoryContextAlloc(mctx, MB(5)), nullptr);
    ASSERT_EQ(MemoryContextAlloc(mctx, MB(500)), nullptr);
    ASSERT_EQ(MemoryContextAlloc(mctx, (size_t)-1), nullptr);
    ASSERT_EQ(MemoryContextAllocEx(mctx, 0, MCTX_ALLOC_FLAG_ZERO), nullptr);
    ASSERT_EQ(MemoryContextAllocEx(mctx, MB(5), MCTX_ALLOC_FLAG_ZERO), nullptr);
    ASSERT_EQ(MemoryContextAllocEx(mctx, MB(500), MCTX_ALLOC_FLAG_ZERO), nullptr);
    ASSERT_EQ(MemoryContextAllocEx(mctx, (size_t)-1, MCTX_ALLOC_FLAG_ZERO), nullptr);

    void *p0 = MemoryContextAlloc(mctx, 500);
    void *p1 = MemoryContextAllocEx(mctx, 500, MCTX_ALLOC_FLAG_ZERO);
    ASSERT_NE(p0, nullptr);
    ASSERT_NE(p1, nullptr);
    MemFree(p0);
    MemFree(p1);
    p0 = MemoryContextAlloc(mctx, KB(500));
    p1 = MemoryContextAllocEx(mctx, KB(500), MCTX_ALLOC_FLAG_ZERO);
    ASSERT_NE(p0, nullptr);
    ASSERT_NE(p1, nullptr);
    ASSERT_NE(MemoryContextAlloc(mctx, 5), nullptr);
    ASSERT_NE(MemoryContextAlloc(mctx, 500), nullptr);
    ASSERT_NE(MemoryContextAlloc(mctx, KB(500)), nullptr);
    ASSERT_NE(MemoryContextAllocEx(mctx, 5, MCTX_ALLOC_FLAG_ZERO), nullptr);
    ASSERT_NE(MemoryContextAllocEx(mctx, 500, MCTX_ALLOC_FLAG_ZERO), nullptr);
    ASSERT_NE(MemoryContextAllocEx(mctx, KB(500), MCTX_ALLOC_FLAG_ZERO), nullptr);
    MemFree(p0);
}

// stack memory context alloc
TEST_F(MemoryContextTest, AllocMemoryByStackContext)
{
    MemoryContext mctx = MemoryContextCreate(g_rootStackMctx, MEM_CXT_TYPE_STACK, "ut-memory_context",
                                             MCTX_UNUSED, MCTX_UNUSED, MB(100));
    ASSERT_NE(mctx, nullptr);

    // root memory context alloc, g_rootStackMctx max is 1000MB
    ASSERT_EQ(MemoryContextAlloc(g_rootStackMctx, MB(1000) + 1), nullptr);
    ASSERT_NE(MemoryContextAlloc(g_rootStackMctx, 100), nullptr);
    ASSERT_NE(MemoryContextAlloc(g_rootStackMctx, KB(100)), nullptr);
    ASSERT_NE(MemoryContextAlloc(g_rootStackMctx, MB(100)), nullptr);
    ASSERT_NE(MemoryContextAllocEx(g_rootStackMctx, 100, MCTX_ALLOC_FLAG_ZERO), nullptr);
    ASSERT_NE(MemoryContextAllocEx(g_rootStackMctx, KB(100), MCTX_ALLOC_FLAG_ZERO), nullptr);
    ASSERT_NE(MemoryContextAllocEx(g_rootStackMctx, MB(10), MCTX_ALLOC_FLAG_ZERO), nullptr);
    // mctx alloc fail
    ASSERT_EQ(MemoryContextAlloc(mctx, MB(100) + 1), nullptr);  // max is 100MB
    ASSERT_EQ(MemoryContextAlloc(mctx, 0), nullptr);
    ASSERT_EQ(MemoryContextAlloc(mctx, (size_t)-1), nullptr);
    ASSERT_EQ(MemoryContextAllocEx(mctx, 0, MCTX_ALLOC_FLAG_ZERO), nullptr);
    ASSERT_EQ(MemoryContextAllocEx(mctx, MB(100) + 1, MCTX_ALLOC_FLAG_ZERO), nullptr);
    ASSERT_EQ(MemoryContextAllocEx(mctx, (size_t)-1, MCTX_ALLOC_FLAG_ZERO), nullptr);
    // mctx alloc success, max is 100MB
    void *p0 = MemoryContextAlloc(mctx, 500);
    void *p1 = MemoryContextAllocEx(mctx, 500, MCTX_ALLOC_FLAG_ZERO);
    ASSERT_NE(p0, nullptr);
    ASSERT_NE(p1, nullptr);
    MemFree(p0);
    MemFree(p1);
    p0 = MemoryContextAlloc(mctx, KB(500));
    p1 = MemoryContextAllocEx(mctx, KB(500), MCTX_ALLOC_FLAG_ZERO);
    ASSERT_NE(p0, nullptr);
    ASSERT_NE(p1, nullptr);
    ASSERT_NE(MemoryContextAlloc(mctx, 5), nullptr);
    ASSERT_NE(MemoryContextAlloc(mctx, 500), nullptr);
    ASSERT_NE(MemoryContextAlloc(mctx, KB(500)), nullptr);
    ASSERT_NE(MemoryContextAllocEx(mctx, 5, MCTX_ALLOC_FLAG_ZERO), nullptr);
    ASSERT_NE(MemoryContextAllocEx(mctx, 500, MCTX_ALLOC_FLAG_ZERO), nullptr);
    ASSERT_NE(MemoryContextAllocEx(mctx, KB(500), MCTX_ALLOC_FLAG_ZERO), nullptr);
    MemFree(p0);
    // test first alloc success, next alloc fail, because no enough space
    mctx = MemoryContextCreate(mctx, MEM_CXT_TYPE_STACK, "ut-memory_context",
                               MCTX_UNUSED, MCTX_UNUSED, MB(10));
    auto point = MemoryContextSavePoint(mctx);
    ASSERT_EQ(point, MemoryContextSavePoint(mctx));
    ASSERT_NE(MemoryContextAllocEx(mctx, MB(9), MCTX_ALLOC_FLAG_ZERO), nullptr);
    // we can't know the max memory can allocate, using for loop to alloc
    while (MemoryContextAllocEx(mctx, 1, MCTX_ALLOC_FLAG_ZERO) != nullptr);
    // repeat call MemoryContextRestorePoint is ok
    MemoryContextRestorePoint(mctx, point);
    MemoryContextRestorePoint(mctx, point);
    ASSERT_NE(MemoryContextAllocEx(mctx, 1, MCTX_ALLOC_FLAG_ZERO), nullptr);
    ASSERT_NE(MemoryContextAllocEx(mctx, MB(9), MCTX_ALLOC_FLAG_ZERO), nullptr);
}

TEST_F(MemoryContextTest, MemoryContextResetTest)
{
    vector<MemoryContext> vctx;
    // generic memory context, max alloc less than KB(8)
    auto ctx = MemoryContextCreate(g_rootGenericMctx, MEM_CXT_TYPE_GENERIC, TEST_MCTX_NAME, KB(0), KB(8), KB(8));
    ASSERT_NE(ctx, nullptr);
    vctx.push_back(ctx);
    // share memory context
    ctx = MemoryContextCreate(g_rootShareMctx, MEM_CXT_TYPE_SHARE, NULL, MCTX_UNUSED, MCTX_UNUSED, KB(8));
    ASSERT_NE(ctx, nullptr);
    vctx.push_back(ctx);
    // simple share memory context, need MemFree by caller themselves
    ctx = MemoryContextCreate(g_rootSimpleShareMctx, MEM_CXT_TYPE_SIMPLE_SHARE, NULL, MCTX_UNUSED, MCTX_UNUSED, KB(8));
    ASSERT_NE(ctx, nullptr);
    auto p0 = MemoryContextAlloc(ctx, KB(7));
    ASSERT_NE(p0, nullptr);
    ASSERT_EQ(MemoryContextAlloc(ctx, KB(2)), nullptr);
    MemoryContextReset(ctx);  // simple share memory context reset is only check empty and print log
    ASSERT_EQ(MemoryContextAlloc(ctx, KB(2)), nullptr);
    ASSERT_EQ(MemoryContextAlloc(ctx, KB(7)), nullptr);
    MemoryContextReset(ctx);
    MemFree(p0);

    MemoryContextDelete(ctx);
    // stack memory context
    ctx = MemoryContextCreate(g_rootStackMctx, MEM_CXT_TYPE_STACK, NULL, MCTX_UNUSED, MCTX_UNUSED, KB(8));
    ASSERT_NE(ctx, nullptr);
    for (int i = 0; i < 5; i++) { // test 5 times
        ASSERT_NE(MemoryContextAlloc(ctx, KB(1)), nullptr);
        while(MemoryContextAlloc(ctx, KB(1)) != nullptr);
        ASSERT_NE(MemoryContextAlloc(ctx, 1), nullptr);
        while(MemoryContextAlloc(ctx, 1) != nullptr);
        ASSERT_EQ(MemoryContextAlloc(ctx, 1), nullptr);
        MemoryContextReset(ctx);
    }
    MemoryContextDelete(ctx);

    for (auto &tmpctx : vctx) {
        // generic share and simple share not alloc to maxSize
        ASSERT_NE(MemoryContextAlloc(tmpctx, KB(7)), nullptr);
        ASSERT_EQ(MemoryContextAlloc(tmpctx, KB(2)), nullptr);
        MemoryContextReset(tmpctx);
        ASSERT_NE(MemoryContextAlloc(tmpctx, KB(2)), nullptr);
        ASSERT_EQ(MemoryContextAlloc(tmpctx, KB(7)), nullptr);
        MemoryContextReset(tmpctx);
        ASSERT_NE(MemoryContextAlloc(tmpctx, KB(7)), nullptr);
        MemoryContextDelete(tmpctx);
    }
}

TEST_F(MemoryContextTest, MiscBaseFuncByCurrentContextTest)
{
    /* alloc */
    auto p0 = MemAlloc0(MB(500));
    auto p1 = MemAlloc0(KB(500));
    auto p2 = MemAlloc0(500);
    ASSERT_NE(p0, nullptr);
    ASSERT_NE(p1, nullptr);
    ASSERT_NE(p2, nullptr);

    /* strdup */
    constexpr const char *oldStr = "hello world!";
    auto newStr = MemoryContextStrdup(g_rootGenericMctx, oldStr);
    /* oldStr and newStr address different, but content is same */
    ASSERT_NE(oldStr, newStr);
    ASSERT_TRUE(strcmp(oldStr, newStr) == 0);
    auto oldCtx = MemoryContextSwitchTo(g_rootGenericMctx);
    ASSERT_NE(MemStrdup(oldStr), newStr);
    ASSERT_EQ(strcmp(MemStrdup(oldStr), newStr), 0);  // string equal
    auto  halfLen = strlen(oldStr) / 2;
    ASSERT_NE(MemNStrdup(oldStr, halfLen), newStr);
    ASSERT_EQ(strncmp(MemNStrdup(oldStr, halfLen), newStr, halfLen), 0);  // string equal
    (void)MemoryContextSwitchTo(oldCtx);

    /* free and set null */
    MemFreeAndSetNull(&p0);
    MemFreeAndSetNull(&p1);
    MemFreeAndSetNull(&p2);
    ASSERT_EQ(p0, nullptr);
    ASSERT_EQ(p1, nullptr);
    ASSERT_EQ(p2, nullptr);

    /* realloc in generic mctx */
    auto oldMctx = MemoryContextSwitchTo(g_rootGenericMctx);
    auto p4 = MemAlloc0(KB(500));
    ASSERT_NE(p4, nullptr);
    p4 = MemRealloc(p4, MB(500));
    ASSERT_NE(p4, nullptr);
    ASSERT_NE(MemRealloc(p4, 500), nullptr);
    /* realloc in share mctx */
    (void)MemoryContextSwitchTo(g_rootShareMctx);
    auto p5 = MemAlloc0(KB(500));
    ASSERT_NE(p5, nullptr);
    p5 = MemRealloc(p5, MB(500));
    ASSERT_NE(p5, nullptr);
    ASSERT_NE(MemRealloc(p5, 500), nullptr);

    (void)MemoryContextSwitchTo(oldMctx);
}

TEST_F(MemoryContextTest, StackAndGenericStatsTest)
{
    auto testMemoryCtx = {g_rootStackMctx, g_rootGenericMctx};
    auto calculateInput = [](size_t &usageSpace, size_t allocSize, size_t &chunks) {
        usageSpace += allocSize;
        chunks++;
    };
    for (auto &ctx : testMemoryCtx) {
        MemoryContextSwitchTo(ctx);
        /* begin allocate test */
        auto testCount = 20;
        size_t checkLine = 0, lineAlloc = 0, totalAlloc = 0, lineChunks = 0, allocChunks = 0;
        for (int i = 0; i < testCount; i++) {
            srand(time(NULL));
            auto allocSize = rand() % MB(1) + 1;  // ignore size of 0
            ASSERT_NE(MemAlloc(allocSize), nullptr);
            calculateInput(totalAlloc, allocSize, allocChunks);
            ASSERT_NE(MemAlloc0(allocSize), nullptr);
            calculateInput(totalAlloc, allocSize, allocChunks);
            ASSERT_NE(MemRealloc(MemAlloc0(allocSize), allocSize * 2), nullptr);
            calculateInput(totalAlloc, allocSize * 2, allocChunks);
            ASSERT_NE(MemAllocEx(allocSize, MCTX_ALLOC_FLAG_DEFAULT), nullptr);
            calculateInput(totalAlloc, allocSize, allocChunks);
            ASSERT_NE(MemAllocEx(allocSize, MCTX_ALLOC_FLAG_ZERO), nullptr);
            calculateInput(totalAlloc, allocSize, allocChunks);

            allocSize = rand() % KB(10) + 1;  // ignore size of 0, test little size memory
            ASSERT_NE(MemRealloc(MemAlloc0(allocSize), allocSize * 2), nullptr);
            calculateInput(totalAlloc, allocSize * 2, allocChunks);
            ASSERT_NE(MemAllocEx(allocSize, MCTX_ALLOC_FLAG_OOM), nullptr);
            calculateInput(totalAlloc, allocSize, allocChunks);
            ASSERT_NE(MemAllocEx(allocSize, MCTX_ALLOC_FLAG_OOM | MCTX_ALLOC_FLAG_ZERO), nullptr);
            calculateInput(totalAlloc, allocSize, allocChunks);
            checkLine = ((uintptr_t)MemAlloc(allocSize), __LINE__);
            calculateInput(totalAlloc, allocSize, allocChunks);
            calculateInput(lineAlloc, allocSize, lineChunks);
        }
        MemStat stat;
        MemStatFilter filter = {MEM_STAT_CURRENT_CTX};
        MemoryContextStats(ctx, true, &filter, &stat);
        ASSERT_GT(stat.totalSpace, totalAlloc); // there are some memory overhead
        ASSERT_EQ(stat.totalChunks - stat.freeChunks, allocChunks);

        filter.type = MEM_STAT_CURRENT_CTX_BY_POSITION;
        filter.pos.line = checkLine;
        filter.pos.file = __FILE__;
        MemoryContextStats(ctx, true, &filter, &stat);
        ASSERT_EQ(stat.totalSpace, lineAlloc);
        ASSERT_EQ(stat.totalChunks, lineChunks);
    }
}

TEST_F(MemoryContextTest, ThreeThreadShareStatsTest)
{
    MemoryContext testCtx = g_rootShareMctx;
    atomic_ulong checkLine(0), lineAlloc(0), totalAlloc(0), lineChunks(0), allocChunks(0);
    /* simple share context can't free all by once, test alone */
    auto calculateInput = [](atomic_ulong &usageSpace, size_t allocSize, atomic_ulong &chunks) {
        usageSpace += allocSize;
        chunks++;
    };
    auto threadMain = [&]() {
        MemoryContextSwitchTo(testCtx);
        auto testCount = 2000;
        for (int i = 0; i < testCount; i++) {
            srand(time(NULL));
            auto allocSize = rand() % KB(100) + 1;  // ignore size of 0
            ASSERT_NE(MemAlloc(allocSize), nullptr);
            calculateInput(totalAlloc, allocSize, allocChunks);
            ASSERT_NE(MemRealloc(MemAlloc0(allocSize), allocSize * 2), nullptr);
            calculateInput(totalAlloc, allocSize * 2, allocChunks);
            ASSERT_NE(MemAllocEx(allocSize, MCTX_ALLOC_FLAG_OOM), nullptr);
            calculateInput(totalAlloc, allocSize, allocChunks);
            ASSERT_NE(MemRealloc(MemAlloc0(allocSize),  allocSize / 2 + 1), nullptr);
            calculateInput(totalAlloc, allocSize / 2 + 1, allocChunks);
            ASSERT_NE(MemAllocEx(allocSize, MCTX_ALLOC_FLAG_OOM | MCTX_ALLOC_FLAG_ZERO), nullptr);
            calculateInput(totalAlloc, allocSize, allocChunks);
            checkLine = ((uintptr_t)MemAlloc(allocSize), __LINE__);
            calculateInput(totalAlloc, allocSize, allocChunks);
            calculateInput(lineAlloc, allocSize, lineChunks);
        }
    };
    vector<thread> vThread;
    vThread.push_back(thread(threadMain));
    vThread.push_back(thread(threadMain));
    vThread.push_back(thread(threadMain));
    for (auto &t : vThread) {
        t.join();
    }

    MemStat stat;
    MemStatFilter filter = {MEM_STAT_CURRENT_CTX};
    MemoryContextStats(testCtx, true, &filter, &stat);
    ASSERT_GT(stat.totalSpace, totalAlloc); // there are some memory overhead
    ASSERT_EQ(stat.totalChunks - stat.freeChunks, allocChunks);

    filter.type = MEM_STAT_CURRENT_CTX_BY_POSITION;
    filter.pos.line = checkLine;
    filter.pos.file = __FILE__;
    MemoryContextStats(testCtx, true, &filter, &stat);
    ASSERT_EQ(stat.totalSpace, lineAlloc);
    ASSERT_EQ(stat.totalChunks, lineChunks);
}

TEST_F(MemoryContextTest, ThreeThreadSimpleShareStatsTest)
{
    MemoryContext testCtx = g_rootSimpleShareMctx;
    MemStat stat;
    MemStatFilter filter = {MEM_STAT_CURRENT_CTX};
    MemoryContextStats(testCtx, true, &filter, &stat); // firstly, query stat as init data
    auto initTotalSpace = stat.totalSpace;
    atomic_ulong totalAlloc(0), allocChunks(0);
    // simple share can't get statistics by file position, and context can't free all by once, test alone */
    auto calculateInput = [](atomic_ulong &usageSpace, size_t allocSize, atomic_ulong &chunks) {
        usageSpace += allocSize;
        chunks++;
    };
    auto threadMain = [&](vector<void *> *v) {
        vector<void *> &vPtr = *v;
        MemoryContextSwitchTo(testCtx);
        auto testCount = 2000;
        for (int i = 0; i < testCount; i++) {
            srand(time(NULL));
            auto allocSize = rand() % KB(100) + 1;  // ignore size of 0
            vPtr.push_back(MemAlloc(allocSize));
            ASSERT_NE(vPtr.back(), nullptr);
            calculateInput(totalAlloc, allocSize, allocChunks);
            vPtr.push_back(MemRealloc(MemAlloc0(allocSize), allocSize * 2));
            ASSERT_NE(vPtr.back(), nullptr);
            calculateInput(totalAlloc, allocSize * 2, allocChunks);
            vPtr.push_back(MemRealloc(MemAlloc0(allocSize),  allocSize / 2 + 1));
            ASSERT_NE(vPtr.back(), nullptr);
            calculateInput(totalAlloc, allocSize / 2 + 1, allocChunks);
        }
    };
    vector<thread> vThread;
    vector<void *>vPtr[3];
    for (auto &v : vPtr) {
        vThread.push_back(thread(threadMain, &v));
    }
    for (auto &t : vThread) {
        t.join();
    }

    MemoryContextStats(testCtx, true, &filter, &stat);
    ASSERT_GT(stat.totalSpace, totalAlloc); // there are some memory overhead
    ASSERT_EQ(stat.totalChunks - stat.freeChunks, allocChunks);

    for (auto &v : vPtr) {
        for (auto p : v) {
            MemFree(p);
        }
    }
    MemoryContextStats(testCtx, true, &filter, &stat);
    ASSERT_EQ(stat.totalSpace, initTotalSpace); // all freed, totalSpace need restore to init state
    ASSERT_EQ(stat.totalChunks - stat.freeChunks, 0);
}

TEST_F(MemoryContextTest, StatsAfterResetTest)
{
    auto testMemoryCtx = {g_rootStackMctx, g_rootGenericMctx, g_rootSimpleShareMctx, g_rootShareMctx};
    auto testCount = 100;
    for (auto ctx : testMemoryCtx) {
        MemoryContextSwitchTo(ctx);

        MemStat initStat;
        MemoryContextStats(ctx, true, NULL, &initStat);

        vector<void *> vPtr;
        for (int i = 0; i < testCount; i++) {
            srand(time(NULL));
            vPtr.push_back(MemAlloc(rand() % KB(10) + 1));
            ASSERT_NE(vPtr.back(), nullptr);

            vPtr.push_back(MemRealloc(MemAlloc0(rand() % KB(5) + 1), rand() % KB(10) + 1));
            ASSERT_NE(vPtr.back(), nullptr);

            vPtr.push_back(MemRealloc(MemAlloc0(rand() % KB(500) + 1),  rand() % MB(1) + 1));
            ASSERT_NE(vPtr.back(), nullptr);
        }
        for (auto ptr : vPtr) {
            MemFree(ptr);
        }
        MemoryContextReset(ctx);
        /* after reset test */
        MemStat stat;
        MemoryContextStats(ctx, true, NULL, &stat);
        ASSERT_EQ(stat.totalSpace,  initStat.totalSpace); // reset to initial statistics data
        ASSERT_EQ(stat.freeSpace,   initStat.freeSpace);
        ASSERT_EQ(stat.totalChunks, initStat.totalChunks);
        ASSERT_EQ(stat.freeChunks,  initStat.freeChunks);
        ASSERT_EQ(stat.totalBlocks, initStat.totalBlocks);
    }
}

TEST_F(MemoryContextTest, GenericRecurseStatsTest)
{
    auto testParentCtx = g_rootGenericMctx;
    auto calculateInput = [](size_t &usageSpace, size_t allocSize, size_t &chunks) {
        usageSpace += allocSize;
        chunks++;
    };
    vector<tuple<MemoryContext, vector<MemoryContext>>> testCtx;
    /* firstly, create 2(level)*3(children) memory context */
    for (int i = 0; i < 3; i++) {      // level 1
#define RECURSE_STATS_TEST_PARAM       0, DEFAULT_UNLIMITED_SIZE, MB(500)
        vector<MemoryContext> vec;
        auto type = GetMemoryContextType(testParentCtx);
        auto mctx = MemoryContextCreate(testParentCtx, type, "RecurseStatsTest", RECURSE_STATS_TEST_PARAM);
        ASSERT_NE(mctx, nullptr);
        for (int j = 0; j < 3; j++) {  // level 2
            vec.push_back(MemoryContextCreate(mctx, type, "RecurseStatsTest", RECURSE_STATS_TEST_PARAM));
            ASSERT_NE(vec.back(), nullptr);
        }
        testCtx.push_back(make_tuple(mctx, vec));
    }
    MemStat stat;
    MemStatFilter filter = {MEM_STAT_ALL_CTX};
    MemoryContextStats(testParentCtx, true, &filter, &stat);  // firstly, query stat as init data
    auto blockSize = (stat.totalSpace - stat.freeSpace) / stat.totalBlocks;
    ASSERT_EQ(stat.totalChunks, stat.freeChunks); // chunks is clear, no inused

    vector<void *>vPtr;
    size_t checkLine = 0, lineAlloc = 0, totalAlloc = 0, lineChunks = 0, allocChunks = 0;
    /* allocate memory, and save the pointer, if we free all the pointer, the statistics need to be same as initital statistics */
    for (auto &tup : testCtx) {
        MemoryContext ctxT;
        vector<MemoryContext> vecT;
        tie(ctxT, vecT) = move(tup);
        auto testFunc = [&](MemoryContext ctxA) {
            MemoryContextSwitchTo(ctxA);
            srand(time(NULL));
            auto allocSize = rand() % MB(1) + 1;  // ignore size of 0
            vPtr.push_back(MemAlloc(allocSize));  // push pointer to vPtr vector
            ASSERT_NE(vPtr.back(), nullptr);
            calculateInput(totalAlloc, allocSize, allocChunks);
            vPtr.push_back(MemRealloc(MemAlloc0(allocSize), allocSize * 2));
            ASSERT_NE(vPtr.back(), nullptr);
            calculateInput(totalAlloc, allocSize * 2, allocChunks);

            allocSize = rand() % KB(10) + 1;  // little size
            vPtr.push_back(MemRealloc(MemAlloc0(allocSize),  allocSize / 2 + 1));
            ASSERT_NE(vPtr.back(), nullptr);
            calculateInput(totalAlloc, allocSize / 2 + 1, allocChunks);
            checkLine = ((uintptr_t)(*vPtr.insert(vPtr.begin() + 1, MemAlloc(allocSize))), __LINE__);  // push pointer to vPtr vector and set checkLine to the line
            calculateInput(totalAlloc, allocSize, allocChunks);
            calculateInput(lineAlloc, allocSize, lineChunks);
        };
        testFunc(ctxT);
        for (MemoryContext &ctxV : vecT) {
            testFunc(ctxV);
        }
    }
    MemoryContextStats(testParentCtx, true, &filter, &stat); // secondly, query stat after test allocate
    ASSERT_GT(stat.totalSpace, totalAlloc);   // there are some memory overhead
    ASSERT_EQ(stat.totalChunks - stat.freeChunks, allocChunks);

    filter.type = MEM_STAT_ALL_CTX_BY_POSITION;
    filter.pos.line = checkLine;
    filter.pos.file = __FILE__;
    MemoryContextStats(testParentCtx, true, &filter, &stat);
    ASSERT_EQ(stat.totalSpace, lineAlloc);
    ASSERT_EQ(stat.totalChunks, lineChunks);

    for (auto ptr : vPtr) {  // release all memory
        MemFree(ptr);
    }
    filter.type = MEM_STAT_CURRENT_CTX;
    MemoryContextStats(testParentCtx, true, &filter, &stat); // lastly, query stat after freed
    auto blockSizeAfterReset = (stat.totalSpace - stat.freeSpace) / stat.totalBlocks;
    ASSERT_EQ(blockSizeAfterReset, blockSize); // all freed, totalSpace is zero
    ASSERT_EQ(stat.totalChunks, stat.freeChunks); // chunks is clear, no inused
}

// ----------------------------- Random test -----------------------------------
#define OP_MALLOC    0
#define OP_REALLOC   1
#define OP_FREE      2
#define OP_MAX       3
typedef struct {
    int32_t op;
    int32_t size;
    int32_t idx;
} TestData;

static void GenerateTestData001(vector<TestData> &data, int testNums)
{
    TestData dat;
    vector<int> allocIndex;
    srand(time(NULL));
    for (auto i = 0; i < testNums; i++) {
        dat.op = rand() % OP_MAX;
        if (allocIndex.size() == 0 && (dat.op == OP_FREE || dat.op == OP_REALLOC)) {
            dat.op = OP_MALLOC;
        }
        if (dat.op == OP_MALLOC) {
            dat.size = rand() % MB(2) + 1;  // ignore size of 0
            allocIndex.push_back(i);
        } else if (dat.op == OP_REALLOC) {
            dat.size = rand() % MB(2) + 1;  // ignore size of 0
            dat.idx = allocIndex[rand() % allocIndex.size()];
        } else {
            auto idx = rand() % allocIndex.size();
            dat.idx = allocIndex[idx];
            allocIndex.erase(allocIndex.begin() + idx);
        }
        data.push_back(dat);
    }
}

// generic memory context alloc
TEST_F(MemoryContextTest, AllocRandomMemoryByCurrentGenericContext)
{
#define NO_CONTEXT_TEST_COUNT  500
    void *p;
    vector<TestData> data;
    vector<void *> pointer(NO_CONTEXT_TEST_COUNT);
    MemoryContext mctx = MemoryContextCreate(g_rootGenericMctx, MEM_CXT_TYPE_GENERIC, TEST_MCTX_NAME,
                                             0, DEFAULT_UNLIMITED_SIZE, DEFAULT_UNLIMITED_SIZE);
    ASSERT_NE(mctx, nullptr);
    MemoryContext old = MemoryContextSwitchTo(mctx);

    GenerateTestData001(data, NO_CONTEXT_TEST_COUNT);
    // make sure system have 1G free memory
    ASSERT_GT(sysconf(_SC_PHYS_PAGES) * sysconf(_SC_PAGE_SIZE), MB(1024));
    // mctx alloc
    for (auto i = 0; i < data.size(); i++) {
        auto &dat = data[i];
        switch (dat.op) {
            case OP_MALLOC:
                if (dat.size < MB(1)) {
                    p = MemAlloc0(dat.size);
                } else {
                    p = MemAlloc(dat.size);
                }
                ASSERT_NE(p, nullptr);
                pointer[i] = p;
                break;
            case OP_REALLOC:
                p = MemRealloc(pointer[dat.idx], dat.size);
                ASSERT_NE(p, nullptr);
                pointer[dat.idx] = p;
                break;
            case OP_FREE:
                MemFree(pointer[dat.idx]);
                break;
            default:
                ASSERT_TRUE(false);
                break;
        }
    }
    MemoryContextSwitchTo(old);
}

void StackContextTest(MemoryContext mctx, int maxDeepth, int *actual, int actualMax)
{
    if (maxDeepth == 0) {
        return;
    }
    auto point = MemoryContextSavePoint(mctx);

    auto size1 = rand() % MB(1);
    auto size2 = rand() % KB(1);
    auto size3 = rand() % MB(10);
    auto p1 = MemAlloc0(size1);
    auto p2 = MemAlloc0(size2);
    auto p3 = MemRealloc(p1, size3);
    MemFree(p2);
    (*actual)++;
    // if p1 or p2 or p3 not NULL, indicate mctx have free memory alloc
    if ((p1 != NULL) || (p2 != NULL) || (p3 != NULL)) {
        while (*actual != actualMax) {
            StackContextTest(mctx, maxDeepth - 1, actual, actualMax);
        }
    }
    MemoryContextRestorePoint(mctx, point);
}

// stack memory context alloc
TEST_F(MemoryContextTest, AllocRandomMemoryByCurrentStackContext)
{
    MemoryContext mctx = MemoryContextCreate(g_rootStackMctx, MEM_CXT_TYPE_STACK, TEST_MCTX_NAME,
                                             MCTX_UNUSED, MCTX_UNUSED, MB(512));
    ASSERT_NE(mctx, nullptr);
    MemoryContextSetSilent(mctx, true);
    MemoryContext old = MemoryContextSwitchTo(mctx);

    // make sure system have 512M free memory
    ASSERT_GT(sysconf(_SC_PHYS_PAGES) * sysconf(_SC_PAGE_SIZE), MB(512));
    srand(time(NULL));
    int actual = 0;
    StackContextTest(mctx, 1000, &actual, 2000); // test 1000 times
    // mctx alloc
    MemoryContextSwitchTo(old);
}

/* generate test data sequence, op is execute malloc or free,
 * when op is malloc, size is the alloc memory size, or op is free, size is the x'th malloc memory pointer
 *
 *  op   : malloc  malloc  malloc free        free         ...
 *  size : 100     208     500    1(free 208) 0(free 100)  ...
 */
static void GenerateTestData002(TestData *Arr, size_t ArrSize, size_t totalMax, size_t singleMax)
{
    size_t curTotal = 0;
    size_t total = 0;
    uint32_t freeIdx = 0;
    srand(time(NULL));
    for (auto count = 0; count < ArrSize; count++) {
        uint32_t size = 0;
        while (size == 0) {
            size = rand() % singleMax;
        }
        curTotal = total;
        total += size;
        auto end = count;
        for (; freeIdx < end; freeIdx++) {
            if (total < totalMax) {
                break;
            }
            if (count + 1 >= ArrSize) {
                size = totalMax - curTotal;
                break;
            }
            if (Arr[freeIdx].op == OP_FREE) {
                continue;
            }
            total -= Arr[freeIdx].size;
            Arr[count].op = OP_FREE;
            Arr[count].idx = freeIdx;
            count++;
        }
        Arr[count].op = OP_MALLOC;
        Arr[count].size = size;
    }
}

static void AllocMemoryMultiThreadMain(MemoryContext mctx, TestData *data, size_t size, bool release)
{
    vector<void *> pointer(size);
    unique_lock<mutex> lk(mu);
    cv.wait(lk, []{ return ready; });
    for (auto i = 0; i < size; i++) {
        if (data[i].op == OP_MALLOC) {
            void *p = MemoryContextAllocEx(mctx, data[i].size, MCTX_ALLOC_FLAG_ZERO);
            ASSERT_NE(p, nullptr);
            pointer[i] = p;
        } else if (data[i].op == OP_FREE) {
            MemFreeAndSetNull(pointer.data() + data[i].idx);
        }
    }
    // if is the simple share memory context, need free memory or memleak
    if (release) {
        for (auto &p : pointer) {
            MemFree(p);
        }
    }
    CloseLogger();
}

TEST_F(MemoryContextTest, AllocRandomMemoryWithMultiThreadByShareContext)
{
#define THREAD_COUNT 8
#define TEST_COUNT 2000
    TestData data[TEST_COUNT];
    thread th[THREAD_COUNT];
    // make sure system have 1G free memory
    ASSERT_GT(sysconf(_SC_PHYS_PAGES) * sysconf(_SC_PAGE_SIZE), MB(1024));
    GenerateTestData002(data, TEST_COUNT, MB(128), MB(2));
    MemoryContextSetSilent(g_rootShareMctx, true);
    for (auto i = 0; i < THREAD_COUNT; i++) {
        th[i] = thread(AllocMemoryMultiThreadMain, g_rootShareMctx, data, TEST_COUNT, false);
    }
    {
        lock_guard<mutex> lk(mu);
        ready = true;
    }
    cv.notify_all();
    for (auto i = 0; i < THREAD_COUNT; i++) {
        th[i].join();
    }
}

TEST_F(MemoryContextTest, AllocRandomMemoryWithMultiThreadBySimpleShareContext)
{
#define THREAD_COUNT 8
#define TEST_COUNT 2000
    TestData data[TEST_COUNT];
    thread th[THREAD_COUNT];
    // make sure system have 1G free memory
    ASSERT_GT(sysconf(_SC_PHYS_PAGES) * sysconf(_SC_PAGE_SIZE), MB(1024));
    GenerateTestData002(data, TEST_COUNT, MB(128), MB(2));
    for (auto i = 0; i < THREAD_COUNT; i++) {
        MemoryContextSetSilent(g_rootSimpleShareMctx, true);
        th[i] = thread(AllocMemoryMultiThreadMain, g_rootSimpleShareMctx, data, TEST_COUNT, true);
    }
    {
        lock_guard<mutex> lk(mu);
        ready = true;
    }
    cv.notify_all();
    for (auto i = 0; i < THREAD_COUNT; i++) {
        th[i].join();
    }
}

TEST_F(MemoryContextTest, MemoryContextCheckTest)
{
    // disable child process memleak check for temporary, becase the leak check run too loog time (about 17s),
    // and gtest dead test also can't run with multi-thread. so disable memleak for this case
    putenv("ASAN_OPTIONS=detect_leaks=0");
    /* SimpleShare memory context not support active check */
    auto testMemoryCtx = {g_rootStackMctx, g_rootGenericMctx, g_rootShareMctx};
    auto testFunc = [](MemoryContext ctx, bool writeOutOfBound) {
        StopLogger(); // firstly, StopLogger then the errlog will output to stdout to checked by gtest
        auto randomSize = (srand(time(NULL)), rand() % (MB(300) + 1));
        auto ptr = (char *)MemoryContextAllocEx(ctx, randomSize, MCTX_ALLOC_FLAG_ZERO);
        if (writeOutOfBound) {
            ptr[randomSize] = 'x'; // [0, randomSize - 1] is vaild
        }
        MemoryContextCheck(ctx);
        exit(0); /* ASSERT_EXIT will check called exit or not, had go here is proved normal exit */
    };
    for (auto &ctx : testMemoryCtx) {
        ASSERT_EXIT(testFunc(ctx, false), testing::ExitedWithCode(0), Not(::testing::ContainsRegex("out of bound write"))); // normal
#if defined(__SANITIZE_ADDRESS__)
        // got "AddressSanitizer: unknown-crash" or "AddressSanitizer: use-after-poison"
        ASSERT_DEATH(testFunc(ctx, true), "AddressSanitizer: (unknown-crash|use-after-poison)");
#else
        ASSERT_DEATH(testFunc(ctx, true), "out of bound write");
#endif
    }
    putenv("ASAN_OPTIONS=detect_leaks=1"); // retore memleak check if enabled by lasn
}

/* test scene, simulate user use memory
 * firstly allocate some memory from a series of predefined size,
 * then, happended a out of bound write a byte in random allocated pointer,
 * lastly, when memory freeing by user
 * expect memory context report some error, (or errlog or abort or asan report) */
TEST_F(MemoryContextTest, OutOfBoundCheckTest)
{
    auto testMemoryCtx = {g_rootStackMctx, g_rootGenericMctx, g_rootSimpleShareMctx, g_rootShareMctx};
    /* out of bound check test main routine
     * @param[in] ctx             - the memory context
     * @param[in] testSize        - the array of test to Malloc size
     * @param[in] allocCount      - allocate 'allocCount' quantity pointers
     */
    auto funcOutOfBoundCheckTest = [](MemoryContext ctx, vector<size_t> testSize, uint32_t allocCount) {
        vector<char *> vPtr(allocCount);
        StopLogger(); // firstly, StopLogger then the errlog will output to stdout to checked by gtest
        for (auto &size : testSize) {
            for (int i = 0; i < allocCount; i++) {
                vPtr[i] = (char *)MemoryContextAlloc(ctx, size);
                ASSERT_NE(vPtr[i], nullptr);
                (void)memset_s(vPtr[i], size, 0, size); // all memory can use
            }
            srand(time(NULL));
            vPtr[rand() % allocCount][size] = 'x';  // pick a pointer by random and out of bound write 1 bytes
            for (auto &ptr : vPtr) {
                MemFree(ptr);  // when free the memory will trigger the out of bound check
            }
            MemoryContextReset(ctx);
        }
    };
    vector<size_t> testTypicalSize = {1, 2, 5, 10, 100, 200, 500, KB(2), KB(10), KB(100), MB(1)};
    for (auto &ctx : testMemoryCtx) {
#if defined(__SANITIZE_ADDRESS__)
        // got "AddressSanitizer: unknown-crash" or "AddressSanitizer: use-after-poison"
        ASSERT_DEATH(funcOutOfBoundCheckTest(ctx, testTypicalSize, 100), "AddressSanitizer: (unknown-crash|use-after-poison)");
#else
        ASSERT_DEATH(funcOutOfBoundCheckTest(ctx, testTypicalSize, 100), "out of bound write");
#endif
    }

    vector<size_t> testBigSize = {MB(5), MB(100), MB(200), MB(400)};
    for (auto &ctx : testMemoryCtx) {
#if defined(__SANITIZE_ADDRESS__)
        ASSERT_DEATH(funcOutOfBoundCheckTest(ctx, testBigSize, 2), "AddressSanitizer: (unknown-crash|use-after-poison)");
#else
        ASSERT_DEATH(funcOutOfBoundCheckTest(ctx, testBigSize, 2), "out of bound write");
#endif
    }
}
