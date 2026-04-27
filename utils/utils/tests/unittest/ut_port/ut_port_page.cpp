
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
 * ut_port_page.cpp
 * Developer test of memory page.
 *
 * ---------------------------------------------------------------------------------
 */
#include <gtest/gtest.h>
#include <mockcpp/mockcpp.hpp>
#include "securec.h"
#include "port/platform_port.h"

/** ************************************************************************************************************* **/

/** ************************************************************************************************************* **/

class PageTest : public testing::Test {
public:
    void SetUp() override
    {
        return;
    };

    void TearDown() override
    {
        GlobalMockObject::verify();
    };
};

/**
 * @tc.name:  PageFunction001_Level0
 * @tc.desc:  Test the page configure and boot.
 * @tc.type: FUNC
 */
TEST_F(PageTest, PageFunction001_Level0)
{
    /**
     * @tc.steps: step1. Test the page configure  functions
     * @tc.expected: step1.The page configure  are success.
     */
    SetMemMapFailureHandleBehavior(MEMORY_FAILURE_HANDLE_BEHAVIOR_ABORT);
    MemoryFailureHandleBehavior failureHandleBehavior = GetMemMapFailureHandleBehavior();
    EXPECT_EQ(failureHandleBehavior, MEMORY_FAILURE_HANDLE_BEHAVIOR_ABORT);
    SetMemMapOsPageSize(MEMORY_PAGE_SIZE);
    size_t size = GetMemMapOsPageSize();
    EXPECT_EQ(size, MEMORY_PAGE_SIZE);
    SetMemMapOvercommit(true);
    bool overcommit = GetMemMapOvercommit();
    ASSERT_TRUE(overcommit);
    SetMemMapTransparentHugePageMode(TRANSPARENT_HUGE_PAGE_MODE_DEFAULT);
    TransparentHugePageMode transparentHugePageMode = GetMemMapTransparentHugePageMode();
    EXPECT_EQ(transparentHugePageMode, TRANSPARENT_HUGE_PAGE_MODE_DEFAULT);

    /**
     * @tc.steps: step2. Test page boot result.
     * @tc.expected: step2.The page boot succeed.
     */
    bool bootResult = MemPagesBoot();
    ASSERT_TRUE(bootResult);
    bool mmapResult = GetMemBootCheckResultForMmap();
    ASSERT_TRUE(mmapResult);
    bool madviseDontneedResult = GetMemBootCheckResultForMadviseDontneed();
    ASSERT_TRUE(madviseDontneedResult);
    bool pageSizeResult = GetMemBootCheckResultForPageSize();
    ASSERT_TRUE(pageSizeResult);
}

/**
 * @tc.name:  PageFunction002_Level0
 * @tc.desc:  Test the page malloc/free and settings.
 * @tc.type: FUNC
 */
TEST_F(PageTest, PageFunction002_Level0)
{
    /**
     * @tc.steps: step1. Test the page alloc functions
     * @tc.expected: step1.The page alloc succeed.
     */
    void *addr = MemPagesAlloc(MEMORY_PAGE_SIZE);
    EXPECT_NE(addr, nullptr);

    /**
     * @tc.steps: step2. Test the page setting functions.
     * @tc.expected: step2.The page set succeed.
     */
    bool result = false;
    result = MemPagesCommit(addr, MEMORY_PAGE_SIZE);
    ASSERT_TRUE(result);
    result = MemPagesDecommit(addr, MEMORY_PAGE_SIZE);
    ASSERT_TRUE(result);
    result = MemPagesDontDump(addr, MEMORY_PAGE_SIZE);
    ASSERT_TRUE(result);
    result = MemPagesDoDump(addr, MEMORY_PAGE_SIZE);
    ASSERT_TRUE(result);
    result = MemPagesMarkGuard(addr, MEMORY_PAGE_SIZE);
    ASSERT_TRUE(result);
    result = MemPagesUnmarkGuard(addr, MEMORY_PAGE_SIZE);
    ASSERT_TRUE(result);
    result = MemPagesHuge(addr, MEMORY_PAGE_SIZE);
    ASSERT_TRUE(result);
    result = MemPagesNoHuge(addr, MEMORY_PAGE_SIZE);
    ASSERT_TRUE(result);
    /* failed in V5 docker, MADV_FREE redefined */
    // result = MemPagesPurgeLazy(addr, MEMORY_PAGE_SIZE); # TODO: need to fix in V5 docker
    // ASSERT_TRUE(result);
    result = MemPagesPurgeForced(addr, MEMORY_PAGE_SIZE);
    ASSERT_TRUE(result);

    /**
     * @tc.steps: step3. Test the page free functions
     * @tc.expected: step3.The page free succeed.
     */
    MemPagesFree(addr, MEMORY_PAGE_SIZE);
    EXPECT_DEATH(
            {
                fprintf(stderr, "write the addr which have freed");
                *(int *)addr = 0;
            },
            "\\w");
}

/**
 * @tc.name:  PageFunction003_Level0
 * @tc.desc:  Test the page map and unmap.
 * @tc.type: FUNC
 */
TEST_F(PageTest, PageFunction003_Level0)
{
    /**
     * @tc.steps: step1. Test the page map attribute set.
     * @tc.expected: step1.The page map attribute set succeed.
     */
    MemMapAttribute memMapAttribute;
    MemMapAttributeInit(&memMapAttribute);
    MemMapAttributeSetAddr(&memMapAttribute, NULL);
    MemMapAttributeSetLength(&memMapAttribute, MEMORY_PAGE_SIZE);
    MemMapAttributeSetProtect(&memMapAttribute, MEM_MAP_PROTECT_READ | MEM_MAP_PROTECT_WRITE);
    MemMapAttributeSetFlags(&memMapAttribute, MEM_MAP_PRIVATE | MEM_MAP_ANONYMOUS | MEM_MAP_FIRST_PROCESS);
    MemMapAttributeSetFd(&memMapAttribute, MEM_MAP_INVALID_FD);
    MemMapAttributeSetOffset(&memMapAttribute, 0);
    MemMapAttributeSetName(&memMapAttribute, "Test User case");
    MemMapBuffer memMapBuffer;
    ErrorCode errorCode;
    errorCode = MemMap(&memMapAttribute, &memMapBuffer);
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    *(int *)memMapBuffer.addr = 0;
    MemUnmap(&memMapBuffer);
}