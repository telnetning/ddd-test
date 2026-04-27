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
 * 1. unit tests of memory_allocator.c
 *
 * ---------------------------------------------------------------------------------
 */

#include <gtest/gtest.h>
#include "securec.h"
#include "memory/memory_allocator.h"

class MemAllocatorTest : public testing::Test {
public:
    void SetUp() override
    {}

    void TearDown() override
    {}
};

void *AllocMock(MemAllocator *self, uint64_t size)
{
    return malloc(size);
}

void FreeMock(MemAllocator *self, void *ptr)
{
    free(ptr);
}

TEST_F(MemAllocatorTest, AllocatorAllocTest)
{
    MemAllocator memAllocator = {0};

    errno = 0;
    void *p = AllocatorAlloc(&memAllocator, (uint64_t)1024);
    EXPECT_TRUE(p != 0);
    AllocatorFree(&memAllocator, p);
    EXPECT_TRUE(errno == 0);

    errno = 0;
    p = AllocatorAlloc(&memAllocator, (uint64_t)0);
    EXPECT_TRUE(p != 0);
    AllocatorFree(&memAllocator, p);
    EXPECT_TRUE(errno == 0);

    /**
    * The test scenario AllocatorAlloc((uint64_t)-1) with the boundary value UINT64_MAX 
    * will crash directly when asan is enabled, so it is not tested for the time being, 
    * and it will be added after the completion of AllocatorAlloc.
    */
    memAllocator.alloc = AllocMock;
    memAllocator.free = FreeMock;
    errno = 0;
    p = AllocatorAlloc(&memAllocator, (uint64_t)1024);
    EXPECT_TRUE(p != 0);
    AllocatorFree(&memAllocator, p);
    EXPECT_TRUE(errno == 0);

    errno = 0;
    p = AllocatorAlloc(&memAllocator, (uint64_t)0);
    EXPECT_TRUE(p != 0);
    AllocatorFree(&memAllocator, p);
    EXPECT_TRUE(errno == 0);
}