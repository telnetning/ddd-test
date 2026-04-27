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
#include "fault_injection/fault_injection.h"

namespace {

constexpr int RANDOM_COUNT = 10000;
constexpr double AVERAGE_ERROR_RATE = 0.05;

}

class PortRandomTest : public testing::Test {
public:
    static void SetUpTestSuite()
    {
        FaultInjectionEntry entries[] = {
            FAULT_INJECTION_ENTRY(FI_OPEN_RANDOM_FILE_FAILED, false, nullptr),
            FAULT_INJECTION_ENTRY(FI_READ_RANDOM_FILE_FAILED, false, nullptr),
        };
        ASSERT_EQ(RegisterFaultInjection(entries, sizeof(entries) / sizeof(entries[0]), FI_GLOBAL), ERROR_SYS_OK);
    }

    static void TearDownTestSuite()
    {
        DestroyFaultInjectionHash(FI_GLOBAL);
    }

    void SetUp() override
    {
    }

    void TearDown() override
    {
    }
};

void TestGetRandomValuesWithRange(uint32_t *randomArray, size_t len, uint32_t range)
{
    for (size_t i = 0; i < len; ++i) {
        randomArray[i] = GetSafeRandomValue();
        if (range != 0) {
            randomArray[i] %= range;
        }
    }
}

/**
 * Get multiple random value and mod with constant range,
 * check the average of random value if in an acceptable error rate
 */
TEST_F(PortRandomTest, GetSafeRandomValueTest001)
{
    uint32_t randomValueVec[RANDOM_COUNT];
    uint32_t randomRange = 1000; /* Get random value in [0, 1000) */
    TestGetRandomValuesWithRange(randomValueVec, RANDOM_COUNT, randomRange);
    uint32_t total = 0;
    for (auto val : randomValueVec) {
        total += val;
    }
    double average = (double)total / RANDOM_COUNT;
    EXPECT_GT(average, (double) randomRange / 2 * (1 - AVERAGE_ERROR_RATE));
    EXPECT_LT(average, (double) randomRange / 2 * (1 + AVERAGE_ERROR_RATE));
}

/**
 * Get multiple random value and mod with constant range in multiple thread,
 * check the average of random value if in an acceptable error rate
 */
TEST_F(PortRandomTest, GetSafeRandomValueTest002)
{
    constexpr int TEST_THREAD_COUNT = 20;
    uint32_t randomValueMatrix[TEST_THREAD_COUNT][RANDOM_COUNT];
    std::thread threads[TEST_THREAD_COUNT];
    for (int i = 0; i < TEST_THREAD_COUNT; ++i) {
        threads[i] = std::thread(TestGetRandomValuesWithRange, randomValueMatrix[i], RANDOM_COUNT, 0);
    }
    for (auto &thread : threads) {
        thread.join();
    }
    uint64_t total = 0;
    for (int i = 0; i < TEST_THREAD_COUNT; ++i) {
        for (int j = 0; j < RANDOM_COUNT; ++j) {
            total += randomValueMatrix[i][j];
        }
    }
    double average = (double)total / TEST_THREAD_COUNT / RANDOM_COUNT;
    EXPECT_GT(average, (double) UINT32_MAX / 2 * (1 - AVERAGE_ERROR_RATE));
    EXPECT_LT(average, (double) UINT32_MAX / 2 * (1 + AVERAGE_ERROR_RATE));
}

/**
 * Generate secure random byte array
 */
TEST_F(PortRandomTest, GenerateSecureRandomByteArrayTest001)
{
    unsigned char value1[RANDOM_COUNT] = {0};
    GenerateSecureRandomByteArray(RANDOM_COUNT, value1);
    unsigned char value2[RANDOM_COUNT] = {0};
    GenerateSecureRandomByteArray(RANDOM_COUNT, value2);
    ASSERT_FALSE(std::equal(std::begin(value1), std::end(value1), std::begin(value2)));
}

#ifdef ENABLE_FAULT_INJECTION

/**
 * Get random value but fail in load random device file
 */
TEST_F(PortRandomTest, GetSafeRandomValueTest003)
{
#ifndef WINDOWS_PLATFORM

    FAULT_INJECTION_ACTIVE(FI_OPEN_RANDOM_FILE_FAILED, FI_GLOBAL);
    /* Fault injection with open random file failed, always get UINT32_MAX */
    uint32_t value1 = GetSafeRandomValue();
    uint32_t value2 = GetSafeRandomValue();
    ASSERT_EQ(value1, UINT32_MAX);
    ASSERT_EQ(value2, UINT32_MAX);
    FAULT_INJECTION_INACTIVE(FI_OPEN_RANDOM_FILE_FAILED, FI_GLOBAL);

    FAULT_INJECTION_ACTIVE(FI_READ_RANDOM_FILE_FAILED, FI_GLOBAL);
    /* Fault injection with read random file failed, always get UINT32_MAX */
    value1 = GetSafeRandomValue();
    value2 = GetSafeRandomValue();
    ASSERT_EQ(value1, UINT32_MAX);
    ASSERT_EQ(value2, UINT32_MAX);
    FAULT_INJECTION_INACTIVE(FI_READ_RANDOM_FILE_FAILED, FI_GLOBAL);

    /* Expect get at least 1 random value less than UINT32_MAX */
    value1 = GetSafeRandomValue();
    value2 = GetSafeRandomValue();
    EXPECT_TRUE(value1 != UINT32_MAX || value2 != UINT32_MAX);

#endif
}

#endif
