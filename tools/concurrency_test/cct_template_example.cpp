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

#include <iostream>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <future>
#include <gtest/gtest.h>

/**
 * CCT::BaseTest1: To check if the global variable will be conflict update
 * CCT::thread-1: {t1:f1, t2:f2, t3:f3, t4:f4, t5:ni,  t6:ni}
 * CCT::thread-2: {t1:f1, t2:ni, t3:f2, t4:f3, t5:ni,  t6:f4}
 * CCT::thread-3: {t1:ni, t2:f1, t3:f2, t4:ni, t5:f3,  t6:f4}
 * CCT::thread-4: {t1:ni, t2:f1, t3:f2, t4:f3, t5:ni,  t6:f4}
 */
TEST(ExampleTest, TestGlbVar)
{
    int count = 0;

    // CCT::b_f1
    for (size_t i = 0; i < 100; i++) {
        count++;
    }
    // CCT::e_f1

    // CCT::b_f2
    for (size_t i = 0; i < 100; i++) {
        count--;
    }
    // CCT::e_f2

    // CCT::b_f3
    for (size_t i = 0; i < 100; i++) {
        count++;
    }
    // CCT::e_f3

    // CCT::b_f4
    for (size_t i = 0; i < 100; i++) {
        count--;
    }
    // CCT::e_f4

    ASSERT_EQ(count, 0);
}

int main(int argc, char **argv)
{
    // g++ main.cpp  libgtest.a -lpthread -std=c++14 -I /home/runner/test/googletest/googletest/include -o m./m
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
