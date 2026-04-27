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
#include "port/dstore_port.h"
#include <stdlib.h>
#include <chrono>

using namespace DSTORE;
class SleepTest : public DSTORETEST {
protected:
    void SetUp() override
    {}
    void TearDown() override
    {}
};

TEST_F(SleepTest, BasicTest)
{
    auto start = std::chrono::system_clock::now();

    GaussUsleep(1000000);

    auto end = std::chrono::system_clock::now();

    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    EXPECT_TRUE((double(abs(duration.count() - 1000)) / 1000) < 0.01);
}
