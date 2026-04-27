
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
 * ut_port_io.cpp
 * Developer test of io.
 *
 * ---------------------------------------------------------------------------------
 */
#include <gtest/gtest.h>
#include <mockcpp/mockcpp.hpp>
#include "securec.h"
#include "port/platform_port.h"

/** ************************************************************************************************************* **/


/** ************************************************************************************************************* **/

class IOTest : public testing::Test {
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
 * @tc.name:  IOFunction001_Level0
 * @tc.desc:  Test IO mode.
 * @tc.type: FUNC
 */
TEST_F(IOTest, IOFunction001_Level0)
{
    SetFileIOMode(stderr, FILE_IO_MODE_TEXT);
}

/**
 * @tc.name:  IOFunction002_Level0
 * @tc.desc:  Test file permission mode.
 * @tc.type: FUNC
 */
TEST_F(IOTest, IOFunction002_Level0)
{
    ASSERT_TRUE(SetFilePermission(stderr, S_IRUSR | S_IWUSR) == 0);
}
