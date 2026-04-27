
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
 * ut_port_rwlock.cpp
 * unit tests of spin lock
 *
 * ---------------------------------------------------------------------------------
 */
#include <gtest/gtest.h>
#include <mockcpp/mockcpp.hpp>
#include "securec.h"
#include "port/platform_port.h"

/** ************************************************************************************************************* **/


/** ************************************************************************************************************* **/

class SyslogTest : public testing::Test {
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
 * @tc.name:  SyslogFunction001_Level0
 * @tc.desc:  Test open,log and close.
 * @tc.type: FUNC
 */
TEST_F(SyslogTest, SyslogFunction001_Level0)
{
    /**
    * @tc.steps: step1. Test open,log and close.
    * @tc.expected: step1.The syslog functions are correct.
    */
    Syslog syslog = SYSLOG_INITIALIZER;
    bool rc = IsSyslogOpen(&syslog);
    ASSERT_FALSE(rc);
    char fullPath[MAX_PATH] = {0};
    ErrorCode errorCode;
    errorCode = GetCurrentProcessName(fullPath, MAX_PATH);
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    OpenSyslog(fullPath, "local0", &syslog);
    rc = IsSyslogOpen(&syslog);
    ASSERT_TRUE(rc);
    ReportSyslog(&syslog, EVENT_LOG_ERROR, "%s", "This is developer test message.");
    CloseSyslog(&syslog);
    rc = IsSyslogOpen(&syslog);
    ASSERT_FALSE(rc);
}
