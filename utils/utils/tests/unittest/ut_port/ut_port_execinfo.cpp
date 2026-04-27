
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
 * ut_port_execinfo.cpp
 * Developer test of backtrace.
 *
 * ---------------------------------------------------------------------------------
 */
#include <gtest/gtest.h>
#include <mockcpp/mockcpp.hpp>
#include "securec.h"
#include "port/platform_port.h"

/** ************************************************************************************************************* **/



/** ************************************************************************************************************* **/

class BacktraceTest : public testing::Test {
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
 * @tc.name:  BacktraceFunction001_Level0
 * @tc.desc:  Test the backtrace.
 * @tc.type: FUNC
 */
#define BACKTRACE_MAX_FRAMES  256
TEST_F(BacktraceTest, BacktraceFunction001_Level0)
{
    /**
   * @tc.steps: step1. Initial the backtrace, get symbols and then free backtrace.
   * @tc.expected: step1.The backtrace functions are correct.
   */
    char *buffer[BACKTRACE_MAX_FRAMES];
    BacktraceContext traceContext;
    int frames = Backtrace((void **) buffer, BACKTRACE_MAX_FRAMES, 0, &traceContext);
    ASSERT_GT(frames, 0);
    int i;
    char *frameSymbols;
    for (i = 0; i < frames; i++) {
        frameSymbols = BacktraceSymbols((void **) buffer, frames, i, &traceContext);
        ASSERT_TRUE((frameSymbols != NULL));
    }
    FreeBacktraceSymbols(&traceContext);
}
