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
 * ut_port_pipe.cpp
 * Developer test of pipe.
 *
 * ---------------------------------------------------------------------------------
 */
#include <gtest/gtest.h>
#include <mockcpp/mockcpp.hpp>
#include "securec.h"
#include "port/platform_port.h"

/** ************************************************************************************************************* **/

/** ************************************************************************************************************* **/

class PipeTest : public testing::Test {
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

#define PIPE_TEST_NAME_LEN 1024
#define PID_TEST_STR_LEN 16
#define PIPE_TEST_BUFFER_CONTENT 0xCE
#define PIPE_TEST_BUFFER_SIZE 10
#define PIPE_TEST_END_CMD 0xFE

ErrorCode ChildProcessForPipeTest(char *parentToChildPipePathName, char *childToParentPipePathName)
{
    Pipe parentToChildPipe;
    Pipe childToParentPipe;
    ErrorCode errCode;
    errCode = MakeNamedPipe(parentToChildPipePathName, PIPE_RDWR, &parentToChildPipe);
    if (errCode != ERROR_SYS_OK && errCode != ERROR_UTILS_PORT_EEXIST) {
        return errCode;
    }
    errCode = MakeNamedPipe(childToParentPipePathName, PIPE_RDWR, &childToParentPipe);
    if (errCode != ERROR_SYS_OK && errCode != ERROR_UTILS_PORT_EEXIST) {
        return errCode;
    }
    errCode = OpenNamedPipe(&parentToChildPipe, PIPE_RDWR);
    if (errCode != ERROR_SYS_OK) {
        return errCode;
    }
    errCode = OpenNamedPipe(&childToParentPipe, PIPE_RDWR);
    if (errCode != ERROR_SYS_OK) {
        return errCode;
    }
    char pipeTestReadBuffer[PIPE_TEST_BUFFER_SIZE] = {0};
    ssize_t readCount;
    char endCmd = PIPE_TEST_END_CMD;
    while (true) {
        errCode = ReadFromPipe(&parentToChildPipe, pipeTestReadBuffer, PIPE_TEST_BUFFER_SIZE, &readCount);
        if (errCode != ERROR_SYS_OK) {
            break;
        }
        /* If the command is an end command, exit the processing. */
        if (!memcmp(pipeTestReadBuffer, &endCmd, 1)) {
            break;
        } else {
            errCode = WriteToPipe(&childToParentPipe, pipeTestReadBuffer, readCount);
            if (errCode != ERROR_SYS_OK) {
                break;
            }
        }
    }
    CloseNamedPipe(&parentToChildPipe);
    CloseNamedPipe(&childToParentPipe);
    return errCode;
}
/**
 * @tc.name:  PipeFunction001_Level0
 * @tc.desc:  Test the pipe create,open,read,write,no block and close.
 * @tc.type: FUNC
 */
TEST_F(PipeTest, PipeFunction001_Level0)
{
    /**
     * @tc.steps: step1. Creating a child process.
     * @tc.expected: step1.The parent and child processes communicate with each other
     * through named pipes to test the correctness of pipes.
     */
    ErrorCode errCode;
    char parentToChildPipePathName[PIPE_TEST_NAME_LEN] = {0};
    char childToParentPipePathName[PIPE_TEST_NAME_LEN] = {0};
    char pidString[PID_TEST_STR_LEN] = {0};
    Pid pid = GetCurrentPid();
    Pid2String(&pid, pidString, PID_TEST_STR_LEN);
    int rc = sprintf_s(parentToChildPipePathName, PIPE_TEST_NAME_LEN, "/tmp/test_parent_%s", pidString);
    ASSERT_GT(rc, 0);
    rc = sprintf_s(childToParentPipePathName, PIPE_TEST_NAME_LEN, "/tmp/test_child_%s", pidString);
    ASSERT_GT(rc, 0);
    pid_t childPid = fork();
    if (childPid == 0) {
        errCode = ChildProcessForPipeTest(parentToChildPipePathName, childToParentPipePathName);
        exit(errCode);
    } else {
        ASSERT_GT(childPid, 0);
        Pipe parentToChildPipe;
        Pipe childToParentPipe;
        errCode = MakeNamedPipe(parentToChildPipePathName, PIPE_RDWR, &parentToChildPipe);
        ASSERT_TRUE(errCode == ERROR_SYS_OK || errCode == ERROR_UTILS_PORT_EEXIST);
        errCode = MakeNamedPipe(childToParentPipePathName, PIPE_RDWR, &childToParentPipe);
        ASSERT_TRUE(errCode == ERROR_SYS_OK || errCode == ERROR_UTILS_PORT_EEXIST);
        errCode = OpenNamedPipe(&parentToChildPipe, PIPE_RDWR);
        ASSERT_EQ(errCode, ERROR_SYS_OK);
        errCode = OpenNamedPipe(&childToParentPipe, PIPE_RDWR);
        ASSERT_EQ(errCode, ERROR_SYS_OK);
        char pipeWriteTestBuffer[PIPE_TEST_BUFFER_SIZE];
        rc = memset_s(pipeWriteTestBuffer, PIPE_TEST_BUFFER_SIZE, PIPE_TEST_BUFFER_CONTENT, PIPE_TEST_BUFFER_SIZE);
        ASSERT_EQ(rc, 0);
        errCode = WriteToPipe(&parentToChildPipe, pipeWriteTestBuffer, PIPE_TEST_BUFFER_SIZE);
        ASSERT_EQ(errCode, ERROR_SYS_OK);
        char pipeReadTestBuffer[PIPE_TEST_BUFFER_SIZE] = {0};
        ssize_t readCount;
        errCode = ReadFromPipe(&childToParentPipe, pipeReadTestBuffer, PIPE_TEST_BUFFER_SIZE, &readCount);
        ASSERT_EQ(errCode, ERROR_SYS_OK);
        ASSERT_EQ(readCount, PIPE_TEST_BUFFER_SIZE);
        int i;
        for (i = 0; i < PIPE_TEST_BUFFER_SIZE; i++) {
            ASSERT_EQ(pipeReadTestBuffer[i], pipeWriteTestBuffer[i]);
        }
        errCode = SetNamedPipeNoBlock(&childToParentPipe);
        ASSERT_EQ(errCode, ERROR_SYS_OK);
        errCode = ReadFromPipe(&childToParentPipe, pipeReadTestBuffer, PIPE_TEST_BUFFER_SIZE, &readCount);
        ASSERT_EQ(errCode, ERROR_UTILS_PORT_EAGAIN);
        rc = memset_s(pipeWriteTestBuffer, PIPE_TEST_BUFFER_SIZE, PIPE_TEST_END_CMD, 1);
        ASSERT_EQ(rc, 0);
        errCode = WriteToPipe(&parentToChildPipe, pipeWriteTestBuffer, 1);
        ASSERT_EQ(errCode, ERROR_SYS_OK);
        CloseNamedPipe(&parentToChildPipe);
        CloseNamedPipe(&childToParentPipe);
        int status;
        pid_t waitPid = waitpid(childPid, &status, 0);
        ASSERT_EQ(waitPid, childPid);
        ASSERT_EQ(status, ERROR_SYS_OK);
    }

    unlink(parentToChildPipePathName);
    unlink(childToParentPipePathName);
}
