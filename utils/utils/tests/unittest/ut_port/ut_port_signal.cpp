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
 * ut_port_signal.cpp
 * Developer test of signal.
 *
 * ---------------------------------------------------------------------------------
 */
#include <gtest/gtest.h>
#include <mockcpp/mockcpp.hpp>
#include<sys/mman.h>
#include<sys/stat.h>
#include<fcntl.h>
#include<stdlib.h>
#include<string.h>
#include "securec.h"
#include "port/platform_port.h"

class SignalTest : public testing::Test {
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

int g_sigupForTest = INVALID_SIG_NUM;
void SigupFunctionForTest(int portSignalNo)
{
    g_sigupForTest = portSignalNo;
}

int g_sigintForTest = INVALID_SIG_NUM;
void SigintFunctionForTest(int portSignalNo)
{
    g_sigintForTest = portSignalNo;
}
char g_signalTestMessage = 0xCE;
int g_signalTestMessageSize = 1;

int g_siguser1ForTest = INVALID_SIG_NUM;
char g_siguser1MessageForTest = 0;
char g_siguser1MessageSizeForTest = 0;
void Siguser1MessageFunctionForTest(int portSignalNo, void *signalMessage, size_t messageSize)
{
    g_siguser1ForTest = portSignalNo;
    g_siguser1MessageForTest = *(char *)signalMessage;
    g_siguser1MessageSizeForTest = messageSize;
}

int g_siguser2ForTest = INVALID_SIG_NUM;
void *g_siguser2MessageForTest = &g_siguser2ForTest;
char g_siguser2MessageSizeForTest = 1;
void Siguser2MessageFunctionForTest(int portSignalNo, void *signalMessage, size_t messageSize)
{
    g_siguser2ForTest = portSignalNo;
    g_siguser2MessageForTest = signalMessage;
    g_siguser2MessageSizeForTest = messageSize;
}

ErrorCode ChildProcessForSignalTest(Pid parentPid)
{
    ErrorCode errorCode;
    errorCode = ProcessKill(parentPid, SIG_HUP);
    if (errorCode != ERROR_SYS_OK) {
        return errorCode;
    }
    errorCode = ProcessKill(parentPid, SIG_INT);
    if (errorCode != ERROR_SYS_OK) {
        return errorCode;
    }
    errorCode = SendProcessSignal(parentPid, SIG_USR1, &g_signalTestMessage, g_signalTestMessageSize);
    if (errorCode != ERROR_SYS_OK) {
        return errorCode;
    }
    errorCode = SendProcessSignal(parentPid, SIG_USR2, NULL, 0);
    if (errorCode != ERROR_SYS_OK) {
        return errorCode;
    }
    return errorCode;
}

void SetSignal()
{
    ErrorCode errorCode;

    SignalSet signalSet3;
    errorCode = SignalFillSet(&signalSet3);
    ASSERT_EQ(errorCode, ERROR_SYS_OK);
    errorCode = SignalDelSet(&signalSet3, SIG_INT);
    ASSERT_EQ(errorCode, ERROR_SYS_OK);
    errorCode = SignalDelSet(&signalSet3, SIG_USR1);
    ASSERT_EQ(errorCode, ERROR_SYS_OK);
    errorCode = SignalDelSet(&signalSet3, SIG_USR2);
    ASSERT_EQ(errorCode, ERROR_SYS_OK);
    errorCode = SignalSetMask(&signalSet3);
    ASSERT_EQ(errorCode, ERROR_SYS_OK);

    SignalFunction signalFunction;
    signalFunction = Signal(SIG_HUP, SigupFunctionForTest);
    ASSERT_EQ(signalFunction, SIG_FUNC_DFL);
    signalFunction = Signal(SIG_INT, SigintFunctionForTest);
    ASSERT_EQ(signalFunction, SIG_FUNC_DFL);

    SignalMessageFunction signalMessageFunction;
    signalMessageFunction = SignalMessage(SIG_USR1, Siguser1MessageFunctionForTest);
    ASSERT_EQ(signalMessageFunction, SIG_MESSAGE_FUNC_DFL);
    signalMessageFunction = SignalMessage(SIG_USR2, Siguser2MessageFunctionForTest);
    ASSERT_EQ(signalMessageFunction, SIG_MESSAGE_FUNC_DFL);
}

Tid g_mainThreadForSignalTest;
void *ThreadForSignalTest(void *arg)
{
    ErrorCode errorCode;
    *(ErrorCode *)arg = ERROR_SYS_OK;
    errorCode = SignalInitialize();
    if (errorCode != ERROR_SYS_OK) {
        *(ErrorCode *)arg = errorCode;
        return arg;
    }

    errorCode = ThreadKill(g_mainThreadForSignalTest, SIG_HUP);
    if (errorCode != ERROR_SYS_OK) {
        *(ErrorCode *)arg = errorCode;
        goto ThreadCleanUp;
    }

    errorCode = ThreadKill(g_mainThreadForSignalTest, SIG_INT);
    if (errorCode != ERROR_SYS_OK) {
        *(ErrorCode *)arg = errorCode;
        goto ThreadCleanUp;
    }

    errorCode = SendThreadSignal(g_mainThreadForSignalTest, SIG_USR1, &g_signalTestMessage, g_signalTestMessageSize);
    if (errorCode != ERROR_SYS_OK) {
        *(ErrorCode *)arg = errorCode;
        goto ThreadCleanUp;
    }
    errorCode = SendThreadSignal(g_mainThreadForSignalTest, SIG_USR2, NULL, 0);
    if (errorCode != ERROR_SYS_OK) {
        *(ErrorCode *)arg = errorCode;
        goto ThreadCleanUp;
    }

ThreadCleanUp:
    SignalDestroy();
    return arg;
}

/**
 * @tc.name:  SignalFunction001_Level0
 * @tc.desc:  Test the signal set operation
 * @tc.type: FUNC
 */
TEST_F(SignalTest, SignalFunction001_Level0)
{
    /**
     * @tc.steps: step1. Test signal set operation.
     * @tc.expected: step1.Signal set results are correct.
     */
    ErrorCode errorCode;

    SignalSet signalSet1;
    errorCode = SignalEmptySet(&signalSet1);
    ASSERT_EQ(errorCode, ERROR_SYS_OK);
    errorCode = SignalAddSet(&signalSet1, SIG_HUP);
    ASSERT_EQ(errorCode, ERROR_SYS_OK);
    errorCode = SignalIsMember(&signalSet1, SIG_HUP);
    ASSERT_EQ(errorCode, ERROR_UTILS_PORT_ETRUE);
    errorCode = SignalIsMember(&signalSet1, SIG_USR1);
    ASSERT_EQ(errorCode, ERROR_UTILS_PORT_EFALSE);

    SignalSet signalSet2;
    errorCode = SignalFillSet(&signalSet2);
    ASSERT_EQ(errorCode, ERROR_SYS_OK);
    errorCode = SignalIsMember(&signalSet2, SIG_HUP);
    ASSERT_EQ(errorCode, ERROR_UTILS_PORT_ETRUE);
    errorCode = SignalDelSet(&signalSet2, SIG_HUP);
    ASSERT_EQ(errorCode, ERROR_SYS_OK);
    errorCode = SignalIsMember(&signalSet2, SIG_HUP);
    ASSERT_EQ(errorCode, ERROR_UTILS_PORT_EFALSE);
}

/**
 * @tc.name:  SignalFunction002_Level0
 * @tc.desc:  Test signal are sent between processes
 * @tc.type: FUNC
 */
TEST_F(SignalTest, SignalFunction002_Level0)
{
    /**
     * @tc.steps: step1. Test signals are sent between processes.
     * @tc.expected: step1.Signals can be sent between processes and callback functions can be triggered.
     */
    ErrorCode errorCode;
    errorCode = SignalInitialize();
    ASSERT_EQ(errorCode, ERROR_SYS_OK);

    SetSignal();

    pid_t childPid = fork();
    if (childPid == 0) {
        // child process
        pid_t pid = getppid();
        Pid parentPid;
        SetPid(&parentPid, pid);
        ErrorCode errorCode;
        errorCode = SignalInitialize();
        ASSERT_EQ(errorCode, ERROR_SYS_OK);
        errorCode = ChildProcessForSignalTest(parentPid);
        SignalDestroy();
        exit(errorCode);
    } else {
        // parent process
        ASSERT_GT(childPid, 0);
        int status;
        pid_t waitPid = waitpid(childPid, &status, 0);
        ASSERT_EQ(waitPid, childPid);
        ASSERT_EQ(status, ERROR_SYS_OK);
        /* SIG_HUP is a blocking signal and should not trigger the signal callback function. */
        /* Message signals are asynchronous signals and need to be received and processed by
         * the signal processing thread. */
        Usleep(10000);
        ASSERT_EQ(g_sigupForTest, INVALID_SIG_NUM);
        /* SIG_INT is a non-blocking signal. The signal callback function should be triggered normally. */
        ASSERT_EQ(g_sigintForTest, SIG_INT);
        /* SIG_USR1 is a non-blocking signal message. It should trigger a signal and receive a message. */
        ASSERT_EQ(g_siguser1ForTest, SIG_USR1);
        ASSERT_EQ(g_siguser1MessageForTest, g_signalTestMessage);
        ASSERT_EQ(g_siguser1MessageSizeForTest, g_signalTestMessageSize);
        /* SIG_USR2 is a non-blocking signal message. It should trigger a signal and receive a null message. */
        ASSERT_EQ(g_siguser2ForTest, SIG_USR2);
        ASSERT_EQ(g_siguser2MessageForTest, nullptr);
        ASSERT_EQ(g_siguser2MessageSizeForTest, 0);
    }

    /* Reset Variable Expected Value. */
    g_sigupForTest = INVALID_SIG_NUM;
    g_sigintForTest = INVALID_SIG_NUM;
    g_siguser1ForTest = INVALID_SIG_NUM;
    g_siguser1MessageForTest = 0;
    g_siguser1MessageSizeForTest = 0;
    g_siguser2ForTest = INVALID_SIG_NUM;
    g_siguser2MessageForTest = &g_siguser2ForTest;
    g_siguser2MessageSizeForTest = 1;

    SignalDestroy();
}

/**
 * @tc.name:  SignalFunction003_Level0
 * @tc.desc:  Test signal are sent between threads
 * @tc.type: FUNC
 */
TEST_F(SignalTest, SignalFunction003_Level0)
{
    /**
     * @tc.steps: step1. Test signals are sent between threads.
     * @tc.expected: step1.Signals can be sent between threads and callback functions can be triggered.
     */
    ErrorCode errorCode;
    errorCode = SignalInitialize();
    ASSERT_EQ(errorCode, ERROR_SYS_OK);

    SetSignal();

    g_mainThreadForSignalTest = GetCurrentTid();
    ErrorCode threadExitErrorCode;
    Tid sendSignalTid;
    errorCode = ThreadCreate(&sendSignalTid, ThreadForSignalTest, &threadExitErrorCode);
    ASSERT_EQ(errorCode, ERROR_SYS_OK);
    ErrorCode *threadExitErrorCodePtr = &threadExitErrorCode;
    errorCode = ThreadJoin(sendSignalTid, (void **)&threadExitErrorCodePtr);
    ASSERT_EQ(errorCode, ERROR_SYS_OK);
    ASSERT_EQ(threadExitErrorCode, ERROR_SYS_OK);

    /* SIG_HUP is a blocking signal and should not trigger the signal callback function. */
    ASSERT_EQ(g_sigupForTest, INVALID_SIG_NUM);
    /* SIG_INT is a non-blocking signal. The signal callback function should be triggered normally. */
    ASSERT_EQ(g_sigintForTest, SIG_INT);
    /* SIG_USR1 is a non-blocking signal message. It should trigger a signal and receive a message. */
    ASSERT_EQ(g_siguser1ForTest, SIG_USR1);
    ASSERT_EQ(g_siguser1MessageForTest, g_signalTestMessage);
    ASSERT_EQ(g_siguser1MessageSizeForTest, g_signalTestMessageSize);
    /* SIG_USR2 is a non-blocking signal message. It should trigger a signal and receive a null message. */
    ASSERT_EQ(g_siguser2ForTest, SIG_USR2);
    ASSERT_EQ(g_siguser2MessageForTest, nullptr);
    ASSERT_EQ(g_siguser2MessageSizeForTest, 0);

    SignalDestroy();
}

TEST_F(SignalTest, SignalFunction004_Level0)
{
    /**
     * @tc.steps: step1. Test signals operation without initialization signal
     * @tc.expected: step1.Signals operation failure
     */
    ErrorCode errorCode;

    SignalSet signalSet;
    errorCode = SignalFillSet(&signalSet);
    ASSERT_EQ(errorCode, ERROR_SYS_OK);
    errorCode = SignalDelSet(&signalSet, SIG_INT);
    errorCode = SignalSetMask(&signalSet);
    ASSERT_EQ(errorCode, ERROR_UTILS_COMMON_FAILED);

    SignalFunction signalFunction;
    signalFunction = Signal(SIG_HUP, SigupFunctionForTest);
    ASSERT_TRUE(signalFunction == SIG_FUNC_ERR);

    SignalMessageFunction signalMessageFunction;
    signalMessageFunction = SignalMessage(SIG_USR1, Siguser1MessageFunctionForTest);
    ASSERT_TRUE(signalMessageFunction == SIG_MESSAGE_FUNC_ERR);
}

/**
 * @tc.name:  SignalFunction005_Level0
 * @tc.desc:  Test loop fork
 * @tc.type: FUNC
 */
TEST_F(SignalTest, SignalFunction005_Level0)
{
    /**
     * @tc.steps: step1. Test loop fork.
     * @tc.expected: step1.loop fork can be running successfully and without dead lock.
     */
    ErrorCode errorCode;
    errorCode = SignalInitialize();
    ASSERT_EQ(errorCode, ERROR_SYS_OK);

    pid_t childPid = fork();
    if (childPid == 0) {
        // child process
        pid_t childPid1 = fork();
        if (childPid1 == 0) {
            exit(0);
        } else {
            ASSERT_GT(childPid1, 0);
            int status1;
            pid_t waitPid1 = waitpid(childPid1, &status1, 0);
            ASSERT_EQ(waitPid1, childPid1);
            ASSERT_EQ(status1, ERROR_SYS_OK);
        }
        exit(errorCode);
    } else {
        // parent process
        ASSERT_GT(childPid, 0);
        int status;
        pid_t waitPid = waitpid(childPid, &status, 0);
        ASSERT_EQ(waitPid, childPid);
        ASSERT_EQ(status, ERROR_SYS_OK);
    }

    SignalDestroy();
}

/**
 * @tc.name:  SignalFunction006_Level0
 * @tc.desc:  Test if parent process can receive signal when child process sends signal after finishing a time consuming
 * task.
 * @tc.type: FUNC
 */
TEST_F(SignalTest, SignalFunction006_Level0)
{
    /**
     * @tc.steps: step1. Test signals are sent between processes.
     * @tc.expected: step1.Signals can be sent between processes and callback functions can be triggered.
     */
    ErrorCode errorCode;
    errorCode = SignalInitialize();
    ASSERT_EQ(errorCode, ERROR_SYS_OK);

    SetSignal();

    pid_t childPid = fork();
    if (childPid == 0) {
        // child process
        pid_t pid = getppid();
        Pid parentPid;
        SetPid(&parentPid, pid);
        ErrorCode errorCode;
        errorCode = SignalInitialize();
        ASSERT_EQ(errorCode, ERROR_SYS_OK);
        sleep(1);
        errorCode = ChildProcessForSignalTest(parentPid);
        SignalDestroy();
        exit(errorCode);
    } else {
        // parent process
        ASSERT_GT(childPid, 0);
        int status;
        pid_t waitPid = waitpid(childPid, &status, 0);
        ASSERT_EQ(waitPid, childPid);
        ASSERT_EQ(status, ERROR_SYS_OK);

        ASSERT_EQ(g_siguser1ForTest, SIG_USR1);
        ASSERT_EQ(g_siguser1MessageForTest, g_signalTestMessage);
        ASSERT_EQ(g_siguser1MessageSizeForTest, g_signalTestMessageSize);
    }

    /* Reset Variable Expected Value. */
    g_sigupForTest = INVALID_SIG_NUM;
    g_sigintForTest = INVALID_SIG_NUM;
    g_siguser1ForTest = INVALID_SIG_NUM;
    g_siguser1MessageForTest = 0;
    g_siguser1MessageSizeForTest = 0;
    g_siguser2ForTest = INVALID_SIG_NUM;
    g_siguser2MessageForTest = &g_siguser2ForTest;
    g_siguser2MessageSizeForTest = 1;

    SignalDestroy();
}

static uint16_t CheckSignalFileNum(void)
{
    /* Traversing each file in the Diectory */
    uint16_t SignalFileNum = 0;
    Directory dir;
#define SIGNAL_TMP_PATH      "/tmp"
    if (access(SIGNAL_TMP_PATH, F_OK) != 0) {
        return SignalFileNum;
    }

    ErrorCode errCode = OpenDirectory(SIGNAL_TMP_PATH, &dir);
    if (errCode != ERROR_SYS_OK) {
        return errCode;
    }
 
    DirectoryEntry dirEntry;
    while (ReadDirectory(&dir, &dirEntry)) {
        /* is file */
        if (!strcmp(dirEntry.name, ".") || !strcmp(dirEntry.name, "..")) {
            continue;
        }
        char signalFilePre[sizeof(SIGNAL_FILE_PREFIX)] = {0};
        (void)strncpy_s(signalFilePre, sizeof(signalFilePre), dirEntry.name, sizeof(signalFilePre) - 1);

        if (strcmp(signalFilePre, SIGNAL_FILE_PREFIX)) {
            continue;
        }

        if (std::string(dirEntry.name).find(std::string(SIGNAL_FILE_PREFIX) + std::to_string(getuid())) !=
            std::string::npos) {
            SignalFileNum++;
        }
    }
    CloseDirectory(&dir);
    return SignalFileNum;
}

static bool IsSignalFileExist(pid_t pid)
{
    std::string processId = std::to_string(pid);

    Directory dir;
    if (access(SIGNAL_TMP_PATH, F_OK) != 0) {
        return false;
    }

    ErrorCode errCode = OpenDirectory(SIGNAL_TMP_PATH, &dir);
    if (errCode != ERROR_SYS_OK) {
        return errCode;
    }
 
    DirectoryEntry dirEntry;
    while (ReadDirectory(&dir, &dirEntry)) {
        /* is file */
        if (!strcmp(dirEntry.name, ".") || !strcmp(dirEntry.name, "..")) {
            continue;
        }
        std::string fileName = dirEntry.name;
        if (fileName.find(SIGNAL_FILE_PREFIX + std::to_string(getuid()) + "_" + processId) != std::string::npos) {
            CloseDirectory(&dir);
            return true;
        }
    }
    CloseDirectory(&dir);
    return false;
}

TEST_F(SignalTest, SignalFileDeleteSingleProcessTest)
{
    int size = 4096;
    /* create shared memory to control behavior of child processes. */
    int fd = shm_open("posixsm", O_CREAT | O_RDWR, 0666);
    ftruncate(fd, size);
    /* using char[0], char [1] to control process A and new A. */
    /* 'i' stands for staring initialization, 'I' stands for initialization is finished. 'd' stands for starting
     * destruction, 'D' stands for destruction is finished. */
#define START_INIT    'i'
#define INIT_DONE     'I'
#define START_DESTROY 'd'
#define DESTROY_DONE  'D'
    char *p = (char *)mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    memset(p, 0, size);
    munmap(p, size);

    /* Start A process */
    pid_t aPid = fork();
    if (aPid == 0) {
        /* In A process */
        p = (char *)mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        /* Wait for the parent process to notify process A to start initializing signal. */
        while (p[0] != START_INIT);
        ErrorCode errorCode = SignalInitialize();
        ASSERT_EQ(errorCode, ERROR_SYS_OK);
        /* notify the parent process that process A has initialized signal. */
        p[0] = INIT_DONE;
        munmap(p, size);
        while (true);
    } else {
        ASSERT_GT(aPid, 0);
        char *p = (char *)mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        /* notify process A to initialize signal. */
        p[0] = START_INIT;
        /* wait for process A to finish its initialization. */
        while (p[0] != INIT_DONE);

        /* Kill process A */
        kill(aPid, SIG_KILL);
        int status;
        pid_t waitPid = waitpid(aPid, &status, 0);
        ASSERT_EQ(waitPid, aPid);
        ASSERT_EQ(status, SIG_KILL);
        /* Check signal file num*/
        ASSERT_EQ(CheckSignalFileNum(), 1);
        /* Check signal file */
        ASSERT_TRUE(IsSignalFileExist(aPid));
    }

    /* start C process */
    pid_t cPid = fork();
    if (cPid == 0) {
        /* In C process */
        char *p = (char *)mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        while (p[1] != START_INIT);
        ErrorCode errorCode = SignalInitialize();
        ASSERT_EQ(errorCode, ERROR_SYS_OK);
        p[1] = INIT_DONE;

        while (p[1] != START_DESTROY);
        SignalDestroy();
        p[1] = DESTROY_DONE;
        exit(0);
    } else {
        ASSERT_GT(cPid, 0);
        char *p = (char *)mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        p[1] = START_INIT;
        while (p[1] != INIT_DONE);
        /* Check signal file num*/
        ASSERT_EQ(CheckSignalFileNum(), 1);
        /* Check signal file */
        ASSERT_FALSE(IsSignalFileExist(aPid));
        ASSERT_TRUE(IsSignalFileExist(cPid));

        p[1] = START_DESTROY;
        while (p[1] != DESTROY_DONE);

        ASSERT_EQ(close(fd), 0);
        ASSERT_EQ(shm_unlink("posixsm"), 0);

        int status;
        pid_t waitPid = waitpid(cPid, &status, 0);
        ASSERT_EQ(waitPid, cPid);
        ASSERT_EQ(status, ERROR_SYS_OK);
        ASSERT_EQ(CheckSignalFileNum(), 0);
    }
}

/**
 * @tc.name:  SignalFileDeleteMultipleProcessTest
 * @tc.desc:  We created process A and B, and after the two processes created their own pipe file, we kill process A, we
 * expect there are 2 pipe files, one is for process A, and the other one is for process B. After we created process
 * C, we expect there are still two pipe files, one is for the process C, and the other one is for process B, and
 * the pipe file of process A is deleted.
 * @tc.type: FUNC
 */
TEST_F(SignalTest, SignalFileDeleteMultipleProcessTest)
{
    int size = 4096;
    /* create shared memory to control behavior of child processes. */
    int fd = shm_open("posixsm", O_CREAT | O_RDWR, 0666);
    ftruncate(fd, size);
    /* using char[0], char [1] char[2] to control process A, B and new A. */
    /* 'i' stands for staring initialization, 'I' stands for initialization is finished. 'd' stands for starting
     * destruction, 'D' stands for destruction is finished. */
    char *p = (char *)mmap(NULL, size, PROT_READ | PROT_WRITE , MAP_SHARED, fd, 0);
    memset(p, 0, size);
    munmap(p, size);

    /* Start A process */
    pid_t aPid = fork();
    if (aPid == 0) {
        /* In A process */
        p = (char *)mmap(NULL, size, PROT_READ | PROT_WRITE , MAP_SHARED, fd, 0);
        /* Wait for the parent process to notify process A to start initializing signal. */
        while(p[0] != 'i');
        ErrorCode errorCode = SignalInitialize();
        ASSERT_EQ(errorCode, ERROR_SYS_OK);
        /* notify the parent process that process A has initialized signal. */
        p[0] = 'I';
        munmap(p, size);
        while(true);
    } else {
        ASSERT_GT(aPid, 0);
        /* Start B process */
        pid_t bPid = fork();
        if (bPid == 0) {
            /* In B process */
            p = (char *)mmap(NULL, size, PROT_READ | PROT_WRITE , MAP_SHARED, fd, 0);
            /* Wait for the parent process to notify process B to start initializing signal. */
            while(p[1] != 'i');
            ErrorCode errorCode = SignalInitialize();
            ASSERT_EQ(errorCode, ERROR_SYS_OK);
            /* notify the parent process that process B has initialized signal. */
            p[1] = 'I';

            /* Wait for the parent process to notify process B to destroy signal. */
            while(p[1] != 'B');
            SignalDestroy();
            /* notify the parent process that process B has destroyed the signal. */
            p[1] = 'D';
            munmap(p, size);
            exit(0);
        } else {
            ASSERT_GT(bPid, 0);

            char *p = (char *)mmap(NULL, size, PROT_READ | PROT_WRITE , MAP_SHARED, fd, 0);
            /* notify process A to initialize signal. */
            p[0] = 'i';
            /* wait for process A to finish its initialization. */
            while(p[0] != 'I');

            /* notify process B to initialize signal. */
            p[1] = 'i';
            /* wait process B to finish its initialization. */
            while(p[1] != 'I');

            /* Kill process A */
            kill(aPid, SIG_KILL);
            int status;
            pid_t waitPid = waitpid(aPid, &status, 0);
            ASSERT_EQ(waitPid, aPid);
            ASSERT_EQ(status, SIG_KILL);
            /* Check signal file num*/
            ASSERT_EQ(CheckSignalFileNum(), 2);
            /* Check signal file */
            ASSERT_TRUE(IsSignalFileExist(aPid));
            ASSERT_TRUE(IsSignalFileExist(bPid));

            /* start C process */
            pid_t cPid = fork();
            if (cPid == 0) {
                /* In C process */
                p = (char *)mmap(NULL, size, PROT_READ | PROT_WRITE , MAP_SHARED, fd, 0);
                /* Wait for the parent process to notify the process C to start initializing signal. */
                while(p[2] != 'i');
                ErrorCode errorCode = SignalInitialize();
                ASSERT_EQ(errorCode, ERROR_SYS_OK);
                /* notify the parent process that the process C has initialized signal. */
                p[2] = 'I';
                
                /* Wait for the parent process to notify the process C to destroy signal. */
                while(p[2] != 'C');
                SignalDestroy();
                /* notify the parent process that the process C has destroyed the signal. */
                p[2] = 'D';
                munmap(p, size);
                exit(0);
            } else {
                ASSERT_GT(cPid, 0);
                
                /* notify the process C to initialize signal. */
                p[2] = 'i';
                /* wait for the process C to finish its initialization. */
                while(p[2] != 'I');

                /* check signal file num */
                ASSERT_EQ(CheckSignalFileNum(), 2);
                /* check signal file */
                ASSERT_FALSE(IsSignalFileExist(aPid));
                ASSERT_TRUE(IsSignalFileExist(bPid));
                ASSERT_TRUE(IsSignalFileExist(cPid));

                /* notify process B to destory signal. */
                p[1] = 'B';
                /* wait for process B to destory signal. */
                while(p[1] != 'D');

                /* notify the process C to destory signal. */
                p[2] = 'C';
                /* wait for the process C to destory signal. */
                while (p[2] != 'D');

                munmap(p, size);
                ASSERT_EQ(CheckSignalFileNum(), 0);
                ASSERT_EQ(close(fd), 0);
                ASSERT_EQ(shm_unlink("posixsm"), 0);

                int status;
                pid_t waitPid = waitpid(cPid, &status, 0);
                ASSERT_EQ(waitPid, cPid);
                ASSERT_EQ(status, ERROR_SYS_OK);

                int status1;
                pid_t waitPid1 = waitpid(bPid, &status1, 0);
                ASSERT_EQ(waitPid1, bPid);
                ASSERT_EQ(status1, ERROR_SYS_OK);
            }
        }
    }
}

static bool IsExistDeletedFile(Pid pid)
{
    char cmd[128];
    sprintf_s(cmd, sizeof(cmd), "lsof -p %d | grep deleted", pid);
    int ret = system(cmd);
    return (WEXITSTATUS(ret) == 0);
}

TEST_F(SignalTest, CheckSignalFdLeakTest)
{
    Pid pid = GetCurrentPid();
    
    ErrorCode errorCode = SignalInitialize();
    ASSERT_EQ(errorCode, ERROR_SYS_OK);

    SignalDestroy();

    /* Check if opened file been deleted */
    ASSERT_FALSE(IsExistDeletedFile(pid));
}