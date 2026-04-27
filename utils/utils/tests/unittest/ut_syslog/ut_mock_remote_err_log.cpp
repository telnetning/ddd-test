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

#include <string>
#include <thread>
#include <gtest/gtest.h>
#include "securec.h"
#include "vfs/vfs_interface.h"
#include "syslog/err_log.h"
#include "syslog/err_log_internal.h"
#include "fault_injection/fault_injection.h"
#include "ut_err_log_common.h"

using std::string;
using std::to_string;

#define ERROR_LOG_WAIT_GAP_US 1000
#define ERROR_LOG_MAX_WAIT_US (10 * 1000 * 1000)

#define TEST_BLOCK_SIZE  8192

#define TEST_REMOTE_LOG_DATA_FILE_ID_1   1
#define TEST_REMOTE_LOG_DATA_FILE_ID_2   2

#define TEST_THREAD_COUNT   3
#define TEST_LOOP_COUNT     20

#define TEST_REMOTE_LOG_COMPONENT_ID    0xFF
#define TEST_REMOTE_LOG_MESSAGE  "RemoteLog"
#define TEST_REMOTE_LOG_DATA_MESSAGE  "RemoteLog, Data"
#define TEST_REMOTE_LOG_RELOAD_MESSAGE  "ReloadRemoteLog"
#define TEST_REMOTE_LOG_UPDATED_SEQUENCE_INFO_MESSAGE  "CurrentFileSeqNum: 0"

#define TEST_MOCK_REMOTE_LOG_FILE_SUFFIX  ".mock.data"

#define TEST_LOCAL_LOG_DIRECTORY  "."

#define NUMBER_BASE 10

VirtualFileSystem *g_localTestVfs = nullptr;

/* Write error log in unique position for fixed log length */
static void WriteOneTestErrorLog()
{
    ErrLog(LOG, ErrMsg(TEST_REMOTE_LOG_MESSAGE));
}

string TestConvertFileIdToFileName(uint16_t fileId)
{
    string sourceErrLogFileName = to_string(fileId);
    return "RemoteLog." + sourceErrLogFileName + TEST_MOCK_REMOTE_LOG_FILE_SUFFIX;
}

static void MockRemoteFileOpen(const FaultInjectionEntry *entry, ErrorCode *errorCode, VirtualFileSystem *vfs,
    uint16_t fileId, const char *pathName, int flags, FileDescriptor **fd)
{
    string mockFileName = TestConvertFileIdToFileName(fileId);
    *errorCode = Open(g_localTestVfs, mockFileName.c_str(), flags, fd);
}

static void MockRemoteFileCreate(const FaultInjectionEntry *entry, ErrorCode *errorCode, VirtualFileSystem *vfs,
    uint16_t fileId, const char *pathName, FileParameter fileParameter, FileDescriptor **fd)
{
    string mockFileName = TestConvertFileIdToFileName(fileId);
    *errorCode = Create(g_localTestVfs, mockFileName.c_str(), fileParameter, fd);
}

static void MockRemoteFileIsExist(const FaultInjectionEntry *entry, ErrorCode *errorCode, VirtualFileSystem *vfs,
    uint16_t fileId, const char *pathName, bool *out)
{
    string mockFileName = TestConvertFileIdToFileName(fileId);
    *errorCode = FileIsExist(g_localTestVfs, mockFileName.c_str(), out);
}

static void *MockLoggerThreadRoutine(void *args)
{
    /* Enable fault injection hash table in logger thread */
    FaultInjectionEntry entries[] = {
        FAULT_INJECTION_ENTRY(MOCK_REMOTE_LOG_OPEN, true, MockRemoteFileOpen),
        FAULT_INJECTION_ENTRY(MOCK_REMOTE_LOG_CREATE, true, MockRemoteFileCreate),
        FAULT_INJECTION_ENTRY(MOCK_REMOTE_LOG_FILE_IS_EXIST, true, MockRemoteFileIsExist),
    };
    EXPECT_EQ(RegisterFaultInjection(entries, sizeof(entries) / sizeof(entries[0]), FI_THREAD), ERROR_SYS_OK);
    /* Running logger thread */
    void *res = LoggerMainThread(args);
    /* Logger thread exit, destroy fault injection hash table */
    DestroyFaultInjectionHash(FI_THREAD);
    return res;
}

static void GetMockLoggerThreadRoutine(const FaultInjectionEntry *entry, ThreadStartRoutine *routinePtr)
{
    *routinePtr = MockLoggerThreadRoutine;
}

static void MockRemoteFileDataWrite(const FaultInjectionEntry *entry, ErrorCode *errorCode)
{
    *errorCode = ERROR_SYS_OK;
    ErrLog(LOG, ErrMsg(TEST_REMOTE_LOG_DATA_MESSAGE));
}

static void MockUpdateRemoteLogCurSeqNumFailed(const FaultInjectionEntry *entry, ErrorCode *errorCode)
{
    *errorCode = -1;
}

bool GetRemoteErrLogStopCompleted()
{
    for (int i = 0; i < ERROR_LOG_MAX_WAIT_US / ERROR_LOG_WAIT_GAP_US; i++) {
        bool res = GetRemoteLogStoppingState();
        if (!res) {
            return true;
        }
        Usleep(ERROR_LOG_WAIT_GAP_US);
    }
    /* Reach max wait time, something wrong in error log flush */
    return false;
}

int64_t GetFileSize(VirtualFileSystem *vfs, uint16_t fileId, const char *fileName)
{
    FileDescriptor *fd = nullptr;
    EXPECT_EQ(Open(vfs, fileName, FILE_READ_AND_WRITE_FLAG, &fd), 0);
    int64_t fileSize = 0;
    EXPECT_EQ(GetSize(fd, &fileSize), 0);
    EXPECT_EQ(Close(fd), 0);
    return fileSize;
}

static int32_t GetSeqNumFromFirstRemoteErrLogFile(const char *remoteLogDataFileName1)
{
    int flags = FILE_READ_AND_WRITE_FLAG;
    FileDescriptor *errorLogFile = NULL;

    ErrorCode errorCode = Open(g_localTestVfs, remoteLogDataFileName1, flags, &errorLogFile);
    if (errorCode != ERROR_SYS_OK) {
        return -1;
    }

    char metaBuf[LOG_SEQUENCE_INFO_SIZE] = {0};
    int64_t readSize = 0;
    errorCode = Pread(errorLogFile, metaBuf, sizeof(metaBuf), 0, &readSize);
    if (errorCode != ERROR_SYS_OK) {
        (void)Close(errorLogFile);
        return -1;
    }

    char *endPtr;
    uint32_t seq = (uint32_t)strtol(metaBuf + strlen(LOG_SEQUENCE_NUM_PREFIX), &endPtr, NUMBER_BASE);

    (void)Close(errorLogFile);

    return seq;
}

#ifdef ENABLE_FAULT_INJECTION

class ErrorLogMockRemoteLogTest : public testing::Test {
protected:
    static void SetUpTestCase()
    {
        FaultInjectionEntry entries[] = {
            FAULT_INJECTION_ENTRY(MOCK_REMOTE_LOG_OPEN, true, MockRemoteFileOpen),
            FAULT_INJECTION_ENTRY(MOCK_REMOTE_LOG_CREATE, true, MockRemoteFileCreate),
            FAULT_INJECTION_ENTRY(MOCK_REMOTE_LOG_FILE_IS_EXIST, true, MockRemoteFileIsExist),
            FAULT_INJECTION_ENTRY(MOCK_LOGGER_THREAD_ROUTINE, true, GetMockLoggerThreadRoutine),
            FAULT_INJECTION_ENTRY(MOCK_REMOTE_LOG_DATA_FILE, false, MockRemoteFileDataWrite),
            FAULT_INJECTION_ENTRY(MOCK_UPDATE_REMOTE_LOG_SEQ_NUM_FAILED, false, MockUpdateRemoteLogCurSeqNumFailed),
            FAULT_INJECTION_ENTRY(MOCK_REMOTE_LOG_INVALID_SEQ_NUM, false, nullptr),
            FAULT_INJECTION_ENTRY(MOCK_REMOTE_LOG_VALID_SEQ_NUM, false, nullptr),
            FAULT_INJECTION_ENTRY(MOCK_GET_REMOTE_LOG_SEQ_NUM_SECCEED, false, nullptr),
        };
        ASSERT_EQ(RegisterFaultInjection(entries, sizeof(entries) / sizeof(entries[0]), FI_GLOBAL), ERROR_SYS_OK);
    }

    static void TearDownTestCase()
    {
        DestroyFaultInjectionHash(FI_GLOBAL);
    }

    void SetUp() override
    {
        RemoveMockRemoteFile();
        ASSERT_EQ(InitVfsModule(nullptr), 0);
        ASSERT_EQ(GetStaticLocalVfsInstance(&g_localTestVfs), 0);
        ASSERT_NE(g_localTestVfs, nullptr);
        RemoveErrorLogFileFromLocalDirectory(g_localTestVfs, TEST_LOCAL_LOG_DIRECTORY);
        SetErrLogDirectory(TEST_LOCAL_LOG_DIRECTORY);
        ASSERT_EQ(StartLogger(), 0);
    }

    void TearDown() override
    {
        usleep(100);
        StopLogger();
        RemoveErrorLogFileFromLocalDirectory(g_localTestVfs, TEST_LOCAL_LOG_DIRECTORY);
        g_localTestVfs = nullptr;
        ASSERT_EQ(ExitVfsModule(), 0);
        RemoveMockRemoteFile();
    }

    static ErrorLogRemoteFileConfigure GetTestRemoteLogCfg(uint32_t totalSpace, uint32_t logFileCount)
    {
        ErrorLogRemoteFileConfigure cfg;
        (void) memset_s(&cfg, sizeof(ErrorLogRemoteFileConfigure), 0, sizeof(ErrorLogRemoteFileConfigure));
        cfg.remoteVfs = g_localTestVfs;
        cfg.totalSpace = totalSpace;
        (void)strcpy_s(cfg.storeSpaceName, sizeof(cfg.storeSpaceName), "storeSpaceName1");
        cfg.errorLogFileCount = logFileCount;
        if (logFileCount == 2) {
            cfg.errorLogFileIds[0] = TEST_REMOTE_LOG_DATA_FILE_ID_1;
            cfg.errorLogFileIds[1] = TEST_REMOTE_LOG_DATA_FILE_ID_2;
        } else {
            cfg.errorLogFileIds[0] = TEST_REMOTE_LOG_DATA_FILE_ID_1;
        }
        return cfg;
    }

    static void CalculateOneLogSize(int64_t *remoteLogSize)
    {
        ErrorLogRemoteFileConfigure cfg = GetTestRemoteLogCfg(UINT32_MAX, 1);

        bool res = EnableRemoteErrorLog(&cfg);
        ASSERT_TRUE(res);
        /* Just write 1 log */
        WriteOneTestErrorLog();
        FlushLogger();
        DisableRemoteErrorLog();
        ASSERT_TRUE(GetRemoteErrLogStopCompleted());

        /* Read log file to get log size */
        int64_t oneLogSize = 0;
        FileDescriptor *dataFd = nullptr;
        string dataFileName = TestConvertFileIdToFileName(TEST_REMOTE_LOG_DATA_FILE_ID_1);
        ASSERT_EQ(Open(g_localTestVfs, dataFileName.c_str(), FILE_READ_AND_WRITE_FLAG, &dataFd), 0);
        ASSERT_EQ(GetSize(dataFd, &oneLogSize), 0);
        ASSERT_EQ(Truncate(dataFd, 0), 0);
        ASSERT_GT(oneLogSize, 0);
        ASSERT_EQ(Close(dataFd), 0);

        ASSERT_EQ(Remove(g_localTestVfs, dataFileName.c_str()), 0);

        *remoteLogSize = oneLogSize;
    }

    std::thread threads[TEST_THREAD_COUNT];

private:
    static void RemoveMockRemoteFile()
    {
        string rmCmd = "rm -rf *";
        rmCmd += TEST_MOCK_REMOTE_LOG_FILE_SUFFIX;
        system(rmCmd.c_str());
    }
};

/**
 * Remote log configure test, including invalid configure test
 */
TEST_F(ErrorLogMockRemoteLogTest, RemoteLogConfigureTest001)
{
    ASSERT_FALSE(EnableRemoteErrorLog(nullptr));
    ErrorLogRemoteFileConfigure cfg = GetTestRemoteLogCfg(UINT32_MAX, 0);
    ASSERT_FALSE(EnableRemoteErrorLog(&cfg));
    cfg = GetTestRemoteLogCfg(0, 1);
    ASSERT_FALSE(EnableRemoteErrorLog(&cfg));
    cfg = GetTestRemoteLogCfg(UINT32_MAX, REMOTE_LOG_FILE_MAX_COUNT + 1);
    ASSERT_FALSE(EnableRemoteErrorLog(&cfg));
    cfg = GetTestRemoteLogCfg(1, 1);
    ASSERT_TRUE(EnableRemoteErrorLog(&cfg));
    ErrorLogRemoteFileConfigure resCfg;
    ASSERT_TRUE(GetRemoteErrorLogConfigure(&resCfg));
    ASSERT_EQ(resCfg.errorLogFileCount, 1);
    ASSERT_EQ(resCfg.totalSpace, 1);
    cfg = GetTestRemoteLogCfg(2, 2);
    ASSERT_FALSE(EnableRemoteErrorLog(&cfg)); /* Does not support switch remote log configure */
    ASSERT_TRUE(GetRemoteErrorLogConfigure(&resCfg));
    ASSERT_EQ(resCfg.errorLogFileCount, 1);
    ASSERT_EQ(resCfg.totalSpace, 1);
    DisableRemoteErrorLog();
    ASSERT_TRUE(GetRemoteErrLogStopCompleted());
    ASSERT_FALSE(GetRemoteErrorLogConfigure(&resCfg));
}

static void PrintLogThread(int loopCount)
{
    ASSERT_EQ(OpenLogger(), 0);
    for (int i = 1; i <= loopCount; ++i) {
        WriteOneTestErrorLog();
        Usleep(10);
    }
    CloseLogger();
}

/**
 * Write log to remote log data file, check file size and file content
 */
TEST_F(ErrorLogMockRemoteLogTest, WriteRemoteErrorLogTest001)
{
    SetErrLogWriteMode(false);
    ASSERT_EQ(OpenLogger(), 0);
    int64_t oneLogSize = 0;
    CalculateOneLogSize(&oneLogSize);
    /* 1 remote log data file with unlimited size */
    ErrorLogRemoteFileConfigure cfg = GetTestRemoteLogCfg(UINT32_MAX, 1);
    /* Enable remote log to write multiple log with same size in multi-threads */
    bool res = EnableRemoteErrorLog(&cfg);
    ASSERT_TRUE(res);
    for (auto &th : threads) {
        th = std::thread(PrintLogThread, TEST_LOOP_COUNT);
        usleep(100);
        FlushLogger();
    }
    for (auto &th : threads) {
        th.join();
    }
    sleep(1);
    DisableRemoteErrorLog();
    ASSERT_TRUE(GetRemoteErrLogStopCompleted());
    CloseLogger();

    string remoteLogDataFileName = TestConvertFileIdToFileName(TEST_REMOTE_LOG_DATA_FILE_ID_1);
    ASSERT_TRUE(IsFileContainedString(g_localTestVfs, remoteLogDataFileName.c_str(), TEST_REMOTE_LOG_MESSAGE, true));
    int64_t fileSize = GetFileSize(g_localTestVfs, TEST_REMOTE_LOG_DATA_FILE_ID_1, remoteLogDataFileName.c_str());
    ASSERT_GE(fileSize - LOG_SEQUENCE_INFO_SIZE, (oneLogSize - LOG_SEQUENCE_INFO_SIZE) * TEST_THREAD_COUNT * TEST_LOOP_COUNT);
}

/**
 * Write log to remote log data file, with 1 file in 1KB, leading to file rotation
 * Check file size in the end
 */
TEST_F(ErrorLogMockRemoteLogTest, WriteRemoteErrorLogTest002)
{
    SetErrLogWriteMode(false);
    ASSERT_EQ(OpenLogger(), 0);
    int64_t oneLogSize = 0;
    CalculateOneLogSize(&oneLogSize);
    /* 1 remote log data file with 1KB size */
    ErrorLogRemoteFileConfigure cfg = GetTestRemoteLogCfg(1, 1);
    /* Enable remote log to write multiple log with same size in multi-threads */
    bool res = EnableRemoteErrorLog(&cfg);
    ASSERT_TRUE(res);
    for (auto &th : threads) {
        th = std::thread(PrintLogThread, TEST_LOOP_COUNT);
        usleep(100);
        FlushLogger();
    }
    for (auto &th : threads) {
        th.join();
    }
    sleep(1);
    DisableRemoteErrorLog();
    ASSERT_TRUE(GetRemoteErrLogStopCompleted());
    CloseLogger();

    string remoteLogDataFileName = TestConvertFileIdToFileName(TEST_REMOTE_LOG_DATA_FILE_ID_1);
    int64_t fileSize = GetFileSize(g_localTestVfs, TEST_REMOTE_LOG_DATA_FILE_ID_1, remoteLogDataFileName.c_str());
    int logCountPerFile = 0;
    while ((oneLogSize - LOG_SEQUENCE_INFO_SIZE) * logCountPerFile <= 1024 - LOG_SEQUENCE_INFO_SIZE) {
        logCountPerFile++;
    }
    int expectLogCount = (TEST_THREAD_COUNT * TEST_LOOP_COUNT) % logCountPerFile;
    EXPECT_GE(fileSize - LOG_SEQUENCE_INFO_SIZE, (oneLogSize - LOG_SEQUENCE_INFO_SIZE) * expectLogCount);
}

/**
 * Write log to remote log data file, with 2 file in 1KB per file, leading to file rotation
 * Check 2 file size in the end
 */
TEST_F(ErrorLogMockRemoteLogTest, WriteRemoteErrorLogTest003)
{
    SetErrLogWriteMode(false);
    ASSERT_EQ(OpenLogger(), 0);
    int64_t oneLogSize = 0;
    CalculateOneLogSize(&oneLogSize);
    /* 2 remote log data file with 1KB size per file */
    ErrorLogRemoteFileConfigure cfg = GetTestRemoteLogCfg(2, 2);
    /* Enable remote log to write multiple log with same size in multi-threads */
    bool res = EnableRemoteErrorLog(&cfg);
    ASSERT_TRUE(res);
    for (auto &th : threads) {
        th = std::thread(PrintLogThread, TEST_LOOP_COUNT);
        usleep(100);
        FlushLogger();
    }
    for (auto &th : threads) {
        th.join();
    }
    sleep(1);
    DisableRemoteErrorLog();
    ASSERT_TRUE(GetRemoteErrLogStopCompleted());
    CloseLogger();

    int64_t fileSize[2];
    string remoteLogDataFileName1 = TestConvertFileIdToFileName(TEST_REMOTE_LOG_DATA_FILE_ID_1);
    fileSize[0] = GetFileSize(g_localTestVfs, TEST_REMOTE_LOG_DATA_FILE_ID_1, remoteLogDataFileName1.c_str());
    string remoteLogDataFileName2 = TestConvertFileIdToFileName(TEST_REMOTE_LOG_DATA_FILE_ID_2);
    fileSize[1] = GetFileSize(g_localTestVfs, TEST_REMOTE_LOG_DATA_FILE_ID_1, remoteLogDataFileName2.c_str());
    int logCountPerFirstFile = 0;
    int logCountPerFile = 0;
    /* log count of the file that contain file sequence info */
    while ((oneLogSize - LOG_SEQUENCE_INFO_SIZE) * logCountPerFirstFile <= 1024 - LOG_SEQUENCE_INFO_SIZE) {
        logCountPerFirstFile++;
    }
    /* log count of the file that not contain file sequence info */
    while ((oneLogSize - LOG_SEQUENCE_INFO_SIZE) * logCountPerFile <= 1024) {
        logCountPerFile++;
    }
    int totalLogCount = TEST_THREAD_COUNT * TEST_LOOP_COUNT;
    /* Check current remote log data file size */
    int curFileSeq = (totalLogCount % (logCountPerFirstFile + logCountPerFile) > logCountPerFirstFile) ? 1 : 0;
    if (curFileSeq == 0) {
        int curFileLogCount = totalLogCount % (logCountPerFirstFile + logCountPerFile);
        EXPECT_EQ(fileSize[curFileSeq] - LOG_SEQUENCE_INFO_SIZE, (oneLogSize - LOG_SEQUENCE_INFO_SIZE) * curFileLogCount);
        EXPECT_EQ(fileSize[1 - curFileSeq], (oneLogSize - LOG_SEQUENCE_INFO_SIZE) * logCountPerFile);
    } else {
        int curFileLogCount = totalLogCount % (logCountPerFirstFile + logCountPerFile) - logCountPerFirstFile;
        EXPECT_EQ(fileSize[curFileSeq], (oneLogSize - LOG_SEQUENCE_INFO_SIZE) * curFileLogCount);
        EXPECT_GE(fileSize[1 - curFileSeq] - LOG_SEQUENCE_INFO_SIZE, (oneLogSize - LOG_SEQUENCE_INFO_SIZE) * logCountPerFirstFile);
    }
}

/**
 * Write log, with one log writen to local and another log writen to remote,
 * Check local file and remote file content
 */
#define UT_NAME "utils_unittest"
TEST_F(ErrorLogMockRemoteLogTest, WriteLocalAndRemoteErrorLogTest001)
{
    SetErrLogWriteMode(false);
    char localLogFileName[MAX_PATH] = {0};
    ASSERT_EQ(OpenLogger(), 0);
    ErrorLogRemoteFileConfigure cfg = GetTestRemoteLogCfg(UINT32_MAX, 1);
    /* Enable remote log to write multiple log with same size in multi-threads */
    bool res = EnableRemoteErrorLog(&cfg);
    ASSERT_TRUE(res);
    WriteOneTestErrorLog();
    FlushLogger();
    ASSERT_EQ(GetErrorLogFileFullPath(localLogFileName, MAX_PATH, GetErrLogDirectory, ERR_LOG_EXTEND, UT_NAME), 0);
    DisableRemoteErrorLog();
    ASSERT_TRUE(GetRemoteErrLogStopCompleted());
#define TEST_LOCAL_LOG_MESSAGE  "LocalLog"
    ErrLog(LOG, ErrMsg(TEST_LOCAL_LOG_MESSAGE));
    CloseLogger();

    /* "RemoteLog" will be writen to remote log file, "LocalLog" will be writen to local log file */
    string remoteLogDataFileName = TestConvertFileIdToFileName(TEST_REMOTE_LOG_DATA_FILE_ID_1);
    ASSERT_TRUE(IsFileContainedString(g_localTestVfs, remoteLogDataFileName.c_str(), TEST_REMOTE_LOG_MESSAGE, true));
    ASSERT_FALSE(IsFileContainedString(g_localTestVfs, remoteLogDataFileName.c_str(), TEST_LOCAL_LOG_MESSAGE, true));
    ASSERT_TRUE(IsFileContainedString(g_localTestVfs, localLogFileName, TEST_LOCAL_LOG_MESSAGE, false));
    ASSERT_FALSE(IsFileContainedString(g_localTestVfs, localLogFileName, TEST_REMOTE_LOG_MESSAGE, true));
}

/**
 * Write log to remote log data file, check file size and file content
 */
TEST_F(ErrorLogMockRemoteLogTest, WriteRemoteErrorLogDeadLoop)
{
    FAULT_INJECTION_ACTIVE(MOCK_REMOTE_LOG_DATA_FILE, FI_GLOBAL);
    SetErrLogWriteMode(false);
    char localLogFileName[MAX_PATH] = {0};
    ASSERT_EQ(OpenLogger(), ERROR_SYS_OK);
    /* 1 remote log data file with unlimited size */
    ErrorLogRemoteFileConfigure cfg = GetTestRemoteLogCfg(UINT32_MAX, 1);
    /* Enable remote log to write multiple log with same size in multi-threads */
    bool res = EnableRemoteErrorLog(&cfg);
    ASSERT_TRUE(res);
    WriteOneTestErrorLog();
    FlushLogger();
    ASSERT_EQ(GetErrorLogFileFullPath(localLogFileName, MAX_PATH, GetErrLogDirectory, ERR_LOG_EXTEND, UT_NAME), ERROR_SYS_OK);
    DisableRemoteErrorLog();
    ASSERT_TRUE(GetRemoteErrLogStopCompleted());
    CloseLogger();
    string remoteLogDataFileName = TestConvertFileIdToFileName(TEST_REMOTE_LOG_DATA_FILE_ID_1);
    ASSERT_FALSE(IsFileContainedString(g_localTestVfs, remoteLogDataFileName.c_str(), TEST_REMOTE_LOG_DATA_MESSAGE, true));
    ASSERT_TRUE(IsFileContainedString(g_localTestVfs, localLogFileName, TEST_REMOTE_LOG_DATA_MESSAGE, false));
    FAULT_INJECTION_INACTIVE(MOCK_REMOTE_LOG_DATA_FILE, FI_GLOBAL);
}

/**
 * Write log to remote log data file, with 2 file in 1KB per file, leading to file rotation
 * Check the location of the newest log after reloading
 */
TEST_F(ErrorLogMockRemoteLogTest, WriteRemoteErrorLogReloadTest01)
{
    SetErrLogWriteMode(false);
    ASSERT_EQ(OpenLogger(), 0);
    /* 2 remote log data file with 1KB size per file */
    ErrorLogRemoteFileConfigure cfg = GetTestRemoteLogCfg(2, 2);
    /* Enable remote log to write multiple log with same size in multi-threads */
    bool res = EnableRemoteErrorLog(&cfg);
    ASSERT_TRUE(res);
    for (auto &th : threads) {
        th = std::thread(PrintLogThread, TEST_LOOP_COUNT);
        usleep(100);
    }
    FlushLogger();
    for (auto &th : threads) {
        th.join();
    }
    sleep(1);
    DisableRemoteErrorLog();
    ASSERT_TRUE(GetRemoteErrLogStopCompleted());
    usleep(100);
    CloseLogger();
    StopLogger();
}

TEST_F(ErrorLogMockRemoteLogTest, WriteRemoteErrorLogReloadTest02)
{
    /* restart logger thread, and reload the log current sequence information */
    ASSERT_EQ(StartLogger(), 0);
    ASSERT_EQ(OpenLogger(), 0);
    /* 2 remote log data file with 1KB size per file */
    ErrorLogRemoteFileConfigure cfg = GetTestRemoteLogCfg(2, 2);
    bool res = EnableRemoteErrorLog(&cfg);
    ASSERT_TRUE(res);
    ErrLog(LOG, ErrMsg(TEST_REMOTE_LOG_RELOAD_MESSAGE));
    FlushLogger();
    DisableRemoteErrorLog();
    ASSERT_TRUE(GetRemoteErrLogStopCompleted());
    CloseLogger();

    /* check which log file that the newest log in */
    string remoteLogDataFileName1 = TestConvertFileIdToFileName(TEST_REMOTE_LOG_DATA_FILE_ID_1);
    string remoteLogDataFileName2 = TestConvertFileIdToFileName(TEST_REMOTE_LOG_DATA_FILE_ID_2);

    ASSERT_TRUE(IsFileContainedString(g_localTestVfs, remoteLogDataFileName1.c_str(), TEST_REMOTE_LOG_RELOAD_MESSAGE, true));
    ASSERT_FALSE(IsFileContainedString(g_localTestVfs, remoteLogDataFileName2.c_str(), TEST_REMOTE_LOG_RELOAD_MESSAGE, true));
}

/**
 * Write log to remote log data file, with 2 file in 1KB per file, leading to file rotation
 * Check when update remote log sequence number failed, the sequence number will rollback to previous number
 */
TEST_F(ErrorLogMockRemoteLogTest, WriteRemoteErrorLogRollbackSeqNumTest)
{
    SetErrLogWriteMode(false);
    ASSERT_EQ(OpenLogger(), 0);
    /* 2 remote log data file with 1KB size per file */
    ErrorLogRemoteFileConfigure cfg = GetTestRemoteLogCfg(2, 2);
    /* Enable remote log to write multiple log with same size */
    bool res = EnableRemoteErrorLog(&cfg);
    ASSERT_TRUE(res);

    uint32_t preSeqNum = GetRemoteLogCurSeqNum();

    /* Mock the situation that update remote log sequence num failed */
    FAULT_INJECTION_ACTIVE(MOCK_UPDATE_REMOTE_LOG_SEQ_NUM_FAILED, FI_GLOBAL);
    PrintLogThread(10);
    sleep(1);
    FlushLogger();
    FAULT_INJECTION_INACTIVE(MOCK_UPDATE_REMOTE_LOG_SEQ_NUM_FAILED, FI_GLOBAL);

    /* After failing to update remote log sequence num, checking the value of sequence number */
    ASSERT_EQ(GetRemoteLogCurSeqNum(), preSeqNum);

    usleep(100);
    DisableRemoteErrorLog();
    ASSERT_TRUE(GetRemoteErrLogStopCompleted());

    usleep(100);
    CloseLogger();
    StopLogger();
}

/**
 * Write log to remote log data file, with 2 file in 1KB per file, leading to file rotation
 * Check when update remote log sequence number succeed, the sequence number will add 1
 */
TEST_F(ErrorLogMockRemoteLogTest, WriteRemoteErrorLogUpdateSeqNumSucceedTest)
{
    SetErrLogWriteMode(false);
    ASSERT_EQ(OpenLogger(), 0);
    /* 2 remote log data file with 1KB size per file */
    ErrorLogRemoteFileConfigure cfg = GetTestRemoteLogCfg(2, 2);
    /* Enable remote log to write multiple log with same size */
    bool res = EnableRemoteErrorLog(&cfg);
    ASSERT_TRUE(res);

    uint32_t preSeqNum = GetRemoteLogCurSeqNum();

    /* Mock the situation that rotate log file and update remote log sequence num successfully */
    PrintLogThread(10);
    sleep(1);
    FlushLogger();

    /* After updating remote log sequence num, checking the value of sequence number */
    ASSERT_EQ(GetRemoteLogCurSeqNum(), preSeqNum + 1);

    usleep(100);
    DisableRemoteErrorLog();
    ASSERT_TRUE(GetRemoteErrLogStopCompleted());

    usleep(100);
    CloseLogger();
    StopLogger();
}

TEST_F(ErrorLogMockRemoteLogTest, EnableRemoteErrorLogWithInvalidSeqNumTest)
{
    SetErrLogWriteMode(false);
    ASSERT_EQ(OpenLogger(), 0);

    ErrorLogRemoteFileConfigure cfg = GetTestRemoteLogCfg(1, 1);
    FAULT_INJECTION_ACTIVE(MOCK_GET_REMOTE_LOG_SEQ_NUM_SECCEED, FI_GLOBAL);
    FAULT_INJECTION_ACTIVE(MOCK_REMOTE_LOG_INVALID_SEQ_NUM, FI_GLOBAL);
    bool res = EnableRemoteErrorLog(&cfg);
    FAULT_INJECTION_INACTIVE(MOCK_GET_REMOTE_LOG_SEQ_NUM_SECCEED, FI_GLOBAL);
    FAULT_INJECTION_INACTIVE(MOCK_REMOTE_LOG_INVALID_SEQ_NUM, FI_GLOBAL);
    ASSERT_TRUE(res);

    /* After getting invalid log sequence num, checking the value of sequence number */
    ASSERT_EQ(GetRemoteLogCurSeqNum(), 0);

    /* Check the size of updated first remote log file */
    ASSERT_EQ(GetRemoteLogCurFileSize(), LOG_SEQUENCE_INFO_SIZE);

    /* Check the content of updated first remote log file */
    string remoteLogDataFileName1 = TestConvertFileIdToFileName(TEST_REMOTE_LOG_DATA_FILE_ID_1);
    ASSERT_TRUE(IsFileContainedString(g_localTestVfs, remoteLogDataFileName1.c_str(), TEST_REMOTE_LOG_UPDATED_SEQUENCE_INFO_MESSAGE, false));

    DisableRemoteErrorLog();

    CloseLogger();
    StopLogger();
}

#endif /* ENABLE_FAULT_INJECTION */
