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
 * ut_config_parser.cpp
 *
 * Description:
 * - test config parser basic function include load/store config and CRUD.
 * - test the exception scenario.
 *
 * ---------------------------------------------------------------------------------
 */
#include "gtest/gtest.h"
#include <limits.h>
#include <fstream>
#include <string>
#include <sstream>
#include "syslog/err_log.h"
#include "syslog/err_log_internal.h"
#include "config_parser/config_parser.h"

using namespace std;

class ConfigParserTest : public testing::Test {
public:
    ConfigParserTest() {
        m_strJsonDir = string(UTILS_UT_DIR) + "/ut_config_parser/json_files/";

        m_strJsonTest = m_strJsonDir + "test.json";
        m_strJsonUpdate = m_strJsonDir + "update.json";
    }

    static void SetUpTestSuite() {
    }

    static void TearDownTestSuite() {
    }

    void SetUp() override {
    }

    void TearDown() override {
    }

    string m_strJsonDir;
    string m_strJsonTest;
    string m_strJsonUpdate;
};

/*
 * @tc.desc:  Test load/store/reload with valid/invalid length of file path
 */
TEST_F(ConfigParserTest, TestWhenFilePathLengthExceedLimitThenFail)
{
    StartLogger();
    OpenLogger();

    /**
     * @tc.steps: step1. Test path length less or equal than CONFIGPARSER_FILE_MAXIMUM
     * @tc.expected: step1.The function
     */
    ErrorCode ret;
    ConfigParserHandle *handle = (ConfigParserHandle *)0x1234;

    // 1024 length for file is OK
    std::string strPath(CONFIGPARSER_FILE_MAXIMUM - 1, 'a');

    ret = ConfigParserLoadFile(strPath.c_str(), &handle);
    EXPECT_EQ(ret, VFS_ERROR_FILE_NOT_EXIST);

    ret = ConfigParserReloadFile(strPath.c_str(), handle, &handle, NULL);
    EXPECT_EQ(ret, VFS_ERROR_FILE_NOT_EXIST);

    /**
     * @tc.steps: step2. Test path length larger than CONFIGPARSER_FILE_MAXIMUM
     * @tc.expected: step2.The function return error
     */
    std::string longPath(CONFIGPARSER_FILE_MAXIMUM, 'a');
    ret = ConfigParserLoadFile(longPath.c_str(), &handle);
    EXPECT_EQ(ret, CONFIG_PARSER_PARAMETER_INCORRECT);

    ret = ConfigParserStoreFile(handle, longPath.c_str());
    EXPECT_EQ(ret, CONFIG_PARSER_ALLOCATOR_FAILED);

    ret = ConfigParserReloadFile(longPath.c_str(), handle, &handle, NULL);
    EXPECT_EQ(ret, CONFIG_PARSER_PARAMETER_INCORRECT);

    usleep(100);
    CloseLogger();
    StopLogger();
}

TEST_F(ConfigParserTest, UseVFSAroundConfigParser)
{
    InitVfsModule(NULL);
    VirtualFileSystem *vfs;
    GetStaticLocalVfsInstance(&vfs);
    FileDescriptor *fd = NULL;
    Open(vfs, m_strJsonTest.c_str(), FILE_READ_AND_WRITE_FLAG, &fd);
    int64_t filesize;
    GetSize(fd, &filesize);
    EXPECT_EQ(filesize, 519);
    Close(fd);
    ExitVfsModule();

    ConfigParserHandle *handle = NULL;
    ConfigParserLoadFile(m_strJsonTest.c_str(), &handle);
    char *str = NULL;
    ConfigParserGetStrValue(handle, "cluster_server.log_level", &str);
    EXPECT_STREQ(str, "INFO");
    ConfigParserDelete(handle, "");

    InitVfsModule(NULL);
    GetStaticLocalVfsInstance(&vfs);
    Open(vfs, m_strJsonTest.c_str(), FILE_READ_AND_WRITE_FLAG, &fd);
    GetSize(fd, &filesize);
    EXPECT_EQ(filesize, 519);
    Close(fd);
    ExitVfsModule();
}

TEST_F(ConfigParserTest, UseVFSBetweenConfigParser)
{
    InitVfsModule(NULL);
    VirtualFileSystem *vfs;
    GetStaticLocalVfsInstance(&vfs);
    FileDescriptor *fd = NULL;
    Open(vfs, m_strJsonTest.c_str(), FILE_READ_AND_WRITE_FLAG, &fd);

    ConfigParserHandle *handle = NULL;
    ConfigParserLoadFile(m_strJsonTest.c_str(), &handle);
    char *str = NULL;
    ConfigParserGetStrValue(handle, "cluster_server.log_level", &str);
    EXPECT_STREQ(str, "INFO");
    ConfigParserDelete(handle, "");

    int64_t filesize;
    GetSize(fd, &filesize);
    EXPECT_EQ(filesize, 519);
    Close(fd);
    ExitVfsModule();
}

TEST_F(ConfigParserTest, TestLoadFileOk)
{
    ConfigParserHandle *handle = NULL;
    ConfigParserLoadFile(m_strJsonTest.c_str(), &handle);
    char *str = NULL;
    ConfigParserGetStrValue(handle, "cluster_server.log_level", &str);
    EXPECT_STREQ(str, "INFO");
    int num = -1;
    ConfigParserGetIntValue(handle, "cluster_server.node_info.1.ip_lists.0.port.3", &num);
    EXPECT_EQ(num, 5555);
    ConfigParserDelete(handle, "");
}

/*
 * @tc.desc:  Test load file by invalid parameters and JSON data
 */
TEST_F(ConfigParserTest, TestLoadFileFail)
{
    StartLogger();
    OpenLogger();

    ErrorCode ret;

    /**
     * @tc.steps: step1. Test by invalid parameters
     * @tc.expected: step1.The function return error
     */
    // parameter is NULL
    ret = ConfigParserLoadFile(m_strJsonTest.c_str(), NULL);
    EXPECT_EQ(ret, CONFIG_PARSER_PARAMETER_INCORRECT);

    // parameter is NULL
    ConfigParserHandle *handle = NULL;
    ret = ConfigParserLoadFile(NULL, &handle);
    EXPECT_EQ(ret, CONFIG_PARSER_PARAMETER_INCORRECT);

    // path is not exist
    ret = ConfigParserLoadFile("/expect:/wrong/path", &handle);
    EXPECT_NE(ret, ERROR_SYS_OK);

    /**
     * @tc.steps: step2. Test by invalid JSON Data
     * @tc.expected: step2.The function return error
     */
    // invalid format of JSON
    string strJsonBad = m_strJsonDir + "bad.json";
    ret = ConfigParserLoadFile(strJsonBad.c_str(), &handle);
    EXPECT_EQ(ret, CONFIG_PARSER_HANDLE_IS_NULL);

    // the content of JSON is empty
    string strJsonEmpty = m_strJsonDir + "empty.json";
    ret = ConfigParserLoadFile(strJsonEmpty.c_str(), &handle);
    EXPECT_EQ(ret, CONFIG_PARSER_HANDLE_IS_NULL);

    // number is too long
    string strJsonOverLongNumber = m_strJsonDir + "testOverLongNumber.json";
    ret = ConfigParserLoadFile(strJsonOverLongNumber.c_str(), &handle);
    EXPECT_EQ(ret, CONFIG_PARSER_HANDLE_IS_NULL);

    usleep(100);
    CloseLogger();
    StopLogger();
}

/*
 * Load number in range is fine, but not for 64 bits
 */
TEST_F(ConfigParserTest, TestLoadFileNumberOK)
{
    int num = 0;
    ConfigParserHandle *handle = NULL;
    string strJsontestInRangeNumber = m_strJsonDir + "testInRangeNumber.json";
    ConfigParserLoadFile(strJsontestInRangeNumber.c_str(), &handle);
    ConfigParserGetIntValue(handle, "0", &num);
    EXPECT_EQ(num, INT_MAX);
    ConfigParserGetIntValue(handle, "1", &num);
    EXPECT_EQ(num, INT_MIN);
    double ll = 0;
    ConfigParserGetDoubleValue(handle, "2", &ll);
    EXPECT_EQ(ll, LONG_MAX);
    ConfigParserGetDoubleValue(handle, "3", &ll);
    EXPECT_EQ(ll, LONG_MIN);

    ConfigParserGetDoubleValue(handle, "4", &ll);
    EXPECT_EQ(ll, UINT_MAX);
    ConfigParserGetDoubleValue(handle, "5", &ll);
    EXPECT_NE(ll, ULONG_MAX);
    ConfigParserDelete(handle, "");
}

/*
 * If not convert to long, double is ok for big number.
 */
TEST_F(ConfigParserTest, TestLoadFileNumberBad)
{
    int num = 0;
    ConfigParserHandle *handle = NULL;
    string strJsonFile = m_strJsonDir + "testNotInRangeNumber.json";
    ConfigParserLoadFile(strJsonFile.c_str(), &handle);

    ConfigParserGetIntValue(handle, "0", &num);
    EXPECT_NE(num, (long)INT_MAX + 1); // out of range

    ConfigParserGetIntValue(handle, "1", &num);
    EXPECT_NE(num, (long)INT_MIN - 1); // out of range

    double ll = 0;
    ConfigParserGetDoubleValue(handle, "2", &ll);
    EXPECT_NE((int64_t)ll, LONG_MIN + 1); // out of range

    ConfigParserGetDoubleValue(handle, "3", &ll);
    EXPECT_NE((int64_t)ll, LONG_MIN - 1); // out of range

    ConfigParserDelete(handle, "");
}

TEST_F(ConfigParserTest, TestStoreFile)
{
    ConfigParserHandle *root = NULL;
    ConfigParserLoadFile(m_strJsonTest.c_str(), &root);
    EXPECT_EQ(ConfigParserSetBoolValue(root, "tmp.0.foo", true), ERROR_SYS_OK);
    double tmpDou = 13.55;
    ConfigParserSetDoubleValue(root, "ccluster", tmpDou);
    ConfigParserSetNullValue(root, "cluster_server.log_level");

    string strJsonStore = m_strJsonDir + "store.json";

    // store to file
    ConfigParserStoreFile(root, strJsonStore.c_str());
    ConfigParserDelete(root, "");

    // load the file and check if correct
    ConfigParserLoadFile(strJsonStore.c_str(), &root);
    bool check;
    ConfigParserGetBoolValue(root, "tmp.0.foo", &check);
    EXPECT_TRUE(check);
    double dou = -1;
    ConfigParserGetDoubleValue(root, "ccluster", &dou);
    EXPECT_EQ(dou, tmpDou);
    ConfigParserGetNullValue(root, "cluster_server.log_level", &check);
    EXPECT_TRUE(check);
    ConfigParserGetNullValue(root, "cluster_server", &check);
    EXPECT_FALSE(check);

    ConfigParserDelete(root, "");

    // remove lock file
    system(("rm " + strJsonStore + ".lock").c_str());
}

TEST_F(ConfigParserTest, TestStoreFileFail)
{
    ConfigParserHandle *root = NULL;
    ConfigParserLoadFile(m_strJsonTest.c_str(), &root);
    EXPECT_EQ(ConfigParserStoreFile(root, NULL), CONFIG_PARSER_PARAMETER_INCORRECT);
    EXPECT_EQ(ConfigParserStoreFile(NULL, ""), CONFIG_PARSER_HANDLE_IS_NULL);
    EXPECT_NE(ConfigParserStoreFile(root, ""), ERROR_SYS_OK);
    ConfigParserDelete(root, "");
}

TEST_F(ConfigParserTest, TestStoreFileMultiProcess)
{
    ConfigParserHandle *root = NULL;
    string strJsonCheckConcur = m_strJsonDir + "checkConcur.json";
    // update same place
    for (int i = 0; i < 3; i++) {
        pid_t pid;
        pid = fork();
        if (pid == 0) {
            ConfigParserHandle *handle;
            ConfigParserLoadFile(m_strJsonTest.c_str(), &handle);
            ConfigParserSetIntValue(handle, "hello", 10);
            ConfigParserStoreFile(handle, strJsonCheckConcur.c_str());
            ConfigParserDelete(handle, "");
            exit(0);
        } else {
            ConfigParserHandle *handle;
            ConfigParserLoadFile(m_strJsonTest.c_str(), &handle);
            ConfigParserSetIntValue(handle, "hello", 20);
            ConfigParserStoreFile(handle, strJsonCheckConcur.c_str());
            ConfigParserDelete(handle, "");
        }
        int k;
        waitpid(pid, &k, 0);
        ConfigParserLoadFile(strJsonCheckConcur.c_str(), &root);
        int num;
        ConfigParserGetIntValue(root, "hello", &num);
        EXPECT_TRUE(num == 10 || num == 20);
        ConfigParserDelete(root, "");
    }

    // update diff place
    pid_t pid;
    pid = fork();
    if (pid == 0) {
        ConfigParserHandle *handle;
        ConfigParserLoadFile(m_strJsonTest.c_str(), &handle);
        ConfigParserSetIntValue(handle, "first", 10);
        ConfigParserStoreFile(handle, strJsonCheckConcur.c_str());
        ConfigParserDelete(handle, "");
        exit(0);
    } else {
        ConfigParserHandle *handle;
        ConfigParserLoadFile(m_strJsonTest.c_str(), &handle);
        ConfigParserSetIntValue(handle, "second", 20);
        ConfigParserStoreFile(handle, strJsonCheckConcur.c_str());
        ConfigParserDelete(handle, "");
    }
    int k;
    waitpid(pid, &k, 0);
    ConfigParserLoadFile(strJsonCheckConcur.c_str(), &root);
    int num;
    ConfigParserGetIntValue(root, "first", &num);
    EXPECT_EQ(num, 10);
    ConfigParserGetIntValue(root, "second", &num);
    EXPECT_EQ(num, 20);
    ConfigParserDelete(root, "");

    // remove lock file
    system(("rm " + strJsonCheckConcur + ".lock").c_str());
}

TEST_F(ConfigParserTest, TestLockFileName)
{
    StartLogger();
    OpenLogger();

    ConfigParserHandle *root = NULL;
    InitVfsModule(NULL);
    VirtualFileSystem *vfs;
    GetStaticLocalVfsInstance(&vfs);
    ConfigParserLoadFile(m_strJsonTest.c_str(), &root);
    bool check;

    // Case 1
    string strJsonTestLockFile = m_strJsonDir + "testLockFile.json";
    ConfigParserStoreFile(root, strJsonTestLockFile.c_str());
    string strLockFile = strJsonTestLockFile + ".lock";
    FileIsExist(vfs, strLockFile.c_str(), &check);
    EXPECT_TRUE(check);
    system(("rm " + strLockFile).c_str());
    check = false;

    // Case 2
    char filePath[CONFIGPARSER_FILE_MAXIMUM];
    char tempFile[CONFIGPARSER_FILE_MAXIMUM];
    char *dir = get_current_dir_name();
    std::string strDir(CONFIGPARSER_FILE_MAXIMUM - strlen(dir) - 27, 'a');
    for (int i = 0; i < strlen(strDir.c_str()); i += 250) {
        strDir[i] = '/';
    }

    sprintf(filePath, "%s/config_parser_test%s/", dir, strDir.c_str());
    std::string strFilePath1(filePath);
    system(("mkdir -p " + strFilePath1).c_str());

    snprintf(tempFile, CONFIGPARSER_FILE_MAXIMUM, "%sabcd", filePath);
    ConfigParserStoreFile(root, tempFile);
    snprintf(tempFile, CONFIGPARSER_FILE_MAXIMUM, "%sa.lock", filePath);
    FileIsExist(vfs, tempFile, &check);
    EXPECT_TRUE(check);
    check = false;

    // Case 3
    std::string longDir(CONFIGPARSER_FILE_MAXIMUM - strlen(dir) - 24, 'a');
    for (int i = 0; i < strlen(longDir.c_str()); i += 250) {
        longDir[i] = '/';
    }
    snprintf(filePath, CONFIGPARSER_FILE_MAXIMUM, "%s/config_parser_test%s/", dir, longDir.c_str());
    std::string strFilePath2(filePath);
    system(("mkdir -p " + strFilePath2).c_str());

    snprintf(tempFile, CONFIGPARSER_FILE_MAXIMUM, "%sabc", filePath);
    ConfigParserStoreFile(root, tempFile);
    snprintf(tempFile, CONFIGPARSER_FILE_MAXIMUM, "%sabl", filePath);
    FileIsExist(vfs, tempFile, &check);
    EXPECT_TRUE(check);
    check = false;
    snprintf(tempFile, CONFIGPARSER_FILE_MAXIMUM, "%sabl", filePath);
    ConfigParserStoreFile(root, tempFile);
    snprintf(tempFile, CONFIGPARSER_FILE_MAXIMUM, "%sabL", filePath);
    FileIsExist(vfs, tempFile, &check);
    EXPECT_TRUE(check);

    free(dir);
    ExitVfsModule();
    ConfigParserDelete(root, "");

    usleep(100);
    CloseLogger();
    StopLogger();
    system("rm -rf config_parser_test");
}

// callback check the update item
int Callback0(char *array[], uint32_t size) {
    EXPECT_EQ(size, 0);
    return 0;
}

TEST_F(ConfigParserTest, TestReloadFile)
{
    ConfigParserHandle *root = NULL;
    ConfigParserLoadFile(m_strJsonTest.c_str(), &root);
    // update
    int tmpNum = 123;
    char *tmpStr = "data";
    ConfigParserSetStrValue(root, ".cluster_server.node_info.0.node_type", tmpStr);
    ConfigParserSetIntValue(root, ".cluster_server.log_level", tmpNum);

    ConfigParserHandle *newRoot = NULL;
    ConfigParserReloadFile(m_strJsonTest.c_str(), root, &newRoot, Callback0);
    ConfigParserDelete(root, "");
    root = newRoot;

    int32_t num = -1;
    ConfigParserGetIntValue(root, ".cluster_server.log_level", &num);
    EXPECT_EQ(num, tmpNum);
    char *str = NULL;
    ConfigParserGetStrValue(root, ".cluster_server.node_info.0.node_type", &str);
    EXPECT_STREQ(str, tmpStr);

    ConfigParserDelete(root, "");
}

// callback check the update item
int Callback1(char *array[], uint32_t size) {
    EXPECT_EQ(size, 5);
    EXPECT_STREQ(array[0], ".cluster_server.node_info.0.heartbeat_time");
    EXPECT_STREQ(array[1], ".cluster_server.node_info.0.ip_lists");
    EXPECT_STREQ(array[2], ".cluster_server.node_info.1.node_type");
    EXPECT_STREQ(array[3], ".cluster_server.node_info.1.ip_lists.0.host.0");
    EXPECT_STREQ(array[4], ".cluster_server.node_info.1.ip_lists.1");
    return 0;
}

TEST_F(ConfigParserTest, TestReloadNotUpdate)
{
    ConfigParserHandle *root = NULL;
    ConfigParserLoadFile(m_strJsonTest.c_str(), &root);
    int num = 0;
    ConfigParserGetIntValue(root, "cluster_server.node_info.1.ip_lists.1.port.2", &num);
    EXPECT_EQ(num, 5562);
    ConfigParserHandle *newRoot = NULL;
    ConfigParserReloadFile(m_strJsonUpdate.c_str(), root, &newRoot, Callback1);
    // after reload, get the updated value
    ConfigParserGetIntValue(newRoot, "cluster_server.node_info.1.ip_lists.1.port.2", &num);
    EXPECT_EQ(num, 5590);
    ConfigParserDelete(root, "");
    ConfigParserDelete(newRoot, "");
}

TEST_F(ConfigParserTest, TestReloadWithUpdateNotSame)
{
    ConfigParserHandle *root = NULL;
    ConfigParserLoadFile(m_strJsonTest.c_str(), &root);
    // update
    int tmpNum = 123;
    char *tmpStr = "data";
    ConfigParserSetStrValue(root, ".cluster_server.node_info.0.node_type", tmpStr);
    ConfigParserSetIntValue(root, ".cluster_server.log_level", tmpNum);
    ConfigParserSetBoolValue(root, ".cluster_server.node_info.0.node_id", true);

    ConfigParserHandle *newRoot = NULL;
    ConfigParserReloadFile(m_strJsonUpdate.c_str(), root, &newRoot, Callback1);
    ConfigParserDelete(root, "");
    root = newRoot;
    // check if the update still there
    int32_t num = -1;
    ConfigParserGetIntValue(root, ".cluster_server.log_level", &num);
    EXPECT_EQ(num, tmpNum);
    char *str = NULL;
    ConfigParserGetStrValue(root, ".cluster_server.node_info.0.node_type", &str);
    EXPECT_STREQ(str, tmpStr);
    bool check;
    ConfigParserGetBoolValue(root, ".cluster_server.node_info.0.node_id", &check);
    EXPECT_TRUE(check);

    ConfigParserDelete(root, "");
}

// callback check the item correct
int Callback2(char *array[], uint32_t size) {
    EXPECT_EQ(size, 2);
    // ignore those update by itself.
    EXPECT_STREQ(array[0], ".cluster_server.node_info.1.ip_lists.0.host.0");
    EXPECT_STREQ(array[1], ".cluster_server.node_info.1.ip_lists.1");
    return 0;
}

TEST_F(ConfigParserTest, TestReloadWithUpdateSame)
{
    ConfigParserHandle *root = NULL;
    ConfigParserLoadFile(m_strJsonTest.c_str(), &root);
    // update
    int tmpNum1 = 100;

    int tmpNum2 = 4449;
    char *tmpStr1 = "test";
    char *tmpStr2 = "localhost";
    ConfigParserSetStrValue(root, ".cluster_server.node_info.1.node_type", tmpStr1);
    ConfigParserSetIntValue(root, ".cluster_server.node_info.0.heartbeat_time", tmpNum1);
    ConfigParserSetStrValue(root, ".cluster_server.node_info.0.ip_lists.2", tmpStr2);
    ConfigParserSetIntValue(root, ".cluster_server.node_info.1.ip_lists.0.port.0", tmpNum2);

    ConfigParserHandle *newRoot = NULL;
    ConfigParserReloadFile(m_strJsonUpdate.c_str(), root, &newRoot, Callback2);
    ConfigParserDelete(root, "");
    root = newRoot;

    // check the update item still there
    char *str = NULL;
    ConfigParserGetStrValue(root, ".cluster_server.node_info.1.node_type", &str);
    EXPECT_STREQ(str, tmpStr1);
    int32_t num = -1;
    ConfigParserGetIntValue(root, ".cluster_server.node_info.0.heartbeat_time", &num);
    EXPECT_EQ(num, tmpNum1);
    ConfigParserGetStrValue(root, ".cluster_server.node_info.0.ip_lists.2", &str);
    EXPECT_STREQ(str, tmpStr2);
    ConfigParserGetIntValue(root, ".cluster_server.node_info.1.ip_lists.0.port.0", &num);
    EXPECT_EQ(num, tmpNum2);

    ConfigParserDelete(root, "");
}

TEST_F(ConfigParserTest, TestReloadCallbackNull)
{
    ConfigParserHandle *root = NULL;
    ConfigParserLoadFile(m_strJsonTest.c_str(), &root);
    // update
    ConfigParserSetStrValue(root, ".cluster_server.node_info.1.node_type", "data");

    ConfigParserHandle *newRoot = NULL;
    ConfigParserReloadFile(m_strJsonUpdate.c_str(), root, &newRoot, NULL);
    ConfigParserDelete(root, "");
    root = newRoot;

    // check the update item still there
    char *str = NULL;
    ConfigParserGetStrValue(root, ".cluster_server.node_info.1.node_type", &str);
    EXPECT_STREQ(str, "data");
    ConfigParserDelete(root, "");
}

/*
 * @tc.desc:  Test reload file by invalid parameters and JSON data
 */
TEST_F(ConfigParserTest, TestReloadFail)
{
    StartLogger();
    OpenLogger();

    ConfigParserHandle *root = NULL;
    ConfigParserLoadFile(m_strJsonTest.c_str(), &root);
    ConfigParserHandle *newRoot = NULL;

    /**
     * @tc.steps: step1. Test by invalid parameters
     * @tc.expected: step1.The function return error
     */
    // parameter is NULL
    EXPECT_EQ(ConfigParserReloadFile(NULL, root, &newRoot, NULL), CONFIG_PARSER_PARAMETER_INCORRECT);
    EXPECT_EQ(ConfigParserReloadFile(m_strJsonUpdate.c_str(), NULL, &newRoot, NULL), CONFIG_PARSER_PARAMETER_INCORRECT);
    EXPECT_EQ(ConfigParserReloadFile(m_strJsonUpdate.c_str(), root, NULL, NULL), CONFIG_PARSER_PARAMETER_INCORRECT);
    // path is not exist
    EXPECT_NE(ConfigParserReloadFile("/expect:/wrong/path", root, &newRoot, NULL), ERROR_SYS_OK);

    /**
     * @tc.steps: step2. Test by invalid JSON Data
     * @tc.expected: step2.The function return error
     */
    // invalid format of JSON
    string strJsonBad = m_strJsonDir + "bad.json";
    EXPECT_EQ(ConfigParserReloadFile(strJsonBad.c_str(), root, &newRoot, NULL), CONFIG_PARSER_HANDLE_IS_NULL);
    // the content of JSON is empty
    string strJsonEmpty = m_strJsonDir + "empty.json";
    EXPECT_EQ(ConfigParserReloadFile(strJsonEmpty.c_str(), root, &newRoot, NULL), CONFIG_PARSER_HANDLE_IS_NULL);

    ConfigParserDelete(root, "");

    usleep(100);
    CloseLogger();
    StopLogger();
}

TEST_F(ConfigParserTest, TestPrintJsonOk)
{
    ConfigParserHandle *root = NULL;
    string strJsontestInRangeNumber = m_strJsonDir + "testInRangeNumber.json";
    ConfigParserLoadFile(strJsontestInRangeNumber.c_str(), &root);

    char *str;
    ConfigParserPrintJson(root, &str);
    EXPECT_STREQ(str, "[2147483647, -2147483648, 9.2233720368547758e+18, "
                      "-9.2233720368547758e+18, 4294967295, 1.8446744073709552e+17]");
    free(str);

    ConfigParserDelete(root, "");
}

TEST_F(ConfigParserTest, TestPrintJsonWhenParamIsNULLThenFail)
{
    char *str;
    EXPECT_EQ(ConfigParserPrintJson(NULL, &str), CONFIG_PARSER_HANDLE_IS_NULL);
    ConfigParserHandle *handle = (ConfigParserHandle *)0x1234;
    EXPECT_EQ(ConfigParserPrintJson(handle, NULL), CONFIG_PARSER_PARAMETER_INCORRECT);
}

TEST_F(ConfigParserTest, TestString)
{
    ConfigParserHandle *root = NULL;
    ConfigParserLoadFile(m_strJsonTest.c_str(), &root);
    // getter
    char *str = NULL;
    ConfigParserGetStrValue(root, "cluster_server.node_info.0.node_type", &str);
    EXPECT_STREQ(str, "coordinator");
    ConfigParserSetStrValue(root, "cluster_server.node_info.1.ip_lists.0.port.1", "hello there");
    ConfigParserGetStrValue(root, "cluster_server.node_info.1.ip_lists.0.port.1", &str);
    EXPECT_STREQ(str, "hello there");
    ConfigParserSetStrValue(root, "test.hello", "???");
    ConfigParserGetStrValue(root, "test.hello", &str);
    EXPECT_STREQ(str, "???");

    // get exception
    EXPECT_EQ(ConfigParserGetStrValue(NULL, "cluster_server", &str), CONFIG_PARSER_HANDLE_IS_NULL);
    EXPECT_EQ(ConfigParserGetStrValue(root, NULL, &str), CONFIG_PARSER_PARAMETER_INCORRECT);
    EXPECT_EQ(ConfigParserGetStrValue(root, "hello", NULL), CONFIG_PARSER_PARAMETER_INCORRECT); // value is null
    EXPECT_EQ(ConfigParserGetStrValue(root, "cluster_server.node_info.0.heartbeat_time", &str), CONFIG_PARSER_WRONG_DATA_TYPE);
    EXPECT_EQ(ConfigParserGetStrValue(root, "???#$%^%&*(Y&*Y(&*wrong path#%^$^&*^&*(???", &str),
              CONFIG_PARSER_PARSE_PATH_FAILED);

    // setter
    EXPECT_EQ(ConfigParserSetStrValue(root, "cluster_server.log_level", "%^*^&*(^$%^*&("), ERROR_SYS_OK);
    ConfigParserGetStrValue(root, "cluster_server.log_level", &str);
    EXPECT_STREQ(str, "%^*^&*(^$%^*&(");
    char longlong[4096];
    for (int i = 0; i < 4095; i++) {
        longlong[i] = 'a';
    }
    longlong[4095] = '\0';
    // could support at lease 4096 bytes string value
    EXPECT_EQ(ConfigParserSetStrValue(root, "testcase", longlong), ERROR_SYS_OK);
    ConfigParserGetStrValue(root, "testcase", &str);
    EXPECT_STREQ(str, longlong);

    // set exception
    EXPECT_EQ(ConfigParserSetStrValue(NULL, "cluster_server", "sad"), CONFIG_PARSER_HANDLE_IS_NULL);
    EXPECT_EQ(ConfigParserSetStrValue(root, "", "dawdsa"), CONFIG_PARSER_CREATE_PATH_FAILED);
    EXPECT_EQ(ConfigParserSetStrValue(root, NULL, "dwqadas"), CONFIG_PARSER_PARAMETER_INCORRECT);
    EXPECT_EQ(ConfigParserSetStrValue(root, "hello", NULL), CONFIG_PARSER_PARAMETER_INCORRECT);

    ConfigParserDelete(root, "");
}

TEST_F(ConfigParserTest, TestInteger)
{
    ConfigParserHandle *root = NULL;
    ConfigParserLoadFile(m_strJsonTest.c_str(), &root);
    // getter
    int num;
    ConfigParserGetIntValue(root, "cluster_server.node_info.0.heartbeat_time", &num);
    EXPECT_EQ(num, 120);
    ConfigParserSetIntValue(root, "cluster_server.node_info.1.ip_lists.0.port.1", 3333);
    ConfigParserGetIntValue(root, "cluster_server.node_info.1.ip_lists.0.port.1", &num);
    EXPECT_EQ(num, 3333);
    ConfigParserSetIntValue(root, "test.127.0.0.1", 25);
    ConfigParserGetIntValue(root, "test.127.0.0.1", &num);
    EXPECT_EQ(num, 25);

    // get exception
    EXPECT_EQ(ConfigParserGetIntValue(NULL, "cluster_server", &num), CONFIG_PARSER_HANDLE_IS_NULL);
    EXPECT_EQ(ConfigParserGetIntValue(root, NULL, &num), CONFIG_PARSER_PARAMETER_INCORRECT);
    EXPECT_EQ(ConfigParserGetIntValue(root, "hello", NULL), CONFIG_PARSER_PARAMETER_INCORRECT); // value is null
    EXPECT_EQ(ConfigParserGetIntValue(root, "cluster_server.log_level", &num), CONFIG_PARSER_WRONG_DATA_TYPE);
    EXPECT_EQ(ConfigParserGetIntValue(root, "???#$%^%&*(Y&*Y(&*wrong path#%^$^&*^&*(???", &num),
              CONFIG_PARSER_PARSE_PATH_FAILED);

    // setter
    EXPECT_EQ(ConfigParserSetIntValue(root, "hello", NULL), ERROR_SYS_OK);
    ConfigParserGetIntValue(root, "hello", &num);
    EXPECT_EQ(num, 0);
    EXPECT_EQ(ConfigParserSetIntValue(root, "cluster_server.log_level", 1.54891), ERROR_SYS_OK);
    ConfigParserGetIntValue(root, "cluster_server.log_level", &num);
    EXPECT_EQ(num, 1);
    EXPECT_EQ(ConfigParserSetIntValue(root, "???#$%^%&*(Y&*Y(&*wrong path#%^$^&*^&*(???", -10), ERROR_SYS_OK);
    ConfigParserGetIntValue(root, "???#$%^%&*(Y&*Y(&*wrong path#%^$^&*^&*(???", &num);
    EXPECT_EQ(num, -10);

    // set exception
    EXPECT_EQ(ConfigParserSetIntValue(NULL, "cluster_server", 123), CONFIG_PARSER_HANDLE_IS_NULL);
    EXPECT_EQ(ConfigParserSetIntValue(root, "", 456), CONFIG_PARSER_CREATE_PATH_FAILED);
    EXPECT_EQ(ConfigParserSetIntValue(root, NULL, 789), CONFIG_PARSER_PARAMETER_INCORRECT);

    ConfigParserDelete(root, "");
}

/*
 * int32 and uint32 promise support.
 * int64 and uint64 is not promising, need to use double to handle.
 */
TEST_F(ConfigParserTest, TestIntegerSize)
{
    ConfigParserHandle *root = NULL;
    ConfigParserLoadFile(m_strJsonTest.c_str(), &root);
    int32_t num32;
    EXPECT_EQ(ConfigParserSetIntValue(root, "tmp.test.int32_MAX", INT_MAX), ERROR_SYS_OK);
    EXPECT_EQ(ConfigParserGetIntValue(root, "tmp.test.int32_MAX", &num32), ERROR_SYS_OK);
    EXPECT_EQ(num32, INT_MAX); // int_max

    EXPECT_EQ(ConfigParserSetIntValue(root, "tmp.test.int32_MIN", INT_MIN), ERROR_SYS_OK);
    EXPECT_EQ(ConfigParserGetIntValue(root, "tmp.test.int32_MIN", &num32), ERROR_SYS_OK);
    EXPECT_EQ(num32, INT_MIN); // int_min

    EXPECT_EQ(ConfigParserSetIntValue(root, "tmp.test.int32_MAX", INT_MAX + 1), ERROR_SYS_OK);
    EXPECT_EQ(ConfigParserGetIntValue(root, "tmp.test.int32_MAX", &num32), ERROR_SYS_OK);
    EXPECT_LT(num32, INT_MAX); // over int32 maximum, then be minimum

    EXPECT_EQ(ConfigParserSetIntValue(root, "tmp.test.int32_MAX", INT_MIN - 1), ERROR_SYS_OK);
    EXPECT_EQ(ConfigParserGetIntValue(root, "tmp.test.int32_MAX", &num32), ERROR_SYS_OK);
    EXPECT_GT(num32, INT_MIN); // over int32 minimum, then be maximum

    EXPECT_EQ(ConfigParserSetIntValue(root, "tmp.test.int32_MAX", UINT_MAX), ERROR_SYS_OK);
    EXPECT_EQ(ConfigParserGetIntValue(root, "tmp.test.int32_MAX", &num32), ERROR_SYS_OK);
    EXPECT_EQ((uint32_t)num32, UINT_MAX); // uint_max

    EXPECT_EQ(ConfigParserSetIntValue(root, "tmp.test.int32_MAX", UINT_MAX + 1), ERROR_SYS_OK);
    EXPECT_EQ(ConfigParserGetIntValue(root, "tmp.test.int32_MAX", &num32), ERROR_SYS_OK);
    EXPECT_LT((uint32_t)num32, UINT_MAX); // over u_int32 maximum, then be 0

    int64_t num53_MAX = 2l << 53;
    int64_t num53_MIN = -(2l << 53);
    double num64;
    EXPECT_EQ(ConfigParserSetDoubleValue(root, "tmp.test.int53_MAX", num53_MAX), ERROR_SYS_OK);
    EXPECT_EQ(ConfigParserGetDoubleValue(root, "tmp.test.int53_MAX", &num64), ERROR_SYS_OK);
    EXPECT_EQ((int64_t)num64, num53_MAX); // 53_max

    EXPECT_EQ(ConfigParserSetDoubleValue(root, "tmp.test.int53_MIN", num53_MIN), ERROR_SYS_OK);
    EXPECT_EQ(ConfigParserGetDoubleValue(root, "tmp.test.int53_MIN", &num64), ERROR_SYS_OK);
    EXPECT_EQ((int64_t)num64, num53_MIN); // 53_min

    EXPECT_EQ(ConfigParserSetDoubleValue(root, "tmp.test.int53_MAX", num53_MAX + 1), ERROR_SYS_OK);
    EXPECT_EQ(ConfigParserGetDoubleValue(root, "tmp.test.int53_MAX", &num64), ERROR_SYS_OK);
    EXPECT_NE((int64_t)num64, num53_MAX + 1); // over int53 maximum, not same

    EXPECT_EQ(ConfigParserSetDoubleValue(root, "tmp.test.int53_MAX", num53_MIN - 1), ERROR_SYS_OK);
    EXPECT_EQ(ConfigParserGetDoubleValue(root, "tmp.test.int53_MAX", &num64), ERROR_SYS_OK);
    EXPECT_NE((int64_t)num64, num53_MIN - 1); // over int53 minimum, not same

    double dou;
    EXPECT_EQ(ConfigParserSetDoubleValue(root, "tmp.test.int64_MAX", LONG_MAX), ERROR_SYS_OK);
    EXPECT_EQ(ConfigParserGetDoubleValue(root, "tmp.test.int64_MAX", &dou), ERROR_SYS_OK);
    EXPECT_EQ(dou, LONG_MAX); // int_max

    EXPECT_EQ(ConfigParserSetDoubleValue(root, "tmp.test.int64_MIN", LONG_MIN), ERROR_SYS_OK);
    EXPECT_EQ(ConfigParserGetDoubleValue(root, "tmp.test.int64_MIN", &dou), ERROR_SYS_OK);
    EXPECT_EQ(dou, LONG_MIN); // int_min

    EXPECT_EQ(ConfigParserSetDoubleValue(root, "tmp.test.int64_MAX", LONG_MAX + 1), ERROR_SYS_OK);
    EXPECT_EQ(ConfigParserGetDoubleValue(root, "tmp.test.int64_MAX", &dou), ERROR_SYS_OK);
    EXPECT_LT(dou, LONG_MAX); // over int32 maximum, then be minimum

    EXPECT_EQ(ConfigParserSetDoubleValue(root, "tmp.test.int64_MAX", LONG_MIN - 1), ERROR_SYS_OK);
    EXPECT_EQ(ConfigParserGetDoubleValue(root, "tmp.test.int64_MAX", &dou), ERROR_SYS_OK);
    EXPECT_GT(dou, LONG_MIN); // over int32 minimum, then be maximum

    EXPECT_EQ(ConfigParserSetDoubleValue(root, "tmp.test.int64_MAX", ULONG_MAX), ERROR_SYS_OK);
    EXPECT_EQ(ConfigParserGetDoubleValue(root, "tmp.test.int64_MAX", &dou), ERROR_SYS_OK);
    EXPECT_EQ(dou, ULONG_MAX); // uint_max

    EXPECT_EQ(ConfigParserSetDoubleValue(root, "tmp.test.int64_MAX", ULONG_MAX + 1), ERROR_SYS_OK);
    EXPECT_EQ(ConfigParserGetDoubleValue(root, "tmp.test.int64_MAX", &dou), ERROR_SYS_OK);
    EXPECT_LT(dou, ULONG_MAX); // over u_int32 maximum, then be 0

    ConfigParserDelete(root, "");
}

TEST_F(ConfigParserTest, TestDouble)
{
    ConfigParserHandle *root = NULL;
    ConfigParserLoadFile(m_strJsonTest.c_str(), &root);
    // getter
    double num;
    ConfigParserGetDoubleValue(root, "cluster_server.node_info.0.heartbeat_time", &num);
    EXPECT_EQ(num, 120);
    ConfigParserSetDoubleValue(root, "cluster_server.node_info.1.ip_lists.0.port.1", 48615.1856156);
    ConfigParserGetDoubleValue(root, "cluster_server.node_info.1.ip_lists.0.port.1", &num);
    EXPECT_EQ(num, 48615.1856156);
    ConfigParserSetDoubleValue(root, "test.hello", -0016548.16548);
    ConfigParserGetDoubleValue(root, "test.hello", &num);
    EXPECT_EQ(num, -0016548.16548);

    // get exception
    EXPECT_EQ(ConfigParserGetDoubleValue(NULL, "cluster_server", &num), CONFIG_PARSER_HANDLE_IS_NULL);
    EXPECT_EQ(ConfigParserGetDoubleValue(root, NULL, &num), CONFIG_PARSER_PARAMETER_INCORRECT);
    EXPECT_EQ(ConfigParserGetDoubleValue(root, "hello", NULL), CONFIG_PARSER_PARAMETER_INCORRECT); // value is null
    EXPECT_EQ(ConfigParserGetDoubleValue(root, "cluster_server.log_level", &num), CONFIG_PARSER_WRONG_DATA_TYPE);
    EXPECT_EQ(ConfigParserGetDoubleValue(root, "???#$%^%&*(Y&*Y(&*wrong path#%^$^&*^&*(???", &num),
              CONFIG_PARSER_PARSE_PATH_FAILED);

    // setter
    EXPECT_EQ(ConfigParserSetDoubleValue(root, "hello", NULL), ERROR_SYS_OK); // NULL = 0
    EXPECT_EQ(ConfigParserGetDoubleValue(root, "hello", &num), ERROR_SYS_OK);
    EXPECT_EQ(ConfigParserSetDoubleValue(root, "cluster_server.log_level", 1.54891), ERROR_SYS_OK);
    ConfigParserGetDoubleValue(root, "cluster_server.log_level", &num);
    EXPECT_EQ(num, 1.54891);
    EXPECT_EQ(ConfigParserSetDoubleValue(root, "???#$%^%&*(Y&*Y(&*wrong path#%^$^&*^&*(???", -0.0000005), ERROR_SYS_OK);
    ConfigParserGetDoubleValue(root, "???#$%^%&*(Y&*Y(&*wrong path#%^$^&*^&*(???", &num);
    EXPECT_EQ(num, -0.0000005);

    // set exception
    EXPECT_EQ(ConfigParserSetDoubleValue(NULL, "cluster_server", 123), CONFIG_PARSER_HANDLE_IS_NULL);
    EXPECT_EQ(ConfigParserSetDoubleValue(root, "", 456), CONFIG_PARSER_CREATE_PATH_FAILED);
    EXPECT_EQ(ConfigParserSetDoubleValue(root, NULL, 789), CONFIG_PARSER_PARAMETER_INCORRECT);

    ConfigParserDelete(root, "");
}

TEST_F(ConfigParserTest, TestBoolean)
{
    ConfigParserHandle *root = NULL;
    // getter
    bool check;
    string strJsontestWithinArray = m_strJsonDir + "testWithinArray.json";
    ConfigParserLoadFile(strJsontestWithinArray.c_str(), &root);
    ConfigParserGetBoolValue(root, "0.cluster_server.node_info.0.node_id", &check);
    EXPECT_FALSE(check);
    ConfigParserSetBoolValue(root, "0.cluster_server.node_info.1.ip_lists.0.port.1", true);
    ConfigParserGetBoolValue(root, "0.cluster_server.node_info.1.ip_lists.0.port.1", &check);
    EXPECT_TRUE(check);
    ConfigParserSetBoolValue(root, "1.test.hello", false);
    ConfigParserGetBoolValue(root, "1.test.hello", &check);
    EXPECT_FALSE(check);

    // get exception
    EXPECT_EQ(ConfigParserGetBoolValue(NULL, "cluster_server", &check), CONFIG_PARSER_HANDLE_IS_NULL);
    EXPECT_EQ(ConfigParserGetBoolValue(root, NULL, &check), CONFIG_PARSER_PARAMETER_INCORRECT);
    EXPECT_EQ(ConfigParserGetBoolValue(root, "hello", NULL), CONFIG_PARSER_PARAMETER_INCORRECT); // value is null
    EXPECT_EQ(ConfigParserGetBoolValue(root, "0.cluster_server.log_level", &check), CONFIG_PARSER_WRONG_DATA_TYPE);
    EXPECT_EQ(ConfigParserGetBoolValue(root, "???#$%^%&*(Y&*Y(&*wrong path#%^$^&*^&*(???", &check),
              CONFIG_PARSER_PARSE_PATH_FAILED);

    // setter
    EXPECT_EQ(ConfigParserSetBoolValue(root, "0.hello", NULL), ERROR_SYS_OK);
    ConfigParserGetBoolValue(root, "0.hello", &check);
    EXPECT_EQ(check, 0); // NULL = 0
    EXPECT_EQ(ConfigParserSetBoolValue(root, "0.cluster_server.log_level", true), ERROR_SYS_OK);
    ConfigParserGetBoolValue(root, "0.cluster_server.log_level", &check);
    EXPECT_TRUE(check);
    EXPECT_EQ(ConfigParserSetBoolValue(root, "1.???#$%^%&*(Y&*Y(&*wrong path#%^$^&*^&*(???", false), ERROR_SYS_OK);
    ConfigParserGetBoolValue(root, "1.???#$%^%&*(Y&*Y(&*wrong path#%^$^&*^&*(???", &check);
    EXPECT_FALSE(check);
    EXPECT_EQ(ConfigParserSetBoolValue(root, "0.num", 789), ERROR_SYS_OK); // bool is int, convert to bool
    EXPECT_EQ(ConfigParserSetBoolValue(root, "0.bignum", 123456789123456789), ERROR_SYS_OK);

    // set exception
    EXPECT_EQ(ConfigParserSetBoolValue(NULL, "cluster_server", true), CONFIG_PARSER_HANDLE_IS_NULL);
    EXPECT_EQ(ConfigParserSetBoolValue(root, "cluster_server", true), CONFIG_PARSER_CREATE_PATH_FAILED);
    EXPECT_EQ(ConfigParserSetBoolValue(root, "", false), CONFIG_PARSER_CREATE_PATH_FAILED);
    EXPECT_EQ(ConfigParserSetBoolValue(root, NULL, 789), CONFIG_PARSER_PARAMETER_INCORRECT);

    ConfigParserDelete(root, "");
}

TEST_F(ConfigParserTest, TestNull)
{
    ConfigParserHandle *root = NULL;
    ConfigParserLoadFile(m_strJsonTest.c_str(), &root);
    // getter
    bool check = false;
    ConfigParserDelete(root, "");
    string strJsontestWithinArray = m_strJsonDir + "testWithinArray.json";
    ConfigParserLoadFile(strJsontestWithinArray.c_str(), &root);
    ConfigParserGetNullValue(root, "0.cluster_server.node_info.1.node_type", &check);
    EXPECT_TRUE(check);
    check = false;
    ConfigParserSetNullValue(root, "0.cluster_server.node_info.1.ip_lists.0.port.0");
    ConfigParserGetNullValue(root, "0.cluster_server.node_info.1.ip_lists.0.port.0", &check);
    EXPECT_TRUE(check);

    // get exception
    EXPECT_EQ(ConfigParserGetNullValue(NULL, "cluster_server", &check), CONFIG_PARSER_HANDLE_IS_NULL);
    EXPECT_EQ(ConfigParserGetNullValue(root, NULL, &check), CONFIG_PARSER_PARAMETER_INCORRECT);
    EXPECT_EQ(ConfigParserGetNullValue(root, "hello", NULL), CONFIG_PARSER_PARAMETER_INCORRECT); // value is null
    EXPECT_EQ(ConfigParserGetNullValue(root, "???#$%^%&*(Y&*Y(&*wrong path#%^$^&*^&*(???", &check),
              CONFIG_PARSER_PARSE_PATH_FAILED);

    // set exception
    EXPECT_EQ(ConfigParserSetNullValue(NULL, "cluster_server"), CONFIG_PARSER_HANDLE_IS_NULL);
    EXPECT_EQ(ConfigParserSetNullValue(root, "cluster_server"), CONFIG_PARSER_CREATE_PATH_FAILED);
    EXPECT_EQ(ConfigParserSetNullValue(root, ""), CONFIG_PARSER_CREATE_PATH_FAILED);
    EXPECT_EQ(ConfigParserSetNullValue(root, NULL), CONFIG_PARSER_PARAMETER_INCORRECT);

    ConfigParserDelete(root, "");
}

TEST_F(ConfigParserTest, TestHandle)
{
    ConfigParserHandle *root = NULL;
    ConfigParserLoadFile(m_strJsonTest.c_str(), &root);
    // getter
    ConfigParserHandle *handle;
    char *str;
    ConfigParserGetHandle(root, "cluster_server.node_info.1.node_type", &handle);
    ConfigParserGetStrValue(handle, "", &str);
    EXPECT_STREQ(str, "data");

    // get exception
    EXPECT_EQ(ConfigParserGetHandle(NULL, "cluster_server", &handle), CONFIG_PARSER_HANDLE_IS_NULL);
    EXPECT_EQ(ConfigParserGetHandle(root, NULL, &handle), CONFIG_PARSER_PARAMETER_INCORRECT);
    EXPECT_EQ(ConfigParserGetHandle(root, "hello", NULL), CONFIG_PARSER_PARAMETER_INCORRECT); // value is null
    EXPECT_EQ(ConfigParserGetHandle(root, "???#$%^%&*(Y&*Y(&*wrong path#%^$^&*^&*(???", &handle),
              CONFIG_PARSER_PARSE_PATH_FAILED);

    // setter
    ConfigParserCreateArrayHandle(&handle);
    ConfigParserSetStrValue(handle, "1", "hi");
    ConfigParserSetHandle(root, "clussss", handle);
    ConfigParserGetStrValue(root, "clussss.1", &str);
    EXPECT_STREQ(str, "hi");

    // set exception
    EXPECT_EQ(ConfigParserSetHandle(NULL, "cluster_server", handle), CONFIG_PARSER_HANDLE_IS_NULL);
    EXPECT_EQ(ConfigParserSetHandle(root, "cluster_server", NULL), CONFIG_PARSER_PARAMETER_INCORRECT);
    EXPECT_EQ(ConfigParserSetHandle(root, "", handle), CONFIG_PARSER_CREATE_PATH_FAILED);
    EXPECT_EQ(ConfigParserSetHandle(root, NULL, handle), CONFIG_PARSER_PARAMETER_INCORRECT);

    ConfigParserDelete(root, "");
}

TEST_F(ConfigParserTest, TestCreateAndDeleteOK)
{
    ConfigParserHandle *root = NULL;
    ConfigParserLoadFile(m_strJsonTest.c_str(), &root);
    // set array
    ConfigParserHandle *array;
    ConfigParserCreateArrayHandle(&array);
    ConfigParserSetStrValue(array, "1", "hi");
    ConfigParserSetIntValue(array, "2", 10);
    ConfigParserSetBoolValue(array, "3", false);
    ConfigParserSetDoubleValue(array, "4", 2.5);
    uint32_t size = 0;
    ConfigParserGetArraySize(array, &size);
    EXPECT_EQ(size, 5);
    // set object
    ConfigParserHandle *object;
    ConfigParserCreateObjectHandle(&object);
    ConfigParserSetStrValue(object, "?", "hi");
    ConfigParserSetIntValue(object, "??", 10);
    ConfigParserSetBoolValue(object, "???", false);
    EXPECT_EQ(ConfigParserSetDoubleValue(object, "?", 2.5), ERROR_SYS_OK);
    ConfigParserSetHandle(object, "a.a.r.r.y", array);

    double num;
    ConfigParserGetDoubleValue(object, "a.a.r.r.y.4", &num);
    EXPECT_EQ(num, 2.5);

    ConfigParserDelete(object, "a.a");
    EXPECT_EQ(ConfigParserGetDoubleValue(object, "a.a.r.r.y.4", &num), CONFIG_PARSER_PARSE_PATH_FAILED);
    ConfigParserGetDoubleValue(object, "?", &num);
    EXPECT_EQ(num, 2.5);

    ConfigParserDelete(object, "");
    ConfigParserDelete(root, "cluster_server.node_info.1");
    char *str = NULL;
    EXPECT_EQ(ConfigParserDelete(root, "cluster_server"), ERROR_SYS_OK);
    // delete successfully then can't get
    EXPECT_EQ(ConfigParserGetStrValue(root, "cluster_server.node_info.1.node_id", &str), CONFIG_PARSER_PARSE_PATH_FAILED);
    EXPECT_EQ(ConfigParserDelete(root, "cccluster_server.1.node_id"), CONFIG_PARSER_PARSE_PATH_FAILED);

    ConfigParserDelete(root, "");
}

TEST_F(ConfigParserTest, TestCreateAndDeleteBad)
{
    EXPECT_EQ(ConfigParserCreateArrayHandle(NULL), CONFIG_PARSER_PARAMETER_INCORRECT);
    EXPECT_EQ(ConfigParserCreateObjectHandle(NULL), CONFIG_PARSER_PARAMETER_INCORRECT);
    ConfigParserHandle *object;
    EXPECT_EQ(ConfigParserDelete(NULL, "a.a"), CONFIG_PARSER_HANDLE_IS_NULL);
    EXPECT_EQ(ConfigParserDelete(object, NULL), CONFIG_PARSER_PARAMETER_INCORRECT);
}

TEST_F(ConfigParserTest, TestArrayOps) {
    ConfigParserHandle *array;
    ConfigParserCreateArrayHandle(&array);

    ConfigParserSetIntValue(array, "15", 15);
    uint32_t size = 0;
    EXPECT_EQ(ConfigParserGetArraySize(array, &size), ERROR_SYS_OK);
    EXPECT_EQ(size, 16);
    int num;
    ConfigParserGetIntValue(array, "15", &num);
    EXPECT_EQ(num, 15);

    bool check;
    ConfigParserGetNullValue(array, "7", &check);
    EXPECT_TRUE(check); // fill with null in middle

    ConfigParserDelete(array, "");
}

/*
 * @tc.desc:  Test get array size with invalid parameters
 */
TEST_F(ConfigParserTest, TestGetArraySizeWhenParamIsInvalidThenFail) {
    /**
     * @tc.steps: step1. Test NULL pointer of parameters
     * @tc.expected: step1.The function return error
     */
    ConfigParserHandle *array = (ConfigParserHandle *)0x1234;
    uint32_t size = 0;
    EXPECT_EQ(ConfigParserGetArraySize(NULL, &size), CONFIG_PARSER_HANDLE_IS_NULL);
    EXPECT_EQ(ConfigParserGetArraySize(array, NULL), CONFIG_PARSER_PARAMETER_INCORRECT);

    /**
     * @tc.steps: step2. Test get array size with an object handle(not an array handle)
     * @tc.expected: step2.The functions return success
     */
    ConfigParserCreateObjectHandle(&array);
    EXPECT_EQ(ConfigParserGetArraySize(array, &size), ERROR_SYS_OK);
    EXPECT_EQ(size, 0);
    ConfigParserDelete(array, "");
}

TEST_F(ConfigParserTest, TestHandleLoop)
{
    ConfigParserHandle *root = NULL;
    ConfigParserLoadFile(m_strJsonTest.c_str(), &root);

    ConfigParserHandle *handle = NULL;
    ConfigParserGetHandle(root, "cluster_server.node_info.1.ip_lists.0.port", &handle);
    ConfigParserHandle *ele = NULL;
    int32_t nums[] = {5550, 5551, 5552, 5555};
    int32_t i = 0;
    CONFIGPARSER_HANDLE_FOR_EACH(ele, handle) {
        int32_t num = -1;
        ConfigParserGetIntValue(ele, "", &num); // using empty to represent itself is ok
        EXPECT_EQ(num, nums[i]);
        i++;
    }
    ConfigParserGetHandle(root, "cluster_server.node_info.0.ip_lists", &handle);
    char *strs[] = {"192.168.0.20", "192.168.0.100"};
    i = 0;
    CONFIGPARSER_HANDLE_FOR_EACH(ele, handle) {
        char *str = NULL;
        ConfigParserGetStrValue(ele, ".", &str); // using dot to represent itself is ok
        EXPECT_STREQ(str, strs[i]);
        i++;
    }

    ConfigParserDelete(root, "");
}

TEST_F(ConfigParserTest, TestPathLengthExceedLimit)
{
    ConfigParserHandle *root = NULL;
    ConfigParserLoadFile(m_strJsonTest.c_str(), &root);
    int32_t num;
    // 2048 length for path is OK
    char path[CONFIGPARSER_PATH_MAXIMUM];
    for (int i = 0; i < CONFIGPARSER_PATH_MAXIMUM; i++) {
        path[i] = 'a';
    }
    path[CONFIGPARSER_PATH_MAXIMUM - 1] = '\0';
    EXPECT_EQ(ConfigParserSetIntValue(root, path, num), ERROR_SYS_OK);
    EXPECT_EQ(ConfigParserDelete(root, path), ERROR_SYS_OK);

    // 2049 length for path is NOT OK
    char longPath[CONFIGPARSER_PATH_MAXIMUM + 1];
    for (int i = 0; i < CONFIGPARSER_PATH_MAXIMUM; i++) {
        longPath[i] = 'a';
    }
    longPath[CONFIGPARSER_PATH_MAXIMUM] = '\0';
    EXPECT_EQ(ConfigParserGetIntValue(root, longPath, &num), CONFIG_PARSER_ALLOCATOR_FAILED);
    EXPECT_EQ(ConfigParserSetIntValue(root, longPath, num), CONFIG_PARSER_ALLOCATOR_FAILED);
    EXPECT_EQ(ConfigParserDelete(root, longPath), CONFIG_PARSER_ALLOCATOR_FAILED);

    ConfigParserDelete(root, "");
}

TEST_F(ConfigParserTest, TestSetOuterObjectToArrayNotAllow)
{
    ConfigParserHandle *root = NULL;
    ConfigParserLoadFile(m_strJsonTest.c_str(), &root);
    // change the outer data structure from object to array
    EXPECT_EQ(ConfigParserSetIntValue(root, "0", 999), CONFIG_PARSER_CREATE_PATH_FAILED);
    int32_t num = -1;
    EXPECT_EQ(ConfigParserGetIntValue(root, "0", &num), CONFIG_PARSER_PARSE_PATH_FAILED);
    EXPECT_EQ(num, -1);
    ConfigParserDelete(root, "");
}

TEST_F(ConfigParserTest, TestSetOuterArrayToObjectNotAllow)
{
    ConfigParserHandle *root = NULL;
    string strJsontestWithinArray = m_strJsonDir + "testWithinArray.json";
    ConfigParserLoadFile(strJsontestWithinArray.c_str(), &root);
    // change the outer data structure from array to object
    EXPECT_EQ(ConfigParserSetIntValue(root, "cluster", 999), CONFIG_PARSER_CREATE_PATH_FAILED);
    int32_t num = -1;
    EXPECT_EQ(ConfigParserGetIntValue(root, "cluster", &num), CONFIG_PARSER_PARSE_PATH_FAILED);
    EXPECT_EQ(num, -1);
    ConfigParserDelete(root, "");
}

static uint32_t g_updatedSize = 0;
int UpdatedSizeCallback(char *[], uint32_t size)
{
    g_updatedSize = size;
    return 0;
}
TEST_F(ConfigParserTest, TestConfigParserLoadString)
{
    ConfigParserHandle *handle = nullptr;
    EXPECT_NE(ConfigParserLoadString(nullptr, &handle), ERROR_SYS_OK);
    EXPECT_NE(ConfigParserLoadString("invalid json", &handle), ERROR_SYS_OK);

    std::ifstream file(m_strJsonTest.c_str());
    std::stringstream buffer;
    buffer << file.rdbuf();
    file.close();
    std::string content = buffer.str();
    EXPECT_EQ(ConfigParserLoadString(content.c_str(), &handle), ERROR_SYS_OK);
    EXPECT_NE(handle, nullptr);
    ConfigParserHandle *newHandle = nullptr;
    g_updatedSize = 1;
    ConfigParserReloadFile(m_strJsonTest.c_str(), handle, &newHandle, UpdatedSizeCallback);
    ConfigParserDelete(handle, "");
    ConfigParserDelete(newHandle, "");
    EXPECT_EQ(g_updatedSize, 0);
}

TEST_F(ConfigParserTest, TestConfigParserDeepCopy)
{
    ConfigParserHandle *handle0 = nullptr;
    EXPECT_NE(ConfigParserDeepCopy(nullptr, &handle0), ERROR_SYS_OK);
    EXPECT_EQ(handle0, nullptr);

    ConfigParserHandle *handle1 = nullptr;
    ConfigParserLoadFile(m_strJsonTest.c_str(), &handle1);
    ConfigParserHandle *handle2 = nullptr;
    EXPECT_EQ(ConfigParserDeepCopy(handle1, &handle2), ERROR_SYS_OK);
    ConfigParserDelete(handle1, "");
    EXPECT_NE(handle2, nullptr);

    ConfigParserHandle *handle3 = nullptr;
    g_updatedSize = 1;
    ConfigParserReloadFile(m_strJsonTest.c_str(), handle2, &handle3, UpdatedSizeCallback);
    ConfigParserDelete(handle2, "");
    ConfigParserDelete(handle3, "");
    EXPECT_EQ(g_updatedSize, 0);
}

TEST_F(ConfigParserTest, TestConfigParserHandleGetName)
{
    char *name = nullptr;
    EXPECT_NE(ConfigParserHandleGetName(nullptr, &name), ERROR_SYS_OK);

    ConfigParserHandle *handle = nullptr;
    ConfigParserLoadFile(m_strJsonTest.c_str(), &handle);
    ConfigParserHandle *subHandle = nullptr;
    ConfigParserGetHandle(handle, "cluster_server.log_level", &subHandle);
    EXPECT_EQ(ConfigParserHandleGetName(subHandle, &name), ERROR_SYS_OK);
    EXPECT_STREQ(name, "log_level");
    ConfigParserDelete(handle, "");
}