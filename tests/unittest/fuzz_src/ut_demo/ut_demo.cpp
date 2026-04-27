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
#include <stdio.h>
#include <string.h>
#include <stdlib.h>  ///

#include "ut_demo/ut_demo.h"

#define MIN(_a, _b) ((_a) > (_b) ? (_b) : (_a))
/******************************************

两个原有单元测试例,这里上下文简单，
你需要写完整的上下文，以便被测函数被正确带调用，
拥有比较好的初始覆盖率

******************************************/

GUNIT_TEST_REGISTRATION(fuzz_test, test_start)

void fuzz_test::SetUp()
{}

void fuzz_test::TearDown()
{}

extern "C" {
void testcasea()
{
    int ret = 0;

    ret = funtesta(1234, "123");
    if (ret != 2)
        printf("call fun001 error\r\n");
}

void testcaseb()
{
    int ret = 0;
    u8 temp_ipv4[4] = {192, 168, 0, 1};

    ret = funtestb(1234, (char*)temp_ipv4);
    if (ret != 2)
        printf("call fun002 error\r\n");
}

/******************************************

改造后的测试例,ok

******************************************/

void testcasea_Fuzz()
{
    printf("start ---- testcasea_Fuzz\n");
    DT_FUZZ_START(0, 1000000, "testcasea_Fuzz", 0)
    {
        printf("\r%d", fuzz_seed + fuzz_i);
        int ret = 0;

        ret = funtesta(
            *(s32*)DT_SetGetS32(&g_Element[0], 1234), DT_SetGetString(&g_Element[1], strlen("123") + 1, 18, "123"));

        ret = ret + 1;  //去告警
    }
    DT_FUZZ_END()
    printf("end ---- testcasea_Fuzz\n");
}

void testcaseb_Fuzz()
{
    printf("start ---- testcaseb_Fuzz\n");
    DT_FUZZ_START(0, 1000000, "testcaseb_Fuzz", 0)
    {
        printf("\r%d", fuzz_seed + fuzz_i);

        int ret = 0;
        u8 temp_ipv4[4] = {192, 168, 0, 1};

        ret = funtestb(*(s32*)DT_SetGetS32(&g_Element[0], 1234), DT_SetGetIpv4(&g_Element[1], (char*)temp_ipv4));

        ret = ret + 1;  //去告警
    }
    DT_FUZZ_END()
    printf("end ---- testcaseb_Fuzz\n");
}

void testcasebug_Fuzz()
{
    printf("start ---- testcasebug_Fuzz\n");
    DT_FUZZ_START(0, 1000000, "testcasebug_Fuzz", 0)
    {
        printf("\r%d", fuzz_seed + fuzz_i);

        int ret = 0;

        ret = funtestbug(
            *(s32*)DT_SetGetS32(&g_Element[0], 1234), DT_SetGetString(&g_Element[1], strlen("123") + 1, 18, "123"));

        ret = ret + 1;  //去告警
    }
    DT_FUZZ_END()
    printf("end ---- testcasebug_Fuzz\n");
}

//测试多参数结构化爬分支
void testMulpara(void)
{

    printf("start ---- testMulpara\n");
    int Number_table[] = {2, 4, 6, 1234, 0x1234, 0x897654, 0x456789};
    char* String_table[] = {"123", "abc", "zhangpeng", "1.1.1", "wanghao", "12345"};

    DT_FUZZ_START(0, 1000000, "testMulpara", 0)
    {
        printf("\r%d", fuzz_seed + fuzz_i);

        int NumberRange = *(s32*)DT_SetGetNumberRange(&g_Element[0], 0x1234, 0x1200, 0x1300);
        int NumberEnum = *(s32*)DT_SetGetNumberEnum(&g_Element[1], 0x1234, Number_table, 7);

        s32 a = *(s32*)DT_SetGetS32(&g_Element[2], 0x1234);

        char ccc[100];
        memset(ccc, 0, 100);
        char* c = DT_SetGetBlob(&g_Element[3], 15, 40, "12340000000000");
        memcpy(ccc, c, MIN(DT_GET_MutatedValueLen(&g_Element[3]), 100));
        ccc[99] = 0;

        char* StringEnum = DT_SetGetStringEnum(&g_Element[4], 6, 20, "1.1.1", String_table, 6);

        fun_multype(NumberRange, NumberEnum, a, ccc, StringEnum);
    }
    DT_FUZZ_END()

    clean_a();
    printf("end ---- testMulpara\n");
}

#include <signal.h>
//测试blob参数爬分支
void testOneBlob(void)
{

    printf("start ---- testOneBlob\n");
    DT_FUZZ_START(0, 10000000, "testOneBlob", 0)
    {
        printf("\r%d", fuzz_seed + fuzz_i);

        char* d = DT_SetGetFixBlob(&g_Element[0], 43, 43, "567800000000005678000000000056780000000000");
        d[42] = 0;

        s32 a = *(s32*)d;
        s8 b = *(s8*)(d + 4);
        char* b001 = d + 15;
        char* b002 = d + 25;

        fun_branch(a, b, b001, b002);
    }
    DT_FUZZ_END()

    clean_a();

    printf("end ---- testOneBlob\n");
}

void testFoundBug(void)
{

    printf("start ---- testFoundBug\n");

    DT_FUZZ_START(0, 200000, "testFoundBug", 0)
    {
        printf("\r%d", fuzz_seed + fuzz_i);

        s32 a = *(s32*)DT_SetGetS32(&g_Element[0], 0x1234);
        s32 b = *(s32*)DT_SetGetS32(&g_Element[1], 0x1234);
        char* String1 = DT_SetGetString(&g_Element[2], 6, 0, "1.1.1");
        char* String2 = DT_SetGetString(&g_Element[3], 6, 20, "1.1.1");

        fun_bug(a, b, String1, String2);
    }
    DT_FUZZ_END()

    clean_a();
    printf("end ---- testFoundBug\n");
}

void testRightRange(void)
{

    printf("start ---- testRightRange\n");
    int ret = 0;
    char* String_table[] = {"123", "abc", "zhangpeng", "1.1.1", "wanghao", "12345"};

    DT_FUZZ_START(0, 100000000, "./examples/corpus/testRightRange", 0)
    {
        printf("\r%d", fuzz_seed + fuzz_i);

        int NumberRange = *(s32*)DT_SetGetNumberRange(&g_Element[0], 0x1222, 0x1200, 0x1300);
        char* StringEnum = DT_SetGetStringEnum(&g_Element[1], 6, 20, "1.1.1", String_table, 6);

        ret = fun_RightRange(NumberRange, StringEnum);
        if (ret != 0) {
            DT_Show_Cur_Corpus();
            break;
        }
    }
    DT_FUZZ_END()

    clean_a();
    printf("end ---- testRightRange\n");
}

void testStruct1(void)
{

    printf("start ---- testStruct1\n");
    TS_TEST2 test = {0};

    DT_FUZZ_START(0, 1000000, "testStruct1", 0)
    {
        printf("\r%d", fuzz_seed + fuzz_i);

        s32 a = *(s32*)DT_SetGetS32(&g_Element[0], 0x1234);

        memcpy(test.name, DT_SetGetFixBlob(&g_Element[1], 16, 16, "abcdef012345678"), 16);
        test.name[15] = 0;
        test.value = *(s32*)DT_SetGetS32(&g_Element[2], 80);
        test.value1 = *(s32*)DT_SetGetS32(&g_Element[3], 80);

        char* String1 = DT_SetGetString(&g_Element[4], 6, 20, "1.1.1");

        funtestc(a, &test, String1);
    }
    DT_FUZZ_END()

    clean_a();

    printf("end ---- testStruct1\n");
}

void testStruct2(void)
{

    printf("start ---- testStruct2\n");
    TS_TEST2 test = {0};

    DT_FUZZ_START(0, 1000000, "testStruct2", 0)
    {
        printf("\r%d", fuzz_seed + fuzz_i);

        s32 a = *(s32*)DT_SetGetS32(&g_Element[0], 0x1234);

        memcpy(test.name, "abcdef012345678", 16);
        test.value = 80;
        test.value1 = 80;

        TS_TEST2* temp_test =
            (TS_TEST2*)DT_SetGetFixBlob(&g_Element[1], sizeof(TS_TEST2), sizeof(TS_TEST2), (char*)&test);
        temp_test->name[15] = 0;

        char* String1 = DT_SetGetString(&g_Element[2], 6, 20, "1.1.1");

        funtestc(a, temp_test, String1);
    }
    DT_FUZZ_END()

    clean_a();

    printf("end ---- testStruct2\n");
}

//测试libfuzz原有测试例爬分支能力
//测试多测试例串行运行

//////////////////////////////////////////////////
void testLibfuzzTestCase(void)
{

    printf("start ---- test1\n");
    DT_FUZZ_START(0, 100000, "testLibfuzz1", 0)
    {
        int ret = 0;
        printf("\r%d", fuzz_seed + fuzz_i);

        char ddd[100];
        char* d = DT_SetGetBlob(&g_Element[0], 15, 100, "56780000000000");
        memcpy(ddd, d, MIN(DT_GET_MutatedValueLen(&g_Element[0]), 100));
        ddd[99] = 0;

        ret = LLVMFuzzerTestOneInput1((uint8_t*)ddd, MIN(DT_GET_MutatedValueLen(&g_Element[0]), 100));

        ret = ret;
        // if(ret)
        // break;
    }
    DT_FUZZ_END()
    printf("end ---- test1\n");

    printf("start ---- test2\n");
    DT_FUZZ_START(0, 100000, "testLibfuzz2", 0)
    {
        int ret = 0;
        printf("\r%d", fuzz_seed + fuzz_i);

        char ddd[100];
        char* d = DT_SetGetBlob(&g_Element[0], 15, 100, "56780000000000");
        memcpy(ddd, d, MIN(DT_GET_MutatedValueLen(&g_Element[0]), 100));
        ddd[99] = 0;

        ret = LLVMFuzzerTestOneInput2((uint8_t*)ddd, MIN(DT_GET_MutatedValueLen(&g_Element[0]), 100));

        ret = ret;
        // if(ret)
        // break;
    }
    DT_FUZZ_END()
    printf("end ---- test2\n");

    printf("start ---- test3\n");
    DT_FUZZ_START(0, 100000, "testLibfuzz3", 0)
    {
        int ret = 0;
        printf("\r%d", fuzz_seed + fuzz_i);

        char ddd[100];
        char* d = DT_SetGetBlob(&g_Element[0], 15, 100, "56780000000000");
        memcpy(ddd, d, MIN(DT_GET_MutatedValueLen(&g_Element[0]), 100));
        ddd[99] = 0;

        ret = LLVMFuzzerTestOneInput3((uint8_t*)ddd, MIN(DT_GET_MutatedValueLen(&g_Element[0]), 100));

        ret = ret;
        // if(ret)
        // break;
    }
    DT_FUZZ_END()
    printf("end ---- test3\n");
}
#ifndef _WIN32
#include <sys/ioctl.h>
#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>

//测试linux系统调用
void test_LinuxSysCall(void)
{

    printf("start ---- test_LinuxSysCall\n");
    DT_FUZZ_START(0, 1000000, "testLibfuzz1", 0)
    {
        printf("\r%d", fuzz_seed + fuzz_i);
        int fd = *(s32*)DT_SetGetS32(&g_Element[0], 0x1234);

        DT_Enable_TracePC(1);
        read(fd, NULL, 0);
        DT_Enable_TracePC(0);
    }
    DT_FUZZ_END()
    printf("end ---- test_LinuxSysCall\n");
}

#define example_IOC_MAGIC 'P'
#define example_IOCTL_METHOD_DATA _IOW(example_IOC_MAGIC, 3, char*)
#define example_IOCTL_METHOD_DATA_SIZE _IO(example_IOC_MAGIC, 4)

typedef struct {
    s32 tint;
    s8 tint1;
} K_TEST;

//测试linux驱动
void test_LinuxIoctl(void)
{

    printf("start ---- test_LinuxIoctl \n");
    int fd = open("/dev/target_kernel_device", 0, O_RDWR);

    DT_FUZZ_START(0, 100000, "testioctl", 0)
    {
        printf("\r%d", fuzz_seed + fuzz_i);

        int aaa = *(s32*)DT_SetGetS32(&g_Element[0], 1111);

        K_TEST bbb;
        bbb.tint = *(s32*)DT_SetGetS32(&g_Element[1], 1111);
        bbb.tint1 = *(s8*)DT_SetGetS8(&g_Element[2], 0);

        //测试驱动，搜集覆盖信息要最小化时间，避免干扰
        DT_Enable_TracePC(1);
        ioctl(fd, example_IOCTL_METHOD_DATA, &bbb);
        ioctl(fd, example_IOCTL_METHOD_DATA_SIZE, aaa);
        DT_Enable_TracePC(0);
    }
    DT_FUZZ_END()
    close(fd);

    printf("end ---- test_LinuxIoctl\n");
}
#endif

#if 0
int __stdio_common_vsprintf_s()
{

	return 0;
}
#endif

//测试blob参数爬循环分支
void testfor(void)
{

    printf("start ---- testfor\n");
    DT_FUZZ_START(0, 1000000, "testfor", 0)
    {
        printf("\r%d", fuzz_seed + fuzz_i);

        s32 a = *(s32*)DT_SetGetS32(&g_Element[0], 0x1234);
        // char *String1 = DT_SetGetString(&g_Element[1], 8, 0,"1.1.1.1" );
        char* String1 = "aaaa";

        fun_for(a, String1);
    }
    DT_FUZZ_END()

    clean_a();

    printf("end ---- testfor\n");
}

int fun_cycle(char* tchar);

void testcycle(void)
{

    printf("start ---- testcycle\n");
    DT_Enable_Support_Loop(1);

    DT_FUZZ_START(0, 1000000, "testcycle", 0)
    {
        printf("\r%d", fuzz_seed + fuzz_i);

        char* String1 = DT_SetGetString(&g_Element[0], 8, 0, "1.1.1.1");
        int len = DT_GET_MutatedValueLen(&g_Element[0]);

        if (len == 0)
            continue;

        fun_cycle(String1);
    }
    DT_FUZZ_END()

    clean_a();

    printf("end ---- testcycle\n");
}

void testFile(void)
{
    char* data;
    int len;

    ReadFromFile(&data, &len, "in.file");

    printf("start ---- testFile\n");
    DT_FUZZ_START(0, 1000000, "testFile", 0)
    {
        printf("\r%d", fuzz_seed + fuzz_i);

        char* dataout = DT_SetGetBlob(&g_Element[0], len, 10000000, data);
        int lenout = DT_GET_MutatedValueLen(&g_Element[0]);

        WriteToFile(dataout, lenout, "out.file");
    }
    DT_FUZZ_END()

    printf("end ---- testFile\n");
}

void test_example(void)
{
    printf("start ---- test_example\n");

    DT_FUZZ_START(0, 1000000, "test_example", 0)
    {
        printf("\r%d", fuzz_seed + fuzz_i);

        s32 number = *(s32*)DT_SetGetS32(&g_Element[0], 0x123456);
        char* string = DT_SetGetString(&g_Element[1], 10, 10000, "zhangpeng");
        char* buf = DT_SetGetFixBlob(&g_Element[2], 10, 10, "aaaaaaaaaa");

        // int  buf_len=DT_GET_MutatedValueLen(&g_Element[2]);
        fun_example(number, string, buf);
    }
    DT_FUZZ_END()

    clean_a();  //这是我的临时函数，产品线编写测试例的时候不需要，删了它
    printf("end ---- test_example\n");
}
}

void fuzz_test::test_start(void)
{
    test_example();
    testStruct2();
    // testRightRange();

    return;
    // DT_Set_Running_Time_Second(10);
    // DT_Enable_Support_Loop(1);
    // DT_Enable_Leak_Check(1);

    // test();
    // DT_Set_Is_Dump_Coverage(1);
    // DT_Enable_Leak_Check(1);
    // DT_Enable_Log(1);
    // DT_Enable_AllMutater(0);
    test_example();
    // testfor();
    // testMulpara();
    // testFoundBug();

    return;
    // DT_Set_TimeOut_Second(10);
    // DT_Set_If_Show_Corpus(1);
    // DT_Set_Is_Dump_Coverage(1);
    // DT_Set_Report_Path("zhangpeng");
    // testLibfuzzTestCase();

    // testFoundBug();
    // testOneBlob();
    // testOneBlob();
    // test();
    // testOneBlob();
    // testStruct();
    // TEST_MutableBlob(enum_DataElementStringStatic);
    // testMulpara();
    // testLibfuzzTestCase();

    // DT_Enable_AllMutater(0);
    // DT_Enable_OneMutater(enum_StringUtf8BomStatic ,1);
    // DT_Enable_Log(1);
    return;
}
