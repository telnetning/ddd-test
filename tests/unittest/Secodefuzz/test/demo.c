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
#include "../Public.h"
#include "../common/PCommon.h"

/*

本文件提供的函数用来指导使用者使用变异接口
*/
int g_iteration = 1000;
int g_iteration_start = 1;
int g_iteration_end = 10;

/******************************************

两个被测函数

******************************************/

int fun001(s32 tint, char* tchar)
{
    if (tint == 1234)
        return 2;

    return 1;
}

int fun002(s32 tint, char* tipv4)
{
    if (tint == 1234)
        return 2;

    return 1;
}

/******************************************

两个原有单元测试例

******************************************/

void testcase001(void)
{
    int ret = 0;

    ret = fun001(1234, "123");
    if (ret != 2)
        hw_printf("call fun001 error\r\n");
}

void testcase002(void)
{
    int ret = 0;
    u8 temp_ipv4[4] = {192, 168, 0, 1};

    ret = fun002(1234, (char*)temp_ipv4);
    if (ret != 2)
        hw_printf("call fun002 error\r\n");
}

///////////////////////////////////////////////////////////////////////////

/******************************************

char *DT_SetGetS64(S_ElementInit *init, s64 initValue);
char *DT_SetGetS32(S_ElementInit *init, s32 initValue);
char *DT_SetGetS16(S_ElementInit *init, s16 initValue);
char *DT_SetGetS8(S_ElementInit *init, s8 initValue);
char *DT_SetGetU64(S_ElementInit *init, u64 initValue);
char *DT_SetGetU32(S_ElementInit *init, u32 initValue);
char *DT_SetGetU16(S_ElementInit *init, u16 initValue);
char *DT_SetGetU8(S_ElementInit *init, u8 initValue);
char *DT_SetGetNumberEnum(S_ElementInit *init, s32 initValue, int * eunmTable, int  eunmCount);
char *DT_SetGetNumberEnum_EX(S_ElementInit *init, s32 initValue, int * eunmTable, int  eunmCount);
char *DT_SetGetNumberRange(S_ElementInit *init, s32 initValue, int min, int  max);
char *DT_SetGetNumberRange_EX(S_ElementInit *init, s32 initValue, int min, int  max);
char *DT_SetGetFloat(S_ElementInit *init, float initValue);
char *DT_SetGetDouble(S_ElementInit *init, double initValue);
char *DT_SetGetString(S_ElementInit *init, int length, int maxLength, char *  initValue);
char *DT_SetGetStringEnum(S_ElementInit *init, int length, int maxLength, char *  initValue, , char * eunmTableS[], int
eunmCount); char *DT_SetGetStringEnum_EX(S_ElementInit *init, int length, int maxLength, char *  initValue, , char *
eunmTableS[], int  eunmCount); char *DT_SetGetBlob(S_ElementInit *init, int length, int maxLength, char *  initValue);
char *DT_SetGetFixBlob(S_ElementInit *init, int length, int maxLength, char *  initValue);
char *DT_SetGetIpv4(S_ElementInit *init,  char *  initValue);
char *DT_SetGetIpv6(S_ElementInit *init,  char *  initValue);
char *DT_SetGetMac(S_ElementInit *init, char *  initValue);

#define DT_Clear(init)
extern  int GET_MutatedValueLen(S_Element *pElement );

1.这套接口为最推荐使用接口，采用random变异算法
2.一个测试例整体随机
3.初始化与获取值同一个函数完成
4.初始值不能为空，没有随便写一个，尽量有初始值
5.内存第二次调用自动释放，最后一个需要主动调用释放
6.第一次迭代不变异，为初值

******************************************/

/******************************************

改造后的测试例,ok

******************************************/

void testcase001_fuzz(void)
{
    INIT_FuzzEnvironment();

    int i;
    for (i = g_iteration_start; i < g_iteration_end; i++) {
        INIT_Seed(i, 0);
        int ret = 0;

        ret = fun001(
            *(s32*)DT_SetGetS32(&g_Element[0], 1234), DT_SetGetString(&g_Element[1], in_strlen("123") + 1, 18, "123"));

        // if(ret != 2)
        // printf("call fun001 error\r\n");
        ret = ret + 1;  //去告警
    }

    DT_Clear(g_Element);
    CLEAR_FuzzEnvironment();
}

void testcase002_fuzz(void)
{
    INIT_FuzzEnvironment();

    int i;
    for (i = g_iteration_start; i < g_iteration_end; i++) {
        INIT_Seed(i, 0);
        int ret = 0;
        u8 temp_ipv4[4] = {192, 168, 0, 1};

        ret = fun002(*(s32*)DT_SetGetS32(&g_Element[0], 1234), DT_SetGetIpv4(&g_Element[1], (char*)temp_ipv4));

        // if(ret != 2)
        // printf("call fun002 error\r\n");
        ret = ret + 1;  //去告警
    }

    DT_Clear(g_Element);
    CLEAR_FuzzEnvironment();
}

void TEST_DT_DT1(void)
{
    hw_printf("start ---- TEST_DT_DT1\n");

    OPEN_Log();
    testcase001_fuzz();
    testcase002_fuzz();
    CLOSE_Log();

    hw_printf("end ---- TEST_DT_DT1\n");
}

/******************************************

#define DT_SetS64(init, initValue)
#define DT_SetS32(init, initValue)
#define DT_SetS16(init, initValue)
#define DT_SetS8(init,  initValue)
#define DT_SetU64(init, initValue)
#define DT_SetU32(init, initValue)
#define DT_SetU16(init, initValue)
#define DT_SetU8(init,  initValue)
#define DT_SetNumberEnum(init, initValue , eunmTable, eunmCount)
#define DT_SetNumberRange(init, initValue , min, max)
#define DT_SetFloat(init,  initValue)
#define DT_SetDouble(init,  initValue)
#define DT_SetString(init, length, maxLength, str)
#define DT_SetStringEnum(init, length, maxLength, str , eunmTableS, eunmCount)
#define DT_SetBlob(init, length, maxLength, buf)
#define DT_SetFixBlob(init, length, maxLength, buf)
#define DT_SetIpv4(init, buf)
#define DT_SetIpv6(init, buf)
#define DT_SetMac(init, buf)

//结构体相关

#define DT_Set_Struct(init, element, member)

#define DT_SetS64_Struct(init, element, member, value)
#define DT_SetS32_Struct(init, element, member, value)
#define DT_SetS16_Struct(init, element, member, value)
#define DT_SetS8_Struct(init, element, member, value)
#define DT_SetU64_Struct(init, element, member, value)
#define DT_SetU32_Struct(init, element, member, value)
#define DT_SetU16_Struct(init, element, member, value)
#define DT_SetU8_Struct(init, element, member, value)
#define DT_SetNumberRange_Struct(init, element, min, max , member, value)
#define DT_SetFloat_Struct(init, element, member, value)
#define DT_SetDouble_Struct(init, element, member, value)
#define DT_SetString_Struct(init, element, member, length,maxLength, value)
#define DT_SetStringEnum_Struct(init, element, eunmTableS, eunmCount , member, length,maxLength, value)
#define DT_SetBlob_Struct(init, element, member, length, maxLength, value)
#define DT_SetFixBlob_Struct(init, element, member, length, maxLength, value)
#define DT_SetIpv4_Struct(init, element, member, value)
#define DT_SetIpv6_Struct(init, element, member,  value)
#define DT_SetMac_Struct(init, element, member, value)

#define DT_GetFuzzValue(init, Value)
#define DT_Clear(init)
extern  int GET_MutatedValueLen(S_Element *pElement );

1.第二套接口
2.初始化函数与获取值函数为2个接口

******************************************/

/******************************************

改造后的测试例

******************************************/

void testcase003_fuzz(void)
{
    INIT_FuzzEnvironment();

    int i;
    for (i = g_iteration_start; i < g_iteration_end; i++) {
        DT_SetS32(g_Element[0], 0x1234);
        DT_SetString(g_Element[1], 4, 20, "123");

        INIT_Seed(i, 0);
        int ret = 0;

        s32 values32 = 0;
        char buf[20];

        DT_GetFuzzValue(g_Element[0], values32);
        DT_GetFuzzValue(g_Element[1], buf);

        ret = fun001(values32, buf);

        // if(ret != 2)
        // printf("call fun001 error\r\n");
        ret = ret + 1;  //去告警
    }

    DT_Clear(g_Element);
    CLEAR_FuzzEnvironment();
}

void testcase004_fuzz(void)
{
    INIT_FuzzEnvironment();

    int i;
    for (i = g_iteration_start; i < g_iteration_end; i++) {
        INIT_Seed(i, 0);

        DT_SetS32(g_Element[0], 0x1234);
        u8 temp[4] = {192, 168, 0, 1};
        DT_SetIpv4(g_Element[1], (char*)temp);

        int ret = 0;
        s32 values32 = 0;
        u8 temp_ipv4[4];

        DT_GetFuzzValue(g_Element[0], values32);
        DT_GetFuzzValue(g_Element[1], temp_ipv4);

        ret = fun002(values32, (char*)temp_ipv4);

        // if(ret != 2)
        // printf("call fun002 error\r\n");
        ret = ret + 1;  //去告警
    }

    DT_Clear(g_Element);
    CLEAR_FuzzEnvironment();
}

void TEST_DT_DT2(void)
{
    hw_printf("start ---- TEST_DT_DT2\n");

    OPEN_Log();
    testcase003_fuzz();
    testcase004_fuzz();
    CLOSE_Log();

    hw_printf("end ---- TEST_DT_DT2\n");
}

/******************************************

extern  S_Element *CREAT_ElementS64(int isHasInitValue, s64 initValue);
extern  S_Element *CREAT_ElementS32(int isHasInitValue, s32 initValue);
extern  S_Element *CREAT_ElementS16(int isHasInitValue, s16 initValue);
extern  S_Element *CREAT_ElementS8(int isHasInitValue, s8 initValue);
extern  S_Element *CREAT_ElementU64(int isHasInitValue, u64 initValue);
extern  S_Element *CREAT_ElementU32(int isHasInitValue, u32 initValue);
extern  S_Element *CREAT_ElementU16(int isHasInitValue, u16 initValue);
extern  S_Element *CREAT_ElementU8(int isHasInitValue, u8 initValue);
extern  S_Element *CREAT_ElementNumberEnum(int isHasInitValue, s32 initValue ,int * eunmTable, int  eunmCount);
extern  S_Element *CREAT_ElementNumberRange(int isHasInitValue, s32 initValue ,int min, int  max);
extern  S_Element *CREAT_ElementString(int isHasInitValue, char* initValue, int len ,int maxLen);
extern  S_Element *CREAT_ElementStringEnum(int isHasInitValue, char* initValue, int len ,int maxLen, char *
eunmTableS[], int  eunmCount); extern  S_Element *CREAT_ElementBlob(int isHasInitValue, char* initValue , int len ,int
maxLen); extern  S_Element *CREAT_ElementFixBlob(int isHasInitValue, char* initValue , int len ,int maxLen); extern
S_Element *CREAT_ElementIpv4(int isHasInitValue, char* initValue ); extern  S_Element *CREAT_ElementIpv6(int
isHasInitValue, char* initValue ); extern  S_Element *CREAT_ElementMac(int isHasInitValue, char* initValue);

//
extern  s64 GET_MutatedValueSequenceS64(S_Element *pElement, int pos);
extern  s32 GET_MutatedValueSequenceS32(S_Element *pElement, int pos);
extern  s16 GET_MutatedValueSequenceS16(S_Element *pElement, int pos);
extern  s8  GET_MutatedValueSequenceS8(S_Element *pElement, int pos);
extern  u64 GET_MutatedValueSequenceU64(S_Element *pElement, int pos);
extern  u32 GET_MutatedValueSequenceU32(S_Element *pElement, int pos);
extern  u16 GET_MutatedValueSequenceU16(S_Element *pElement, int pos);
extern  u8  GET_MutatedValueSequenceU8(S_Element *pElement, int pos);
extern  s32 GET_MutatedValueSequenceNumberEnum(S_Element *pElement, int pos);
extern  s32 GET_MutatedValueSequenceNumberRange(S_Element *pElement, int pos);
extern  char *GET_MutatedValueSequenceString(S_Element *pElement, int pos);
extern  char *GET_MutatedValueSequenceStringEnum(S_Element *pElement, int pos);
extern  char *GET_MutatedValueSequenceBlob(S_Element *pElement, int pos);
extern  char *GET_MutatedValueSequenceFixBlob(S_Element *pElement, int pos);
extern  char *GET_MutatedValueSequenceIpv4(S_Element *pElement, int pos);
extern  char *GET_MutatedValueSequenceIpv6(S_Element *pElement, int pos);
extern  char *GET_MutatedValueSequenceMac(S_Element *pElement, int pos);



extern  s64 GET_MutatedValueRandomS64(S_Element *pElement);
extern  s32 GET_MutatedValueRandomS32(S_Element *pElement);
extern  s16 GET_MutatedValueRandomS16(S_Element *pElement);
extern  s8  GET_MutatedValueRandomS8(S_Element *pElement);
extern  u64 GET_MutatedValueRandomU64(S_Element *pElement);
extern  u32 GET_MutatedValueRandomU32(S_Element *pElement);
extern  u16 GET_MutatedValueRandomU16(S_Element *pElement);
extern  u8  GET_MutatedValueRandomU8(S_Element *pElement);
extern  s32 GET_MutatedValueRandomNumberEnum(S_Element *pElement);
extern  s32 GET_MutatedValueRandomNumberRange(S_Element *pElement);
extern  char *GET_MutatedValueRandomString(S_Element *pElement);
extern  char *GET_MutatedValueRandomStringEnum(S_Element *pElement);
extern  char *GET_MutatedValueRandomBlob(S_Element *pElement);
extern  char *GET_MutatedValueRandomFixBlob(S_Element *pElement);
extern  char *GET_MutatedValueRandomIpv4(S_Element *pElement);
extern  char *GET_MutatedValueRandomIpv6(S_Element *pElement);
extern  char *GET_MutatedValueRandomMac(S_Element *pElement);



extern  char * GET_MutatedValueRandom(S_Element *pElement );
extern  char * GET_MutatedValueSequence(S_Element *pElement , int pos);
extern  int GET_MutatedValueLen(S_Element *pElement );

extern  void CREAT_ElementEX(S_Element *pElement ,int isNeedFree,int isHasInitValue ,int type, char * inBuf, int inLen);
extern  void FREE_MutatedValue(S_Element *pElement);
extern  void FREE_Element(S_Element *pElement);

1.第三套接口
2.初始化函数与获取值函数为2个接口
3.这套接口为最原始接口，最为灵活

******************************************/

/******************************************

改造后的测试例

******************************************/

void testcase005_fuzz(void)
{
    INIT_FuzzEnvironment();

    int i;
    S_Element* e001;
    S_Element* e002;

    e001 = CREAT_ElementS32(enum_Yes, 0x1234);
    e002 = CREAT_ElementString(enum_Yes, "123", 4, 0);

    for (i = g_iteration_start; i < g_iteration_end; i++) {
        INIT_Seed(i, 0);
        int ret = 0;

        ret = fun001(GET_MutatedValueRandomS32(e001), GET_MutatedValueRandomString(e002));

        // if(ret != 2)
        // printf("call fun001 error\r\n");
        ret = ret + 1;  //去告警

        FREE_MutatedValue(e001);
        FREE_MutatedValue(e002);
    }

    FREE_Element(e001);
    FREE_Element(e002);

    CLEAR_FuzzEnvironment();
}

void testcase006_fuzz(void)
{
    INIT_FuzzEnvironment();

    int i;
    S_Element* e001;
    S_Element* e002;
    u8 temp[4] = {192, 168, 0, 1};

    e001 = CREAT_ElementS32(enum_Yes, 0x1234);
    e002 = CREAT_ElementIpv4(enum_Yes, (char*)temp);

    for (i = g_iteration_start; i < g_iteration_end; i++) {
        INIT_Seed(i, 0);

        int ret = 0;

        ret = fun002(GET_MutatedValueRandomS32(e001), GET_MutatedValueRandomIpv4(e002));

        // if(ret != 2)
        // printf("call fun002 error\r\n");
        ret = ret + 1;  //去告警

        FREE_MutatedValue(e001);
        FREE_MutatedValue(e002);
    }

    FREE_Element(e001);
    FREE_Element(e002);

    CLEAR_FuzzEnvironment();
}

void TEST_DT_DT3(void)
{
    hw_printf("start ---- TEST_DT_DT3\n");

    OPEN_Log();
    testcase005_fuzz();
    testcase006_fuzz();
    CLOSE_Log();

    hw_printf("end ---- TEST_DT_DT3\n");
}

/******************************************

使用DT接口获取变异值

******************************************/

/*

测试得到一个整数变量

*/
void TEST_GetOneValueNumber(void)
{
    hw_printf("start ---- TEST_GetOneValueNumber\n");

    INIT_FuzzEnvironment();

    int i;

    for (i = 0; i < g_iteration; i++) {
        hw_printf("%d value = %#010x\r\n", i, *(s32*)DT_SetGetS32(&g_Element[0], 0x1234));
    }

    DT_Clear(g_Element);
    CLEAR_FuzzEnvironment();

    hw_printf("end ---- TEST_GetOneValueNumber\n");
}

void TEST_GetOneValueNumber1(void)
{
    hw_printf("start ---- TEST_GetOneValueNumber1\n");

    INIT_FuzzEnvironment();

    int i;
    s32 values32 = 0;

    for (i = 0; i < g_iteration; i++) {
        DT_SetS32(g_Element[0], 0x1234);
        DT_GetFuzzValue(g_Element[0], values32);
        hw_printf("s32 value = %#010x\r\n", (u32)values32);
    }

    DT_Clear(g_Element);
    CLEAR_FuzzEnvironment();

    hw_printf("end ---- TEST_GetOneValueNumber1\n");
}

void TEST_GetOneValueNumberEnum(void)
{
    hw_printf("start ---- TEST_GetOneValueNumberEnum\n");

    INIT_FuzzEnvironment();

    int i;
    int Number_table[] = {2, 4, 6, 1234, 0x1234};

    for (i = 0; i < g_iteration; i++) {
        hw_printf("%d value = %#010x\r\n", i, *(s32*)DT_SetGetNumberEnum(&g_Element[0], 0x1234, Number_table, 5));
    }

    DT_Clear(g_Element);
    CLEAR_FuzzEnvironment();

    hw_printf("end ---- TEST_GetOneValueNumberEnum\n");
}

void TEST_GetOneValueNumberEnum_EX(void)
{
    hw_printf("start ---- TEST_GetOneValueNumberEnum_EX\n");

    INIT_FuzzEnvironment();

    int i;
    int Number_table[] = {2, 4, 6, 1234, 0x1234};

    for (i = 0; i < g_iteration; i++) {
        hw_printf("%d value = %#010x\r\n", i, *(s32*)DT_SetGetNumberEnum_EX(&g_Element[0], 0xff, Number_table, 5));
    }

    DT_Clear(g_Element);
    CLEAR_FuzzEnvironment();

    hw_printf("end ---- TEST_GetOneValueNumberEnum_EX\n");
}

void TEST_GetOneValueNumberRange(void)
{
    hw_printf("start ---- TEST_GetOneValueNumberRange\n");

    INIT_FuzzEnvironment();

    int i;

    for (i = 0; i < g_iteration; i++) {
        hw_printf("%d value = %#010x\r\n", i, *(s32*)DT_SetGetNumberRange(&g_Element[0], 0x1234, 0x1200, 0x1300));
    }

    DT_Clear(g_Element);
    CLEAR_FuzzEnvironment();

    hw_printf("end ---- TEST_GetOneValueNumberRange\n");
}

void TEST_GetOneValueNumberRange_EX(void)
{
    hw_printf("start ---- TEST_GetOneValueNumberRange_EX\n");

    INIT_FuzzEnvironment();

    int i;

    for (i = 0; i < g_iteration; i++) {
        hw_printf("%d value = %#010x\r\n", i, *(s32*)DT_SetGetNumberRange_EX(&g_Element[0], 0xff, 0x1200, 0x1300));
    }

    DT_Clear(g_Element);
    CLEAR_FuzzEnvironment();

    hw_printf("end ---- TEST_GetOneValueNumberRange_EX\n");
}

/*

测试得到一个浮点型变量

*/
#ifndef __KERNEL__
void TEST_GetOneValueFloat(void)
{
    hw_printf("start ---- TEST_GetOneValueFloat\n");

    INIT_FuzzEnvironment();

    int i;

    for (i = 0; i < g_iteration; i++) {
        hw_printf("%d value = %e\r\n", i, *(float*)DT_SetGetFloat(&g_Element[0], 0x1234));
    }

    DT_Clear(g_Element);
    CLEAR_FuzzEnvironment();

    hw_printf("end ---- TEST_GetOneValueFloat\n");
}

/*

测试得到一个double型变量

*/
void TEST_GetOneValueDouble(void)
{
    hw_printf("start ---- TEST_GetOneValueDouble\n");

    INIT_FuzzEnvironment();

    int i;

    for (i = 0; i < g_iteration; i++) {
        hw_printf("%d value = %e\r\n", i, *(double*)DT_SetGetDouble(&g_Element[0], 0x1234));
    }

    DT_Clear(g_Element);
    CLEAR_FuzzEnvironment();

    hw_printf("end ---- TEST_GetOneValueDouble\n");
}
#endif

/*

测试得到一个字符串变量

*/
void TEST_GetOneValueString(void)
{
    hw_printf("start ---- TEST_GetOneValueString\n");

    INIT_FuzzEnvironment();

    int i;

    for (i = 0; i < g_iteration; i++) {
        char* aaa = DT_SetGetString(&g_Element[0], 6, 20, "12345");
        // 字符串的最大长度要比实际buff小1
        hw_printf("%d value = \r\n", i);
        HEX_Dump((u8*)aaa, GET_MutatedValueLen(g_Element[0].element));
    }
    DT_Clear(g_Element);
    CLEAR_FuzzEnvironment();

    hw_printf("end ---- TEST_GetOneValueString\n");
}

/*

测试得到一个字符串枚举变量

*/
void TEST_GetOneValueStringEnum(void)
{
    hw_printf("start ---- TEST_GetOneValueStringEnum\n");

    INIT_FuzzEnvironment();

    int i;
    char* String_table[] = {"123", "abc", "xxx", "1.1.1", "aaa", "12345"};

    for (i = 0; i < g_iteration; i++) {
        char* aaa = DT_SetGetStringEnum(&g_Element[0], 6, 20, "12345", String_table, 6);
        // 字符串的最大长度要比实际buff小1
        hw_printf("%d value = \r\n", i);
        HEX_Dump((u8*)aaa, GET_MutatedValueLen(g_Element[0].element));
    }
    DT_Clear(g_Element);
    CLEAR_FuzzEnvironment();

    hw_printf("end ---- TEST_GetOneValueStringEnum\n");
}

void TEST_GetOneValueStringEnum_EX(void)
{
    hw_printf("start ---- TEST_GetOneValueStringEnum_EX\n");

    INIT_FuzzEnvironment();

    int i;
    char* String_table[] = {"123", "abc", "xxx", "1.1.1", "aaa", "12345"};

    for (i = 0; i < g_iteration; i++) {
        char* aaa = DT_SetGetStringEnum_EX(&g_Element[0], 6, 20, "12346", String_table, 6);
        // 字符串的最大长度要比实际buff小1
        hw_printf("%d value = \r\n", i);
        HEX_Dump((u8*)aaa, GET_MutatedValueLen(g_Element[0].element));
    }
    DT_Clear(g_Element);
    CLEAR_FuzzEnvironment();

    hw_printf("end ---- TEST_GetOneValueStringEnum_EX\n");
}

/*

测试得到一个内存块变量

*/
void TEST_GetOneValueBlob(void)
{
    hw_printf("start ---- TEST_GetOneValueBlob\n");

    INIT_FuzzEnvironment();

    int i;

    for (i = 0; i < g_iteration; i++) {
        char* aaa = DT_SetGetBlob(&g_Element[0], 10, 20, "\xffwwAAA\00012");
        hw_printf("%d value = \r\n", i);
        HEX_Dump((u8*)aaa, GET_MutatedValueLen(g_Element[0].element));
    }
    DT_Clear(g_Element);
    CLEAR_FuzzEnvironment();

    hw_printf("end ---- TEST_GetOneValueBlob\n");
}

/*

测试得到一个内存块枚举变量

*/
void TEST_GetOneValueBlobEnum(void)
{
    hw_printf("start ---- TEST_GetOneValueBlobEnum\n");

    INIT_FuzzEnvironment();

    int i;
    char* Blob_table[] = {"123111", "abcaaa", "\x00\x01\x02\x00\xff", "6666666666666"};
    int Blob_L_table[] = {7, 7, 5, 14};

    for (i = 0; i < g_iteration; i++) {
        char* aaa = DT_SetGetBlobEnum(&g_Element[0], 14, 20, "6666666666666", Blob_table, Blob_L_table, 4);
        hw_printf("%d value = \r\n", i);
        HEX_Dump((u8*)aaa, GET_MutatedValueLen(g_Element[0].element));
    }
    DT_Clear(g_Element);
    CLEAR_FuzzEnvironment();

    hw_printf("end ---- TEST_GetOneValueBlobEnum\n");
}

void TEST_GetOneValueBlobEnum_EX(void)
{
    hw_printf("start ---- TEST_GetOneValueBlobEnum_EX\n");

    INIT_FuzzEnvironment();

    int i;
    char* Blob_table[] = {"123111", "abcaaa", "\x00\x01\x02\x00\xff", "6666666666666"};
    int Blob_L_table[] = {7, 7, 5, 14};

    for (i = 0; i < g_iteration; i++) {
        char* aaa = DT_SetGetBlobEnum_EX(&g_Element[0], 14, 20, "6666666666667", Blob_table, Blob_L_table, 4);
        hw_printf("%d value = \r\n", i);
        HEX_Dump((u8*)aaa, GET_MutatedValueLen(g_Element[0].element));
    }
    DT_Clear(g_Element);
    CLEAR_FuzzEnvironment();

    hw_printf("end ---- TEST_GetOneValueBlobEnum_EX\n");
}

/*

测试得到一个定长内存块变量

*/
void TEST_GetOneValueFixBlob(void)
{
    hw_printf("start ---- TEST_GetOneValueFixBlob\n");

    INIT_FuzzEnvironment();

    int i;

    for (i = 0; i < g_iteration; i++) {
        char* aaa = DT_SetGetFixBlob(&g_Element[0], 10, 20, "\xffwwAAA\00012");
        hw_printf("%d value = \r\n", i);
        HEX_Dump((u8*)aaa, GET_MutatedValueLen(g_Element[0].element));
    }
    DT_Clear(g_Element);
    CLEAR_FuzzEnvironment();

    hw_printf("end ---- TEST_GetOneValueFixBlob\n");
}

/*

测试得到一个ipv4变量

*/
void TEST_GetOneValueIpv4(void)
{
    hw_printf("start ---- TEST_GetOneValueIpv4\n");

    INIT_FuzzEnvironment();

    int i;
    u8 temp_ipv4[4] = {192, 168, 0, 1};

    for (i = 0; i < g_iteration; i++) {
        char* cipv4 = DT_SetGetIpv4(&g_Element[0], (char*)temp_ipv4);
        hw_printf(
            "testcase is %d  ipv4 is %d.%d.%d.%d----\n", i, (u8)cipv4[0], (u8)cipv4[1], (u8)cipv4[2], (u8)cipv4[3]);
    }
    DT_Clear(g_Element);
    CLEAR_FuzzEnvironment();

    hw_printf("end ---- TEST_GetOneValueIpv4\n");
}

/*

测试得到一个ipv6变量

*/
#ifndef __KERNEL__
#ifndef _WIN32
#include <arpa/inet.h>
void TEST_GetOneValueIpv6(void)
{
    hw_printf("start ---- TEST_GetOneValueIpv6\n");

    INIT_FuzzEnvironment();

    int i;
    u8 temp_ipv6[16] = {0x20, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01};

    for (i = 0; i < g_iteration; i++) {
        char* uipv6 = DT_SetGetIpv6(&g_Element[0], (char*)temp_ipv6);
        char ip_str[64];

        inet_ntop(AF_INET6, uipv6, ip_str, sizeof(ip_str));
        hw_printf("testcase is %d ipv6 is   %s  \r\n ", i, ip_str);
    }
    DT_Clear(g_Element);
    CLEAR_FuzzEnvironment();

    hw_printf("end ---- TEST_GetOneValueIpv6\n");
}
#endif
#endif
/*

测试得到一个mac变量

*/
void TEST_GetOneValueMac(void)
{
    hw_printf("start ---- TEST_GetOneValueMac\n");

    INIT_FuzzEnvironment();

    int i;
    u8 temp_mac[6] = {0x28, 0x6e, 0xd4, 0x89, 0x26, 0xa8};

    for (i = 0; i < g_iteration; i++) {
        char* cmac = DT_SetGetMac(&g_Element[0], (char*)temp_mac);
        hw_printf("testcase is %d mac is %02x%02x-%02x%02x-%02x%02x-----\n",
            i,
            (u8)cmac[0],
            (u8)cmac[1],
            (u8)cmac[2],
            (u8)cmac[3],
            (u8)cmac[4],
            (u8)cmac[5]);
    }
    DT_Clear(g_Element);
    CLEAR_FuzzEnvironment();

    hw_printf("end ---- TEST_GetOneValueMac\n");
}
#ifndef __KERNEL__

typedef struct {
    char name[11];
    int value;
} TS_TEST;

typedef struct {
    char name[100];
    char char_value;
    int int_value;
    long long_value;
    long long longlong_value;
    double double_value;
    float float_value;
    int* p_value;
    TS_TEST struct_value;
    short Short_value;
} TS_TEST1;

/*

测试得到一个结构体变量

*/

void TEST_GetOneValueStruct(void)
{
    hw_printf("start ---- TEST_GetOneValueStruct\n");

    INIT_FuzzEnvironment();

    int i;
    TS_TEST1 test = {{0}};

    for (i = 0; i < g_iteration; i++) {
        hw_printf("g_iteration is %d  -----\n", i);
        DT_SetString_Struct(g_ElementS[0], test, name, 4, 100, "www");
        DT_SetS8_Struct(g_ElementS[1], test, char_value, 90);
        DT_SetS32_Struct(g_ElementS[2], test, int_value, 80);
        DT_SetS32_Struct(g_ElementS[3], test, long_value, 70);
        DT_SetS64_Struct(g_ElementS[4], test, longlong_value, 60);
        DT_SetDouble_Struct(g_ElementS[5], test, double_value, 50.8);
        DT_SetFloat_Struct(g_ElementS[6], test, float_value, 99.9);
        DT_SetBlob_Struct(g_ElementS[7], test, struct_value.name, 3, 11, "abc");
        // DT_SetFixBlob_Struct(g_ElementS[7], test, struct_value.name, 3, 11, "abc");
        DT_SetS32_Struct(g_ElementS[8], test, struct_value.value, 30);
        DT_SetS16_Struct(g_ElementS[9], test, Short_value, 10);

        // 字符串的最大长度要比实际buff小1
        DT_GetFuzzValue(g_ElementS, test);
        hw_printf("%d %s\n", i, (char*)test.name);
        hw_printf("%d %d\n", i, test.char_value);
        hw_printf("%d %d\n", i, test.int_value);
        hw_printf("%d %ld\n", i, test.long_value);
        hw_printf("%d %lld\n", i, test.longlong_value);
        hw_printf("%d %e\n", i, test.double_value);
        hw_printf("%d %e\n", i, test.float_value);
        HEX_Dump((u8*)test.struct_value.name, GET_MutatedValueLen(g_ElementS[7].element));
        hw_printf("%d %d\n", i, test.struct_value.value);
        hw_printf("%d %d\n", i, test.Short_value);
    }
    DT_Clear(g_ElementS);
    CLEAR_FuzzEnvironment();

    hw_printf("end ---- TEST_GetOneValueStruct\n");
}
#endif

/*

测试同时获取多个参数变量

*/
void TEST_GetMultipleValue(void)
{
    hw_printf("start ---- TEST_GetMultipleValue\n");

    INIT_FuzzEnvironment();

    int i;

    for (i = 0; i < g_iteration; i++) {

        hw_printf("----g_iteration is %d  \n", i);

        hw_printf("%d value = %#010x\r\n", i, *(s32*)DT_SetGetS32(&g_Element[0], 0x1234));

        // 字符串的最大长度要比实际buff小1
        char* aaa = DT_SetGetString(&g_Element[1], 12, 12, "123456abcde");
        hw_printf("%d value = \r\n", i);
        HEX_Dump((u8*)aaa, GET_MutatedValueLen(g_Element[1].element));

        aaa = DT_SetGetBlob(&g_Element[2], 10, 18, "aaaaaaaaaa");
        hw_printf("%d value = \r\n", i);
        HEX_Dump((u8*)aaa, GET_MutatedValueLen(g_Element[2].element));
    }

    DT_Clear(g_Element);
    CLEAR_FuzzEnvironment();

    hw_printf("end ---- TEST_GetMultipleValue\n");
}

/*

测试同时获取多个参数变量,含一个变量是结构体

*/
#ifndef __KERNEL__
void TEST_GetMultipleValue1(void)
{
    hw_printf("start ---- TEST_GetMultipleValue1\n");
    INIT_FuzzEnvironment();

    int i;
    s32 values32 = 0;
    char buf1[19];
    u8 buf[18];
    TS_TEST1 test = {{0}};

    for (i = 0; i < g_iteration; i++) {
        DT_SetS32(g_Element[0], 0x1234);
        DT_SetString(g_Element[1], 10, 12, "wwwAAA12q");
        DT_SetBlob(g_Element[2], 10, 18, "\xffwwAAA\00012");

        DT_SetString_Struct(g_ElementS[0], test, name, 4, 100, "www");
        DT_SetS8_Struct(g_ElementS[1], test, char_value, 90);
        DT_SetS32_Struct(g_ElementS[2], test, int_value, 80);
        DT_SetS32_Struct(g_ElementS[3], test, long_value, 70);
        DT_SetS64_Struct(g_ElementS[4], test, longlong_value, 60);
        DT_SetDouble_Struct(g_ElementS[5], test, double_value, 50.8);
        DT_SetFloat_Struct(g_ElementS[6], test, float_value, 99.9);
        DT_SetBlob_Struct(g_ElementS[7], test, struct_value.name, 3, 11, "abc");
        DT_SetS32_Struct(g_ElementS[8], test, struct_value.value, 30);
        DT_SetS16_Struct(g_ElementS[9], test, Short_value, 10);

        DT_GetFuzzValue(g_Element[0], values32);
        hw_printf("%d value = %#010x\r\n", i, (u32)values32);

        // 字符串的最大长度要比实际buff小1
        DT_GetFuzzValue(g_Element[1], buf1);
        hw_printf("%d value = \r\n", i);
        HEX_Dump((u8*)buf1, GET_MutatedValueLen(g_Element[1].element));

        DT_GetFuzzValue(g_Element[2], buf);
        hw_printf("%d value = \r\n", i);
        HEX_Dump((u8*)buf, GET_MutatedValueLen(g_Element[2].element));

        // 字符串的最大长度要比实际buff小1
        DT_GetFuzzValue(g_ElementS, test);
        hw_printf("%d %s\n", i, (char*)test.name);
        hw_printf("%d %d\n", i, test.char_value);
        hw_printf("%d %d\n", i, test.int_value);
        hw_printf("%d %ld\n", i, test.long_value);
        hw_printf("%d %lld\n", i, test.longlong_value);
        hw_printf("%d %e\n", i, test.double_value);
        hw_printf("%d %e\n", i, test.float_value);
        HEX_Dump((u8*)test.struct_value.name, GET_MutatedValueLen(g_ElementS[7].element));
        hw_printf("%d %d\n", i, test.struct_value.value);
        hw_printf("%d %d\n", i, test.Short_value);
    }

    DT_Clear(g_Element);
    DT_Clear(g_ElementS);
    CLEAR_FuzzEnvironment();

    hw_printf("end ---- TEST_GetMultipleValue1\n");
}
#endif

////////////////////////

/******************************************

如何扩展编译算法

******************************************/

/*

测试简单变异函数

*/

//定制获取一定范围的变量
void TEST_Mutater_NumberRange(void)
{
    hw_printf("start ---- TEST_Mutater_NumberRange\n");
    int i;

    for (i = 0; i < 10; i++) {
        int numtemp = Mutater_NumberRange(5, 10);
        hw_printf(" temp is %d \n", numtemp);
    }

    hw_printf("start ---- TEST_Mutater_NumberRange\n");
}

void TEST_Mutater_NumberEnum(void)
{
    hw_printf("start ---- TEST_Mutater_NumberEnum\n");
    int i;
    int Number_table[] = {2, 4, 6, 8, 10};

    for (i = 0; i < 10; i++) {
        int numtemp = Mutater_NumberEnum(Number_table, 5);
        hw_printf(" temp is %d \n", numtemp);
    }
    hw_printf("start ---- TEST_Mutater_NumberEnum\n");
}

void TEST_Mutater_StringEnum(void)
{
    hw_printf("start ---- TEST_Mutater_StringEnum\n");
    int i;
    char* String_table[] = {"123", "abc", "xxx", "1.1.1", "aaa"};

    for (i = 0; i < 10; i++) {
        char* stringtemp = Mutater_StringEnum(String_table, 5);
        hw_printf(" temp is %s \n", stringtemp);
    }
    hw_printf("start ---- TEST_Mutater_StringEnum\n");
}
#if 0

/*

下边的例子写的是ipv4的变异算法，您能够参考着写个mac地址的变异算法呢?

*/
u8 CustomBlob_table1[][4] =

{
{0,0,0,0},
{0,0,0,255},
{0,0,255,255},
{0,255,255,255},
{255,255,255,255},
{255,255,255,0},
{255,255,0,0},
{255,0,0,0},
{1,0,0,0},
{1,0,0,1},
{1,0,0,255},
{1,255,255,255},
{128,0,0,0},
{128,0,0,1},
{128,0,0,255},
{128,0,255,255},
{192,0,0,0},
{192,0,0,1},
{192,0,0,255},
{224,0,0,0},
{224,0,0,1},
{224,0,0,255},
{127,0,0,1},
};

u8 CustomBlob_table2[][4] =
{
{255,255,255,0},
{255,255,0,0},
{255,0,0,0},
};

u8 CustomBlob_table3[][4] =
{
{0,0,0,255},
{0,0,255,255},
{0,255,255,255},
};

 int Ipv4_getcount(S_Element *pElement )
{
	ASSERT_NULL(pElement);

	return sizeof(CustomBlob_table1)/4+sizeof(CustomBlob_table2)/4+sizeof(CustomBlob_table3)/4;
}

char * Ipv4_getvalue(S_Element *pElement, int pos)
{

	ASSERT_NULL(pElement);
	int pos1=sizeof(CustomBlob_table1)/4;
	int pos2=sizeof(CustomBlob_table2)/4 + pos1;
	int pos3=sizeof(CustomBlob_table3)/4 + pos2;

	pElement->outBuf = hw_Malloc(4);
	pElement->isNeedFreeOutBuf = enum_Yes;
	pElement->outLen = 4;

	if(pos <pos1)
		*((u32 *)pElement->outBuf) = *(u32 *)CustomBlob_table1[pos];
	else if(pos <pos2)
		*((u32 *)pElement->outBuf) = (*(u32 *)CustomBlob_table2[pos-pos1])&(*((u32 *)pElement->inBuf));
	else if(pos <pos3)
		*((u32 *)pElement->outBuf) = (*(u32 *)CustomBlob_table3[pos-pos2])|(*((u32 *)pElement->inBuf));

	
	return pElement->outBuf;
}


int Ipv4_getissupport(S_Element *pElement)
{
	ASSERT_NULL(pElement);
	if(pElement->type ==enum_Ipv4)
		return enum_Yes;

	return enum_No;
}

const struct Mutater_group Ipv4_group = {
	 "Ipv4",
	 Ipv4_getcount,
	 Ipv4_getvalue,
	 Ipv4_getissupport,
	 1
};

void init_Ipv4(void)
{
	register_Mutater(&Ipv4_group, enum_MIpv4);
}

u8 temp_ipv4[4] ={192,168,0,1};

void TEST_Ipv4(void)
{
	init_Ipv4();

	int i=0;

	S_Element pElement ={{0}};

	CREAT_ElementEX(&pElement, enum_No, enum_Yes, enum_Ipv4, (char*)temp_ipv4, 32);


	for(i=0; i<pElement.count; i++)
	{
		u32 uipv4 = *(u32 *)GET_MutatedValueSequence(&pElement, i);

		char cipv4[4];

		hw_Memcpy(cipv4,&uipv4,4);
		hw_printf("start2----ip is %d.%d.%d.%d----\n", (u8)cipv4[0],(u8)cipv4[1],(u8)cipv4[2],(u8)cipv4[3]);
		FREE_MutatedValue(&pElement);
	}	


    	FREE_Element(&pElement);

}

#endif
/*

扩展fill arg变异算法

*/

#define tempmaxInterfaceNo 10

int getInterfaceIndexFromNo(int InterfaceNo)
{
    if (InterfaceNo >= tempmaxInterfaceNo)
        return -1;

    return 0xff000000 | InterfaceNo << 3;
}

unsigned long get_InterfaceIndex(void)
{
    int i;
    i = hw_Rand() % 10;

    switch (i) {
        case 0:
            return -1;
#ifndef _MSC_VER
        case 1 ... 3:
            return hw_Rand();
        case 4 ... 9:
            return getInterfaceIndexFromNo(hw_Rand() % 10);
#endif
    }
    return 0;
}

void TEST_Mutater_fill_arg(void)
{
    hw_printf("start ---- TEST_Mutater_fill_arg\n");

    register_Mutater_fill_arg(ARG_InterfaceIndex, get_InterfaceIndex);

    int i = 0;

    for (i = 0; i < 100; i++) {

        hw_printf(" test muator is %d ----\n", i);
        hw_printf(" u32   value  is 0x%lx \n", Mutater_fill_arg(ARG_InterfaceIndex));
    }

    hw_printf("end ---- TEST_Mutater_fill_arg\n");
}

/*****************************************

设置最大输出尺寸，单位byte,	默认值为65535

*****************************************/

void TEST_SET_MaxOutputSize(void)
{
    hw_printf("start ---- TEST_SET_MaxOutputSize\n");

    INIT_FuzzEnvironment();

    DT_Set_MaxOutputSize(100);

    SET_CloseAllMutater();
    SET_OpenOneMutater(enum_StringLengthEdgeCase);

    int i;
    S_Element* eeee;

    char* aaa = "-100aaaaaaaaaaaaaaaaaaaaaaaaCCCCCCCCCCCCCCCCCCCCCCCCCCCCC";

    eeee = CREAT_ElementString(enum_Yes, aaa, in_strlen(aaa) + 1, 0);

    for (i = 0; i < eeee->count; i++) {

        char* ccc = (char*)GET_MutatedValueSequence(eeee, i);
        int bbb = GET_MutatedValueLen(eeee);
        hw_printf("%d value =    len=%d\r\n", i, bbb);
        HEX_Dump((u8*)ccc, bbb);

        FREE_MutatedValue(eeee);
    }

    FREE_Element(eeee);
    CLEAR_FuzzEnvironment();
    hw_printf("end ---- TEST_SET_MaxOutputSize\n");
}

/******************************************

Seed，如何重放测试例

******************************************/
void TEST_Reproduce(void)
{
    hw_printf("start ---- TEST_Reproduce\n");
    INIT_FuzzEnvironment();

    int i;
    S_Element* eeee = CREAT_ElementU32(enum_Yes, 0x12345678);

    for (i = 0; i < 20; i++) {
        INIT_Seed(i + 1, 0);
        hw_printf("----seed is %d\n", i + 1);
        u32 u32temp = 0;
        u32temp = *(u32*)GET_MutatedValueRandom(eeee);
        hw_printf(" temp is 0x%x \n", u32temp);

        FREE_MutatedValue(eeee);
    }

    FREE_Element(eeee);

    //回放第14个测试例
    eeee = CREAT_ElementU32(enum_Yes, 0x12345678);

    for (i = 13; i < 14; i++) {
        INIT_Seed(i + 1, 0);
        hw_printf("reproduce :----seed is %d\n", i + 1);
        u32 u32temp = 0;
        u32temp = *(u32*)GET_MutatedValueRandom(eeee);
        hw_printf(" temp is 0x%x \n", u32temp);

        FREE_MutatedValue(eeee);
    }

    FREE_Element(eeee);

    CLEAR_FuzzEnvironment();
    hw_printf("end ---- TEST_Reproduce\n");
}

/******************************************

测试log开关

******************************************/

void TEST_Log1(void)
{
    hw_printf("start ---- TEST_GetMultipleValue\n");

    OPEN_Log();
    INIT_FuzzEnvironment();

    int i;
    S_ElementInit eee[3] = {{0}};
    s32 values32 = 0;
    char buf1[19];
    u8 buf[18];

    for (i = 0; i < 10; i++) {
        DT_SetS32(eee[0], 0x1234);
        DT_SetString(eee[1], 12, 12, "123456abcde");
        DT_SetBlob(eee[2], 10, 18, "aaaaaaaaaa");

        hw_printf("----g_iteration is %d  \n", i);

        DT_GetFuzzValue(eee[0], values32);
        hw_printf("s32 value = %#010x\r\n", (u32)values32);

        // 字符串的最大长度要比实际buff小1
        DT_GetFuzzValue(eee[1], buf1);
        HEX_Dump((u8*)buf1, GET_MutatedValueLen(eee[1].element));

        DT_GetFuzzValue(eee[2], buf);
        HEX_Dump((u8*)buf, GET_MutatedValueLen(eee[2].element));
    }

    DT_Clear(eee);
    CLEAR_FuzzEnvironment();

    CLOSE_Log();

    hw_printf("end ---- TEST_GetMultipleValue\n");
}

void test(void)
{

    hw_printf("\r\n *********** test demo  star ***********t\r\n");

    TEST_DT_DT1();
    TEST_DT_DT2();
    TEST_DT_DT3();

    TEST_GetOneValueNumber();
    TEST_GetOneValueNumber1();
    TEST_GetOneValueNumberEnum();
    TEST_GetOneValueNumberEnum_EX();
    TEST_GetOneValueNumberRange();
    TEST_GetOneValueNumberRange_EX();
#ifndef __KERNEL__
    TEST_GetOneValueFloat();
    TEST_GetOneValueDouble();
#endif
    TEST_GetOneValueString();
    TEST_GetOneValueStringEnum();
    TEST_GetOneValueStringEnum_EX();
    TEST_GetOneValueBlob();
    TEST_GetOneValueFixBlob();
#ifndef __KERNEL__
    TEST_GetOneValueStruct();
#endif
    TEST_GetOneValueIpv4();
#ifndef __KERNEL__
#ifndef _WIN32
    TEST_GetOneValueIpv6();
#endif
#endif
    TEST_GetOneValueMac();

    TEST_GetMultipleValue();
#ifndef __KERNEL__
    TEST_GetMultipleValue1();
#endif
    TEST_Mutater_NumberRange();
    TEST_Mutater_NumberEnum();
    TEST_Mutater_StringEnum();

    TEST_Mutater_fill_arg();

    TEST_SET_MaxOutputSize();

    TEST_Reproduce();
    TEST_Log1();

    hw_printf("\r\n *********** test demo  end ***********\r\n");

    hw_printf("\r\n *********** test test  star ***********t\r\n");

    TEST_MutableNumber(enum_MutatedMAX);
    TEST_MutableNumberEnum(enum_MutatedMAX);
    TEST_MutableNumberRange(enum_MutatedMAX);
    TEST_MutableString(enum_MutatedMAX);
    TEST_MutableStringEnum(enum_MutatedMAX);
    TEST_MutableBlob(enum_MutatedMAX);
    TEST_MutableBlobEnum(enum_MutatedMAX);
    TEST_MutableFixBlob(enum_MutatedMAX);
    TEST_Ipv4();
#ifndef __KERNEL__
#ifndef _WIN32
    TEST_Ipv6();
#endif
#endif
    TEST_Mac();

    TEST_CREAT_Element();
    TEST_GetSequenceValue();
    TEST_GetRandomValue();
    TEST_GetS32SequenceValue();
    TEST_GetNumberEnumSequenceValue();
    TEST_GetNumberRangeSequenceValue();
    TEST_GetStringSequenceValue();
    TEST_GetStringEnumSequenceValue();
    TEST_GetBlobSequenceValue();
    TEST_GetBlobEnumSequenceValue();
    TEST_GetFixBlobSequenceValue();
    TEST_GetS32RandomValue();
    TEST_GetNumberEnumRandomValue();
    TEST_GetNumberRangeRandomValue();
    TEST_GetStringRandomValue();
    TEST_GetStringEnumRandomValue();
    TEST_GetBlobRandomValue();
    TEST_GetBlobEnumRandomValue();
    TEST_GetFixBlobRandomValue();
    TEST_INIT_Seed(0);
    TEST_WholeMultiVarientRandom();
    TEST_WholeMutiVarientSequence();
    TEST_CloseAllMutater();
    TEST_OpenAllMutater();
    TEST_CloseOneMutater();
    TEST_OpenOneMutater();
    TEST_ElementCloseAllMutater();
    TEST_ElementOpenAllMutater();
    TEST_ElementCloseOneMutater();
    TEST_ElementOpenOneMutater();
    TEST_Log();
    TEST_HEX_Dump();
    TEST_DEBUG_Element();

    hw_printf("\r\n *********** test test  end ***********\r\n");

    return;
}
