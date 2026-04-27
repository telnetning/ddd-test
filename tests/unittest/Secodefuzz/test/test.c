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

本文件提供的函数用来测试各种接口
为使用者提供实例

*/

/*

测试整形变异算法

*/
void TEST_MutableNumber(int MutatedNo)
{
    hw_printf("start ---- test_MutableNumber\n");

    INIT_FuzzEnvironment();

    if (MutatedNo != enum_MutatedMAX) {
        SET_CloseAllMutater();
        SET_OpenOneMutater((enum enum_Mutated)MutatedNo);
    }

    int i;
    S_Element* eeee;

    eeee = CREAT_ElementU32(enum_Yes, 0x1234);

    for (i = 0; i < eeee->count; i++) {

        hw_printf("%d value =   0x%x\r\n", i, *(u32*)GET_MutatedValueSequence(eeee, i));
        FREE_MutatedValue(eeee);
    }

    FREE_Element(eeee);
    SET_OpenAllMutater();
    CLEAR_FuzzEnvironment();
    hw_printf("end ---- test_MutableNumber\n");
}

/*

测试整形枚举变异算法

*/
void TEST_MutableNumberEnum(int MutatedNo)
{
    hw_printf("start ---- TEST_MutableNumberEnum\n");

    INIT_FuzzEnvironment();

    if (MutatedNo != enum_MutatedMAX) {
        SET_CloseAllMutater();
        SET_OpenOneMutater((enum enum_Mutated)MutatedNo);
    }

    int i;
    S_Element* eeee;

    int Number_table[] = {2, 4, 6, 1234, 0x1234};

    eeee = CREAT_ElementNumberEnum(enum_Yes, 0x1234, Number_table, 5);

    for (i = 0; i < eeee->count; i++) {

        hw_printf("%d value =   0x%x\r\n", i, *(s32*)GET_MutatedValueSequence(eeee, i));
        FREE_MutatedValue(eeee);
    }

    FREE_Element(eeee);
    SET_OpenAllMutater();
    CLEAR_FuzzEnvironment();
    hw_printf("end ---- TEST_MutableNumberEnum\n");
}

/*

测试整形范围变异算法

*/
void TEST_MutableNumberRange(int MutatedNo)
{
    hw_printf("start ---- TEST_MutableNumberRange\n");

    INIT_FuzzEnvironment();

    if (MutatedNo != enum_MutatedMAX) {
        SET_CloseAllMutater();
        SET_OpenOneMutater((enum enum_Mutated)MutatedNo);
    }

    int i;
    S_Element* eeee;

    eeee = CREAT_ElementNumberRange(enum_Yes, 0x1234, 0x1200, 0x1300);

    for (i = 0; i < eeee->count; i++) {

        hw_printf("%d value =   0x%x\r\n", i, *(s32*)GET_MutatedValueSequence(eeee, i));
        FREE_MutatedValue(eeee);
    }

    FREE_Element(eeee);
    SET_OpenAllMutater();
    CLEAR_FuzzEnvironment();
    hw_printf("end ---- TEST_MutableNumberRange\n");
}

/*

测试字符串类型变异算法

*/
void TEST_MutableString(int MutatedNo)
{
    hw_printf("start ---- test_MutableString\n");

    INIT_FuzzEnvironment();

    if (MutatedNo != enum_MutatedMAX) {
        SET_CloseAllMutater();
        SET_OpenOneMutater((enum enum_Mutated)MutatedNo);
    }

    int i;
    S_Element* eeee;

    char* aaa = "-100aaaaaaaaaaaaaaaaaaaaaaaaCCCCCCCCCCCCCCCCCCCCCCCCCCCCC";

    eeee = CREAT_ElementString(enum_Yes, aaa, in_strlen(aaa) + 1, 20000);

    for (i = 0; i < eeee->count; i++) {

        char* ccc = (char*)GET_MutatedValueSequence(eeee, i);
        int bbb = GET_MutatedValueLen(eeee);
        hw_printf("%d value =    len=%d\r\n", i, bbb);
        HEX_Dump((u8*)ccc, bbb);

        FREE_MutatedValue(eeee);
    }

    FREE_Element(eeee);
    SET_OpenAllMutater();
    CLEAR_FuzzEnvironment();
    hw_printf("end ---- test_MutableString\n");
}

/*

测试字符串类型变异算法

*/
void TEST_MutableStringEnum(int MutatedNo)
{
    hw_printf("start ---- TEST_MutableStringEnum\n");

    INIT_FuzzEnvironment();

    if (MutatedNo != enum_MutatedMAX) {
        SET_CloseAllMutater();
        SET_OpenOneMutater((enum enum_Mutated)MutatedNo);
    }

    int i;
    S_Element* eeee;

    char* aaa = "12345";

    char* String_table[] = {"123", "abc", "xxx", "1.1.1", "aaa", "12345"};

    eeee = CREAT_ElementStringEnum(enum_Yes, aaa, in_strlen(aaa) + 1, 20000, String_table, 6);

    for (i = 0; i < eeee->count; i++) {

        char* ccc = (char*)GET_MutatedValueSequence(eeee, i);
        int bbb = GET_MutatedValueLen(eeee);
        hw_printf("%d value =    len=%d\r\n", i, bbb);
        HEX_Dump((u8*)ccc, bbb);

        FREE_MutatedValue(eeee);
    }

    FREE_Element(eeee);
    SET_OpenAllMutater();
    CLEAR_FuzzEnvironment();
    hw_printf("end ---- TEST_MutableStringEnum\n");
}

/*****************************************

测试blob类型变异算法

*****************************************/
void TEST_MutableBlob(int MutatedNo)
{
    hw_printf("start ---- test_MutableBlob\n");

    INIT_FuzzEnvironment();
    OPEN_Log();

    if (MutatedNo != enum_MutatedMAX) {
        SET_CloseAllMutater();
        SET_OpenOneMutater((enum enum_Mutated)MutatedNo);
    }

    int i;
    S_Element* eeee;

    // char aaa[15]={0x31,0x31,0x31,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0xFF,0xFF,0xFF};
    char aaa[15] = {0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x40, 0x41, 0x42, 0x43, 0x44, 0x45};

    eeee = CREAT_ElementBlob(enum_Yes, aaa, 15, 0);

    for (i = 0; i < eeee->count; i++) {
        char* ccc = (char*)GET_MutatedValueSequence(eeee, i);
        hw_printf("%d value =  len = %d\r\n", i, GET_MutatedValueLen(eeee));
        HEX_Dump((u8*)ccc, GET_MutatedValueLen(eeee));

        FREE_MutatedValue(eeee);
    }

    FREE_Element(eeee);
    SET_OpenAllMutater();
    CLEAR_FuzzEnvironment();
    CLOSE_Log();
    hw_printf("end ---- test_MutableBlob\n");
}

/*****************************************

测试blob类型变异算法

*****************************************/
void TEST_MutableBlobEnum(int MutatedNo)
{
    hw_printf("start ---- TEST_MutableBlobEnum\n");

    INIT_FuzzEnvironment();
    OPEN_Log();

    if (MutatedNo != enum_MutatedMAX) {
        SET_CloseAllMutater();
        SET_OpenOneMutater((enum enum_Mutated)MutatedNo);
    }

    int i;
    S_Element* eeee;
    char* Blob_table[] = {"123111", "abcaaa", "\x00\x01\x02\x00\xff", "6666666666666"};
    int Blob_L_table[] = {7, 7, 5, 14};

    eeee = CREAT_ElementBlobEnum(enum_Yes, "123111", 7, 0, Blob_table, Blob_L_table, 4);

    for (i = 0; i < eeee->count; i++) {
        char* ccc = (char*)GET_MutatedValueSequence(eeee, i);
        hw_printf("%d value =  len = %d\r\n", i, GET_MutatedValueLen(eeee));
        HEX_Dump((u8*)ccc, GET_MutatedValueLen(eeee));

        FREE_MutatedValue(eeee);
    }

    FREE_Element(eeee);
    SET_OpenAllMutater();
    CLEAR_FuzzEnvironment();
    CLOSE_Log();
    hw_printf("end ---- TEST_MutableBlobEnum\n");
}

/*****************************************

测试fixblob类型变异算法

*****************************************/
void TEST_MutableFixBlob(int MutatedNo)
{
    hw_printf("start ---- TEST_MutableFixBlob\n");

    INIT_FuzzEnvironment();

    if (MutatedNo != enum_MutatedMAX) {
        SET_CloseAllMutater();
        SET_OpenOneMutater((enum enum_Mutated)MutatedNo);
    }

    int i;
    S_Element* eeee;

    char aaa[15] = {0x31, 0x31, 0x31, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF};

    eeee = CREAT_ElementFixBlob(enum_Yes, aaa, 15, 0);

    for (i = 0; i < eeee->count; i++) {
        char* ccc = (char*)GET_MutatedValueSequence(eeee, i);
        hw_printf("%d value =  len = %d\r\n", i, GET_MutatedValueLen(eeee));
        HEX_Dump((u8*)ccc, GET_MutatedValueLen(eeee));

        FREE_MutatedValue(eeee);
    }

    FREE_Element(eeee);
    SET_OpenAllMutater();
    CLEAR_FuzzEnvironment();
    hw_printf("end ---- TEST_MutableFixBlob\n");
}

/*****************************************

测试ipv4类型变异算法

*****************************************/

//////////////////////////
static u8 temp_ipv4[4] = {192, 168, 0, 1};

void TEST_Ipv4(void)
{
    hw_printf("start ---- TEST_Ipv4\n");
    INIT_FuzzEnvironment();
    SET_CloseAllMutater();
    SET_OpenOneMutater(enum_MIpv4);

    int i = 0;
    S_Element pElement = {{{0}}};
    CREAT_ElementEX(&pElement, enum_No, enum_Yes, enum_Ipv4, (char*)temp_ipv4, 32);

    for (i = 0; i < pElement.count; i++) {
        u32 uipv4 = *(u32*)GET_MutatedValueSequence(&pElement, i);
        char cipv4[4];

        hw_Memcpy(cipv4, &uipv4, 4);
        hw_printf(
            "testcase is %d  ipv4 is %d.%d.%d.%d----\n", i, (u8)cipv4[0], (u8)cipv4[1], (u8)cipv4[2], (u8)cipv4[3]);
        FREE_MutatedValue(&pElement);
    }

    FREE_Element(&pElement);
    CLEAR_FuzzEnvironment();
    hw_printf("end ---- TEST_Ipv4\n");
}

/*****************************************

测试ipv6类型变异算法

*****************************************/
#ifndef __KERNEL__
#ifndef _WIN32
#include <arpa/inet.h>
static u8 temp_ipv6[16] = {
    0x20, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01};

void TEST_Ipv6(void)
{
    hw_printf("start ---- TEST_Ipv6\n");
    INIT_FuzzEnvironment();
    SET_CloseAllMutater();
    SET_OpenOneMutater(enum_MIpv6);

    int i = 0;
    S_Element pElement = {{{0}}};
    CREAT_ElementEX(&pElement, enum_No, enum_Yes, enum_Ipv6, (char*)temp_ipv6, 128);

    for (i = 0; i < pElement.count; i++) {
        char* uipv6 = GET_MutatedValueSequence(&pElement, i);
        char ip_str[64];

        inet_ntop(AF_INET6, uipv6, ip_str, sizeof(ip_str));
        hw_printf("testcase is %d ipv6 is   %s  \r\n ", i, ip_str);

        FREE_MutatedValue(&pElement);
    }

    FREE_Element(&pElement);
    CLEAR_FuzzEnvironment();
    hw_printf("end ---- TEST_Ipv6\n");
}
#endif
#endif
/*****************************************

测试mac类型变异算法

*****************************************/
static u8 temp_mac[6] = {0x28, 0x6e, 0xd4, 0x89, 0x26, 0xa8};

void TEST_Mac(void)
{
    hw_printf("start ---- TEST_Mac\n");
    INIT_FuzzEnvironment();
    SET_CloseAllMutater();
    SET_OpenOneMutater(enum_MMac);

    int i = 0;
    S_Element pElement = {{{0}}};
    CREAT_ElementEX(&pElement, enum_No, enum_Yes, enum_Mac, (char*)temp_mac, 48);

    for (i = 0; i < pElement.count; i++) {
        char* umac = GET_MutatedValueSequence(&pElement, i);

        char cmac[6];

        hw_Memcpy(cmac, umac, 6);
        hw_printf("testcase is %d mac is %02x%02x-%02x%02x-%02x%02x-----\n",
            i,
            (u8)cmac[0],
            (u8)cmac[1],
            (u8)cmac[2],
            (u8)cmac[3],
            (u8)cmac[4],
            (u8)cmac[5]);
        FREE_MutatedValue(&pElement);
    }

    FREE_Element(&pElement);
    CLEAR_FuzzEnvironment();
    hw_printf("end ---- TEST_Mac\n");
}

/******************************************

创建元素的接口

******************************************/

void TEST_CREAT_Element()
{

    hw_printf("start ---- TEST_CREAT_Element\n");

    INIT_FuzzEnvironment();

    int i;
    S_Element* eeee;
    void* value;

    hw_printf("test  CREAT_ElementU8");
    eeee = CREAT_ElementU8(enum_Yes, 18);
    for (i = 0; i < 10; i++) {
        value = GET_MutatedValueRandom(eeee);
        hw_printf("%d    value = %#04x\n", i, *(u8*)value);
        FREE_MutatedValue(eeee);
    }
    FREE_Element(eeee);

    hw_printf("test  CREAT_ElementU16");
    eeee = CREAT_ElementU16(enum_Yes, 18);
    for (i = 0; i < 10; i++) {
        value = GET_MutatedValueRandom(eeee);
        hw_printf("%d    value = %#06x\n", i, *(u16*)value);
        FREE_MutatedValue(eeee);
    }
    FREE_Element(eeee);

    hw_printf("test  CREAT_ElementU32");
    eeee = CREAT_ElementU32(enum_Yes, 18);
    for (i = 0; i < 10; i++) {
        value = GET_MutatedValueRandom(eeee);
        hw_printf("%d    value = %#010x\n", i, *(u32*)value);
        FREE_MutatedValue(eeee);
    }
    FREE_Element(eeee);

    hw_printf("test  CREAT_ElementU64");
    eeee = CREAT_ElementU64(enum_Yes, 18);
    for (i = 0; i < 10; i++) {
        value = GET_MutatedValueRandom(eeee);
        hw_printf("%d    value = %#018llx\n", i, *(u64*)value);
        FREE_MutatedValue(eeee);
    }
    FREE_Element(eeee);

    hw_printf("test  CREAT_ElementS8");
    eeee = CREAT_ElementS8(enum_Yes, 18);
    for (i = 0; i < 10; i++) {
        value = GET_MutatedValueRandom(eeee);
        hw_printf("%d    value = %#04x\n", i, *(u8*)value);
        FREE_MutatedValue(eeee);
    }
    FREE_Element(eeee);

    hw_printf("test  CREAT_ElementS16");
    eeee = CREAT_ElementS16(enum_Yes, 18);
    for (i = 0; i < 10; i++) {
        value = GET_MutatedValueRandom(eeee);
        hw_printf("%d    value = %#06x\n", i, *(u16*)value);
        FREE_MutatedValue(eeee);
    }
    FREE_Element(eeee);

    hw_printf("test  CREAT_ElementS32");
    eeee = CREAT_ElementS32(enum_Yes, 18);
    for (i = 0; i < 10; i++) {
        value = GET_MutatedValueRandom(eeee);
        hw_printf("%d    value = %#010x\n", i, *(u32*)value);
        FREE_MutatedValue(eeee);
    }
    FREE_Element(eeee);

    hw_printf("test  CREAT_ElementS64");
    eeee = CREAT_ElementS64(enum_Yes, 18);
    for (i = 0; i < 10; i++) {
        value = GET_MutatedValueRandom(eeee);
        hw_printf("%d    value = %#018llx\n", i, *(u64*)value);
        FREE_MutatedValue(eeee);
    }
    FREE_Element(eeee);

    hw_printf("test  CREAT_ElementNumberEnum");
    int Number_table[] = {2, 4, 6, 1234, 0x1234};
    eeee = CREAT_ElementNumberEnum(enum_Yes, 0x1234, Number_table, 5);
    for (i = 0; i < 10; i++) {
        value = GET_MutatedValueRandom(eeee);
        hw_printf("%d    value = %#010x\n", i, *(u32*)value);
        FREE_MutatedValue(eeee);
    }
    FREE_Element(eeee);

    hw_printf("test  CREAT_ElementNumberRange");
    eeee = CREAT_ElementNumberRange(enum_Yes, 0x1234, 0x1200, 0x1300);
    for (i = 0; i < 10; i++) {
        value = GET_MutatedValueRandom(eeee);
        hw_printf("%d    value = %#010x\n", i, *(u32*)value);
        FREE_MutatedValue(eeee);
    }
    FREE_Element(eeee);

    hw_printf("test  CREAT_ElementString");
    eeee = CREAT_ElementString(enum_Yes, "aaDbbWbcc", 10, 0);
    for (i = 0; i < 10; i++) {
        value = GET_MutatedValueRandom(eeee);

        //字符串变异算法有可能将\0变异掉，所以不用%s
        hw_printf("%d string value is :\n", i);
        HEX_Dump((u8*)value, (u32)GET_MutatedValueLen(eeee));
        FREE_MutatedValue(eeee);
    }
    FREE_Element(eeee);

    hw_printf("test  CREAT_ElementStringEnum");
    char* String_table[] = {"123", "abc", "xxx", "1.1.1", "aaa", "12345"};
    eeee = CREAT_ElementStringEnum(enum_Yes, "12345", 6, 0, String_table, 6);
    for (i = 0; i < 10; i++) {
        value = GET_MutatedValueRandom(eeee);

        //字符串变异算法有可能将\0变异掉，所以不用%s
        hw_printf("%d string value is :\n", i);
        HEX_Dump((u8*)value, (u32)GET_MutatedValueLen(eeee));
        FREE_MutatedValue(eeee);
    }
    FREE_Element(eeee);

    hw_printf("test  CREAT_ElementBlob");
    eeee = CREAT_ElementBlob(enum_Yes, "aaDbbWbcc", 9, 0);
    for (i = 0; i < 10; i++) {
        value = GET_MutatedValueRandom(eeee);
        hw_printf("%d ", i);
        HEX_Dump((u8*)value, GET_MutatedValueLen(eeee));
        FREE_MutatedValue(eeee);
    }
    FREE_Element(eeee);

    hw_printf("test  CREAT_ElementBlobEnum");
    char* Blob_table[] = {"123111", "abcaaa", "\x00\x01\x02\x00\xff", "6666666666666"};
    int Blob_L_table[] = {7, 7, 5, 14};
    eeee = CREAT_ElementBlobEnum(enum_Yes, "123111", 7, 0, Blob_table, Blob_L_table, 4);
    for (i = 0; i < 10; i++) {
        value = GET_MutatedValueRandom(eeee);
        hw_printf("%d ", i);
        HEX_Dump((u8*)value, GET_MutatedValueLen(eeee));
        FREE_MutatedValue(eeee);
    }
    FREE_Element(eeee);

    hw_printf("test  CREAT_ElementFixBlob");
    eeee = CREAT_ElementFixBlob(enum_Yes, "aaDbbWbcc", 9, 0);
    for (i = 0; i < 10; i++) {
        value = GET_MutatedValueRandom(eeee);
        hw_printf("%d ", i);
        HEX_Dump((u8*)value, GET_MutatedValueLen(eeee));
        FREE_MutatedValue(eeee);
    }
    FREE_Element(eeee);

    CLEAR_FuzzEnvironment();
    hw_printf("end ---- TEST_CREAT_Element\n");
}

/******************************************

获取测试例的接口

******************************************/

void TEST_GetSequenceValue(void)
{
    hw_printf("start ---- TEST_GetSequenceValue\n");

    INIT_FuzzEnvironment();

    INIT_Seed(100, 0);

    int i;

    S_Element* dddd = CREAT_ElementU32(enum_Yes, 0x12345678);

    for (i = 0; i < dddd->count; i++) {
        u32 u32temp = 0;

        u32temp = *(u32*)GET_MutatedValueRandom(dddd);

        hw_printf(" test muator is %d ----\n", i);
        hw_printf(" u32    value is 0x%x \n", u32temp);

        FREE_MutatedValue(dddd);
    }

    FREE_Element(dddd);

    CLEAR_FuzzEnvironment();
    hw_printf("end ---- TEST_GetSequenceValue\n");
}

void TEST_GetRandomValue(void)
{
    hw_printf("start ---- TEST_GetRandomValue\n");

    INIT_FuzzEnvironment();

    int i;
    S_Element* dddd = CREAT_ElementU32(enum_Yes, 0x12345678);

    ADD_WholeSequence(dddd);

    for (i = 0; i < 10; i++) {
        u32 u32temp = 0;
        u32temp = *(u32*)GET_MutatedValueSequence(dddd, i);

        hw_printf(" test muator is %d ----\n", i);
        hw_printf(" u32    value is 0x%x \n", u32temp);

        FREE_MutatedValue(dddd);
    }

    FREE_Element(dddd);

    CLEAR_FuzzEnvironment();
    hw_printf("end ---- TEST_GetRandomValue\n");
}

void TEST_GetS32SequenceValue()
{
    hw_printf("start ---- TEST_GetS32SequenceValue\n");

    INIT_FuzzEnvironment();

    int i;
    S_Element* eeee;

    eeee = CREAT_ElementS32(enum_Yes, 0x1234);

    for (i = 0; i < eeee->count; i++) {

        hw_printf("%d value =   %d\r\n", i, GET_MutatedValueSequenceS32(eeee, i));
        FREE_MutatedValue(eeee);
    }

    FREE_Element(eeee);
    CLEAR_FuzzEnvironment();
    hw_printf("end ---- TEST_GetS32SequenceValue\n");
}

void TEST_GetNumberEnumSequenceValue()
{
    hw_printf("start ---- TEST_GetNumberEnumSequenceValue\n");

    INIT_FuzzEnvironment();

    int i;
    S_Element* eeee;
    int Number_table[] = {2, 4, 6, 1234, 0x1234};

    eeee = CREAT_ElementNumberEnum(enum_Yes, 0x1234, Number_table, 5);

    for (i = 0; i < eeee->count; i++) {

        hw_printf("%d value =   %d\r\n", i, GET_MutatedValueSequenceNumberEnum(eeee, i));
        FREE_MutatedValue(eeee);
    }

    FREE_Element(eeee);
    CLEAR_FuzzEnvironment();
    hw_printf("end ---- TEST_GetNumberEnumSequenceValue\n");
}

void TEST_GetNumberRangeSequenceValue()
{
    hw_printf("start ---- TEST_GetNumberRangeSequenceValue\n");

    INIT_FuzzEnvironment();

    int i;
    S_Element* eeee;

    eeee = CREAT_ElementNumberRange(enum_Yes, 0x1234, 0x1200, 0x1300);

    for (i = 0; i < eeee->count; i++) {

        hw_printf("%d value =   %d\r\n", i, GET_MutatedValueSequenceNumberRange(eeee, i));
        FREE_MutatedValue(eeee);
    }

    FREE_Element(eeee);
    CLEAR_FuzzEnvironment();
    hw_printf("end ---- TEST_GetNumberRangeSequenceValue\n");
}

void TEST_GetStringSequenceValue()
{
    hw_printf("start ---- TEST_GetStringSequenceValue\n");

    INIT_FuzzEnvironment();

    int i;
    S_Element* eeee;

    char* aaa = "-100aaaaaaaaaaaaaaaaaaaaaaaaCCCCCCCCCCCCCCCCCCCCCCCCCCCCC";

    eeee = CREAT_ElementString(enum_Yes, aaa, in_strlen(aaa) + 1, 0);

    for (i = 0; i < eeee->count; i++) {

        char* ccc = GET_MutatedValueSequenceString(eeee, i);
        int bbb = GET_MutatedValueLen(eeee);
        hw_printf("%d value =    len=%d\r\n", i, bbb);
        HEX_Dump((u8*)ccc, bbb);

        FREE_MutatedValue(eeee);
    }

    FREE_Element(eeee);
    CLEAR_FuzzEnvironment();
    hw_printf("end ---- TEST_GetStringSequenceValue\n");
}

void TEST_GetStringEnumSequenceValue()
{
    hw_printf("start ---- TEST_GetStringEnumSequenceValue\n");

    INIT_FuzzEnvironment();

    int i;
    S_Element* eeee;

    char* aaa = "12345";

    char* String_table[] = {"123", "abc", "xxx", "1.1.1", "aaa", "12345"};

    eeee = CREAT_ElementStringEnum(enum_Yes, aaa, in_strlen(aaa) + 1, 0, String_table, 6);

    for (i = 0; i < eeee->count; i++) {

        char* ccc = GET_MutatedValueSequenceStringEnum(eeee, i);
        int bbb = GET_MutatedValueLen(eeee);
        hw_printf("%d value =    len=%d\r\n", i, bbb);
        HEX_Dump((u8*)ccc, bbb);

        FREE_MutatedValue(eeee);
    }

    FREE_Element(eeee);
    CLEAR_FuzzEnvironment();
    hw_printf("end ---- TEST_GetStringEnumSequenceValue\n");
}

void TEST_GetBlobSequenceValue()
{
    hw_printf("start ---- TEST_GetBlobSequenceValue\n");

    INIT_FuzzEnvironment();

    int i;
    S_Element* eeee;

    char aaa[15] = {0x31, 0x31, 0x31, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF};

    eeee = CREAT_ElementBlob(enum_Yes, aaa, 15, 0);

    for (i = 0; i < eeee->count; i++) {
        char* ccc = GET_MutatedValueSequenceBlob(eeee, i);
        hw_printf("%d value =  len = %d\r\n", i, GET_MutatedValueLen(eeee));
        HEX_Dump((u8*)ccc, GET_MutatedValueLen(eeee));

        FREE_MutatedValue(eeee);
    }

    FREE_Element(eeee);
    CLEAR_FuzzEnvironment();
    hw_printf("end ---- TEST_GetBlobSequenceValue\n");
}

void TEST_GetBlobEnumSequenceValue()
{
    hw_printf("start ---- TEST_GetBlobEnumSequenceValue\n");

    INIT_FuzzEnvironment();

    int i;
    S_Element* eeee;

    char* Blob_table[] = {"123111", "abcaaa", "\x00\x01\x02\x00\xff", "6666666666666"};
    int Blob_L_table[] = {7, 7, 5, 14};

    eeee = CREAT_ElementBlobEnum(enum_Yes, "123111", 7, 0, Blob_table, Blob_L_table, 4);

    for (i = 0; i < eeee->count; i++) {
        char* ccc = GET_MutatedValueSequenceBlobEnum(eeee, i);
        hw_printf("%d value =  len = %d\r\n", i, GET_MutatedValueLen(eeee));
        HEX_Dump((u8*)ccc, GET_MutatedValueLen(eeee));

        FREE_MutatedValue(eeee);
    }

    FREE_Element(eeee);
    CLEAR_FuzzEnvironment();
    hw_printf("end ---- TEST_GetBlobEnumSequenceValue\n");
}

void TEST_GetFixBlobSequenceValue()
{
    hw_printf("start ---- TEST_GetFixBlobSequenceValue\n");

    INIT_FuzzEnvironment();

    int i;
    S_Element* eeee;

    char aaa[15] = {0x31, 0x31, 0x31, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF};

    eeee = CREAT_ElementFixBlob(enum_Yes, aaa, 15, 0);

    for (i = 0; i < eeee->count; i++) {
        char* ccc = GET_MutatedValueSequenceFixBlob(eeee, i);
        hw_printf("%d value =  len = %d\r\n", i, GET_MutatedValueLen(eeee));
        HEX_Dump((u8*)ccc, GET_MutatedValueLen(eeee));

        FREE_MutatedValue(eeee);
    }

    FREE_Element(eeee);
    CLEAR_FuzzEnvironment();
    hw_printf("end ---- TEST_GetFixBlobSequenceValue\n");
}

void TEST_GetS32RandomValue()
{
    hw_printf("start ---- TEST_GetS32RandomValue\n");

    INIT_FuzzEnvironment();

    int i;
    S_Element* eeee;

    eeee = CREAT_ElementS32(enum_Yes, 0x1234);

    for (i = 0; i < eeee->count; i++) {
        INIT_Seed(1, i);
        hw_printf("%d value =   %d\r\n", i, GET_MutatedValueRandomS32(eeee));
        FREE_MutatedValue(eeee);
    }

    FREE_Element(eeee);
    CLEAR_FuzzEnvironment();
    hw_printf("end ---- TEST_GetS32RandomValue\n");
}

void TEST_GetNumberEnumRandomValue()
{
    hw_printf("start ---- TEST_GetNumberEnumRandomValue\n");

    INIT_FuzzEnvironment();

    int i;
    S_Element* eeee;

    int Number_table[] = {2, 4, 6, 1234, 0x1234};
    eeee = CREAT_ElementNumberEnum(enum_Yes, 0x1234, Number_table, 5);

    for (i = 0; i < eeee->count; i++) {
        INIT_Seed(1, i);
        hw_printf("%d value =   %d\r\n", i, GET_MutatedValueRandomNumberEnum(eeee));
        FREE_MutatedValue(eeee);
    }

    FREE_Element(eeee);
    CLEAR_FuzzEnvironment();
    hw_printf("end ---- TEST_GetNumberEnumRandomValue\n");
}

void TEST_GetNumberRangeRandomValue()
{
    hw_printf("start ---- TEST_GetNumberRangeRandomValue\n");

    INIT_FuzzEnvironment();

    int i;
    S_Element* eeee;

    eeee = CREAT_ElementNumberRange(enum_Yes, 0x1234, 0x1200, 0x1300);

    for (i = 0; i < eeee->count; i++) {
        INIT_Seed(1, i);
        hw_printf("%d value =   %d\r\n", i, GET_MutatedValueRandomNumberRange(eeee));
        FREE_MutatedValue(eeee);
    }

    FREE_Element(eeee);
    CLEAR_FuzzEnvironment();
    hw_printf("end ---- TEST_GetNumberRangeRandomValue\n");
}

void TEST_GetStringRandomValue()
{
    hw_printf("start ---- TEST_GetStringRandomValue\n");

    INIT_FuzzEnvironment();

    int i;
    S_Element* eeee;

    char* aaa = "-100aaaaaaaaaaaaaaaaaaaaaaaaCCCCCCCCCCCCCCCCCCCCCCCCCCCCC";

    eeee = CREAT_ElementString(enum_Yes, aaa, in_strlen(aaa) + 1, 0);

    for (i = 0; i < eeee->count; i++) {
        INIT_Seed(1, i);
        char* ccc = GET_MutatedValueRandomString(eeee);
        int bbb = GET_MutatedValueLen(eeee);
        hw_printf("%d value =    len=%d\r\n", i, bbb);
        HEX_Dump((u8*)ccc, bbb);

        FREE_MutatedValue(eeee);
    }

    FREE_Element(eeee);
    CLEAR_FuzzEnvironment();
    hw_printf("end ---- TEST_GetStringRandomValue\n");
}

void TEST_GetStringEnumRandomValue()
{
    hw_printf("start ---- TEST_GetStringEnumRandomValue\n");

    INIT_FuzzEnvironment();

    int i;
    S_Element* eeee;

    char* aaa = "12345";

    char* String_table[] = {"123", "abc", "xxx", "1.1.1", "aaa", "12345"};

    eeee = CREAT_ElementStringEnum(enum_Yes, aaa, in_strlen(aaa) + 1, 0, String_table, 6);

    for (i = 0; i < eeee->count; i++) {
        INIT_Seed(1, i);
        char* ccc = GET_MutatedValueRandomStringEnum(eeee);
        int bbb = GET_MutatedValueLen(eeee);
        hw_printf("%d value =    len=%d\r\n", i, bbb);
        HEX_Dump((u8*)ccc, bbb);

        FREE_MutatedValue(eeee);
    }

    FREE_Element(eeee);
    CLEAR_FuzzEnvironment();
    hw_printf("end ---- TEST_GetStringEnumRandomValue\n");
}

void TEST_GetBlobRandomValue()
{
    hw_printf("start ---- TEST_GetBlobRandomValue\n");

    INIT_FuzzEnvironment();

    int i;
    S_Element* eeee;

    char aaa[15] = {0x31, 0x31, 0x31, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF};

    eeee = CREAT_ElementBlob(enum_Yes, aaa, 15, 0);

    for (i = 0; i < eeee->count; i++) {
        INIT_Seed(1, i);
        char* ccc = GET_MutatedValueRandomBlob(eeee);
        hw_printf("%d value =  len = %d\r\n", i, GET_MutatedValueLen(eeee));
        HEX_Dump((u8*)ccc, GET_MutatedValueLen(eeee));

        FREE_MutatedValue(eeee);
    }

    FREE_Element(eeee);
    CLEAR_FuzzEnvironment();
    hw_printf("end ---- TEST_GetBlobRandomValue\n");
}

void TEST_GetBlobEnumRandomValue()
{
    hw_printf("start ---- TEST_GetBlobEnumRandomValue\n");

    INIT_FuzzEnvironment();

    int i;
    S_Element* eeee;

    char* Blob_table[] = {"123111", "abcaaa", "\x00\x01\x02\x00\xff", "6666666666666"};
    int Blob_L_table[] = {7, 7, 5, 14};

    eeee = CREAT_ElementBlobEnum(enum_Yes, "123111", 7, 0, Blob_table, Blob_L_table, 4);

    for (i = 0; i < eeee->count; i++) {
        INIT_Seed(1, i);
        char* ccc = GET_MutatedValueRandomBlobEnum(eeee);
        hw_printf("%d value =  len = %d\r\n", i, GET_MutatedValueLen(eeee));
        HEX_Dump((u8*)ccc, GET_MutatedValueLen(eeee));

        FREE_MutatedValue(eeee);
    }

    FREE_Element(eeee);
    CLEAR_FuzzEnvironment();
    hw_printf("end ---- TEST_GetBlobEnumRandomValue\n");
}

void TEST_GetFixBlobRandomValue()
{
    hw_printf("start ---- TEST_GetFixBlobRandomValue\n");

    INIT_FuzzEnvironment();

    int i;
    S_Element* eeee;

    char aaa[15] = {0x31, 0x31, 0x31, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF};

    eeee = CREAT_ElementFixBlob(enum_Yes, aaa, 15, 0);

    for (i = 0; i < eeee->count; i++) {
        INIT_Seed(1, i);
        char* ccc = GET_MutatedValueRandomFixBlob(eeee);
        hw_printf("%d value =  len = %d\r\n", i, GET_MutatedValueLen(eeee));
        HEX_Dump((u8*)ccc, GET_MutatedValueLen(eeee));

        FREE_MutatedValue(eeee);
    }

    FREE_Element(eeee);
    CLEAR_FuzzEnvironment();
    hw_printf("end ---- TEST_GetFixBlobRandomValue\n");
}

/******************************************

//设置变异种子，同样的seed和range回放出来的测试例是一样的
//如果seed设置为0，则系统会随机根据time函数生成seed,如果系统不支持time函数，则seed设置成1

******************************************/

void TEST_INIT_Seed(int MutatedNo)
{
    hw_printf("start ---- TEST_INIT_Seed\n");

    INIT_FuzzEnvironment();

    int i;
    S_Element* eeee;

    char* aaa = "-100aaaaaaaaaaaaaaaaaaaaaaaaCCCCCCCCCCCCCCCCCCCCCCCCCCCCC";

    eeee = CREAT_ElementString(enum_Yes, aaa, in_strlen(aaa) + 1, 0);

    for (i = 0; i < 10; i++) {  //如果想要回放，则随机fuzz时每次获取变量时先设置种子
        INIT_Seed(100, i);
        char* ccc = (char*)GET_MutatedValueRandom(eeee);
        int bbb = GET_MutatedValueLen(eeee);
        hw_printf("%d value =    len=%d\r\n", i, bbb);
        HEX_Dump((u8*)ccc, bbb);

        FREE_MutatedValue(eeee);
    }

    hw_printf("------------\r\n");
    INIT_Seed(100, 5);
    char* ccc = (char*)GET_MutatedValueRandom(eeee);
    int bbb = GET_MutatedValueLen(eeee);
    hw_printf("%d value =    len=%d\r\n", 105, bbb);
    HEX_Dump((u8*)ccc, bbb);

    FREE_MutatedValue(eeee);

    FREE_Element(eeee);
    CLEAR_FuzzEnvironment();
    hw_printf("end ---- TEST_INIT_Seed\n");
}

/*****************************************

全局随机函数

*****************************************/

////多个变量整体随机变异
void TEST_WholeMultiVarientRandom(void)
{
    hw_printf("start ---- TEST_WholeMultiVarientRandom\n");

    INIT_FuzzEnvironment();
    OPEN_Log();

    INIT_Seed(100, 0);

    int i;

    S_Element* cccc = CREAT_ElementString(enum_Yes, "aaaabbbb", 9, 0);
    S_Element* dddd = CREAT_ElementU32(enum_Yes, 0x12345678);
    S_Element* eeee = CREAT_ElementBlob(enum_Yes, "192.168.0.100", 14, 0);

    ADD_WholeRandom(cccc);
    ADD_WholeRandom(dddd);
    ADD_WholeRandom(eeee);

    for (i = 0; i < 10; i++) {
        char* stringtemp = 0;
        u32 u32temp = 0;
        char* blobtemp = 0;

        stringtemp = (char*)GET_MutatedValueRandom(cccc);
        u32temp = *(u32*)GET_MutatedValueRandom(dddd);
        blobtemp = (char*)GET_MutatedValueRandom(eeee);

        hw_printf(" test muator is %d ----\n", i);
        //字符串变异算法有可能将\0变异掉，所以不用%s
        hw_printf(" string value is :\n");
        HEX_Dump((u8*)stringtemp, (u32)GET_MutatedValueLen(cccc));
        //
        hw_printf(" u32    value is 0x%x \n", u32temp);
        //
        HEX_Dump((u8*)blobtemp, (u32)GET_MutatedValueLen(eeee));

        FREE_MutatedValue(cccc);
        FREE_MutatedValue(dddd);
        FREE_MutatedValue(eeee);
    }

    FREE_Element(cccc);
    FREE_Element(dddd);
    FREE_Element(eeee);

    CLEAR_FuzzEnvironment();
    hw_printf("end ---- TEST_WholeMultiVarientRandom\n");
}

////多个变量整体顺序变异
void TEST_WholeMutiVarientSequence(void)
{
    hw_printf("start ---- TEST_WholeMutiVarientSequence\n");

    INIT_FuzzEnvironment();

    int i;
    S_Element* cccc = CREAT_ElementString(enum_Yes, "aaaabbbb", 9, 0);
    S_Element* dddd = CREAT_ElementU32(enum_Yes, 0x12345678);
    S_Element* eeee = CREAT_ElementBlob(enum_Yes, "192.168.0.100", 14, 0);

    ADD_WholeSequence(cccc);
    ADD_WholeSequence(dddd);
    ADD_WholeSequence(eeee);

    hw_printf(" WholeSequenceTotalNumber is %d \n", GET_WholeSequenceTotalNumber());

    for (i = 0; i < GET_WholeSequenceTotalNumber(); i++) {
        char* stringtemp = 0;
        u32 u32temp = 0;
        char* blobtemp = 0;

        stringtemp = (char*)GET_MutatedValueSequence(cccc, i);
        u32temp = *(u32*)GET_MutatedValueSequence(dddd, i);
        blobtemp = (char*)GET_MutatedValueSequence(eeee, i);

        hw_printf(" test muator is %d ----\n", i);
        //字符串变异算法有可能将\0变异掉，所以不用%s
        hw_printf(" string value is :\n");
        HEX_Dump((u8*)stringtemp, (u32)GET_MutatedValueLen(cccc));
        //
        hw_printf(" u32    value is 0x%x \n", u32temp);
        //
        HEX_Dump((u8*)blobtemp, (u32)GET_MutatedValueLen(eeee));

        FREE_MutatedValue(cccc);
        FREE_MutatedValue(dddd);
        FREE_MutatedValue(eeee);
    }

    FREE_Element(cccc);
    FREE_Element(dddd);
    FREE_Element(eeee);

    CLEAR_FuzzEnvironment();
    hw_printf("end ---- TEST_WholeMutiVarientSequence\n");
}

/*****************************************

变异算法开关

*****************************************/

void TEST_CloseAllMutater()
{
    hw_printf("start ---- TEST_CloseAllMutater\n");

    INIT_FuzzEnvironment();

    OPEN_Log();
    SET_CloseAllMutater();

    int i;
    S_Element* eeee;

    eeee = CREAT_ElementU32(enum_Yes, 0x1234);

    for (i = 0; i < eeee->count; i++) {
        char* aaa = GET_MutatedValueSequence(eeee, i);
        hw_printf("%d value =   0x%x\r\n", i, *(u32*)aaa);
        FREE_MutatedValue(eeee);
    }

    FREE_Element(eeee);
    CLOSE_Log();
    CLEAR_FuzzEnvironment();
    hw_printf("end ---- TEST_CloseAllMutater\n");
}

void TEST_OpenAllMutater()
{
    hw_printf("start ---- TEST_OpenAllMutater\n");

    INIT_FuzzEnvironment();

    OPEN_Log();
    SET_OpenAllMutater();

    int i;
    S_Element* eeee;

    eeee = CREAT_ElementU32(enum_Yes, 0x1234);

    for (i = 0; i < eeee->count; i++) {
        char* aaa = GET_MutatedValueSequence(eeee, i);
        hw_printf("%d value =   0x%x\r\n", i, *(u32*)aaa);
        FREE_MutatedValue(eeee);
    }

    FREE_Element(eeee);
    CLOSE_Log();
    CLEAR_FuzzEnvironment();
    hw_printf("end ---- TEST_OpenAllMutater\n");
}

void TEST_CloseOneMutater()
{
    hw_printf("start ---- TEST_CloseOneMutater\n");

    INIT_FuzzEnvironment();

    OPEN_Log();
    SET_OpenAllMutater();
    SET_CloseOneMutater(enum_DataElementBitFlipper);

    int i;
    S_Element* eeee;

    eeee = CREAT_ElementU32(enum_Yes, 0x1234);

    for (i = 0; i < eeee->count; i++) {
        char* aaa = GET_MutatedValueSequence(eeee, i);
        hw_printf("%d value =   0x%x\r\n", i, *(u32*)aaa);
        FREE_MutatedValue(eeee);
    }

    FREE_Element(eeee);
    CLOSE_Log();
    CLEAR_FuzzEnvironment();
    hw_printf("end ---- TEST_CloseOneMutater\n");
}

void TEST_OpenOneMutater()
{
    hw_printf("start ---- TEST_OpenOneMutater\n");

    INIT_FuzzEnvironment();

    OPEN_Log();
    SET_CloseAllMutater();
    SET_OpenOneMutater(enum_DataElementBitFlipper);

    int i;
    S_Element* eeee;

    eeee = CREAT_ElementU32(enum_Yes, 0x1234);

    for (i = 0; i < eeee->count; i++) {
        char* aaa = GET_MutatedValueSequence(eeee, i);
        hw_printf("%d value =   0x%x\r\n", i, *(u32*)aaa);
        FREE_MutatedValue(eeee);
    }

    FREE_Element(eeee);
    CLOSE_Log();
    CLEAR_FuzzEnvironment();
    hw_printf("end ---- TEST_OpenOneMutater\n");
}

void TEST_ElementCloseAllMutater()
{
    hw_printf("start ---- TEST_ElementCloseAllMutater\n");

    INIT_FuzzEnvironment();

    OPEN_Log();

    S_Element dddd = {{{0}}};
    S_Element cccc = {{{0}}};
    s32 temp = 0x12345678;

    SET_ElementCloseAllMutater(&dddd);

    hw_printf("creat dddd\n");
    CREAT_ElementEX(&dddd, enum_No, enum_Yes, enum_NumberU, (char*)&temp, 32);
    hw_printf("creat cccc\n");
    CREAT_ElementEX(&cccc, enum_No, enum_Yes, enum_NumberU, (char*)&temp, 32);

    FREE_Element(&dddd);
    FREE_Element(&cccc);
    CLOSE_Log();
    CLEAR_FuzzEnvironment();
    hw_printf("end ---- TEST_ElementCloseAllMutater\n");
}

void TEST_ElementOpenAllMutater()
{
    hw_printf("start ---- TEST_ElementOpenAllMutater\n");

    INIT_FuzzEnvironment();

    OPEN_Log();

    S_Element dddd = {{{0}}};
    S_Element cccc = {{{0}}};
    s32 temp = 0x12345678;

    SET_ElementOpenAllMutater(&dddd);

    hw_printf("creat dddd\n");
    CREAT_ElementEX(&dddd, enum_No, enum_Yes, enum_NumberU, (char*)&temp, 32);
    hw_printf("creat cccc\n");
    CREAT_ElementEX(&cccc, enum_No, enum_Yes, enum_NumberU, (char*)&temp, 32);

    FREE_Element(&dddd);
    FREE_Element(&cccc);
    CLOSE_Log();
    CLEAR_FuzzEnvironment();
    hw_printf("end ---- TEST_ElementOpenAllMutater\n");
}

void TEST_ElementCloseOneMutater()
{
    hw_printf("start ---- TEST_ElementCloseOneMutater\n");

    INIT_FuzzEnvironment();

    OPEN_Log();

    S_Element dddd = {{{0}}};
    S_Element cccc = {{{0}}};
    s32 temp = 0x12345678;

    SET_ElementCloseOneMutater(&dddd, enum_DataElementBitFlipper);

    hw_printf("creat dddd\n");
    CREAT_ElementEX(&dddd, enum_No, enum_Yes, enum_NumberU, (char*)&temp, 32);
    hw_printf("creat cccc\n");
    CREAT_ElementEX(&cccc, enum_No, enum_Yes, enum_NumberU, (char*)&temp, 32);

    FREE_Element(&dddd);
    FREE_Element(&cccc);
    CLOSE_Log();
    CLEAR_FuzzEnvironment();
    hw_printf("end ---- TEST_ElementCloseOneMutater\n");
}

void TEST_ElementOpenOneMutater()
{
    hw_printf("start ---- TEST_ElementOpenOneMutater\n");

    INIT_FuzzEnvironment();

    OPEN_Log();

    S_Element dddd = {{{0}}};
    S_Element cccc = {{{0}}};
    s32 temp = 0x12345678;

    SET_ElementCloseAllMutater(&dddd);
    SET_ElementOpenOneMutater(&dddd, enum_DataElementBitFlipper);

    hw_printf("creat dddd\n");
    CREAT_ElementEX(&dddd, enum_No, enum_Yes, enum_NumberU, (char*)&temp, 32);
    hw_printf("creat cccc\n");
    CREAT_ElementEX(&cccc, enum_No, enum_Yes, enum_NumberU, (char*)&temp, 32);

    FREE_Element(&dddd);
    FREE_Element(&cccc);
    CLOSE_Log();
    CLEAR_FuzzEnvironment();
    hw_printf("end ---- TEST_ElementOpenOneMutater\n");
}

/*****************************************

log函数
OPEN_Log
CLOSE_Log

*****************************************/

void TEST_Log()
{
    hw_printf("start ---- TEST_Log\n");

    INIT_FuzzEnvironment();
    OPEN_Log();

    int i;
    S_Element* eeee;

    eeee = CREAT_ElementU32(enum_Yes, 0x1234);

    // for(i=0; i<eeee->count; i++)
    for (i = 0; i < 5; i++) {
        char* aaa = GET_MutatedValueSequence(eeee, i);
        hw_printf("%d value =   0x%x\r\n", i, *(u32*)aaa);
        FREE_MutatedValue(eeee);
    }

    FREE_Element(eeee);
    CLOSE_Log();
    CLEAR_FuzzEnvironment();
    hw_printf("end ---- TEST_Log\n");
}

/*****************************************

调试函数
HEX_Dump
DEBUG_Element

*****************************************/

void TEST_HEX_Dump()
{
    hw_printf("start ---- TEST_HEX_Dump\n");

    INIT_FuzzEnvironment();

    int i;
    S_Element* eeee;

    eeee = CREAT_ElementU32(enum_Yes, 0x1234);

    for (i = 0; i < eeee->count; i++) {
        char* aaa = GET_MutatedValueSequence(eeee, i);
        hw_printf("%d value =   0x%x\r\n", i, *(u32*)aaa);

        HEX_Dump((u8*)aaa, GET_MutatedValueLen(eeee));
        FREE_MutatedValue(eeee);
    }

    FREE_Element(eeee);
    CLEAR_FuzzEnvironment();
    hw_printf("end ---- TEST_HEX_Dump\n");
}

void TEST_DEBUG_Element(void)
{
    hw_printf("start ---- TEST_DEBUG_Element\n");

    INIT_FuzzEnvironment();

    S_Element* eeee;

    eeee = CREAT_ElementU32(enum_Yes, 0x1234);

    DEBUG_Element(eeee);

    FREE_Element(eeee);
    CLEAR_FuzzEnvironment();
    hw_printf("end ---- TEST_DEBUG_Element\n");
}
