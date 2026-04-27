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
/*

本文件函数仅仅为对外接口的简单二次封装
为了用户调用方便
禁止体现核心逻辑

*/

#include "PCommon.h"

int fuzz_seed;
int fuzz_i;
int fuzz_start;
int fuzz_end;
S_ElementInit g_Element[max_para_num] = {{0}};
S_ElementInit g_ElementS[max_para_num] = {{0}};

static int temp_is_reproduce;
static int temp_count;
static int temp_run_time;
static char* temp_testcase_name;

void INIT_FuzzEnvironment(void)
{
    INIT_Common();
}

void CLEAR_FuzzEnvironment(void)
{
    hw_Memset(g_Element, 0, sizeof(g_Element));
    hw_Memset(g_ElementS, 0, sizeof(g_ElementS));
    CLEAR_Common();
}

void dt_start(int seed, int count, char* testcase_name, int is_reproduce)
{
    fuzz_seed = seed;
    INIT_FuzzEnvironment();
    Corpus_Set_Path(testcase_name);
    Report_Set_Running_Testcase_Name(testcase_name);
    temp_is_reproduce = is_reproduce;
    temp_count = count;
    temp_run_time = Report_get_time();
    temp_testcase_name = testcase_name;
    if ((is_reproduce) && (is_reproduce != 0xffffffff)) {
        fuzz_start = 0;
        fuzz_end = 1;
    } else {
        fuzz_start = 0;
        fuzz_end = count;
    }

    Running_Time_get_start();
    TimeOut_get_start_time();
}

void dt_for_start(void)
{
    INIT_Seed(fuzz_seed, fuzz_i);
    Corpus_Start(temp_is_reproduce);
}

void dt_for_end(void)
{
    TimeOut_Is_Bug();
    Llvm_Do_8bit_counters();
    Llvm_Do_Leak_Check();
    Corpus_End();
}

void dt_end(void)
{
    temp_run_time = Report_get_time() - temp_run_time;
    Llvm_Dump_Coverage();
    if (temp_is_reproduce == 0)
        Report_write_succeed_testcase(temp_testcase_name, fuzz_seed, temp_count, temp_run_time);
    Report_Set_Running_Testcase_Name(NULL);
    Corpus_Show_ALL();
    DT_Clear(g_Element);
    CLEAR_FuzzEnvironment();
}

static int FuzzCreateElement(S_ElementInit* pInit)
{
    S_Element* p = NULL;
    switch (pInit->type) {
        case enum_String:
            p = CREAT_ElementString(pInit->isHasInitValue, pInit->initValueBuffer, pInit->len, pInit->maxLen);
            break;

        case enum_String_Enum:
            p = CREAT_ElementStringEnum(pInit->isHasInitValue,
                pInit->initValueBuffer,
                pInit->len,
                pInit->maxLen,
                pInit->Enum_string_table,
                pInit->Enum_count);
            break;

        case enum_Blob:
            p = CREAT_ElementBlob(pInit->isHasInitValue, pInit->initValueBuffer, pInit->len, pInit->maxLen);
            break;

        case enum_Blob_Enum:
            p = CREAT_ElementBlobEnum(pInit->isHasInitValue,
                pInit->initValueBuffer,
                pInit->len,
                pInit->maxLen,
                pInit->Enum_blob_table,
                pInit->Enum_blob_l_table,
                pInit->Enum_count);
            break;

        case enum_FixBlob:
            p = CREAT_ElementFixBlob(pInit->isHasInitValue, pInit->initValueBuffer, pInit->len, pInit->maxLen);
            break;

        case enum_Ipv4:
            p = CREAT_ElementIpv4(pInit->isHasInitValue, pInit->initValueBuffer);
            break;

        case enum_Ipv6:
            p = CREAT_ElementIpv6(pInit->isHasInitValue, pInit->initValueBuffer);
            break;

        case enum_Mac:
            p = CREAT_ElementMac(pInit->isHasInitValue, pInit->initValueBuffer);
            break;
        case enum_AFL:
            break;
        case enum_NumberU:
            if (8 >= pInit->len)
                p = CREAT_ElementU8(pInit->isHasInitValue, (u8)pInit->initValue);
            else if (16 >= pInit->len)
                p = CREAT_ElementU16(pInit->isHasInitValue, (u16)pInit->initValue);
            else if (32 >= pInit->len)
                p = CREAT_ElementU32(pInit->isHasInitValue, (u32)pInit->initValue);
            else if (64 >= pInit->len)
                p = CREAT_ElementU64(pInit->isHasInitValue, (u64)pInit->initValue);
            break;
        case enum_NumberS:
            if (8 >= pInit->len)
                p = CREAT_ElementS8(pInit->isHasInitValue, (s8)pInit->initValue);
            else if (16 >= pInit->len)
                p = CREAT_ElementS16(pInit->isHasInitValue, (s16)pInit->initValue);
            else if (32 >= pInit->len)
                p = CREAT_ElementS32(pInit->isHasInitValue, (s32)pInit->initValue);
            else if (64 >= pInit->len)
                p = CREAT_ElementS64(pInit->isHasInitValue, (s64)pInit->initValue);
            break;
        case enum_Number_Enum:
            p = CREAT_ElementNumberEnum(
                pInit->isHasInitValue, (s32)pInit->initValue, pInit->Enum_number_table, pInit->Enum_count);
            break;
        case enum_Number_Range:
            p = CREAT_ElementNumberRange(pInit->isHasInitValue, (s32)pInit->initValue, pInit->min, pInit->max);
            break;
        default:
            M_DEBUG(1, "do not find type of the element\n");
            return -1;
    }
    pInit->element = p;
    return 0;
}

void DT_CleanElementInit(S_ElementInit* pInit, int count)
{
    int i;

    ASSERT_NULL(pInit);
    ASSERT(count <= 0);
    ASSERT(count > max_para_num);

    for (i = 0; i < count; i++) {
        if (pInit[i].isneedfree == enum_Yes) {
            hw_Free(pInit[i].initValueBuffer);
            pInit[i].initValueBuffer = NULL;
            pInit[i].isneedfree = enum_No;
        }

        if (pInit[i].element) {
            FREE_MutatedValue(pInit[i].element);
            FREE_Element(pInit[i].element);
            pInit[i].element = NULL;
        }
    }
}
extern int is_need_mutator;
char* DT_GetFuzzValueEX(S_ElementInit* pInit, int count, char* pValue)
{
    int first = 0;  // 第一次调迭代不变异控制
    int i;
    int len = 0;
    char* src = NULL;
    char* dest;

    ASSERT_NULL(pInit);
    ASSERT(count <= 0);
    ASSERT(count > max_para_num);

    if (1 != pInit[0].first) {
        for (i = 0; i < count; i++) {
            if (pInit[i].isuse == 0)
                continue;

            pInit[i].first = 1;
            first = 1;

            if (pInit[i].len > 0)
                pInit[i].isHasInitValue = enum_Yes;  // 调用此接口都有初始值,谁说的
            else
                pInit[i].isHasInitValue = enum_No;
            FuzzCreateElement(&pInit[i]);
            ADD_WholeRandom(pInit[i].element);
        }
    }

    for (i = 0; i < count; i++) {
        if (pInit[i].isuse == 0)
            continue;

        // get mutation data value
        if (pInit[i].element) {
            FREE_MutatedValue(pInit[i].element);
        }
        dest = pValue;
        if ((i == 0 && pInit[0].structStart > 0) || (i > 0)) {
            if (dest != NULL)
                dest += pInit[i].structStart;
        }

        //如果在执行样本本身，则不变异
        if ((first) || (is_need_mutator == 0)) {
            src = pInit[i].element->inBuf;
            pInit[i].element->para.value = src;
            pInit[i].element->para.len = pInit[i].element->inLen / 8;
            pInit[i].element->pos = PosOriginal;
            hw_Memcpy(pInit[i].element->para.mutater_name, "default value", in_strlen("default value") + 1);
        } else {
            src = GET_MutatedValueRandom(pInit[i].element);
        }

        len = (int)(pInit[i].element->para.len);
        if (dest != NULL) {
            hw_Memcpy(dest, src, len);
        }
    }
    return src;
}

char* DT_SetGetS64(S_ElementInit* init, s64 initValue)
{
    if (init->first != 1) {
        init->isuse = 1;
        init->type = enum_NumberS;
        init->len = 64;
        init->initValue = initValue;
    }

    return DT_GetFuzzValueEX(init, 1, 0);
}

char* DT_SetGetS32(S_ElementInit* init, s32 initValue)
{
    if (init->first != 1) {
        init->isuse = 1;
        init->type = enum_NumberS;
        init->len = 32;
        init->initValue = initValue;
    }

    return DT_GetFuzzValueEX(init, 1, 0);
}

char* DT_SetGetS16(S_ElementInit* init, s16 initValue)
{
    if (init->first != 1) {
        init->isuse = 1;
        init->type = enum_NumberS;
        init->len = 16;
        init->initValue = initValue;
    }

    return DT_GetFuzzValueEX(init, 1, 0);
}

char* DT_SetGetS8(S_ElementInit* init, s8 initValue)
{
    if (init->first != 1) {
        init->isuse = 1;
        init->type = enum_NumberS;
        init->len = 8;
        init->initValue = initValue;
    }

    return DT_GetFuzzValueEX(init, 1, 0);
}

char* DT_SetGetU64(S_ElementInit* init, u64 initValue)
{
    if (init->first != 1) {
        init->isuse = 1;
        init->type = enum_NumberU;
        init->len = 64;
        init->initValue = initValue;
    }

    return DT_GetFuzzValueEX(init, 1, 0);
}

char* DT_SetGetU32(S_ElementInit* init, u32 initValue)
{
    if (init->first != 1) {
        init->isuse = 1;
        init->type = enum_NumberU;
        init->len = 32;
        init->initValue = initValue;
    }

    return DT_GetFuzzValueEX(init, 1, 0);
}

char* DT_SetGetU16(S_ElementInit* init, u16 initValue)
{
    if (init->first != 1) {
        init->isuse = 1;
        init->type = enum_NumberU;
        init->len = 16;
        init->initValue = initValue;
    }

    return DT_GetFuzzValueEX(init, 1, 0);
}

char* DT_SetGetU8(S_ElementInit* init, u8 initValue)
{
    if (init->first != 1) {
        init->isuse = 1;
        init->type = enum_NumberU;
        init->len = 8;
        init->initValue = initValue;
    }

    return DT_GetFuzzValueEX(init, 1, 0);
}

char* DT_SetGetNumberEnum_U8(S_ElementInit* init, u8 initValue, u8* eunmTable, int eunmCount)
{
    if (init->first != 1) {
        init->isuse = 1;
        init->type = enum_Number_Enum;
        init->len = 8;
        init->initValue = initValue;
 
        init->Enum_number_table = eunmTable;
        init->Enum_count = eunmCount;
    }
 
    return DT_GetFuzzValueEX(init, 1, 0);
}

char* DT_SetGetNumberEnum(S_ElementInit* init, s32 initValue, int* eunmTable, int eunmCount)
{
    if (init->first != 1) {
        init->isuse = 1;
        init->type = enum_Number_Enum;
        init->len = 32;
        init->initValue = initValue;

        init->Enum_number_table = eunmTable;
        init->Enum_count = eunmCount;
    }

    return DT_GetFuzzValueEX(init, 1, 0);
}

char* DT_SetGetNumberEnum_EX(S_ElementInit* init, s32 initValue, int* eunmTable, int eunmCount)
{
    if (init->first != 1) {
        init->isuse = 1;
        init->type = enum_NumberS;
        init->len = 32;
        init->initValue = eunmTable[RAND_32() % eunmCount];
    }

    char* aaa = DT_GetFuzzValueEX(init, 1, 0);

    int i = 0;
    //初值一定不要在枚举里
    //判断如果变异出来的数据在枚举里，则换为初值
    for (i = 0; i < eunmCount; i++) {
        if (*((s32*)aaa) == eunmTable[i]) {
            *((s32*)aaa) = initValue;
        }
    }

    return aaa;
}

char* DT_SetGetNumberRange(S_ElementInit* init, s32 initValue, int min, int max)
{
    if (init->first != 1) {
        init->isuse = 1;
        init->type = enum_Number_Range;
        init->len = 32;
        init->initValue = initValue;

        init->min = min;
        init->max = max;
    }

    return DT_GetFuzzValueEX(init, 1, 0);
}

char* DT_SetGetNumberRange_EX(S_ElementInit* init, s32 initValue, int min, int max)
{
    if (init->first != 1) {
        init->isuse = 1;
        init->type = enum_NumberS;
        init->len = 32;
        init->initValue = RAND_RANGE(min, max);
    }

    char* aaa = DT_GetFuzzValueEX(init, 1, 0);

    //初值一定不要在枚举里
    //判断如果变异出来的数据在范围里，则换为初值
    if (((*((s32*)aaa)) >= min) && ((*((s32*)aaa)) <= max)) {
        *((s32*)aaa) = initValue;
    }

    return aaa;
}

char* DT_SetGetFloat(S_ElementInit* init, float initValue)
{
    if (init->first != 1) {
        init->isuse = 1;
        init->type = enum_NumberU;
        init->len = 32;
        init->initValue = *(u32*)&initValue;
    }

    return DT_GetFuzzValueEX(init, 1, 0);
}

char* DT_SetGetDouble(S_ElementInit* init, double initValue)
{
    if (init->first != 1) {
        init->isuse = 1;
        init->type = enum_NumberU;
        init->len = 64;
        init->initValue = *(u64*)&initValue;
    }

    return DT_GetFuzzValueEX(init, 1, 0);
}

void DT_initValueBuffer(S_ElementInit* init, int length, char* initValue)
{
    if (length > 0) {
        init->isneedfree = enum_Yes;
        init->initValueBuffer = hw_Malloc(length);
        hw_Memcpy(init->initValueBuffer, initValue, length);
    }
}

char* DT_SetGetString(S_ElementInit* init, int length, int maxLength, char* initValue)
{
    if (init->first != 1) {
        init->isuse = 1;
        init->type = enum_String;
        init->len = length;  //字符串长度，strlen(str) + 1
        init->maxLen = maxLength;
        DT_initValueBuffer(init, length, initValue);
    }

    return DT_GetFuzzValueEX(init, 1, 0);
}

char* DT_SetGetStringEnum(
    S_ElementInit* init, int length, int maxLength, char* initValue, char* eunmTableS[], int eunmCount)
{
    if (init->first != 1) {
        init->isuse = 1;
        init->type = enum_String_Enum;
        init->len = length;  //字符串长度，strlen(str) + 1
        init->maxLen = maxLength;
        DT_initValueBuffer(init, length, initValue);

        init->Enum_string_table = eunmTableS;
        init->Enum_count = eunmCount;
    }

    return DT_GetFuzzValueEX(init, 1, 0);
}

char* DT_SetGetStringEnum_EX(
    S_ElementInit* init, int length, int maxLength, char* initValue, char* eunmTableS[], int eunmCount)
{
    if (init->first != 1) {
        init->isuse = 1;
        init->type = enum_String;
        init->len = length;  //字符串长度，strlen(str) + 1
        init->maxLen = maxLength;

        int pos = RAND_32() % eunmCount;
        DT_initValueBuffer(init, in_strlen(eunmTableS[pos]) + 1, eunmTableS[pos]);
        init->len = in_strlen(eunmTableS[pos]) + 1;
    }

    DT_GetFuzzValueEX(init, 1, 0);

    int i = 0;
    //初值一定不要在枚举里
    //判断如果变异出来的数据在枚举里，则换为初值
    for (i = 0; i < eunmCount; i++) {
        if ((in_strlen(eunmTableS[i]) + 1) == init->element->para.len) {
            if (hw_Memcmp(eunmTableS[i], init->element->para.value, init->element->para.len) == 0) {
                if (init->element->isNeedFreeOutBuf == enum_Yes)
                    if (init->element->para.value != NULL) {
                        hw_Free(init->element->para.value);
                        init->element->para.value = NULL;
                        init->element->isNeedFreeOutBuf = enum_No;
                    }
                init->element->para.value = hw_Malloc(length);
                hw_Memcpy(init->element->para.value, initValue, length);
                init->element->para.len = length;
                init->element->isNeedFreeOutBuf = enum_Yes;
            }
        }
    }

    return init->element->para.value;
}

char* DT_SetGetBlob(S_ElementInit* init, int length, int maxLength, char* initValue)
{
    if (init->first != 1) {
        init->isuse = 1;
        init->type = enum_Blob;
        init->len = length;
        init->maxLen = maxLength;
        DT_initValueBuffer(init, length, initValue);
    }

    return DT_GetFuzzValueEX(init, 1, 0);
}

char* DT_SetGetBlobEnum(S_ElementInit* init, int length, int maxLength, char* initValue, char* eunmTableB[],
    int eunmTableL[], int eunmCount)
{
    if (init->first != 1) {
        init->isuse = 1;
        init->type = enum_Blob_Enum;
        init->len = length;
        init->maxLen = maxLength;
        DT_initValueBuffer(init, length, initValue);

        init->Enum_blob_table = eunmTableB;
        init->Enum_blob_l_table = eunmTableL;
        init->Enum_count = eunmCount;
    }

    return DT_GetFuzzValueEX(init, 1, 0);
}

char* DT_SetGetBlobEnum_EX(S_ElementInit* init, int length, int maxLength, char* initValue, char* eunmTableB[],
    int eunmTableL[], int eunmCount)
{
    if (init->first != 1) {
        init->isuse = 1;
        init->type = enum_Blob;
        init->len = length;  //字符串长度，strlen(str) + 1
        init->maxLen = maxLength;

        int pos = RAND_32() % eunmCount;
        DT_initValueBuffer(init, eunmTableL[pos], eunmTableB[pos]);
        init->len = eunmTableL[pos];
    }

    DT_GetFuzzValueEX(init, 1, 0);

    int i = 0;
    //初值一定不要在枚举里
    //判断如果变异出来的数据在枚举里，则换为初值
    for (i = 0; i < eunmCount; i++) {
        if (eunmTableL[i] == init->element->para.len) {
            if (hw_Memcmp(eunmTableB[i], init->element->para.value, init->element->para.len) == 0) {
                if (init->element->isNeedFreeOutBuf == enum_Yes)
                    if (init->element->para.value != NULL) {
                        hw_Free(init->element->para.value);
                        init->element->para.value = NULL;
                        init->element->isNeedFreeOutBuf = enum_No;
                    }
                init->element->para.value = hw_Malloc(length);
                hw_Memcpy(init->element->para.value, initValue, length);
                init->element->para.len = length;
                init->element->isNeedFreeOutBuf = enum_Yes;
            }
        }
    }

    return init->element->para.value;
}

char* DT_SetGetFixBlob(S_ElementInit* init, int length, int maxLength, char* initValue)
{
    if (init->first != 1) {
        init->isuse = 1;
        init->type = enum_FixBlob;
        init->len = length;
        init->maxLen = length;  //忽略最大长度参数，使用len
        DT_initValueBuffer(init, length, initValue);
    }

    return DT_GetFuzzValueEX(init, 1, 0);
}

char* DT_SetGetIpv4(S_ElementInit* init, char* initValue)
{
    if (init->first != 1) {
        init->isuse = 1;
        init->type = enum_Ipv4;
        init->len = 4;
        init->maxLen = 0;
        DT_initValueBuffer(init, 4, initValue);
    }

    return DT_GetFuzzValueEX(init, 1, 0);
}

char* DT_SetGetIpv6(S_ElementInit* init, char* initValue)
{
    if (init->first != 1) {
        init->isuse = 1;
        init->type = enum_Ipv6;
        init->len = 16;
        init->maxLen = 0;
        DT_initValueBuffer(init, 16, initValue);
    }

    return DT_GetFuzzValueEX(init, 1, 0);
}

char* DT_SetGetMac(S_ElementInit* init, char* initValue)
{
    if (init->first != 1) {
        init->isuse = 1;
        init->type = enum_Mac;
        init->len = 6;
        init->maxLen = 0;
        DT_initValueBuffer(init, 6, initValue);
    }

    return DT_GetFuzzValueEX(init, 1, 0);
}

void DT_Set_If_Show_crash(int is_show_all)
{
    Corpus_Set_If_show_crash(is_show_all);
}

void DT_Set_If_Show_Corpus(int is_show_all)
{
    Corpus_Set_If_Show(is_show_all);
}

void DT_Set_Report_Path(char* path)
{
    Report_Set_Path(path);
}

void DT_Set_Is_Dump_Coverage(int is_dump_coverage)
{
    Llvm_Set_Is_Dump_Coverage(is_dump_coverage);
}

void DT_Set_Is_Print_New_PC(int isPrintPC)
{
    Llvm_Set_Is_Print_New_PC(isPrintPC);
}

char* DT_Get_Version(void)
{
    return GET_Version();
}

void DT_Printf_Bin_To_Corpus(char* Path)
{
    Corpus_bin_printf(Path);
}

void DT_Write_Bin_To_Corpus(char* Path)
{
    Corpus_bin_write(Path);
}

void DT_Write_Corpus_To_Bin(char* Path)
{
    Corpus_corpus_write(Path);
}

void DT_Enable_TracePC(int isenable)
{
    if (isenable == 0)
        Corpus_start_feature();
    else
        Corpus_end_feature();
}

void DT_Show_Cur_Corpus(void)
{
    Corpus_Show_Cur();
}

void DT_Enable_Log(int isenable)
{
    if (isenable == 0)
        CLOSE_Log();
    else
        OPEN_Log();
}

int DT_GET_MutatedValueLen(S_ElementInit* init)
{
    return GET_MutatedValueLen(init->element);
}

int DT_GET_IsBeMutated(S_ElementInit* init)
{
    return GET_IsBeMutated(init->element);
}

void DT_Enable_AllMutater(int isenable)
{
    if (isenable == 0)
        SET_CloseAllMutater();
    else
        SET_OpenAllMutater();
}

void DT_Enable_OneMutater(enum enum_Mutated MutatedNum, int isenable)
{
    if (isenable == 0)
        SET_CloseOneMutater(MutatedNum);
    else
        SET_OpenOneMutater(MutatedNum);
}

void DT_Enable_Support_Loop(int isenable)
{
    if (isenable == 0)
        Llvm_Disable_Do_8bit_counters();
    else
        Llvm_Enable_Do_8bit_counters();
}

void DT_Enable_Leak_Check(int isenable)
{
    if (isenable == 0)
        Llvm_Disable_Leak_Check();
    else
        Llvm_Enable_Leak_Check();
}

//下边为封装后的创建元素结构体的函数，节省用户代码行数
S_Element* CREAT_ElementS64(int isHasInitValue, s64 initValue)
{
    S_Element* pElement = (S_Element*)hw_Malloc(sizeof(S_Element));
    hw_Memset(pElement, 0, sizeof(S_Element));

    *(s64*)pElement->numberValue = initValue;

    CREAT_ElementEX(pElement, enum_Yes, isHasInitValue, enum_NumberS, pElement->numberValue, 64);
    return pElement;
}

S_Element* CREAT_ElementS32(int isHasInitValue, s32 initValue)
{
    S_Element* pElement = (S_Element*)hw_Malloc(sizeof(S_Element));
    hw_Memset(pElement, 0, sizeof(S_Element));

    *(s32*)pElement->numberValue = initValue;

    CREAT_ElementEX(pElement, enum_Yes, isHasInitValue, enum_NumberS, pElement->numberValue, 32);
    return pElement;
}

S_Element* CREAT_ElementS16(int isHasInitValue, s16 initValue)
{
    S_Element* pElement = (S_Element*)hw_Malloc(sizeof(S_Element));
    hw_Memset(pElement, 0, sizeof(S_Element));

    *(s16*)pElement->numberValue = initValue;

    CREAT_ElementEX(pElement, enum_Yes, isHasInitValue, enum_NumberS, pElement->numberValue, 16);
    return pElement;
}

S_Element* CREAT_ElementS8(int isHasInitValue, s8 initValue)
{
    S_Element* pElement = (S_Element*)hw_Malloc(sizeof(S_Element));
    hw_Memset(pElement, 0, sizeof(S_Element));

    *(s8*)pElement->numberValue = initValue;

    CREAT_ElementEX(pElement, enum_Yes, isHasInitValue, enum_NumberS, pElement->numberValue, 8);
    return pElement;
}

S_Element* CREAT_ElementU64(int isHasInitValue, u64 initValue)
{
    S_Element* pElement = (S_Element*)hw_Malloc(sizeof(S_Element));
    hw_Memset(pElement, 0, sizeof(S_Element));

    *(u64*)pElement->numberValue = initValue;

    CREAT_ElementEX(pElement, enum_Yes, isHasInitValue, enum_NumberU, pElement->numberValue, 64);
    return pElement;
}

S_Element* CREAT_ElementU32(int isHasInitValue, u32 initValue)
{
    S_Element* pElement = (S_Element*)hw_Malloc(sizeof(S_Element));
    hw_Memset(pElement, 0, sizeof(S_Element));

    *(u32*)pElement->numberValue = initValue;

    CREAT_ElementEX(pElement, enum_Yes, isHasInitValue, enum_NumberU, pElement->numberValue, 32);
    return pElement;
}

S_Element* CREAT_ElementU16(int isHasInitValue, u16 initValue)
{
    S_Element* pElement = (S_Element*)hw_Malloc(sizeof(S_Element));
    hw_Memset(pElement, 0, sizeof(S_Element));

    *(u16*)pElement->numberValue = initValue;

    CREAT_ElementEX(pElement, enum_Yes, isHasInitValue, enum_NumberU, pElement->numberValue, 16);
    return pElement;
}

S_Element* CREAT_ElementU8(int isHasInitValue, u8 initValue)
{
    S_Element* pElement = (S_Element*)hw_Malloc(sizeof(S_Element));
    hw_Memset(pElement, 0, sizeof(S_Element));

    *(u8*)pElement->numberValue = initValue;

    CREAT_ElementEX(pElement, enum_Yes, isHasInitValue, enum_NumberU, pElement->numberValue, 8);
    return pElement;
}

S_Element* CREAT_ElementNumberEnum(int isHasInitValue, u32 initValue, int* eunmTable, int eunmCount)
{
    S_Element* pElement = (S_Element*)hw_Malloc(sizeof(S_Element));
    hw_Memset(pElement, 0, sizeof(S_Element));

    *(u32*)pElement->numberValue = initValue;

    pElement->Enum_count = eunmCount;
    pElement->Enum_number_table = eunmTable;

    CREAT_ElementEX(pElement, enum_Yes, isHasInitValue, enum_Number_Enum, pElement->numberValue, 32);
    return pElement;
}

S_Element* CREAT_ElementNumberRange(int isHasInitValue, u32 initValue, int min, int max)
{
    S_Element* pElement = (S_Element*)hw_Malloc(sizeof(S_Element));
    hw_Memset(pElement, 0, sizeof(S_Element));

    *(u32*)pElement->numberValue = initValue;

    pElement->min = min;
    pElement->max = max;

    CREAT_ElementEX(pElement, enum_Yes, isHasInitValue, enum_Number_Range, pElement->numberValue, 32);
    return pElement;
}

S_Element* CREAT_ElementFloat(int isHasInitValue, float initValue)
{
    S_Element* pElement = (S_Element*)hw_Malloc(sizeof(S_Element));
    hw_Memset(pElement, 0, sizeof(S_Element));

    *(u32*)pElement->numberValue = *(u32*)&initValue;

    CREAT_ElementEX(pElement, enum_Yes, isHasInitValue, enum_NumberU, pElement->numberValue, 32);
    return pElement;
}

S_Element* CREAT_ElementDouble(int isHasInitValue, double initValue)
{
    S_Element* pElement = (S_Element*)hw_Malloc(sizeof(S_Element));
    hw_Memset(pElement, 0, sizeof(S_Element));

    *(u64*)pElement->numberValue = *(u64*)&initValue;

    CREAT_ElementEX(pElement, enum_Yes, isHasInitValue, enum_NumberU, pElement->numberValue, 64);
    return pElement;
}

S_Element* CREAT_ElementString(int isHasInitValue, char* initValue, int len, int maxLen)
{
    S_Element* pElement = NULL;

    pElement = (S_Element*)hw_Malloc(sizeof(S_Element));
    hw_Memset(pElement, 0, sizeof(S_Element));

    pElement->para.max_len = maxLen;

    CREAT_ElementEX(pElement, enum_Yes, isHasInitValue, enum_String, initValue, len << 3);
    return pElement;
}

S_Element* CREAT_ElementStringEnum(
    int isHasInitValue, char* initValue, int len, int maxLen, char* eunmTableS[], int eunmCount)
{
    S_Element* pElement = NULL;

    pElement = (S_Element*)hw_Malloc(sizeof(S_Element));
    hw_Memset(pElement, 0, sizeof(S_Element));

    pElement->para.max_len = maxLen;

    pElement->Enum_count = eunmCount;
    pElement->Enum_string_table = eunmTableS;

    CREAT_ElementEX(pElement, enum_Yes, isHasInitValue, enum_String_Enum, initValue, len << 3);
    return pElement;
}

S_Element* CREAT_ElementBlob(int isHasInitValue, char* initValue, int len, int maxLen)
{
    S_Element* pElement = NULL;

    pElement = (S_Element*)hw_Malloc(sizeof(S_Element));
    hw_Memset(pElement, 0, sizeof(S_Element));
    pElement->para.max_len = maxLen;

    CREAT_ElementEX(pElement, enum_Yes, isHasInitValue, enum_Blob, initValue, len << 3);
    return pElement;
}

S_Element* CREAT_ElementBlobEnum(
    int isHasInitValue, char* initValue, int len, int maxLen, char* eunmTableB[], int eunmTableL[], int eunmCount)
{
    S_Element* pElement = NULL;

    pElement = (S_Element*)hw_Malloc(sizeof(S_Element));
    hw_Memset(pElement, 0, sizeof(S_Element));
    pElement->para.max_len = maxLen;

    pElement->Enum_count = eunmCount;
    pElement->Enum_blob_table = eunmTableB;
    pElement->Enum_blob_l_table = eunmTableL;

    CREAT_ElementEX(pElement, enum_Yes, isHasInitValue, enum_Blob_Enum, initValue, len << 3);
    return pElement;
}

S_Element* CREAT_ElementFixBlob(int isHasInitValue, char* initValue, int len, int maxLen)
{
    S_Element* pElement = NULL;

    pElement = (S_Element*)hw_Malloc(sizeof(S_Element));
    hw_Memset(pElement, 0, sizeof(S_Element));
    pElement->para.max_len = len;  //忽略最大长度参数，使用len

    CREAT_ElementEX(pElement, enum_Yes, isHasInitValue, enum_FixBlob, initValue, len << 3);
    return pElement;
}

S_Element* CREAT_ElementIpv4(int isHasInitValue, char* initValue)
{
    S_Element* pElement = NULL;

    pElement = (S_Element*)hw_Malloc(sizeof(S_Element));
    hw_Memset(pElement, 0, sizeof(S_Element));
    pElement->para.max_len = 0;

    CREAT_ElementEX(pElement, enum_Yes, isHasInitValue, enum_Ipv4, initValue, 4 << 3);
    return pElement;
}

S_Element* CREAT_ElementIpv6(int isHasInitValue, char* initValue)
{
    S_Element* pElement = NULL;

    pElement = (S_Element*)hw_Malloc(sizeof(S_Element));
    hw_Memset(pElement, 0, sizeof(S_Element));
    pElement->para.max_len = 0;

    CREAT_ElementEX(pElement, enum_Yes, isHasInitValue, enum_Ipv6, initValue, 16 << 3);
    return pElement;
}

S_Element* CREAT_ElementMac(int isHasInitValue, char* initValue)
{
    S_Element* pElement = NULL;

    pElement = (S_Element*)hw_Malloc(sizeof(S_Element));
    hw_Memset(pElement, 0, sizeof(S_Element));
    pElement->para.max_len = 0;

    CREAT_ElementEX(pElement, enum_Yes, isHasInitValue, enum_Mac, initValue, 6 << 3);
    return pElement;
}

//获取相应类型的测试例
s64 GET_MutatedValueSequenceS64(S_Element* pElement, int pos)
{
    char* tempdata = GET_MutatedValueSequence(pElement, pos);

    if (tempdata != NULL)
        return *(s64*)tempdata;

    return 0;
}

s64 GET_MutatedValueRandomS64(S_Element* pElement)
{
    char* tempdata = GET_MutatedValueRandom(pElement);

    if (tempdata != NULL)
        return *(s64*)tempdata;

    return 0;
}

s32 GET_MutatedValueSequenceS32(S_Element* pElement, int pos)
{
    char* tempdata = GET_MutatedValueSequence(pElement, pos);

    if (tempdata != NULL)
        return *(s32*)tempdata;

    return 0;
}

s32 GET_MutatedValueRandomS32(S_Element* pElement)
{
    char* tempdata = GET_MutatedValueRandom(pElement);

    if (tempdata != NULL)
        return *(s32*)tempdata;

    return 0;
}

s16 GET_MutatedValueSequenceS16(S_Element* pElement, int pos)
{
    char* tempdata = GET_MutatedValueSequence(pElement, pos);

    if (tempdata != NULL)
        return *(s16*)tempdata;

    return 0;
}

s16 GET_MutatedValueRandomS16(S_Element* pElement)
{
    char* tempdata = GET_MutatedValueRandom(pElement);

    if (tempdata != NULL)
        return *(s16*)tempdata;

    return 0;
}

s8 GET_MutatedValueSequenceS8(S_Element* pElement, int pos)
{
    char* tempdata = GET_MutatedValueSequence(pElement, pos);

    if (tempdata != NULL)
        return *(s8*)tempdata;

    return 0;
}

s8 GET_MutatedValueRandomS8(S_Element* pElement)
{
    char* tempdata = GET_MutatedValueRandom(pElement);

    if (tempdata != NULL)
        return *(s8*)tempdata;

    return 0;
}

u64 GET_MutatedValueSequenceU64(S_Element* pElement, int pos)
{
    char* tempdata = GET_MutatedValueSequence(pElement, pos);

    if (tempdata != NULL)
        return *(u64*)tempdata;

    return 0;
}

u64 GET_MutatedValueRandomU64(S_Element* pElement)
{
    char* tempdata = GET_MutatedValueRandom(pElement);

    if (tempdata != NULL)
        return *(u64*)tempdata;

    return 0;
}

u32 GET_MutatedValueSequenceU32(S_Element* pElement, int pos)
{
    char* tempdata = GET_MutatedValueSequence(pElement, pos);

    if (tempdata != NULL)
        return *(u32*)tempdata;

    return 0;
}

u32 GET_MutatedValueRandomU32(S_Element* pElement)
{
    char* tempdata = GET_MutatedValueRandom(pElement);

    if (tempdata != NULL)
        return *(u32*)tempdata;

    return 0;
}
u16 GET_MutatedValueSequenceU16(S_Element* pElement, int pos)
{
    char* tempdata = GET_MutatedValueSequence(pElement, pos);

    if (tempdata != NULL)
        return *(u16*)tempdata;

    return 0;
}

u16 GET_MutatedValueRandomU16(S_Element* pElement)
{
    char* tempdata = GET_MutatedValueRandom(pElement);

    if (tempdata != NULL)
        return *(u16*)tempdata;

    return 0;
}

u8 GET_MutatedValueSequenceU8(S_Element* pElement, int pos)
{
    char* tempdata = GET_MutatedValueSequence(pElement, pos);

    if (tempdata != NULL)
        return *(u8*)tempdata;

    return 0;
}

u8 GET_MutatedValueRandomU8(S_Element* pElement)
{
    char* tempdata = GET_MutatedValueRandom(pElement);

    if (tempdata != NULL)
        return *(u8*)tempdata;

    return 0;
}

s32 GET_MutatedValueSequenceNumberEnum(S_Element* pElement, int pos)
{
    char* tempdata = GET_MutatedValueSequence(pElement, pos);

    if (tempdata != NULL)
        return *(s32*)tempdata;

    return 0;
}

s32 GET_MutatedValueRandomNumberEnum(S_Element* pElement)
{
    char* tempdata = GET_MutatedValueRandom(pElement);

    if (tempdata != NULL)
        return *(s32*)tempdata;

    return 0;
}

s32 GET_MutatedValueSequenceNumberRange(S_Element* pElement, int pos)
{
    char* tempdata = GET_MutatedValueSequence(pElement, pos);

    if (tempdata != NULL)
        return *(s32*)tempdata;

    return 0;
}

s32 GET_MutatedValueRandomNumberRange(S_Element* pElement)
{
    char* tempdata = GET_MutatedValueRandom(pElement);

    if (tempdata != NULL)
        return *(s32*)tempdata;

    return 0;
}
#ifndef __KERNEL__
float GET_MutatedValueSequenceFloat(S_Element* pElement, int pos)
{
    char* tempdata = GET_MutatedValueSequence(pElement, pos);

    if (tempdata != NULL)
        return *(float*)tempdata;

    return 0;
}

float GET_MutatedValueRandomFloat(S_Element* pElement)
{
    char* tempdata = GET_MutatedValueRandom(pElement);

    if (tempdata != NULL)
        return *(float*)tempdata;

    return 0;
}

double GET_MutatedValueSequenceDouble(S_Element* pElement, int pos)
{
    char* tempdata = GET_MutatedValueSequence(pElement, pos);

    if (tempdata != NULL)
        return *(double*)tempdata;

    return 0;
}

double GET_MutatedValueRandomDouble(S_Element* pElement)
{
    char* tempdata = GET_MutatedValueRandom(pElement);

    if (tempdata != NULL)
        return *(double*)tempdata;

    return 0;
}
#endif

char* GET_MutatedValueSequenceString(S_Element* pElement, int pos)
{
    char* tempdata = GET_MutatedValueSequence(pElement, pos);

    if (tempdata != NULL)
        return tempdata;

    return NULL;
}

char* GET_MutatedValueRandomString(S_Element* pElement)
{
    char* tempdata = GET_MutatedValueRandom(pElement);

    if (tempdata != NULL)
        return tempdata;

    return NULL;
}

char* GET_MutatedValueSequenceStringEnum(S_Element* pElement, int pos)
{
    char* tempdata = GET_MutatedValueSequence(pElement, pos);

    if (tempdata != NULL)
        return tempdata;

    return NULL;
}

char* GET_MutatedValueRandomStringEnum(S_Element* pElement)
{
    char* tempdata = GET_MutatedValueRandom(pElement);

    if (tempdata != NULL)
        return tempdata;

    return NULL;
}

char* GET_MutatedValueSequenceBlob(S_Element* pElement, int pos)
{
    char* tempdata = GET_MutatedValueSequence(pElement, pos);

    if (tempdata != NULL)
        return tempdata;

    return NULL;
}

char* GET_MutatedValueRandomBlob(S_Element* pElement)
{
    char* tempdata = GET_MutatedValueRandom(pElement);

    if (tempdata != NULL)
        return tempdata;

    return NULL;
}

char* GET_MutatedValueSequenceBlobEnum(S_Element* pElement, int pos)
{
    char* tempdata = GET_MutatedValueSequence(pElement, pos);

    if (tempdata != NULL)
        return tempdata;

    return NULL;
}

char* GET_MutatedValueRandomBlobEnum(S_Element* pElement)
{
    char* tempdata = GET_MutatedValueRandom(pElement);

    if (tempdata != NULL)
        return tempdata;

    return NULL;
}

char* GET_MutatedValueSequenceFixBlob(S_Element* pElement, int pos)
{
    char* tempdata = GET_MutatedValueSequence(pElement, pos);

    if (tempdata != NULL)
        return tempdata;

    return NULL;
}

char* GET_MutatedValueRandomFixBlob(S_Element* pElement)
{
    char* tempdata = GET_MutatedValueRandom(pElement);

    if (tempdata != NULL)
        return tempdata;

    return NULL;
}

char* GET_MutatedValueSequenceIpv4(S_Element* pElement, int pos)
{
    char* tempdata = GET_MutatedValueSequence(pElement, pos);

    if (tempdata != NULL)
        return tempdata;

    return NULL;
}

char* GET_MutatedValueRandomIpv4(S_Element* pElement)
{
    char* tempdata = GET_MutatedValueRandom(pElement);

    if (tempdata != NULL)
        return tempdata;

    return NULL;
}

char* GET_MutatedValueSequenceIpv6(S_Element* pElement, int pos)
{
    char* tempdata = GET_MutatedValueSequence(pElement, pos);

    if (tempdata != NULL)
        return tempdata;

    return NULL;
}

char* GET_MutatedValueRandomIpv6(S_Element* pElement)
{
    char* tempdata = GET_MutatedValueRandom(pElement);

    if (tempdata != NULL)
        return tempdata;

    return NULL;
}

char* GET_MutatedValueSequenceMac(S_Element* pElement, int pos)
{
    char* tempdata = GET_MutatedValueSequence(pElement, pos);

    if (tempdata != NULL)
        return tempdata;

    return NULL;
}

char* GET_MutatedValueRandomMac(S_Element* pElement)
{
    char* tempdata = GET_MutatedValueRandom(pElement);

    if (tempdata != NULL)
        return tempdata;

    return NULL;
}
