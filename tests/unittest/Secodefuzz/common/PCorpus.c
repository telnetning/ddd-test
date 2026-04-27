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

本模块提供样本管理

IO模块存在，则可以实现多样本变异，如果不支持，则无法从外部读取多样本，
WriteToFile
WriteToFileFail
ReadFromFile



llvm模块存在，则可以实现样本遗传算法，下三个函数只要实现即可，无论用什么方法
llvmtracepc_is_has_new_feature
llvmtracepc_start_feature
llvmtracepc_end_feature


IO与llvm模块只要有一个存在即可实现部分功能，
如果都不存在，则本模块仅下个函数提供打印目前样本功能
Corpus_Show_Cur


本模块对外部提供接口，用户可见，但也被宏封装，尽量不直接使用


*/

#include "PCommon.h"

typedef struct {
    S_para para[max_para_num];
    int para_num;
    char corpus_hash_name[64];
} S_corpus;

typedef struct {
    S_corpus corpus[max_corpus_num];

    int* Enum_number_table[max_para_num];
    char** Enum_string_table[max_para_num];
    char** Enum_blob_table[max_para_num];
    int* Enum_blob_l_table[max_para_num];
    int Enum_count[max_para_num];

    int min[max_para_num];
    int max[max_para_num];

    int corpus_num;
    int para_num;
    int run_count;

    char* corpus_path;

} S_corpus_module;

S_corpus_module* g_corpus_module = NULL;
int is_need_mutator = 1;
int is_need_record_corpus = 0;
int temp_g_para_num = 0;
int is_need_show_corpus = 0;

int g_is_printf_crash_corpus = 1;

//程序运行中需要的临时内存，设计成全局变量速度能够更快些
static char* g_txt = NULL;
static char* g_txt1 = NULL;
static int g_corpus_is_has_init = 0;
static S_corpus g_temp_corpus;

static void init_corpus_malloc(void)
{
    if (g_corpus_is_has_init == 0) {
        g_corpus_is_has_init = 1;

        g_txt = hw_Malloc(max_one_line);
        g_txt1 = hw_Malloc(max_one_line);

        g_corpus_module = (S_corpus_module*)hw_Malloc(sizeof(S_corpus_module));
        hw_Memset(g_corpus_module, 0, sizeof(S_corpus_module));
    }
}

static void init_element_form_corpus(S_para* para, S_ElementInit* Element, int i)
{
    if (para->type == enum_NumberU) {
        if (para->len == 1) {
            DT_SetGetU8(Element, *(u8*)para->value);
        } else if (para->len == 2) {
            DT_SetGetU16(Element, *(u16*)para->value);
        } else if (para->len == 4) {
            DT_SetGetU32(Element, *(u32*)para->value);
        } else if (para->len == 8) {
            DT_SetGetU64(Element, *(u64*)para->value);
        }
    } else if (para->type == enum_NumberS) {
        if (para->len == 1) {
            DT_SetGetS8(Element, *(s8*)para->value);
        } else if (para->len == 2) {
            DT_SetGetS16(Element, *(s16*)para->value);
        } else if (para->len == 4) {
            DT_SetGetS32(Element, *(s32*)para->value);
        } else if (para->len == 8) {
            DT_SetGetS64(Element, *(s64*)para->value);
        }
    } else if (para->type == enum_Number_Enum) {
        DT_SetGetNumberEnum(
            Element, *(s32*)para->value, g_corpus_module->Enum_number_table[i], g_corpus_module->Enum_count[i]);
    } else if (para->type == enum_Number_Range) {
        DT_SetGetNumberRange(Element, *(s32*)para->value, g_corpus_module->min[i], g_corpus_module->max[i]);
    } else if (para->type == enum_String) {
        if (para->len > 0)
            para->value[para->len - 1] = 0;

        DT_SetGetString(Element, para->len, para->max_len, para->value);
    } else if (para->type == enum_String_Enum) {
        if (para->len > 0)
            para->value[para->len - 1] = 0;

        DT_SetGetStringEnum(Element,
            para->len,
            para->max_len,
            para->value,
            g_corpus_module->Enum_string_table[i],
            g_corpus_module->Enum_count[i]);
    } else if (para->type == enum_Blob) {
        DT_SetGetBlob(Element, para->len, para->max_len, para->value);
    } else if (para->type == enum_Blob_Enum) {
        DT_SetGetBlobEnum(Element,
            para->len,
            para->max_len,
            para->value,
            g_corpus_module->Enum_blob_table[i],
            g_corpus_module->Enum_blob_l_table[i],
            g_corpus_module->Enum_count[i]);
    } else if (para->type == enum_FixBlob) {
        DT_SetGetFixBlob(Element, para->len, para->max_len, para->value);
    } else if (para->type == enum_Ipv4) {
        DT_SetGetIpv4(Element, para->value);
    } else if (para->type == enum_Ipv6) {
        DT_SetGetIpv6(Element, para->value);
    } else if (para->type == enum_Mac) {
        DT_SetGetMac(Element, para->value);
    }
}

static void get_value_string(char* in_txt, char* out_txt)
{
    //获取以等号为开始，\r或者\n为结尾的字符串
    // hw_sscanf(in_txt,"%*[^=]=%[^\r^\n]",out_txt);
    int length = strlen(in_txt);
    int start = 0;
    int k = 0;
    int i = 0;

    for (i = 0; i < length; i++) {
        if ((start == 0) && (in_txt[i] == '=')) {
            start = 1;
            continue;
        }

        if (start == 0)
            continue;

        if ((in_txt[i] == '\r') || (in_txt[i] == '\n'))
            break;

        out_txt[k] = in_txt[i];
        k++;
    }
    out_txt[k] = 0;
}

static void parse_one_line(char* txt)
{
    // 去掉前边的空格
    while (*txt == ' ') {
        txt++;
    }

    //注释不去管它，这里显示的写出来
    if (txt[0] == '#') {
        return;
    }

    //以!为单个样本获取的结束符
    if (txt[0] == '!') {
        g_corpus_module->corpus_num++;

        if ((g_corpus_module->para_num == 0) && (temp_g_para_num != 0)) {
            g_corpus_module->para_num = temp_g_para_num;
        }
        temp_g_para_num = 0;
    }

    if (txt[0] == '-') {
        temp_g_para_num++;
    }

    S_para* temp_corpus = &g_corpus_module->corpus[g_corpus_module->corpus_num].para[temp_g_para_num];

    if (txt[0] == 'n') {
        get_value_string(txt, g_txt1);
        in_delSpace(g_txt1);
        hw_Memcpy(temp_corpus->name, g_txt1, in_strlen(g_txt1) + 1);
    }
    if (txt[0] == 't') {
        get_value_string(txt, g_txt1);
        in_delSpace(g_txt1);
        temp_corpus->type = in_GetTypeFromString(g_txt1);
    }
    if (txt[0] == 'l') {
        get_value_string(txt, g_txt1);
        in_delSpace(g_txt1);
        temp_corpus->len = in_atol(g_txt1);
    }
    if ((txt[0] == 'm') && (txt[1] == 'a')) {
        get_value_string(txt, g_txt1);
        in_delSpace(g_txt1);
        temp_corpus->max_len = in_atol(g_txt1);
    }
    if ((txt[0] == 'm') && (txt[1] == 'u')) {
        get_value_string(txt, g_txt1);
        in_delSpace(g_txt1);
        hw_Memcpy(temp_corpus->mutater_name, g_txt1, in_strlen(g_txt1) + 1);
    }

    if (txt[0] == 'v') {
        if (txt[5] == 'N') {
            get_value_string(txt, g_txt1);
            in_delSpace(g_txt1);
            temp_corpus->value = hw_Malloc(temp_corpus->len);
            s64 temp_number = 0;

            if (txt[6] == 'A')
                temp_number = in_atol(g_txt1);
            if (txt[6] == 'X')
                temp_number = in_htol(g_txt1);

            //有无符号分别对待
            if (temp_corpus->type == enum_NumberS) {
                if (temp_corpus->len == 1)
                    *((s8*)temp_corpus->value) = (s8)temp_number;
                else if (temp_corpus->len == 2)
                    *((s16*)temp_corpus->value) = (s16)temp_number;
                else if (temp_corpus->len == 4)
                    *((s32*)temp_corpus->value) = (s32)temp_number;
                else if (temp_corpus->len == 8)
                    *((s64*)temp_corpus->value) = (s64)temp_number;
            } else  // if(temp_corpus->type == enum_NumberU )
            {
                if (temp_corpus->len == 1)
                    *((u8*)temp_corpus->value) = (u8)temp_number;
                else if (temp_corpus->len == 2)
                    *((u16*)temp_corpus->value) = (u16)temp_number;
                else if (temp_corpus->len == 4)
                    *((u32*)temp_corpus->value) = (u32)temp_number;
                else if (temp_corpus->len == 8)
                    *((u64*)temp_corpus->value) = (u64)temp_number;
            }
        } else {
            get_value_string(txt, g_txt1);
            temp_corpus->value = hw_Malloc(temp_corpus->len);
            in_parse_string_to_bin(g_txt1, temp_corpus->value);
        }
    }
}

//从样本文件中读取样本到全局变量中
static void read_all_corpus(char* Path)
{
    char* data = NULL;
    int len;
    int i = 0;
    int j = 0;

    ReadFromFile(&data, &len, Path);
    if (len == 0)
        return;

    for (i = 0; i < len; i++) {
        g_txt[j++] = data[i];

        //遇到\n，开始解析一行
        if (data[i] == '\n') {
            g_txt[j] = 0;
            parse_one_line(g_txt);
            j = 0;
        }
    }
    //最后一行
    g_txt[j] = 0;
    parse_one_line(g_txt);

    if (data != NULL) {
        hw_Free(data);
        data = NULL;
    }
    return;
}

//得到一个参数的完整打印
static int get_para_print(S_para* temp_corpus, char* buf)
{
    int size = 0;

    size += hw_sprintf(buf + size, "name\t\t=%s\r\n", temp_corpus->name);
    size += hw_sprintf(buf + size, "type\t\t=%s\r\n", in_GetStringFromType(temp_corpus->type));
    size += hw_sprintf(buf + size, "mutater_name\t=%s\r\n", temp_corpus->mutater_name);
    size += hw_sprintf(buf + size, "len\t\t=%d\r\n", temp_corpus->len);
    size += hw_sprintf(buf + size, "max_len\t\t=%d\r\n", temp_corpus->max_len);

    //增加对number的数字打印
    if ((temp_corpus->type == enum_NumberU) || (temp_corpus->type == enum_NumberS)) {
        s64 temp_value = 0;

        //有无符号分别对待
        if (temp_corpus->type == enum_NumberS) {
            if (temp_corpus->len == 1)
                temp_value = *((s8*)temp_corpus->value);
            else if (temp_corpus->len == 2)
                temp_value = *((s16*)temp_corpus->value);
            else if (temp_corpus->len == 4)
                temp_value = *((s32*)temp_corpus->value);
            else if (temp_corpus->len == 8)
                temp_value = *((s64*)temp_corpus->value);
        } else  // if(temp_corpus->type == enum_NumberU )
        {
            if (temp_corpus->len == 1)
                temp_value = *((u8*)temp_corpus->value);
            else if (temp_corpus->len == 2)
                temp_value = *((u16*)temp_corpus->value);
            else if (temp_corpus->len == 4)
                temp_value = *((u32*)temp_corpus->value);
            else if (temp_corpus->len == 8)
                temp_value = *((u64*)temp_corpus->value);
        }

        size += hw_sprintf(buf + size, "#number_value\t=%ld,0x%lx\r\n", temp_value, temp_value);
    }

    g_txt[0] = 0;
    in_parse_bin_to_string(g_txt, temp_corpus->value, temp_corpus->len);

    size += hw_sprintf(buf + size, "%s", g_txt);

    return size;
}

//得到一个样本的完整打印
static int get_corpus_print(int j, char* buf)
{
    int size = 0;
    int i = 0;

    for (i = 0; i < g_corpus_module->para_num; i++) {
        S_para* temp_corpus = &g_corpus_module->corpus[j].para[i];

        size += get_para_print(temp_corpus, buf + size);
        size += hw_sprintf(buf + size, "------------para=%d\r\n", i);
    }
    size += hw_sprintf(buf + size, "!!!!!!!!!!!!!!!!!!!!!!!!!!!above is corpus %d\r\n", j);

    return size;
}

void Corpus_bin_printf(char* Path)
{

    char* data = NULL;
    int len;

    init_corpus_malloc();

    ReadFromFile(&data, &len, Path);
    if (len == 0)
        return;

    in_parse_bin_to_string(g_txt, data, len);
    hw_printf("%s", g_txt);

    if (data != NULL)
        hw_Free(data);
    return;
}

void Corpus_bin_write(char* Path)
{

    char* data = NULL;
    int len;
    char temp_path[500];

    hw_sprintf(temp_path, "%s%s", Path, ".txt");

    init_corpus_malloc();

    ReadFromFile(&data, &len, Path);
    if (len == 0)
        return;

    in_parse_bin_to_string(g_txt, data, len);

    WriteToFile(g_txt, in_strlen(g_txt) + 1, temp_path);

    if (data != NULL)
        hw_Free(data);
    return;
}

void Corpus_corpus_write(char* Path)
{

    char* data = NULL;
    int len;
    char temp_path[500];

    hw_sprintf(temp_path, "%s%s", Path, ".bin");

    init_corpus_malloc();

    ReadFromFile(&data, &len, Path);
    if (len == 0)
        return;

    get_value_string(data, g_txt1);
    len = in_parse_string_to_bin(g_txt1, g_txt);

    WriteToFile(g_txt, len, temp_path);

    if (data != NULL)
        hw_Free(data);
    return;
}

void Corpus_Start(int is_reproduce)
{
    //第一次执行，读取样本
    if (g_corpus_module->run_count == 0) {

        read_all_corpus(g_corpus_module->corpus_path);
    }

    //样本复现测试，
    //如果is_reproduce非0，并且小于等于样本数量
    //使用第is_reproduce个样本复现
    //不变异，不记录样本
    if ((is_reproduce > 0) && (is_reproduce <= g_corpus_module->corpus_num)) {
        //选取一个样本重新跑
        DT_Clear(g_Element);
        CLEAR_FuzzEnvironment();
        int i = 0;
        for (i = 0; i < g_corpus_module->para_num; i++) {
            init_element_form_corpus(&g_corpus_module->corpus[is_reproduce - 1].para[i], &g_Element[i], i);
        }
        is_need_mutator = 0;
        is_need_record_corpus = 0;
        return;
    }

    //非复现模式
    //最早n次运行使用样本文件中的样本运行，
    //不变异，不进行新样本记录
    if ((is_reproduce == 0) && (g_corpus_module->corpus_num > 0) &&
        (g_corpus_module->run_count < g_corpus_module->corpus_num)) {

        //选取一个样本重新跑
        DT_Clear(g_Element);
        CLEAR_FuzzEnvironment();
        int i = 0;

        for (i = 0; i < g_corpus_module->para_num; i++) {
            init_element_form_corpus(&g_corpus_module->corpus[g_corpus_module->run_count].para[i], &g_Element[i], i);
        }
        is_need_mutator = 0;
        is_need_record_corpus = 0;

        //不记录样本，但是需要记录覆盖信息，一面以后同样的样本被记录
        Corpus_start_feature();
        return;
    }

    //非复现模式
    //运行完文件样本后，使用代码里的样本进行测试，
    //不变异，如果产生新分支，记录新样本
    if ((is_reproduce == 0) && (g_corpus_module->run_count == g_corpus_module->corpus_num)) {
        DT_Clear(g_Element);
        CLEAR_FuzzEnvironment();
        is_need_mutator = 0;
        is_need_record_corpus = 1;
        Corpus_start_feature();
        return;
    }

    int temp = g_corpus_module->run_count % switch_corpus_count;

    //如果样本数量大于0，
    //每约100次换个样本进行变异，
    if ((g_corpus_module->corpus_num > 0) && (temp == 0)) {
        //选取一个样本重新跑
        DT_Clear(g_Element);
        CLEAR_FuzzEnvironment();

        int pos1 = RAND_RANGE(0, g_corpus_module->corpus_num - 1);
        int pos2 = RAND_RANGE(0, g_corpus_module->corpus_num - 1);
        int pos = 0;

        // 20%100的测试例使用交叉参数，即其中一个参数来自另外样本
        //遗传算法之杂交
        int is_cross = (RAND_32() % 5 == 0);
        int para_pos = RAND_RANGE(0, g_corpus_module->para_num - 1);

        int i = 0;

        for (i = 0; i < g_corpus_module->para_num; i++) {

            if (is_cross && (i == para_pos))
                pos = pos1;
            else
                pos = pos2;

            init_element_form_corpus(&g_corpus_module->corpus[pos].para[i], &g_Element[i], i);
        }

        if (is_reproduce == 0xffffffff) {
            is_need_record_corpus = 0;
            is_need_mutator = 1;
            return;
        }

        is_need_mutator = 1;
        is_need_record_corpus = 1;
        Corpus_start_feature();
        return;
    }

    //如果样本数量大于0，
    //前百分之50运行样本一次变异，之后运行基于变异值得再次变异
    if ((g_corpus_module->corpus_num > 0) && (temp > (switch_corpus_count / 2)) &&
        (((temp - (switch_corpus_count / 2)) % ((switch_corpus_count / 2) / switch_sub_count)) == 0)) {

        int i = 0;

        hw_Memset(&g_temp_corpus, 0, sizeof(g_temp_corpus));

        for (i = 0; i < g_corpus_module->para_num; i++) {
            g_temp_corpus.para[i] = g_Element[i].element->para;

            if (g_temp_corpus.para[i].len > 0) {
                g_temp_corpus.para[i].value = hw_Malloc(g_temp_corpus.para[i].len);
                hw_Memcpy(g_temp_corpus.para[i].value, g_Element[i].element->para.value, g_temp_corpus.para[i].len);
            } else
                g_temp_corpus.para[i].value = NULL;
        }

        //选取一个样本重新跑
        DT_Clear(g_Element);
        CLEAR_FuzzEnvironment();

        for (i = 0; i < g_corpus_module->para_num; i++) {
            init_element_form_corpus(&g_temp_corpus.para[i], &g_Element[i], i);

            if (g_temp_corpus.para[i].value != NULL) {
                hw_Free(g_temp_corpus.para[i].value);
                g_temp_corpus.para[i].value = NULL;
            }
        }

        if (is_reproduce == 0xffffffff) {
            is_need_record_corpus = 0;
            is_need_mutator = 1;
            return;
        }

        is_need_mutator = 1;
        is_need_record_corpus = 1;
        Corpus_start_feature();
        return;
    }

    if (is_reproduce == 0xffffffff) {
        is_need_record_corpus = 0;
        is_need_mutator = 1;
        return;
    }

    is_need_record_corpus = 1;
    Corpus_start_feature();
    is_need_mutator = 1;
    return;
}

void Corpus_start_feature(void)
{
    kcov_start_feature();
    llvmtracepc_start_feature();
}

void Corpus_end_feature(void)
{
    kcov_end_feature();
    llvmtracepc_end_feature();
}

static int Corpus_is_has_new_feature(void)
{
    int temp = kcov_is_has_new_feature();
    int temp1 = llvmtracepc_is_has_new_feature();
    int is_has_new_feature = temp || temp1;

    return is_has_new_feature;
}

void Corpus_End()
{
    Corpus_end_feature();
    int is_has_new_feature = Corpus_is_has_new_feature();
    int i = 0;

    //只有加入整体随机的变量才能加入遗传算法
    if (g_corpus_module->para_num == 0)
        g_corpus_module->para_num = addWholeRandomNum;

    //记录参数其他数据
    for (i = 0; i < g_corpus_module->para_num; i++) {
        g_corpus_module->Enum_number_table[i] = g_Element[i].Enum_number_table;
        g_corpus_module->Enum_string_table[i] = g_Element[i].Enum_string_table;
        g_corpus_module->Enum_count[i] = g_Element[i].Enum_count;
        g_corpus_module->min[i] = g_Element[i].min;
        g_corpus_module->max[i] = g_Element[i].max;
    }

    //如果产生新的分支，则记录样本  遗传算法之进化
    //如果达到最大样本数量，则不再记录样本，很遗憾哈!!!!
    if ((is_need_record_corpus == 1) && is_has_new_feature && (g_corpus_module->corpus_num < max_corpus_num)) {

        int i = 0;
        for (i = 0; i < g_corpus_module->para_num; i++) {
            S_para* temp_corpus = &g_corpus_module->corpus[g_corpus_module->corpus_num].para[i];

            hw_Memcpy(temp_corpus->name, g_Element[i].element->para.name, 64);
            hw_Memcpy(temp_corpus->mutater_name, g_Element[i].element->para.mutater_name, 64);

            temp_corpus->type = g_Element[i].element->para.type;
            temp_corpus->max_len = g_Element[i].element->para.max_len;
            temp_corpus->len = g_Element[i].element->para.len;

            temp_corpus->value = hw_Malloc(g_Element[i].element->para.len);

            hw_Memcpy(temp_corpus->value, g_Element[i].element->para.value, g_Element[i].element->para.len);
        }

        //记录到样本文件尾部
        {
            int size = 0;
            char* txt;
            txt = hw_Malloc(max_one_line);
            txt[0] = 0;
            size = get_corpus_print(g_corpus_module->corpus_num, txt);
            WriteToFileFail(txt, size, g_corpus_module->corpus_path);

            hw_Free(txt);
        }

        g_corpus_module->corpus_num++;
    }

    g_corpus_module->run_count++;
}

void Corpus_Show_Cur(void)
{
    int i = 0;
    int is_has_para = 0;

    int size = 0;
    int size1 = 0;
    char* txt;
    char* txt1;
    char crash_name[max_file_path + 100];
    int crash_name_zize = 0;

    //记录失败的测试例报告
    Report_write_failed_testcase();

    crash_name[0] = 0;

    if (g_corpus_module->para_num <= 0)
        return;

    txt = hw_Malloc(max_one_line);
    txt1 = hw_Malloc(max_one_line);

    size = hw_sprintf(txt, "\r\n*************************** crash corpus is \r\n");
    for (i = 0; i < g_corpus_module->para_num; i++) {
        if (g_Element[i].element != NULL) {
            is_has_para = 1;
            S_para* temp_corpus = &g_Element[i].element->para;

            g_txt1[0] = 0;
            get_para_print(temp_corpus, g_txt1);

            size += hw_sprintf(txt + size, "%s", g_txt1);
            size += hw_sprintf(txt + size, "------------para=%d\r\n", i);
        }
    }

    size += hw_sprintf(txt + size, "!!!!!!!!!!!!!!!!!!!!!!!!!!!above is crash corpus\r\n");

    if (g_is_printf_crash_corpus)
        hw_printf("%s", txt);

    if ((is_need_record_corpus != 0) && is_has_para) {

        /////////////
        size1 = 0;
        for (i = 0; i < g_corpus_module->para_num; i++) {
            if (g_Element[i].element != NULL) {
                hw_Memcpy(txt1 + size1, g_Element[i].element->para.value, g_Element[i].element->para.len);
                size1 += g_Element[i].element->para.len;
            }
        }

        crash_name_zize = hw_sprintf(crash_name, "%s_", g_corpus_module->corpus_path);
        crash_name_zize += hw_sprintf(crash_name + crash_name_zize, "crash_");
        crash_name_zize += hw_sprintf(crash_name + crash_name_zize, "_");
        crash_name_zize += hw_sprintf(crash_name + crash_name_zize, "%s", Hash(txt1, size1));

        //是否写入crash文件
        WriteToFile(txt, size, crash_name);

        //最后挣扎的时候，打印覆盖率文件:)
        Llvm_Dump_Coverage();
    }

    hw_Free(txt);
    hw_Free(txt1);
}

void Corpus_Set_If_show_crash(int is_show_all)
{
    g_is_printf_crash_corpus = is_show_all;
}

void Corpus_Set_If_Show(int is_show_all)
{
    is_need_show_corpus = is_show_all;
}

void Corpus_Show_ALL()
{
    int j = 0;
    char* txt;

    if (is_need_show_corpus == 0)
        return;

    if (g_corpus_module->corpus_num <= 0)
        return;

    txt = hw_Malloc(max_one_line);

    //一个样本打印一次，节省内存
    for (j = 0; j < g_corpus_module->corpus_num; j++) {
        txt[0] = 0;
        get_corpus_print(j, txt);
        hw_printf("%s", txt);
    }

    hw_Free(txt);
}

void Corpus_Set_Path(char* path)
{
    g_corpus_module->corpus_path = path;
}

//不同测试例之间，要调用初始化函数清内存
void init_Corpus(void)
{
    int j = 0;
    int i = 0;

    init_corpus_malloc();

    for (j = 0; j < g_corpus_module->corpus_num; j++) {
        for (i = 0; i < g_corpus_module->para_num; i++) {
            S_para* temp_corpus = &g_corpus_module->corpus[j].para[i];

            if (temp_corpus->value) {
                hw_Free(temp_corpus->value);
            }
            temp_corpus->value = NULL;
        }
    }

    //模块内部数据，全部清0
    hw_Memset(g_corpus_module, 0, sizeof(S_corpus_module));
}
