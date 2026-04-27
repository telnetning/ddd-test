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

本模块维护编译器内存钩子调出来的数据

如果不支持llvm，则本模块数据都为空，获取为空或者0

*/

#include "PCommon.h"

//目前先与内存hook功能绑定，以后可能会修改
#ifdef HAS_HOOK

typedef struct llvmmem_table {
    int has_value[SIZE_llvmmem_table];
    char A[SIZE_llvmmem_table][SIZE_llvmmem_data + 1];  //加1放置最大长度的/0
    char B[SIZE_llvmmem_table][SIZE_llvmmem_data + 1];
    int len1[SIZE_llvmmem_table];
    int len2[SIZE_llvmmem_table];
    int has_value_table[SIZE_llvmmem_table];
    int has_value_total;
} S_llvmmem_table;

typedef struct llvmnumber_table_u64 {
    int has_value[SIZE_llvmnumber_table_u64];
    u64 A[SIZE_llvmnumber_table_u64];
    u64 B[SIZE_llvmnumber_table_u64];
    int has_value_table[SIZE_llvmnumber_table_u64];
    int has_value_total;
} S_llvmnumber_table_u64;

static S_llvmnumber_table_u64* g_llvmnumber_table_u64 = NULL;
static S_llvmmem_table* g_llvmmem_table = NULL;

//如果没有初始化，则本模块不工作.目前不设置关闭本模块的功能
static int g_llvmdata_is_has_init = 0;

static void init_llvmdata_malloc(void)
{
    if (g_llvmdata_is_has_init == 0) {
        g_llvmdata_is_has_init = 1;
        g_llvmnumber_table_u64 = (S_llvmnumber_table_u64*)hw_Malloc(sizeof(S_llvmnumber_table_u64));
        g_llvmmem_table = (S_llvmmem_table*)hw_Malloc(sizeof(S_llvmmem_table));

        hw_Memset(g_llvmnumber_table_u64, 0, sizeof(S_llvmnumber_table_u64));
        hw_Memset(g_llvmmem_table, 0, sizeof(S_llvmmem_table));
    }
}

void llvmdata_number_add_value(void* caller_pc, u64 s1, u64 s2)
{
    size_t Idx;

    if (g_llvmdata_is_has_init == 0)
        return;

    //太小的数还是依赖其他变异算法吧
    if ((s1 < 256) && (s2 < 256))
        return;

    //排除一些内存比较语句返回值
    if ((s2 == 0) && (((s32)s1 <= 0xffffffff) && ((s32)s1 >= 0xffffff00)))
        return;

    Idx = (size_t)caller_pc % SIZE_llvmnumber_table_u64;
    g_llvmnumber_table_u64->A[Idx] = s1;
    g_llvmnumber_table_u64->B[Idx] = s2;

    if (g_llvmnumber_table_u64->has_value[Idx] == 0) {
        g_llvmnumber_table_u64->has_value[Idx] = 1;
        g_llvmnumber_table_u64->has_value_table[g_llvmnumber_table_u64->has_value_total] = Idx;
        g_llvmnumber_table_u64->has_value_total++;
    }
}

int llvmdata_number_get_count(void)
{
    if (g_llvmdata_is_has_init == 0)
        return 0;

    return g_llvmnumber_table_u64->has_value_total;
}

//库里随机抽取一个
u64 llvmdata_number_get_value(void)
{
    size_t Idx;

    if (g_llvmdata_is_has_init == 0)
        return 0;

    if (g_llvmnumber_table_u64->has_value_total == 0)
        return 0;

    Idx = RAND_32() % g_llvmnumber_table_u64->has_value_total;
    Idx = g_llvmnumber_table_u64->has_value_table[Idx];
    int is_even = RAND_BOOL();
    u64 temp;

    if (is_even)
        temp = g_llvmnumber_table_u64->A[Idx];
    else
        temp = g_llvmnumber_table_u64->B[Idx];

    //小于256则换另一个值
    if (temp < 256) {
        if (is_even)
            temp = g_llvmnumber_table_u64->B[Idx];
        else
            temp = g_llvmnumber_table_u64->A[Idx];
    }

    return temp;
}

void llvmdata_mem_add_value(void* caller_pc, const char* s1, const char* s2, size_t n1, size_t n2)
{
    if (g_llvmdata_is_has_init == 0)
        return;

    size_t Len1 = MIN(n1, SIZE_llvmmem_data);
    size_t Len2 = MIN(n2, SIZE_llvmmem_data);

    size_t Idx = (size_t)caller_pc % SIZE_llvmmem_table;
    size_t i = 0;

    for (i = 0; i < Len1; i++) {
        g_llvmmem_table->A[Idx][i] = s1[i];
    }
    g_llvmmem_table->A[Idx][i] = 0;  //放置/0

    for (i = 0; i < Len2; i++) {
        g_llvmmem_table->B[Idx][i] = s2[i];
    }
    g_llvmmem_table->B[Idx][i] = 0;  //放置/0

    g_llvmmem_table->len1[Idx] = Len1;
    g_llvmmem_table->len2[Idx] = Len2;

    if (g_llvmmem_table->has_value[Idx] == 0) {
        g_llvmmem_table->has_value[Idx] = 1;
        g_llvmmem_table->has_value_table[g_llvmmem_table->has_value_total] = Idx;
        g_llvmmem_table->has_value_total++;
    }
}

void llvmdata_mem_add_value_ex(void* caller_pc, const char* s1, const char* s2)
{

    if (g_llvmdata_is_has_init == 0)
        return;

    size_t Len1 = 0;
    if (s1 != NULL) {
        //遇0结束,只保存非0内容
        for (; s1[Len1]; Len1++) {
        }
    }

    size_t Len2 = 0;
    if (s2 != NULL) {
        for (; s2[Len2]; Len2++) {
        }
    }

    llvmdata_mem_add_value(caller_pc, s1, s2, Len1, Len2);
}

int llvmdata_mem_get_count(void)
{
    if (g_llvmdata_is_has_init == 0)
        return 0;

    return g_llvmmem_table->has_value_total;
}

//库里随机抽取一个
char* llvmdata_mem_get_value(int* len)
{
    if (g_llvmdata_is_has_init == 0) {
        *len = 0;
        return 0;
    }

    if (g_llvmmem_table->has_value_total == 0) {
        *len = 0;
        return 0;
    }

    size_t Idx = RAND_32() % g_llvmmem_table->has_value_total;
    Idx = g_llvmmem_table->has_value_table[Idx];
    int is_even = RAND_32() % 2;
    int is_haszero = RAND_32() % 3;  //%33的可能性增加/0,因为字符串存入的时候len没有算上/0的长度

    if (is_even)
        *len = g_llvmmem_table->len1[Idx];
    else
        *len = g_llvmmem_table->len2[Idx];

    if (is_haszero == 1)
        *len = *len + 1;

    if (is_even)
        return g_llvmmem_table->A[Idx];
    else
        return g_llvmmem_table->B[Idx];
}

void init_llvmdata(void)
{
    init_llvmdata_malloc();

    hw_Memset(g_llvmnumber_table_u64, 0, sizeof(S_llvmnumber_table_u64));
    hw_Memset(g_llvmmem_table, 0, sizeof(S_llvmmem_table));
}

#else

void llvmdata_number_add_value(void* caller_pc, u64 s1, u64 s2)
{}

u64 llvmdata_number_get_value(void)
{}

int llvmdata_number_get_count(void)
{
    return 0;
}

void llvmdata_mem_add_value(void* caller_pc, const char* s1, const char* s2, size_t n1, size_t n2)
{}

void llvmdata_mem_add_value_ex(void* caller_pc, const char* s1, const char* s2)
{}

char* llvmdata_mem_get_value(int* len)
{}

int llvmdata_mem_get_count(void)
{

    return 0;
}

void init_llvmdata(void)
{}

#endif
