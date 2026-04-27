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

本模块实现支持循环语句覆盖反馈的功能

*/

#include "PCommon.h"

#ifdef HAS_TRACE_PC

static char* __sancov_trace_pc_guard_8bit_counters = NULL;  //存一个完整测试例的循环运行状态，每个循环存8个状态
static int* __sancov_trace_pc_guard_counters = NULL;  //存运行一次测试用例，每个代码块运行的次数，因为会很大，所以用整数

static char* g_cycle_pc_has_been_recorded =
    NULL;                                 //存放这个循环代码块指针是否已经被记录过，一个指针只记录一次，多了影响速度
static int* g_cycle_pc_idx_table = NULL;  //存pc指针，全局变量可能不太好
static int g_cycle_pc_tatol = 0;          //一共有多少个循环

static char* g_pc_has_been_recorded = NULL;  //存放这个代码块指针是否已经被记录过，一个指针只记录一次，多了影响速度
static int* g_pc_idx_table = NULL;           //存放运行一次测试用例，所以执行过得代码块指针'索引'
static int g_pc_tatol = 0;                   //存放运行一次测试用例执行的代码块总数

static int g_Enable_Do_8bit_counters = 0;

extern int is_has_new_feature1;

void Llvm_Record_8bit_counters(int Idx)
{
    if (g_Enable_Do_8bit_counters == 0)
        return;

    __sancov_trace_pc_guard_counters[Idx]++;
    //用来简化清空的
    if ((__sancov_trace_pc_guard_counters[Idx] == 1) && (g_pc_has_been_recorded[Idx] == 0)) {
        g_pc_has_been_recorded[Idx] = 1;
        g_pc_idx_table[g_pc_tatol++] = Idx;
    }

    //新发现的循环语句，也可能是同一个函数被调用多次，管他
    if ((__sancov_trace_pc_guard_counters[Idx] != 1) && (g_cycle_pc_has_been_recorded[Idx] == 0)) {
        g_cycle_pc_has_been_recorded[Idx] = 1;
        g_cycle_pc_idx_table[g_cycle_pc_tatol++] = Idx;  //记录索引就行啊
    }
}

void Llvm_Do_8bit_counters(void)
{
    int i = 0;

    if (g_Enable_Do_8bit_counters == 0)
        return;

    for (i = 0; i < g_cycle_pc_tatol; i++) {
        int Counter = __sancov_trace_pc_guard_counters[g_cycle_pc_idx_table[i]];
        char bit_counters = __sancov_trace_pc_guard_8bit_counters[g_cycle_pc_idx_table[i]];
        int Bit = 0;
        if (Counter < 2)
            continue;

        if (Counter >= 65535)
            Bit = 7;
        else if (Counter >= 1024)
            Bit = 6;
        else if (Counter >= 256)
            Bit = 5;
        else if (Counter >= 255)
            Bit = 4;
        else if (Counter >= 16)
            Bit = 3;
        else if (Counter >= 4)
            Bit = 2;
        else if (Counter >= 3)
            Bit = 1;
        else if (Counter >= 2)
            Bit = 0;

        if ((bit_counters & (1 << Bit)) == 0) {
            is_has_new_feature1 = 1;
            __sancov_trace_pc_guard_8bit_counters[g_cycle_pc_idx_table[i]] =
                __sancov_trace_pc_guard_8bit_counters[g_cycle_pc_idx_table[i]] | (1 << Bit);
        }
    }

    for (i = 0; i < g_pc_tatol; i++) {
        __sancov_trace_pc_guard_counters[g_pc_idx_table[i]] = 0;
        g_cycle_pc_has_been_recorded[g_pc_idx_table[i]] = 0;
        g_pc_has_been_recorded[g_pc_idx_table[i]] = 0;

        g_cycle_pc_idx_table[i] = 0;
        g_pc_idx_table[i] = 0;
    }

    g_cycle_pc_tatol = 0;
    g_pc_tatol = 0;
}

void Llvm_Enable_Do_8bit_counters(void)
{
    if (g_Enable_Do_8bit_counters == 0) {
        //开启功能才分配内存，这样在大多数场景可以节省内存
        __sancov_trace_pc_guard_8bit_counters = (char*)hw_Malloc(sizeof(char) * MAX_PC_NUM);
        __sancov_trace_pc_guard_counters = (int*)hw_Malloc(sizeof(int) * MAX_PC_NUM);
        g_cycle_pc_has_been_recorded = (char*)hw_Malloc(sizeof(char) * MAX_PC_NUM);
        g_cycle_pc_idx_table = (int*)hw_Malloc(sizeof(int) * MAX_PC_NUM);
        g_pc_has_been_recorded = (char*)hw_Malloc(sizeof(char) * MAX_PC_NUM);
        g_pc_idx_table = (int*)hw_Malloc(sizeof(int) * MAX_PC_NUM);

        hw_Memset(__sancov_trace_pc_guard_8bit_counters, 0, sizeof(char) * MAX_PC_NUM);
        hw_Memset(__sancov_trace_pc_guard_counters, 0, sizeof(int) * MAX_PC_NUM);
        hw_Memset(g_cycle_pc_has_been_recorded, 0, sizeof(char) * MAX_PC_NUM);
        hw_Memset(g_cycle_pc_idx_table, 0, sizeof(int) * MAX_PC_NUM);
        hw_Memset(g_pc_has_been_recorded, 0, sizeof(char) * MAX_PC_NUM);
        hw_Memset(g_pc_idx_table, 0, sizeof(int) * MAX_PC_NUM);

        g_cycle_pc_tatol = 0;
        g_pc_tatol = 0;
    }

    g_Enable_Do_8bit_counters = 1;
}

void Llvm_Disable_Do_8bit_counters(void)
{

    if (g_Enable_Do_8bit_counters == 1) {
        if (__sancov_trace_pc_guard_8bit_counters != NULL)
            hw_Free(__sancov_trace_pc_guard_8bit_counters);
        __sancov_trace_pc_guard_8bit_counters = NULL;

        if (__sancov_trace_pc_guard_counters != NULL)
            hw_Free(__sancov_trace_pc_guard_counters);
        __sancov_trace_pc_guard_counters = NULL;

        if (g_cycle_pc_has_been_recorded != NULL)
            hw_Free(g_cycle_pc_has_been_recorded);
        g_cycle_pc_has_been_recorded = NULL;

        if (g_cycle_pc_idx_table != NULL)
            hw_Free(g_cycle_pc_idx_table);
        g_cycle_pc_idx_table = NULL;

        if (g_pc_has_been_recorded != NULL)
            hw_Free(g_pc_has_been_recorded);
        g_pc_has_been_recorded = NULL;

        if (g_pc_idx_table != NULL)
            hw_Free(g_pc_idx_table);
        g_pc_idx_table = NULL;

        g_cycle_pc_tatol = 0;
        g_pc_tatol = 0;
    }

    g_Enable_Do_8bit_counters = 0;
}

void init_8bit_counters(void)
{
    if (g_Enable_Do_8bit_counters == 1) {
        hw_Memset(__sancov_trace_pc_guard_8bit_counters, 0, sizeof(char) * MAX_PC_NUM);
        hw_Memset(__sancov_trace_pc_guard_counters, 0, sizeof(int) * MAX_PC_NUM);
        hw_Memset(g_cycle_pc_has_been_recorded, 0, sizeof(char) * MAX_PC_NUM);
        hw_Memset(g_cycle_pc_idx_table, 0, sizeof(int) * MAX_PC_NUM);
        hw_Memset(g_pc_has_been_recorded, 0, sizeof(char) * MAX_PC_NUM);
        hw_Memset(g_pc_idx_table, 0, sizeof(int) * MAX_PC_NUM);
        g_cycle_pc_tatol = 0;
        g_pc_tatol = 0;
    }
}

#else

void Llvm_Record_8bit_counters(int Idx)
{}

void Llvm_Do_8bit_counters(void)
{}

void Llvm_Enable_Do_8bit_counters(void)
{}

void Llvm_Disable_Do_8bit_counters(void)
{}

void init_8bit_counters(void)
{}

#endif
