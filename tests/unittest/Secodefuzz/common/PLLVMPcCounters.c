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

本模块实现编译器回调函数

实现记录覆盖率的功能

如果编译器不支持，需要注释掉HAS_LLVM声明


下边两个函数为本模块实现的对外提供的覆盖率反馈函数，
如果有其他方法也能实现覆盖率反馈，如afl，则实现下边两函数即可替换本模块
llvmtracepc_is_has_new_feature
llvmtracepc_start_feature
llvmtracepc_end_feature

*/

#include "PCommon.h"

#ifdef HAS_TRACE_PC

typedef struct {
    uint32_t *Start, *Stop;
} Module;

static Module Modules[MAX_MODULE_NUM];
static int NumModules = 0;  // linker-initialized.--一共加载了多少个带覆盖率反馈的模块
static int NumGuards = 0;   // linker-initialized.--一共多少个代码块

static uintptr_t* __sancov_trace_pc_pcs = NULL;
static uintptr_t* __sancov_trace_pc_pcs_temp = NULL;
static int g_llvmtracepc_is_has_init = 0;

static int is_print_pc = 0;
static int has_cov_pc_num = 0;
static int g_is_dump_coverage = 0;
static char PcDescr[MAX_PcDescr_SIZE];

extern int is_has_new_feature1;

__attribute__((weak)) void __sanitizer_symbolize_pc(void*, const char* fmt, char* out_buf, size_t out_buf_size);
__attribute__((weak)) void __sanitizer_dump_coverage(const uintptr_t*, uintptr_t);

static void init_malloc(void)
{
    if (g_llvmtracepc_is_has_init == 0) {
        g_llvmtracepc_is_has_init = 1;
        __sancov_trace_pc_pcs = (uintptr_t*)hw_Malloc(sizeof(uintptr_t) * MAX_PC_NUM);
        __sancov_trace_pc_pcs_temp = (uintptr_t*)hw_Malloc(sizeof(uintptr_t) * MAX_PC_NUM);

        hw_Memset(__sancov_trace_pc_pcs, 0, sizeof(uintptr_t) * MAX_PC_NUM);
        hw_Memset(__sancov_trace_pc_pcs_temp, 0, sizeof(uintptr_t) * MAX_PC_NUM);
    }
}

void Llvm_Record_Guards(uint32_t* Start, uint32_t* Stop)
{
    uint32_t* P;
    if (Start == Stop || *Start)
        return;

    for (P = Start; P < Stop; P++) {
        NumGuards++;
        if (NumGuards == MAX_PC_NUM) {
            hw_printf("WARNING: The binary has too many instrumented PCs.\n"
                      "         You may want to reduce the size of the binary\n"
                      "         for more efficient fuzzing and precise coverage data\n");
        }
        *P = NumGuards % MAX_PC_NUM;
    }
    Modules[NumModules].Start = Start;
    Modules[NumModules].Stop = Stop;
    NumModules++;
}

void Llvm_Record_pc_counters(int Idx, uintptr_t PC)
{
    //只对第一次负责，目前忽略其他
    if (__sancov_trace_pc_pcs[Idx] == 0) {
        is_has_new_feature1 = 1;
        has_cov_pc_num++;

        if (is_print_pc) {
            PcDescr[0] = 0;
            if (__sanitizer_symbolize_pc)
                __sanitizer_symbolize_pc((void*)PC, "%p %F %L", PcDescr, sizeof(PcDescr));
            PcDescr[sizeof(PcDescr) - 1] = 0;
            hw_printf("\tNEW_PC(Idx-%d;CovRate-%d\\%d): %s\n", Idx, has_cov_pc_num, NumGuards, PcDescr);
        }

        __sancov_trace_pc_pcs[Idx] = PC;
    }
}

ALWAYS_INLINE uintptr_t GetPreviousInstructionPc(uintptr_t PC)
{
    // TODO: this implementation is x86 only.
    // see sanitizer_common GetPreviousInstructionPc for full implementation.
    return PC - 1;
}

void Llvm_Set_Is_Dump_Coverage(int is_dump_coverage)
{
    g_is_dump_coverage = is_dump_coverage;
}

void Llvm_Dump_Coverage(void)
{
    int i = 0;

    if (g_is_dump_coverage == 0)
        return;

    for (i = 0; i < MAX_PC_NUM; i++) {
        if (__sancov_trace_pc_pcs[i] != 0)
            __sancov_trace_pc_pcs_temp[i] = GetPreviousInstructionPc(__sancov_trace_pc_pcs[i]);
    }

    if (__sanitizer_dump_coverage)
        __sanitizer_dump_coverage(__sancov_trace_pc_pcs_temp, MAX_PC_NUM);
}

void Llvm_Set_Is_Print_New_PC(int isPrintPC)
{
    is_print_pc = isPrintPC;
}

void init_pc_counters(void)
{
    init_malloc();

    hw_Memset(__sancov_trace_pc_pcs, 0, sizeof(uintptr_t) * MAX_PC_NUM);
    has_cov_pc_num = 0;
}

#else

void Llvm_Record_Guards(uint32_t* Start, uint32_t* Stop)
{}

void Llvm_Record_pc_counters(int Idx, uintptr_t PC)
{}

void Llvm_Set_Is_Dump_Coverage(int is_dump_coverage)
{}

void Llvm_Set_Is_Print_New_PC(int isPrintPC)
{}

void Llvm_Dump_Coverage(void)
{}

void init_pc_counters(void)
{}

#endif
