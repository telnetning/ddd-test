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

int is_has_new_feature1 = 0;
static int is_enable_feature = 0;

///////////////////////////////////////////
ATTRIBUTE_INTERFACE
void __sanitizer_cov_trace_pc_guard_init(uint32_t* Start, uint32_t* Stop)
{

    // hw_printf("\r\n __sanitizer_cov_trace_pc_guard_init  \r\n" );
    Llvm_Record_Guards(Start, Stop);
}

ATTRIBUTE_INTERFACE
void __sanitizer_cov_8bit_counters_init(uint8_t* Start, uint8_t* Stop)
{

    hw_printf("\r\n __sanitizer_cov_8bit_counters_init  %p  %p   \r\n", Start, Stop);
}

ATTRIBUTE_INTERFACE
void __sanitizer_cov_pcs_init(const uint8_t* pcs_beg, const uint8_t* pcs_end)
{
    hw_printf("\r\n __sanitizer_cov_pcs_init  %p  %p   \r\n", pcs_beg, pcs_end);
}

ATTRIBUTE_INTERFACE
ATTRIBUTE_NO_SANITIZE_ALL
void __sanitizer_cov_trace_pc_indirect(uintptr_t Callee)
{
    // hw_printf("\r\n __sanitizer_cov_trace_pc_indir  \r\n" );
}

ATTRIBUTE_INTERFACE
ATTRIBUTE_NO_SANITIZE_ALL
void __sanitizer_cov_trace_pc_indir(uintptr_t Callee)
{
    // hw_printf("\r\n __sanitizer_cov_trace_pc_indir  %p\r\n" ,Callee);
    // uintptr_t PC = reinterpret_cast<uintptr_t>(__builtin_return_address(0));
    // fuzzer::TPC.HandleCallerCallee(PC, Callee);
}

#ifdef _WIN32__

#pragma section(".SCOV$A", read, write)  // NOLINT
#pragma section(".SCOV$Z", read, write)  // NOLINT

__declspec(allocate(".SCOV$A")) uint32_t __start___sancov_guards = 0;
__declspec(allocate(".SCOV$Z")) uint32_t __stop___sancov_guards = 0;
#endif

ATTRIBUTE_INTERFACE
ATTRIBUTE_NO_SANITIZE_ALL
void __sanitizer_cov_trace_pc_guard(uint32_t* Guard)
{

    if (is_enable_feature == 0)
        return;

    uintptr_t PC = (uintptr_t)(__builtin_return_address(0));
#ifndef _WIN32
    int Idx = *Guard;
#else
    int Idx = (size_t)PC % (MAX_PC_NUM);
#endif

    Llvm_Record_pc_counters(Idx, PC);
    Llvm_Record_8bit_counters(Idx);
}

// Best-effort support for -fsanitize-coverage=trace-pc, which is available
// in both Clang and GCC.
ATTRIBUTE_INTERFACE
ATTRIBUTE_NO_SANITIZE_ALL
void __sanitizer_cov_trace_pc()
{

    if (is_enable_feature == 0)
        return;

    uintptr_t PC = (uintptr_t)(__builtin_return_address(0));
    int Idx = (size_t)PC % (MAX_PC_NUM);

    Llvm_Record_pc_counters(Idx, PC);
    Llvm_Record_8bit_counters(Idx);
}

int llvmtracepc_is_has_new_feature(void)
{
    int ret = is_has_new_feature1;
    is_has_new_feature1 = 0;
    return ret;
}

void llvmtracepc_start_feature(void)
{
    is_has_new_feature1 = 0;
    is_enable_feature = 1;
}

void llvmtracepc_end_feature(void)
{
    is_enable_feature = 0;
}

#else

int llvmtracepc_is_has_new_feature()
{
    return 0;
}

void llvmtracepc_start_feature()
{}

void llvmtracepc_end_feature(void)
{}
#endif
