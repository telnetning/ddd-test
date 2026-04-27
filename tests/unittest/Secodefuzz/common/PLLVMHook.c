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

实现收集内存数据的功能

如果编译器不支持，需要注释掉HAS_LLVM声明
本模块只在高版本的gcc(6.0)和clang(3.1)以上支持

*/

#include "PCommon.h"

#ifdef HAS_HOOK

__attribute__((weak)) void __sanitizer_set_death_callback(void (*)(void));
__attribute__((weak)) void __sanitizer_print_stack_trace();

ATTRIBUTE_INTERFACE
ATTRIBUTE_NO_SANITIZE_ALL
ATTRIBUTE_TARGET_POPCNT
void __sanitizer_cov_trace_cmp8(uint64_t Arg1, uint64_t Arg2)
{
    int* PC = (int*)(__builtin_return_address(0));

    llvmdata_number_add_value(PC, Arg1, Arg2);
}

ATTRIBUTE_INTERFACE
ATTRIBUTE_NO_SANITIZE_ALL
ATTRIBUTE_TARGET_POPCNT
void __sanitizer_cov_trace_const_cmp8(uint64_t Arg1, uint64_t Arg2)
{
    int* PC = (int*)(__builtin_return_address(0));

    llvmdata_number_add_value(PC, Arg1, Arg2);
}

ATTRIBUTE_INTERFACE
ATTRIBUTE_NO_SANITIZE_ALL
ATTRIBUTE_TARGET_POPCNT
void __sanitizer_cov_trace_cmp4(uint32_t Arg1, uint32_t Arg2)
{
    int* PC = (int*)(__builtin_return_address(0));

    llvmdata_number_add_value(PC, Arg1, Arg2);
}

ATTRIBUTE_INTERFACE
ATTRIBUTE_NO_SANITIZE_ALL
ATTRIBUTE_TARGET_POPCNT
void __sanitizer_cov_trace_const_cmp4(uint32_t Arg1, uint32_t Arg2)
{
    int* PC = (int*)(__builtin_return_address(0));

    llvmdata_number_add_value(PC, Arg1, Arg2);
}

ATTRIBUTE_INTERFACE
ATTRIBUTE_NO_SANITIZE_ALL
ATTRIBUTE_TARGET_POPCNT
void __sanitizer_cov_trace_cmp2(uint16_t Arg1, uint16_t Arg2)
{
    int* PC = (int*)(__builtin_return_address(0));

    llvmdata_number_add_value(PC, Arg1, Arg2);
}

ATTRIBUTE_INTERFACE
ATTRIBUTE_NO_SANITIZE_ALL
ATTRIBUTE_TARGET_POPCNT
void __sanitizer_cov_trace_const_cmp2(uint16_t Arg1, uint16_t Arg2)
{
    int* PC = (int*)(__builtin_return_address(0));

    llvmdata_number_add_value(PC, Arg1, Arg2);
}

ATTRIBUTE_INTERFACE
ATTRIBUTE_NO_SANITIZE_ALL
ATTRIBUTE_TARGET_POPCNT
void __sanitizer_cov_trace_cmp1(uint8_t Arg1, uint8_t Arg2)
{
    //数值小，数量多，靠变异吧
    return;
}

ATTRIBUTE_INTERFACE
ATTRIBUTE_NO_SANITIZE_ALL
ATTRIBUTE_TARGET_POPCNT
void __sanitizer_cov_trace_const_cmp1(uint8_t Arg1, uint8_t Arg2)
{
    //数值小，数量多，靠变异吧
    return;
}

ATTRIBUTE_INTERFACE
ATTRIBUTE_NO_SANITIZE_ALL
ATTRIBUTE_TARGET_POPCNT
void __sanitizer_cov_trace_switch(uint64_t Val, uint64_t* Cases)
{

    int* PC = (int*)(__builtin_return_address(0));

    uint64_t N = Cases[0];
    int i = 0;

    for (i = 0; i < N; i++) {

        if (i == 0)
            llvmdata_number_add_value(PC + i, Cases[i + 2], Val);
        else
            llvmdata_number_add_value(PC + i, Cases[i + 2], 0);
    }
}

ATTRIBUTE_INTERFACE
ATTRIBUTE_NO_SANITIZE_ALL
ATTRIBUTE_TARGET_POPCNT
void __sanitizer_cov_trace_div4(uint32_t Val)
{
    int* PC = (int*)(__builtin_return_address(0));

    llvmdata_number_add_value(PC, Val, 0);
}

ATTRIBUTE_INTERFACE
ATTRIBUTE_NO_SANITIZE_ALL
ATTRIBUTE_TARGET_POPCNT
void __sanitizer_cov_trace_div8(uint64_t Val)
{
    int* PC = (int*)(__builtin_return_address(0));

    llvmdata_number_add_value(PC, Val, 0);
}

ATTRIBUTE_INTERFACE
ATTRIBUTE_NO_SANITIZE_ALL
ATTRIBUTE_TARGET_POPCNT
void __sanitizer_cov_trace_gep(uintptr_t Idx)
{
    int* PC = (int*)(__builtin_return_address(0));

    llvmdata_number_add_value(PC, Idx, 0);
}

ATTRIBUTE_INTERFACE ATTRIBUTE_NO_SANITIZE_MEMORY void __sanitizer_weak_hook_memcmp(
    void* caller_pc, const void* s1, const void* s2, size_t n, int result)
{

    // hw_printf("\r\n __sanitizer_weak_hook_memcmp    \r\n" );

    if (n <= 1)
        return;  // Not interesting.
    llvmdata_mem_add_value(caller_pc, (char*)s1, (char*)s2, n, n);
}

ATTRIBUTE_INTERFACE ATTRIBUTE_NO_SANITIZE_MEMORY void __sanitizer_weak_hook_strncmp(
    void* caller_pc, const char* s1, const char* s2, size_t n, int result)
{
    if (n <= 1)
        return;  // Not interesting.
    llvmdata_mem_add_value_ex(caller_pc, s1, s2);
}

ATTRIBUTE_INTERFACE ATTRIBUTE_NO_SANITIZE_MEMORY void __sanitizer_weak_hook_strcmp(
    void* caller_pc, const char* s1, const char* s2, int result)
{

    llvmdata_mem_add_value_ex(caller_pc, s1, s2);
}

ATTRIBUTE_INTERFACE ATTRIBUTE_NO_SANITIZE_MEMORY void __sanitizer_weak_hook_strncasecmp(
    void* called_pc, const char* s1, const char* s2, size_t n, int result)
{
    llvmdata_mem_add_value_ex(called_pc, s1, s2);
}

ATTRIBUTE_INTERFACE ATTRIBUTE_NO_SANITIZE_MEMORY void __sanitizer_weak_hook_strcasecmp(
    void* called_pc, const char* s1, const char* s2, int result)
{

    llvmdata_mem_add_value_ex(called_pc, s1, s2);
}

ATTRIBUTE_INTERFACE ATTRIBUTE_NO_SANITIZE_MEMORY void __sanitizer_weak_hook_strstr(
    void* called_pc, const char* s1, const char* s2, char* result)
{

    llvmdata_mem_add_value_ex(called_pc, s1, s2);
}

ATTRIBUTE_INTERFACE ATTRIBUTE_NO_SANITIZE_MEMORY void __sanitizer_weak_hook_strcasestr(
    void* called_pc, const char* s1, const char* s2, char* result)
{

    llvmdata_mem_add_value_ex(called_pc, s1, s2);
}

ATTRIBUTE_INTERFACE ATTRIBUTE_NO_SANITIZE_MEMORY void __sanitizer_weak_hook_memmem(
    void* called_pc, const void* s1, size_t len1, const void* s2, size_t len2, void* result)
{

    llvmdata_mem_add_value(called_pc, (char*)s1, (char*)s2, len1, len2);
}

int llvmhook_is_support()
{
    return 1;
}

void llvmhook_register_asan_callback(void (*fun)(void))
{
    if (__sanitizer_set_death_callback)
        __sanitizer_set_death_callback(fun);
}

void llvmhook_print_stack_trace(void)
{
    if (__sanitizer_print_stack_trace)
        __sanitizer_print_stack_trace();
}

#else

int llvmhook_is_support()
{
    return 0;
}

void llvmhook_register_asan_callback(void (*fun)(void))
{}

void llvmhook_print_stack_trace(void)
{}
#endif
