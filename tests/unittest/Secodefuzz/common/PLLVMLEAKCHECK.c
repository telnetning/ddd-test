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

本模块实现运行单测试用例后进行内存泄露检测
*/

#include "PCommon.h"

#ifdef HAS_LEAKCHECK

__attribute__((weak)) int __lsan_do_recoverable_leak_check(void);
__attribute__((weak)) int __lsan_do_leak_check(void);
__attribute__((weak)) int __lsan_disable(void);
__attribute__((weak)) int __lsan_enable(void);

static int g_Enable_Do_Leak_Check = 0;

int is_doing_leak = 0;
int g_do_leak_count = 0;

void Llvm_Enable_Leak_Check(void)
{
    g_Enable_Do_Leak_Check = 1;
    g_do_leak_count = 1000;
}

void Llvm_Disable_Leak_Check(void)
{
    g_Enable_Do_Leak_Check = 0;
    g_do_leak_count = 0;
}

void Llvm_Do_Leak_Check(void)
{
    if (g_Enable_Do_Leak_Check == 0)
        return;
    if (g_do_leak_count == 0)
        return;
    g_do_leak_count--;

    int i = 0;

    is_doing_leak = 1;
    if (__lsan_do_recoverable_leak_check)
        i = __lsan_do_recoverable_leak_check();

    is_doing_leak = 0;

    if (i) {
        if (__lsan_disable)
            __lsan_disable();
        i = 0 / 0;
    }

    if (__lsan_disable)
        __lsan_disable();

    if (__lsan_enable)
        __lsan_enable();
}

#else

void Llvm_Enable_Leak_Check(void)
{}

void Llvm_Disable_Leak_Check(void)
{}

void Llvm_Do_Leak_Check(void)
{}

#endif
