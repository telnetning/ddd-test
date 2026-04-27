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

本模块提供检查测试例执行超时bug的功能
并提供设置测试例运行时间的能力

*/

#include "PCommon.h"

static int g_Running_Time_Second = 0;
static int g_Running_Time_start_time = 0;

void DT_Set_Running_Time_Second(int second)
{
    g_Running_Time_Second = second;
}

void Running_Time_get_start(void)
{
    if (g_Running_Time_Second == 0)
        return;

    g_Running_Time_start_time = hw_Get_Time();
}

int Running_Time_Is_Over(void)
{
    if (g_Running_Time_Second == 0)
        return 0;

    int temp_second = hw_Get_Time() - g_Running_Time_start_time;
    if (temp_second > g_Running_Time_Second) {
        return 1;
    }

    return 0;
}

static int g_TimeOut_Second = 0;
static int g_TimeOut_start_time = 0;

void DT_Set_TimeOut_Second(int second)
{
    g_TimeOut_Second = second;
}

void TimeOut_get_start_time(void)
{
    if (g_TimeOut_Second == 0)
        return;

    g_TimeOut_start_time = hw_Get_Time();
}

void TimeOut_Is_Bug(void)
{

    if (g_TimeOut_Second == 0)
        return;

    int temp_second = hw_Get_Time() - g_TimeOut_start_time;
    if (temp_second > g_TimeOut_Second) {
        int i = 0;

        hw_printf("\r\n  ********The program exit because one testcase timeout(running %d secode) !!!********  \r\n ",
            temp_second);
        i = i / 0;
    }
}
