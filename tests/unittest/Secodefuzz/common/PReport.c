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

本模块提供记录报告功能

*/

#include "PCommon.h"

static char g_report_path[max_file_path] = {0};
static char g_running_testcase_name[max_file_path] = {0};

int Report_get_time(void)
{
    return hw_Get_Time();
}

void Report_Set_Running_Testcase_Name(char* name)
{
    if (name != NULL)
        hw_Memcpy(g_running_testcase_name, name, in_strlen(name) + 1);
    else
        g_running_testcase_name[0] = 0;
}

void Report_Set_Path(char* path)
{
    int size = 0;

    if (path == NULL) {
        g_report_path[0] = 0;
        return;
    }

    g_report_path[0] = 0;
    size = hw_sprintf(g_report_path, "%s_", path);

    size += hw_sprintf(g_report_path + size, "%s", hw_Get_Date());

    char* temp_head = "##############################################\r\n";

    WriteToFileFail(temp_head, sizeof(temp_head), g_report_path);

    temp_head = GET_Version();

    WriteToFileFail(temp_head, sizeof(temp_head), g_report_path);

    temp_head = "\r\n";

    WriteToFileFail(temp_head, sizeof(temp_head), g_report_path);

    temp_head = "##############################################\r\n";

    WriteToFileFail(temp_head, sizeof(temp_head), g_report_path);

    temp_head = "#\r\n";

    WriteToFileFail(temp_head, sizeof(temp_head), g_report_path);

    temp_head = "-------testcase_name----seed----run_count----run_time----result\r\n";

    WriteToFileFail(temp_head, sizeof(temp_head), g_report_path);
}

void Report_write_succeed_testcase(char* testcase_name, int seed, int run_count, int run_time)
{
    if (in_strlen(g_report_path) == 0)
        return;

    char* txt;
    txt = hw_Malloc(1000);
    txt[0] = 0;
    int size = 0;

    size += hw_sprintf(txt + size, "%20s%8d%13d%11ds   ...pass\r\n", testcase_name, seed, run_count, run_time);
    WriteToFileFail(txt, size, g_report_path);
    hw_Free(txt);
}

void Report_write_failed_testcase(void)
{

    if (in_strlen(g_report_path) == 0)
        return;

    if (in_strlen(g_running_testcase_name) == 0)
        return;

    char* txt;
    txt = hw_Malloc(1000);
    txt[0] = 0;
    int size = 0;

    size += hw_sprintf(txt + size, "%20s                                  ...failed\r\n", g_running_testcase_name);
    WriteToFileFail(txt, size, g_report_path);
    hw_Free(txt);
}

//不同测试例之间，要调用初始化函数清内存
void init_Report(void)
{}
