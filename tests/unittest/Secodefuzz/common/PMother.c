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
#include "PCommon.h"

//提供几个简单的变异函数，非常简单，欢迎脑洞增加之
//只能在有效值之间来回变异，每次都变
int Mutater_NumberRange(int min, int max)
{
    return RAND_RANGE(min, max);
}

int Mutater_NumberEnum(int Number_table[], int count)
{

    return Number_table[RAND_RANGE(0, count - 1)];
}

char* Mutater_StringEnum(char* String_table[], int count)
{

    return String_table[RAND_RANGE(0, count - 1)];
}

unsigned int num_online_cpus = 8;

unsigned long get_cpu1(void)
{
    int i;
    i = hw_Rand() % 100;

    switch (i) {
        case 0:
            return -1;
        case 1:
            return hw_Rand() % 4096;
#ifndef _MSC_VER
        case 2 ... 99:
            return hw_Rand() % num_online_cpus;
#endif
    }
    return 0;
}

struct Mutater_Fill_Arg {
    unsigned long (*fill_arg)(void);
};

struct Mutater_Fill_Arg g_Mutater_fill_arg[ARG_MAX1];

int register_Mutater_fill_arg(enum argtype argType, unsigned long (*fill_arg)(void))
{
    g_Mutater_fill_arg[argType].fill_arg = fill_arg;
    return 1;
}

unsigned long Mutater_fill_arg(enum argtype argType)
{

    if (g_Mutater_fill_arg[argType].fill_arg)
        return g_Mutater_fill_arg[argType].fill_arg();
    return -1;
}