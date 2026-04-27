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

static int seed = 0;
int addWholeRandomNum = 0;
int addWholeSequenceNum = 0;
int tempSequenceStartPos = 0;

void INIT_Seed(int initSeed, int startRange)
{
    if (initSeed == 0)
        initSeed = hw_time();

    seed = initSeed + startRange;
    hw_Srand(seed);

    if (isLogOpen) {
        hw_printf("[*] seed = %d\r\n", seed);
    }
}

//如果参数大于等于10时，平均变异1.6个元素
//如果参数大于100，平均变异两个
int get_RandomRate(void)
{

    if (addWholeRandomNum == 1)
        return 100;

    if (addWholeRandomNum == 2)
        return 110 / addWholeRandomNum;

    if (addWholeRandomNum == 3)
        return 120 / addWholeRandomNum;

    if (addWholeRandomNum == 4)
        return 130 / addWholeRandomNum;

    if (addWholeRandomNum == 5)
        return 140 / addWholeRandomNum;

    if (addWholeRandomNum >= 100)
        return 2;

    if (addWholeRandomNum >= 10)
        return 160 / addWholeRandomNum;

    if (addWholeRandomNum >= 6)
        return 150 / addWholeRandomNum;

    return 100;
}

int get_IsMutated()
{

    return (hw_Rand() % 100 < get_RandomRate()) ? enum_Yes : enum_No;
}

//仅用来做数量变异的时候使用，所以int足够
u32 gaussRandu32(u32 pos)
{
    u32 value = 0;
    value = RAND_RANGE(1, 1 << pos);

    return value;
}

s32 gaussRands32(u32 pos)
{
    s32 value = 0;
    value = RAND_RANGE(-1 * (1 << (pos)), 1 << (pos));

    return value;
}

u64 gaussRandu64(u32 pos)
{
    u64 value = 0;
    value = RAND_RANGE64(1, (0ULL | 1) << pos);

    return value;
}

s64 gaussRands64(u32 pos)
{
    s64 value = 0;
    value = RAND_RANGE64((0ULL | -1) * ((0ULL | 1) << pos), (0ULL | 1) << pos);

    return value;
}