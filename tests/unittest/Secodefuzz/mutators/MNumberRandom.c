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
#include "../common/PCommon.h"

/*

原理:					产生基础元素范围内的随机数

长度:					长度不变			(字符串的话最大33 或最大输出)

数量:					最大数开平方与500的最小值

支持数据类型: 	整数和有数字初始值的字符串

*/

#define maxValue64 0xffffffffffffffff
#define maxValue 0xffffffff

///////////////////////////////

int NumberRandom_getcount(S_Element* pElement)
{

    ASSERT_NULL(pElement);
    //字符串数字按照s64等同
    if (pElement->para.type == enum_String) {
        return MIN(in_sqrt(maxValue64 >> (64 - 64)), MAXCOUNT);
    }

    return MIN(in_sqrt(maxValue64 >> (64 - pElement->inLen)), MAXCOUNT);
}

char* NumberRandom_getvalue(S_Element* pElement, int pos)
{
    int len;
    ASSERT_NULL(pElement);

    if (pElement->para.type == enum_String) {
        set_ElementInitoutBuf_ex(pElement, String_Number_len);

        s64 temp = (s64)(RAND_64() & (maxValue64 >> (64 - pElement->inLen)));
        in_ltoa(temp, pElement->para.value, 10);

        len = in_strlen(pElement->para.value) + 1;
        if (len > pElement->para.max_len)
            len = pElement->para.max_len;

        pElement->para.value[len - 1] = 0;

        //重置长度为字符串实际长度
        pElement->para.len = len;

        return pElement->para.value;
    }

    //如果bit长度不被8整除，则加1
    len = (int)(pElement->inLen / 8) + IsAddone(pElement->inLen % 8);

    set_ElementInitoutBuf_ex(pElement, len);

    //有无符号分别对待
    if (pElement->para.type == enum_NumberS) {
        if (pElement->inLen <= 8)
            *((s8*)pElement->para.value) = (u8)(RAND_64() & (maxValue64 >> (64 - pElement->inLen)));
        else if (pElement->inLen <= 16)
            *((s16*)pElement->para.value) = (u16)(RAND_64() & (maxValue64 >> (64 - pElement->inLen)));
        else if (pElement->inLen <= 32)
            *((s32*)pElement->para.value) = (u32)(RAND_64() & (maxValue64 >> (64 - pElement->inLen)));
        else if (pElement->inLen <= 64)
            *((s64*)pElement->para.value) = (u64)(RAND_64() & (maxValue64 >> (64 - pElement->inLen)));
    } else if (pElement->para.type == enum_NumberU) {
        if (pElement->inLen <= 8)
            *((u8*)pElement->para.value) = (u8)(RAND_64() & (maxValue64 >> (64 - pElement->inLen)));
        else if (pElement->inLen <= 16)
            *((u16*)pElement->para.value) = (u16)(RAND_64() & (maxValue64 >> (64 - pElement->inLen)));
        else if (pElement->inLen <= 32)
            *((u32*)pElement->para.value) = (u32)(RAND_64() & (maxValue64 >> (64 - pElement->inLen)));
        else if (pElement->inLen <= 64)
            *((u64*)pElement->para.value) = (u64)(RAND_64() & (maxValue64 >> (64 - pElement->inLen)));
    }

    return pElement->para.value;
}

int NumberRandom_getissupport(S_Element* pElement)
{
    ASSERT_NULL(pElement);
    if ((pElement->para.type == enum_NumberS) || (pElement->para.type == enum_NumberU))
        return enum_Yes;

    if (in_StringIsNumber(pElement) == enum_Yes)
        return enum_Yes;

    return enum_No;
}

const struct Mutater_group NumberRandom_group = {
    "NumberRandom", NumberRandom_getcount, NumberRandom_getvalue, NumberRandom_getissupport, 1};

void init_NumberRandom(void)
{
    register_Mutater(&NumberRandom_group, enum_NumberRandom);
}
