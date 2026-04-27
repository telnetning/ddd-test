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

原理:					范围数字变异

长度:					长度不变

数量:					范围个数与500的最小值

支持数据类型: 	范围数字类型
*/

///////////////////////////////

int NumberRange_getcount(S_Element* pElement)
{

    ASSERT_NULL(pElement);

    return MIN(pElement->max - pElement->min, MAXCOUNT);
}

char* NumberRange_getvalue(S_Element* pElement, int pos)
{
    int len;
    ASSERT_NULL(pElement);

    //如果bit长度不被8整除，则加1
    len = (int)(pElement->inLen / 8) + IsAddone(pElement->inLen % 8);

    set_ElementInitoutBuf_ex(pElement, len);

    *((s32*)pElement->para.value) = RAND_RANGE(pElement->min, pElement->max);

    return pElement->para.value;
}

int NumberRange_getissupport(S_Element* pElement)
{
    ASSERT_NULL(pElement);
    if (pElement->para.type == enum_Number_Range)
        return enum_Yes;

    return enum_No;
}

const struct Mutater_group NumberRange_group = {
    "NumberRange", NumberRange_getcount, NumberRange_getvalue, NumberRange_getissupport, 1};

void init_NumberRange(void)
{
    register_Mutater(&NumberRange_group, enum_NumberRange);
}
