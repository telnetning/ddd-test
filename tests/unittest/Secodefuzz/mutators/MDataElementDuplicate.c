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

原理:					通过复制元素的个数来生成测试例
                        个数多少使用高斯变异


长度:					原始数据长度的整数倍

数量:					最大输出长度开平方与MAXCOUNT/5的最小值

支持数据类型: 	有初始值的可变数据

*/

int DataElementDuplicate_getcount(S_Element* pElement)
{
    ASSERT_NULL(pElement);
    return MIN(in_sqrt(pElement->para.max_len), MAXCOUNT / 5);
}

char* DataElementDuplicate_getvalue(S_Element* pElement, int pos)
{
    int i;
    int in_len;
    int times, maxTimes = 2;
    ASSERT_NULL(pElement);

    in_len = (int)(pElement->inLen / 8);
    maxTimes = pElement->para.max_len / in_len;

    //得到重复几次
    times = gaussRandu32(pos % in_GetBitNumber(maxTimes));

    set_ElementInitoutBuf_ex(pElement, in_len * times);

    for (i = 0; i < times; i++) {
        hw_Memcpy(pElement->para.value + i * in_len, pElement->inBuf, in_len);
    }

    return pElement->para.value;
}

int DataElementDuplicate_getissupport(S_Element* pElement)
{

    ASSERT_NULL(pElement);
    //有初始值的可变数据
    if (((pElement->para.type == enum_String) || (pElement->para.type == enum_Blob)) &&
        (pElement->isHasInitValue == enum_Yes))
        return enum_Yes;

    return enum_No;
}

const struct Mutater_group DataElementDuplicate_group = {"DataElementDuplicate",
    DataElementDuplicate_getcount,
    DataElementDuplicate_getvalue,
    DataElementDuplicate_getissupport,
    1};

void init_DataElementDuplicate(void)
{
    register_Mutater(&DataElementDuplicate_group, enum_DataElementDuplicate);
}
