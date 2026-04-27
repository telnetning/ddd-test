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

原理:					变异数据长度  ，
                        变异算法为宽度临界值变异，
                        使用数据本身填充


长度:					长度在最大值和1之间

测试例数量:		测试例数量为最大输出长度的bit宽度

支持数据类型: 	有初始值的可变数据
*/

static int DataElementLengthEdgeCase_getcount(S_Element* pElement)
{
    u32 count;

    ASSERT_NULL(pElement);

    count = in_GetBitNumber(pElement->para.max_len);

    return count * 2 - 1;
}

static char* DataElementLengthEdgeCase_getvalue(S_Element* pElement, int pos)
{
    int i;
    int in_len;
    int out_len;
    ASSERT_NULL(pElement);

    in_len = (int)(pElement->inLen / 8);
    out_len = EdgeCase_table[pos];

    if ((out_len > pElement->para.max_len) || (out_len == 0)) {
        return set_ElementOriginalValue(pElement);
    }

    set_ElementInitoutBuf_ex(pElement, out_len);

    for (i = 0; i < out_len / in_len; i++) {
        hw_Memcpy(pElement->para.value + i * in_len, pElement->inBuf, in_len);
    }

    //拷贝剩下的
    hw_Memcpy(pElement->para.value + i * in_len, pElement->inBuf, out_len - (out_len / in_len) * in_len);

    return pElement->para.value;
}

static int DataElementLengthEdgeCase_getissupport(S_Element* pElement)
{
    ASSERT_NULL(pElement);
    //有初始值的可变数据
    if (((pElement->para.type == enum_String) || (pElement->para.type == enum_Blob)) &&
        (pElement->isHasInitValue == enum_Yes))
        return enum_Yes;

    return enum_No;
}

const struct Mutater_group DataElementLengthEdgeCase_group = {"DataElementLengthEdgeCase",
    DataElementLengthEdgeCase_getcount,
    DataElementLengthEdgeCase_getvalue,
    DataElementLengthEdgeCase_getissupport,
    1};

void init_DataElementLengthEdgeCase(void)
{
    register_Mutater(&DataElementLengthEdgeCase_group, enum_DataElementLengthEdgeCase);
}
