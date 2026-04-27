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

原理:					变异字符串长度  ，
                        变异算法为宽度临界值变异，
                        使用字符串本身填充


长度:					最大值和1之间

数量:					最大输出长度的bit宽度

支持数据类型: 	有初始值的字符串类型
*/

static int StringLengthEdgeCase_getcount(S_Element* pElement)
{
    u32 count;
    int in_len;
    ASSERT_NULL(pElement);

    in_len = (int)(pElement->inLen / 8);

    if (in_len <= 1)
        return 0;

    count = in_GetBitNumber(pElement->para.max_len);
    return count * 2 - 1;
}

static char* StringLengthEdgeCase_getvalue(S_Element* pElement, int pos)
{
    int i;
    int out_len;
    int in_len;

    ASSERT_NULL(pElement);

    if (pos == 0)
        pos = 1;

    in_len = (int)(pElement->inLen / 8);
    out_len = EdgeCase_table[pos];

    set_ElementInitoutBuf_ex(pElement, out_len);

    for (i = 0; i < out_len / (in_len - 1); i++) {
        hw_Memcpy(pElement->para.value + i * (in_len - 1), pElement->inBuf, (in_len - 1));
    }

    //拷贝剩下的
    hw_Memcpy(
        pElement->para.value + i * (in_len - 1), pElement->inBuf, out_len - (out_len / (in_len - 1)) * (in_len - 1));

    ((char*)pElement->para.value)[out_len - 1] = 0;
    return pElement->para.value;
}

static int StringLengthEdgeCase_getissupport(S_Element* pElement)
{
    ASSERT_NULL(pElement);
    //有初始值的字符串
    if ((pElement->para.type == enum_String) && (pElement->isHasInitValue == enum_Yes) &&
        ((int)(pElement->inLen / 8) > 1))
        return enum_Yes;

    return enum_No;
}

const struct Mutater_group StringLengthEdgeCase_group = {"StringLengthEdgeCase",
    StringLengthEdgeCase_getcount,
    StringLengthEdgeCase_getvalue,
    StringLengthEdgeCase_getissupport,
    1};

void init_StringLengthEdgeCase(void)
{
    register_Mutater(&StringLengthEdgeCase_group, enum_StringLengthEdgeCase);
}
