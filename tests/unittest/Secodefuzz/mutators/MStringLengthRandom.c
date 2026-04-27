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
                        变异算法为平均变异，
                        使用字符串本身填充


长度:					最大值和1之间变异，

数量:					最大输出长度开平方与MAXCOUNT/5的最小值

支持数据类型: 	有初始值的字符串类型
*/

static int StringLengthRandom_getcount(S_Element* pElement)
{

    ASSERT_NULL(pElement);
    return MIN(in_sqrt(pElement->para.max_len), MAXCOUNT / 5);
}

static char* StringLengthRandom_getvalue(S_Element* pElement, int pos)
{
    int i;
    int out_len;
    int in_len;

    ASSERT_NULL(pElement);

    if (pos == 0)
        pos = 1;

    in_len = (int)(pElement->inLen / 8);
    out_len = RAND_RANGE(1, pElement->para.max_len);

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

static int StringLengthRandom_getissupport(S_Element* pElement)
{
    ASSERT_NULL(pElement);
    //有初始值的字符串
    if ((pElement->para.type == enum_String) && (pElement->isHasInitValue == enum_Yes) &&
        ((int)(pElement->inLen / 8) > 1))
        return enum_Yes;

    return enum_No;
}

const struct Mutater_group StringLengthRandom_group = {
    .name = "StringLengthRandom",
    .getCount = StringLengthRandom_getcount,
    .getValue = StringLengthRandom_getvalue,
    .getIsSupport = StringLengthRandom_getissupport,
};

void init_StringLengthRandom(void)
{
    register_Mutater(&StringLengthRandom_group, enum_StringLengthRandom);
}
