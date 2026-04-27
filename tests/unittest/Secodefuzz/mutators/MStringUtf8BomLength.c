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
                        变异算法为高斯变异，
                        使用字符串本身填充
                        之后在插入1-6各bom


长度:					最大值和1之间变异，

数量:					最大输出长度开平方与MAXCOUNT/5的最小值

支持数据类型: 	有初始值的字符串类型
*/

extern u8 bom[];
extern int bom_len;

int StringUtf8BomLength_getcount(S_Element* pElement)
{
    ASSERT_NULL(pElement);

    //次数少点吧，这东西没啥大用
    return (MAXCOUNT / 50);
}

char* StringUtf8BomLength_getvalue(S_Element* pElement, int pos)
{
    int i;
    int j;
    int out_len;
    int in_len;

    ASSERT_NULL(pElement);

    //增加几个bom
    int bom_num = RAND_RANGE(1, 6);

    if (pos == 0)
        pos = 1;

    in_len = (int)(pElement->inLen / 8);
    out_len = gaussRandu32(pos % in_GetBitNumber(pElement->para.max_len));

    if (((out_len + bom_num * bom_len) > pElement->para.max_len) || (out_len == 0)) {
        return set_ElementOriginalValue(pElement);
    }

    set_ElementInitoutBuf_ex(pElement, out_len + bom_num * bom_len);

    for (i = 0; i < out_len / (in_len - 1); i++) {
        hw_Memcpy(pElement->para.value + i * (in_len - 1), pElement->inBuf, (in_len - 1));
    }

    //拷贝剩下的
    hw_Memcpy(
        pElement->para.value + i * (in_len - 1), pElement->inBuf, out_len - (out_len / (in_len - 1)) * (in_len - 1));

    for (i = 0; i < bom_num; i++) {
        //计算插入bom的位置
        int pos1 = RAND_RANGE(0, out_len);

        //先copy后边，在copy前边
        for (j = out_len; j > pos1; j--) {
            pElement->para.value[j - 1 + bom_len] = pElement->para.value[j - 1];
        }

        hw_Memcpy(pElement->para.value + pos1, bom, bom_len);

        out_len = out_len + bom_len;
    }

    ((char*)pElement->para.value)[out_len - 1] = 0;
    return pElement->para.value;
}

int StringUtf8BomLength_getissupport(S_Element* pElement)
{
    ASSERT_NULL(pElement);

    //有初始值的字符串
    if ((pElement->para.type == enum_String) && (pElement->isHasInitValue == enum_Yes) &&
        ((int)(pElement->inLen / 8) > 1))
        return enum_Yes;

    return enum_No;
}

const struct Mutater_group StringUtf8BomLength_group = {
    .name = "StringUtf8BomLength",
    .getCount = StringUtf8BomLength_getcount,
    .getValue = StringUtf8BomLength_getvalue,
    .getIsSupport = StringUtf8BomLength_getissupport,
};

void init_StringUtf8BomLength(void)
{
    register_Mutater(&StringUtf8BomLength_group, enum_StringUtf8BomLength);
}
