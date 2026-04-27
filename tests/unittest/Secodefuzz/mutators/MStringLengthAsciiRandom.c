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
                        使用随机的ascii来扩充字符串，
                        偶数测试例在原有字符串上扩充，
                        奇数测试例不使用原有字符串

长度:					长度在最大值和1之间

数量:					最大输出长度开平方与MAXCOUNT的最小值

支持数据类型: 	字符串类型

*/

int StringLengthAsciiRandom_getcount(S_Element* pElement)
{
    ASSERT_NULL(pElement);

    return MIN(in_sqrt(pElement->para.max_len), MAXCOUNT);
}

char* StringLengthAsciiRandom_getvalue(S_Element* pElement, int pos)
{
    int i = 0;
    int start, change_len, in_len = 0, out_len = 0;

    change_len = gaussRandu32(pos % in_GetBitNumber(pElement->para.max_len));

    //偶数测试例在源字符串上追加
    //奇数测试例不使用原始字符串
    if (pos % 2 == 0) {
        if (pElement->isHasInitValue == enum_Yes)
            in_len = (int)(pElement->inLen / 8);
        else
            in_len = 0;
    }
    out_len = in_len + change_len;

    if (out_len > pElement->para.max_len)
        out_len = pElement->para.max_len;

    set_ElementInitoutBuf_ex(pElement, out_len);

    if (in_len > 0) {
        hw_Memcpy(pElement->para.value, pElement->inBuf, in_len);
        start = in_len - 1;
    } else
        start = 0;

    for (i = start; i < out_len - 1; i++) {
        ((char*)pElement->para.value)[i] = (char)RAND_RANGE(0x20, 127);
    }

    ((char*)pElement->para.value)[out_len - 1] = 0;

    return pElement->para.value;
}

int StringLengthAsciiRandom_getissupport(S_Element* pElement)
{
    //本测试例不需要有初始值
    if (pElement->para.type == enum_String)
        return enum_Yes;

    return enum_No;
}

const struct Mutater_group StringLengthAsciiRandom_group = {
    .name = "StringLengthAsciiRandom",
    .getCount = StringLengthAsciiRandom_getcount,
    .getValue = StringLengthAsciiRandom_getvalue,
    .getIsSupport = StringLengthAsciiRandom_getissupport,
};

void init_StringLengthAsciiRandom(void)
{
    register_Mutater(&StringLengthAsciiRandom_group, enum_StringLengthAsciiRandom);
}
