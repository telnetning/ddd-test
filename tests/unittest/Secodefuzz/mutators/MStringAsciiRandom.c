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

原理:					每次变异n个byte,使用随机ascii的值
                        变异几个byte使用高斯变异


长度:					长度不变

数量:					bit数量乘4与MAXCOUNT的最小值

支持数据类型: 	有初始值的字符串类型

*/

int StringAsciiRandom_getcount(S_Element* pElement)
{
    ASSERT_NULL(pElement);

    //目前设置这个变异算法数量为bit数乘4与500的最小值
    return MIN(pElement->inLen * 4, MAXCOUNT);
}

char* StringAsciiRandom_getvalue(S_Element* pElement, int pos)
{
    int i = 0;
    int in_len;

    in_len = (int)(pElement->inLen / 8);

    set_ElementInitoutBuf_ex(pElement, in_len);

    //计算这次变异变几个字母
    int number1 = gaussRandu32(pos % in_GetBitNumber(in_len));

    hw_Memcpy(pElement->para.value, pElement->inBuf, in_len);

    for (i = 0; i < in_len; i++) {
        //计算这个字母是否需要变异
        if (RAND_RANGE(1, in_len) <= number1) {
            ((char*)pElement->para.value)[i] = (char)RAND_RANGE(1, 127);
        }
    }

    ((char*)pElement->para.value)[in_len - 1] = 0;

    return pElement->para.value;
}

int StringAsciiRandom_getissupport(S_Element* pElement)
{
    //有初始值的字符串
    if ((pElement->para.type == enum_String) && (pElement->isHasInitValue == enum_Yes))
        return enum_Yes;

    return enum_No;
}

const struct Mutater_group StringAsciiRandom_group = {
    .name = "StringAsciiRandom",
    .getCount = StringAsciiRandom_getcount,
    .getValue = StringAsciiRandom_getvalue,
    .getIsSupport = StringAsciiRandom_getissupport,
};

void init_StringAsciiRandom(void)
{
    register_Mutater(&StringAsciiRandom_group, enum_StringAsciiRandom);
}
