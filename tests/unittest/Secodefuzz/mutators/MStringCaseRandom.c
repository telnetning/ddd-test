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

原理:					随机改变字符串中字符的大小写。
                        每次改变多少个字母使用高斯变异


长度:					长度不变

数量:					字母数量乘8与MAXCOUNT的最小值

支持数据类型: 	有初始值的字符串类型

*/

int StringCaseRandom_getcount(S_Element* pElement)
{

    ASSERT_NULL(pElement);

    //目前设置这个变异算法数量为bit数与500的最小值
    return MIN(in_GetLetterNumber((char*)pElement->inBuf) * 8, MAXCOUNT);
}

char* StringCaseRandom_getvalue(S_Element* pElement, int pos)
{
    int i = 0;
    int in_len;
    char tmp1, tmp2;

    in_len = (int)(pElement->inLen / 8);

    set_ElementInitoutBuf_ex(pElement, in_len);

    int number = in_GetLetterNumber((char*)pElement->inBuf);

    //计算这次变异变几个字母
    int number1 = gaussRandu32(pos % in_GetBitNumber(number));

    for (i = 0; i < in_len; i++) {
        tmp1 = ((char*)pElement->inBuf)[i];

        if (in_IsLetter(tmp1)) {
            //计算这个字母是否需要变异
            if (RAND_RANGE(1, number) <= number1) {
                tmp2 = (char)in_toupper(tmp1);
                if (tmp1 == tmp2)
                    tmp2 = (char)in_tolower(tmp1);
                tmp1 = tmp2;
            }
        }
        ((char*)pElement->para.value)[i] = tmp1;
    }
    //最后为0，无所谓

    return pElement->para.value;
}

int StringCaseRandom_getissupport(S_Element* pElement)
{
    //有初始值的字符串
    if ((pElement->para.type == enum_String) && (pElement->isHasInitValue == enum_Yes))
        return enum_Yes;

    return enum_No;
}

const struct Mutater_group StringCaseRandom_group = {
    .name = "StringCaseRandom",
    .getCount = StringCaseRandom_getcount,
    .getValue = StringCaseRandom_getvalue,
    .getIsSupport = StringCaseRandom_getissupport,
};

void init_StringCaseRandom(void)
{
    register_Mutater(&StringCaseRandom_group, enum_StringCaseRandom);
}
