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

原理:					随机复制某部分数据，
                        复制个数随机,复制1次的个数为25%


长度:					最大值和原长度之间变异，

数量:					MAXCOUNT

支持数据类型: 	有初始值的可变数据
*/

static int DataElementLengthRepeatPart_getcount(S_Element* pElement)
{

    ASSERT_NULL(pElement);
    return MAXCOUNT;
}

static char* DataElementLengthRepeatPart_getvalue(S_Element* pElement, int pos)
{
    int i;
    int in_len;
    int RepeatLen;
    int count;
    int start;
    ASSERT_NULL(pElement);

    in_len = (int)(pElement->inLen / 8);

    //得到要复制的byte个数
    RepeatLen = gaussRandu32(RAND_32() % in_GetBitNumber(in_len));

    //减少复制1byte的个数，因为意义比较小，被随机的概率又太高
    if (RepeatLen == 1)
        RepeatLen = gaussRandu32(RAND_32() % in_GetBitNumber(in_len));

    //随机得到要插入的起始位置
    start = RAND_RANGE(0, in_len - RepeatLen);

    //随机得到要复制的个数
    count = gaussRandu32(RAND_32() % in_GetBitNumber(pElement->para.max_len / RepeatLen));

    //增加复制1次的个数为%25
    if (RAND_32() % 4 == 0)
        count = 1;

    if ((in_len + count * RepeatLen) > maxOutputSize) {
        return set_ElementOriginalValue(pElement);
    }

    set_ElementInitoutBuf_ex(pElement, (in_len + count * RepeatLen));

    //拷贝开始
    hw_Memcpy(pElement->para.value, pElement->inBuf, start);

    for (i = 0; i < count; i++) {
        hw_Memcpy(pElement->para.value + start + i * RepeatLen, pElement->inBuf + start, RepeatLen);
    }

    //拷贝剩下的
    hw_Memcpy(pElement->para.value + start + i * RepeatLen, pElement->inBuf + start, in_len - start);

    return pElement->para.value;
}

static int DataElementLengthRepeatPart_getissupport(S_Element* pElement)
{
    ASSERT_NULL(pElement);
    //有初始值的可变数据
    if (((pElement->para.type == enum_String) || (pElement->para.type == enum_Blob) ||
            (pElement->para.type == enum_FixBlob)) &&
        (pElement->isHasInitValue == enum_Yes))
        return enum_Yes;

    return enum_No;
}

const struct Mutater_group DataElementLengthRepeatPart_group = {
    .name = "DataElementLengthRepeatPart",
    .getCount = DataElementLengthRepeatPart_getcount,
    .getValue = DataElementLengthRepeatPart_getvalue,
    .getIsSupport = DataElementLengthRepeatPart_getissupport,
};

void init_DataElementLengthRepeatPart(void)
{
    register_Mutater(&DataElementLengthRepeatPart_group, enum_DataElementLengthRepeatPart);
}
