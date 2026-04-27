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

原理:					随机交换相邻两部分数据
                        随机交换不相邻两部分数据


长度:					长度不变

数量:					MAXCOUNT

支持数据类型: 	有初始值的数据类型,enum_String,enum_Blob,enum_FixBlob
*/

static int DataElementSwapTwoPart_getcount(S_Element* pElement)
{

    ASSERT_NULL(pElement);
    return MAXCOUNT;
}

//交换不相邻两部分
static char* DataElementSwapNoNearTwoPart_getvalue(S_Element* pElement, int pos)
{
    int in_len;
    int PartLen1;
    int PartLen2;
    int start1;
    int start2;
    ASSERT_NULL(pElement);

    in_len = (int)(pElement->inLen / 8);

    //得到第一部分长度
    PartLen1 = gaussRandu32(RAND_32() % in_GetBitNumber(in_len));

    //得到第二部分长度
    PartLen2 = gaussRandu32(RAND_32() % in_GetBitNumber(in_len));

    if ((PartLen1 + PartLen2) > in_len) {
        return set_ElementOriginalValue(pElement);
    }

    //随机第一部分的起始位置
    start1 = RAND_RANGE(0, in_len - (PartLen1 + PartLen2));

    //随机第二部分的起始位置
    start2 = RAND_RANGE(0, in_len - (PartLen1 + PartLen2 + start1)) + start1 + PartLen1;

    set_ElementInitoutBuf_ex(pElement, in_len);

    //拷贝开始
    hw_Memcpy(pElement->para.value, pElement->inBuf, start1);

    //拷贝第二部分
    hw_Memcpy(pElement->para.value + start1, pElement->inBuf + start2, PartLen2);

    //拷贝中间
    hw_Memcpy(
        pElement->para.value + start1 + PartLen2, pElement->inBuf + start1 + PartLen1, start2 - start1 - PartLen1);

    //拷贝第一部分
    hw_Memcpy(pElement->para.value + start2 + PartLen2 - PartLen1, pElement->inBuf + start1, PartLen1);

    //拷贝剩下的
    hw_Memcpy(
        pElement->para.value + start2 + PartLen2, pElement->inBuf + start2 + PartLen2, in_len - (start2 + PartLen2));

    return pElement->para.value;
}

//交换相邻两部分
static char* DataElementSwapNearTwoPart_getvalue(S_Element* pElement, int pos)
{
    int in_len;
    int PartLen1;
    int PartLen2;
    int start;
    ASSERT_NULL(pElement);

    in_len = (int)(pElement->inLen / 8);

    //得到第一部分长度
    PartLen1 = gaussRandu32(RAND_32() % in_GetBitNumber(in_len));

    //得到第二部分长度
    PartLen2 = gaussRandu32(RAND_32() % in_GetBitNumber(in_len));

    if ((PartLen1 + PartLen2) > in_len) {
        return set_ElementOriginalValue(pElement);
    }

    //随机得到要交换的起始位置
    start = RAND_RANGE(0, in_len - (PartLen1 + PartLen2));

    set_ElementInitoutBuf_ex(pElement, in_len);

    //拷贝开始
    hw_Memcpy(pElement->para.value, pElement->inBuf, start);

    //拷贝第二部分
    hw_Memcpy(pElement->para.value + start, pElement->inBuf + start + PartLen1, PartLen2);

    //拷贝第一部分
    hw_Memcpy(pElement->para.value + start + PartLen2, pElement->inBuf + start, PartLen1);

    //拷贝剩下的
    hw_Memcpy(pElement->para.value + start + PartLen1 + PartLen2,
        pElement->inBuf + start + PartLen1 + PartLen2,
        in_len - (start + PartLen1 + PartLen2));

    return pElement->para.value;
}

static char* DataElementSwapTwoPart_getvalue(S_Element* pElement, int pos)
{
    if (RAND_32() % 2 == 0)
        return DataElementSwapNearTwoPart_getvalue(pElement, pos);
    else
        return DataElementSwapNoNearTwoPart_getvalue(pElement, pos);
}

static int DataElementSwapTwoPart_getissupport(S_Element* pElement)
{
    ASSERT_NULL(pElement);
    //有初始值的可变数据
    if (((pElement->para.type == enum_String) || (pElement->para.type == enum_Blob) ||
            (pElement->para.type == enum_FixBlob)) &&
        (pElement->isHasInitValue == enum_Yes))
        return enum_Yes;

    return enum_No;
}

const struct Mutater_group DataElementSwapTwoPart_group = {
    .name = "DataElementSwapTwoPart",
    .getCount = DataElementSwapTwoPart_getcount,
    .getValue = DataElementSwapTwoPart_getvalue,
    .getIsSupport = DataElementSwapTwoPart_getissupport,
};

void init_DataElementSwapTwoPart(void)
{
    register_Mutater(&DataElementSwapTwoPart_group, enum_DataElementSwapTwoPart);
}
