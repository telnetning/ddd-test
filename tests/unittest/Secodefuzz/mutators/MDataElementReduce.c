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

原理:					原始数据的随机数量的连续的byte会被删除.

长度:					0到原始长度之间

数量:					byte数量乘8与MAXCOUNT的最小值

支持数据类型: 	有初始值的可变数据

*/

int DataElementReduce_getcount(S_Element* pElement)
{
    ASSERT_NULL(pElement);

    return MIN(pElement->inLen, MAXCOUNT);
}

char* DataElementReduce_getvalue(S_Element* pElement, int pos)
{
    int len;
    int start;
    int deleteLen;

    ASSERT_NULL(pElement);

    len = (int)(pElement->inLen / 8);

    //得到要删除的byte个数
    deleteLen = gaussRandu32(pos % in_GetBitNumber(len));

    if (len == deleteLen) {
        return set_ElementOriginalValue(pElement);
    }

    //随机得到要删除的位置
    start = RAND_RANGE(0, len - deleteLen);

    set_ElementInitoutBuf_ex(pElement, len - deleteLen);

    hw_Memcpy(pElement->para.value, pElement->inBuf, start);
    hw_Memcpy(pElement->para.value + start, pElement->inBuf + deleteLen + start, len - deleteLen - start);

    return pElement->para.value;
}

int DataElementReduce_getissupport(S_Element* pElement)
{
    ASSERT_NULL(pElement);
    //有初始值的可变数据
    if (((pElement->para.type == enum_String) || (pElement->para.type == enum_Blob) ||
            (pElement->para.type == enum_FixBlob)) &&
        (pElement->isHasInitValue == enum_Yes))
        return enum_Yes;

    return enum_No;
}

const struct Mutater_group DataElementReduce_group = {
    "DataElementReduce", DataElementReduce_getcount, DataElementReduce_getvalue, DataElementReduce_getissupport, 1};

void init_DataElementReduce(void)
{
    register_Mutater(&DataElementReduce_group, enum_DataElementReduce);
}
