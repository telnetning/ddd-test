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

原理:					原始数据的随机位置被插入随机byte

长度:					原始长度+1

数量:					MAXCOUNT

支持数据类型: 	可变长数据类型

*/

int DataElementOneByteInsert_getcount(S_Element* pElement)
{
    ASSERT_NULL(pElement);

    //爬分支变异手段，因此变异数量有所倾向
    return MAXCOUNT;
}

char* DataElementOneByteInsert_getvalue(S_Element* pElement, int pos)
{
    int in_len;
    int start;
    ASSERT_NULL(pElement);

    in_len = (int)(pElement->inLen / 8);

    //最大长度算法要所有算法必须遵守
    if ((in_len + 1) > maxOutputSize) {
        return set_ElementOriginalValue(pElement);
    }

    //本算法支持无初始值元素

    start = RAND_RANGE(0, in_len);

    set_ElementInitoutBuf_ex(pElement, in_len + 1);

    hw_Memcpy(pElement->para.value, pElement->inBuf, start);
    pElement->para.value[start] = RAND_BYTE();
    hw_Memcpy(pElement->para.value + start + 1, pElement->inBuf + start, in_len - start);

    return pElement->para.value;
}

int DataElementOneByteInsert_getissupport(S_Element* pElement)
{
    ASSERT_NULL(pElement);
    //可变数据
    if ((pElement->para.type == enum_String) || (pElement->para.type == enum_Blob) ||
        (pElement->para.type == enum_FixBlob))
        return enum_Yes;

    return enum_No;
}

const struct Mutater_group DataElementOneByteInsert_group = {"DataElementOneByteInsert",
    DataElementOneByteInsert_getcount,
    DataElementOneByteInsert_getvalue,
    DataElementOneByteInsert_getissupport,
    1};

void init_DataElementOneByteInsert(void)
{
    register_Mutater(&DataElementOneByteInsert_group, enum_DataElementOneByteInsert);
}
