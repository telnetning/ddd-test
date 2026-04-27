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

原理:					原始数据的某个byte的值被随机

长度:					长度不变

数量:					byte数量乘256与MAXCOUNT*4的最小值

支持数据类型: 	有初值,枚举类除外

*/

int DataElementByteRandom_getcount(S_Element* pElement)
{
    ASSERT_NULL(pElement);

    //爬分支最好的变异手段，因此变异数量有所倾向
    return MIN((pElement->inLen / 8) * 256, MAXCOUNT * 4);
}

char* DataElementByteRandom_getvalue(S_Element* pElement, int pos)
{
    int in_len;
    int start;

    ASSERT_NULL(pElement);

    in_len = (int)(pElement->inLen / 8);

    //随机得到 位置,因为有初值，所以不用考虑in_len=0
    start = RAND_RANGE(0, in_len - 1);

    set_ElementInitoutBuf_ex(pElement, in_len);

    hw_Memcpy(pElement->para.value, pElement->inBuf, in_len);

    pElement->para.value[start] = RAND_BYTE();
    return pElement->para.value;
}

int DataElementByteRandom_getissupport(S_Element* pElement)
{
    ASSERT_NULL(pElement);

    //枚举不支持
    if ((pElement->para.type == enum_Number_Enum) || (pElement->para.type == enum_Number_Range) ||
        (pElement->para.type == enum_String_Enum) || (pElement->para.type == enum_Blob_Enum))
        return enum_No;

    //有初始值的可变数据
    if (pElement->isHasInitValue == enum_Yes)
        return enum_Yes;

    return enum_No;
}

const struct Mutater_group DataElementByteRandom_group = {"DataElementByteRandom",
    DataElementByteRandom_getcount,
    DataElementByteRandom_getvalue,
    DataElementByteRandom_getissupport,
    1};

void init_DataElementByteRandom(void)
{
    register_Mutater(&DataElementByteRandom_group, enum_DataElementByteRandom);
}
