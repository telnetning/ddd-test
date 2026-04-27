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

原理:					对数据bit置1，从头置1和从尾置1，置1个数依次为1到置满


长度:					长度不变

数量:					bit数乘2

支持数据类型: 	有初值,枚举类除外

*/

int DataElementBitFill_getcount(S_Element* pElement)
{

    ASSERT_NULL(pElement);

    return MIN(pElement->inLen * 2, MAXCOUNT / 5);
}

char* DataElementBitFill_getvalue(S_Element* pElement, int pos)
{
    //如果bit长度不被8整除，则加1
    int in_len, i = 0;
    ASSERT_NULL(pElement);

    pos = RAND_RANGE(0, pElement->inLen * 2 - 1);

    in_len = (int)(pElement->inLen / 8) + IsAddone(pElement->inLen % 8);

    set_ElementInitoutBuf_ex(pElement, in_len);

    hw_Memcpy(pElement->para.value, pElement->inBuf, in_len);

    if (pos < pElement->inLen) {
        for (i = 0; i <= pos; i++) {
            FILL_BIT(pElement->para.value, i);
        }
    } else {
        pos = pos - pElement->inLen;
        for (i = pos; i < pElement->inLen; i++) {
            FILL_BIT(pElement->para.value, i);
        }
    }

    return pElement->para.value;
}

int DataElementBitFill_getissupport(S_Element* pElement)
{
    ASSERT_NULL(pElement);

    //枚举不支持
    if ((pElement->para.type == enum_Number_Enum) || (pElement->para.type == enum_Number_Range) ||
        (pElement->para.type == enum_String_Enum) || (pElement->para.type == enum_Blob_Enum))
        return enum_No;

    //只要有初始值，就支持，对所有数据类型开放有意义否?
    if (pElement->isHasInitValue == enum_Yes)
        return enum_Yes;
    return enum_No;
}

const struct Mutater_group DataElementBitFill_group = {
    "DataElementBitFill", DataElementBitFill_getcount, DataElementBitFill_getvalue, DataElementBitFill_getissupport, 1};

void init_DataElementBitFill(void)
{
    register_Mutater(&DataElementBitFill_group, enum_DataElementBitFill);
}
