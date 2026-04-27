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

原理:					算法通过改变数据几个bit值来产生测试用例,
                        一个测试用例只改变2-6个bit


长度:					长度不变

数量:					bit数乘2与MAXCOUNT/5的最小值

支持数据类型: 	有初值,枚举类除外

*/

int DataElementMBitFlipper_getcount(S_Element* pElement)
{
    ASSERT_NULL(pElement);

    return MIN((int)pElement->inLen * 2, MAXCOUNT / 5);
}

char* DataElementMBitFlipper_getvalue(S_Element* pElement, int pos)
{
    //如果bit长度不被8整除，则加1
    int in_len;
    int count;
    ASSERT_NULL(pElement);

    in_len = (int)(pElement->inLen / 8) + IsAddone(pElement->inLen % 8);

    set_ElementInitoutBuf_ex(pElement, in_len);

    hw_Memcpy(pElement->para.value, pElement->inBuf, in_len);

    count = RAND_RANGE(2, 6);
    while (count--) {
        FLIP_BIT(pElement->para.value, RAND_RANGE(0, pElement->inLen - 1));
    }

    return pElement->para.value;
}

int DataElementMBitFlipper_getissupport(S_Element* pElement)
{
    ASSERT_NULL(pElement);

    //枚举不支持
    if ((pElement->para.type == enum_Number_Enum) || (pElement->para.type == enum_Number_Range) ||
        (pElement->para.type == enum_String_Enum) || (pElement->para.type == enum_Blob_Enum))
        return enum_No;

    //只要有初始值，就支持
    if (pElement->isHasInitValue == enum_Yes)
        return enum_Yes;

    return enum_No;
}

const struct Mutater_group DataElementMBitFlipper_group = {"DataElementMBitFlipper",
    DataElementMBitFlipper_getcount,
    DataElementMBitFlipper_getvalue,
    DataElementMBitFlipper_getissupport,
    1};

void init_DataElementMBitFlipper(void)
{
    register_Mutater(&DataElementMBitFlipper_group, enum_DataElementMBitFlipper);
}
