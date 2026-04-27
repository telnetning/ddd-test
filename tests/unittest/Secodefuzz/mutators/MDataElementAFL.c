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

原理:					变异算法来自AFL

长度:					0到原始长度之间

数量:					变异的数量是byte数量乘8

支持数据类型: 	有初始值的可变数据类型

*/

int DataElementAFL_getcount(S_Element* pElement)
{
    ASSERT_NULL(pElement);

    //爬分支变异手段，因此变异数量有所倾向
    return 0;
}

char* DataElementAFL_getvalue(S_Element* pElement, int pos)
{
    return NULL;
}

int DataElementAFL_getissupport(S_Element* pElement)
{
    ASSERT_NULL(pElement);
    //有初始值的可变数据
    if (((pElement->para.type == enum_Blob) || (pElement->para.type == enum_FixBlob)) &&
        (pElement->isHasInitValue == enum_Yes))
        return enum_Yes;

    return enum_No;
}

const struct Mutater_group DataElementAFL_group = {
    "DataElementAFL", DataElementAFL_getcount, DataElementAFL_getvalue, DataElementAFL_getissupport, 1};

void init_DataElementAFL(void)
{
    register_Mutater(&DataElementAFL_group, enum_DataElementAFL);
}
