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

原理:					字符串枚举变异


长度:

数量:					枚举数量

支持数据类型: 	字符串枚举类型

*/

int StringEnum_getcount(S_Element* pElement)
{

    ASSERT_NULL(pElement);
    return pElement->Enum_count;
}

char* StringEnum_getvalue(S_Element* pElement, int pos)
{
    int temp_pos = pos % pElement->Enum_count;
    int len = in_strlen(pElement->Enum_string_table[temp_pos]) + 1;
    set_ElementInitoutBuf_ex(pElement, len);
    hw_Memcpy(pElement->para.value, pElement->Enum_string_table[temp_pos], len);
    return pElement->para.value;
}

int StringEnum_getissupport(S_Element* pElement)
{
    if (pElement->para.type == enum_String_Enum)
        return enum_Yes;

    return enum_No;
}

const struct Mutater_group StringEnum_group = {
    .name = "StringEnum",
    .getCount = StringEnum_getcount,
    .getValue = StringEnum_getvalue,
    .getIsSupport = StringEnum_getissupport,
};

void init_StringEnum(void)
{
    register_Mutater(&StringEnum_group, enum_StringEnum);
}
