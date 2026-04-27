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

原理:					所有小写字母变成大写,如果没有小写字母，则为原值


长度:					长度不变

数量:					1

支持数据类型: 	有初始值的字符串类型

*/

int StringCaseUpper_getcount(S_Element* pElement)
{

    ASSERT_NULL(pElement);
    return 1;
}

char* StringCaseUpper_getvalue(S_Element* pElement, int pos)
{
    int i = 0;
    int in_len = (int)(pElement->inLen / 8);

    set_ElementInitoutBuf_ex(pElement, in_len);

    //最后为0，无所谓
    for (i = 0; i < in_len; i++) {
        ((char*)pElement->para.value)[i] = in_toupper(((char*)pElement->inBuf)[i]);
    }

    return pElement->para.value;
}

int StringCaseUpper_getissupport(S_Element* pElement)
{
    //有初始值的字符串
    if ((pElement->para.type == enum_String) && (pElement->isHasInitValue == enum_Yes))
        return enum_Yes;

    return enum_No;
}

const struct Mutater_group StringCaseUpper_group = {
    .name = "StringCaseUpper",
    .getCount = StringCaseUpper_getcount,
    .getValue = StringCaseUpper_getvalue,
    .getIsSupport = StringCaseUpper_getissupport,
};

void init_StringCaseUpper(void)
{
    register_Mutater(&StringCaseUpper_group, enum_StringCaseUpper);
}
