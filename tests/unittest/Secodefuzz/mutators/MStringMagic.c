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

原理:					利用hook来源数据插入或覆盖

长度:

数量:					MAXCOUNT*2

支持数据类型: 	enum_String
*/

static int StringMagic_getcount(S_Element* pElement)
{
    ASSERT_NULL(pElement);

    if (llvmdata_mem_get_count() == 0)
        return 0;

    return MAXCOUNT * 2;
}

char* StringMagic_getvalue(S_Element* pElement, int pos)
{
    int llvm_len;
    char* value = NULL;

    ASSERT_NULL(pElement);

    // \0增加的逻辑在函数里边
    value = llvmdata_mem_get_value(&llvm_len);

    //库没有值，则空
    if (llvm_len == 0) {
        return set_ElementOriginalValue(pElement);
    }

    int is_insert = RAND_32() % 3;

    // 0 Insert 1 Overwrite 2 replace
    magic_getvalue(pElement, value, llvm_len, is_insert);

    return pElement->para.value;
}

static int StringMagic_getissupport(S_Element* pElement)
{
    ASSERT_NULL(pElement);
    //字符串
    if (pElement->para.type == enum_String)
        return enum_Yes;

    return enum_No;
}

const struct Mutater_group StringMagic_group = {
    .name = "StringMagic",
    .getCount = StringMagic_getcount,
    .getValue = StringMagic_getvalue,
    .getIsSupport = StringMagic_getissupport,
};

void init_StringMagic(void)
{
    if (llvmhook_is_support() == 0)
        return;

    register_Mutater(&StringMagic_group, enum_StringMagic);
}
