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

使用变异数据库来替换字符串
*/

extern char* StringStatic_table[];
extern int StringStatic_table_len;

int StringStatic_getcount(S_Element* pElement)
{
    return MAXCOUNT;
}

char* StringStatic_getvalue(S_Element* pElement, int pos)
{
    int out_len;
    ASSERT_NULL(pElement);

    pos = RAND_32() % StringStatic_table_len;
    out_len = in_strlen(StringStatic_table[pos]) + 1;
    if (out_len >= pElement->para.max_len)
        out_len = pElement->para.max_len;

    set_ElementInitoutBuf_ex(pElement, out_len);

    hw_Memcpy(pElement->para.value, StringStatic_table[pos], out_len);
    return pElement->para.value;
}

int StringStatic_getissupport(S_Element* pElement)
{
    //只要是字符串就支持
    if (pElement->para.type == enum_String)
        return enum_Yes;

    return enum_No;
}

const struct Mutater_group StringStatic_group = {
    "StringStatic", StringStatic_getcount, StringStatic_getvalue, StringStatic_getissupport, 1};

void init_StringStatic(void)
{
    register_Mutater(&StringStatic_group, enum_StringStatic);
}
