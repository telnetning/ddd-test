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
之后在插入1-6个bom
*/

extern char* StringStatic_table[];
extern int StringStatic_table_len;
extern u8 bom[];
extern int bom_len;

int StringUtf8BomStatic_getcount(S_Element* pElement)
{
    ASSERT_NULL(pElement);

    //次数少点吧，这东西没啥大用
    return (MAXCOUNT / 50);
}

char* StringUtf8BomStatic_getvalue(S_Element* pElement, int pos)
{
    int i;
    int j;
    int out_len;
    ASSERT_NULL(pElement);

    //增加几个bom
    int bom_num = RAND_RANGE(1, 6);

    pos = RAND_32() % StringStatic_table_len;
    out_len = in_strlen(StringStatic_table[pos]) + 1;
    if (out_len >= pElement->para.max_len)
        out_len = pElement->para.max_len;

    if (((out_len + bom_num * bom_len) > pElement->para.max_len) || (out_len == 0)) {
        return set_ElementOriginalValue(pElement);
    }

    set_ElementInitoutBuf_ex(pElement, out_len + bom_num * bom_len);
    hw_Memcpy(pElement->para.value, StringStatic_table[pos], out_len);

    for (i = 0; i < bom_num; i++) {
        //计算插入bom的位置
        int pos1 = RAND_RANGE(0, out_len);
        //先copy后边，在copy前边

        for (j = out_len; j > pos1; j--) {
            pElement->para.value[j - 1 + bom_len] = pElement->para.value[j - 1];
        }

        hw_Memcpy(pElement->para.value + pos1, bom, bom_len);

        out_len = out_len + bom_len;
    }

    ((char*)pElement->para.value)[out_len - 1] = 0;
    return pElement->para.value;
}

int StringUtf8BomStatic_getissupport(S_Element* pElement)
{
    //本测试例不需要有初始值
    if (pElement->para.type == enum_String)
        return enum_Yes;

    return enum_No;
}

const struct Mutater_group StringUtf8BomStatic_group = {
    .name = "StringUtf8BomStatic",
    .getCount = StringUtf8BomStatic_getcount,
    .getValue = StringUtf8BomStatic_getvalue,
    .getIsSupport = StringUtf8BomStatic_getissupport,
};

void init_StringUtf8BomStatic(void)
{
    register_Mutater(&StringUtf8BomStatic_group, enum_StringUtf8BomStatic);
}
