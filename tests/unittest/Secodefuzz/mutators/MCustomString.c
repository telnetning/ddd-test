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

简单的定制化string类型变异算法，
只要在表中填入数据，你的数据就会出现在string类型元素的测试例里


*/

extern char* CustomString_table[];

extern int CustomString_table_count;

int CustomString_getcount(S_Element* pElement)
{
    ASSERT_NULL(pElement);
    int count = CustomString_table_count;

    if (count == 0)
        return 0;

    return MAXCOUNT / 2;
}

char* CustomString_getvalue(S_Element* pElement, int pos)
{
    int count = CustomString_table_count;
    int llvm_len;

    ASSERT_NULL(pElement);

    //库没有值，则空
    if (count == 0) {
        return set_ElementOriginalValue(pElement);
    }

    //库里随机抽取一个
    size_t Idx = RAND_32() % count;

    llvm_len = in_strlen(CustomString_table[Idx]);

    //一半加上/0
    if (RAND_32() % 2)
        llvm_len = llvm_len + 1;

    int is_insert = RAND_32() % 3;

    // 0 Insert 1 Overwrite 2 replace
    magic_getvalue(pElement, CustomString_table[Idx], llvm_len, is_insert);

    return pElement->para.value;
}

int CustomString_getissupport(S_Element* pElement)
{
    ASSERT_NULL(pElement);
    //只要是字符串就支持
    if (pElement->para.type == enum_String)
        return enum_Yes;

    return enum_No;
}

const struct Mutater_group CustomString_group = {
    "CustomString", CustomString_getcount, CustomString_getvalue, CustomString_getissupport, 1};

void init_CustomString(void)
{
    register_Mutater(&CustomString_group, enum_CustomString);
}
