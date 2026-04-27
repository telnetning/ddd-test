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

原理:						常见容易引起问题的字符串，被插入，覆盖，替换元素
                            只要在表中填入数据，你的数据就会出现在测试例里

长度:						0到最大长度之间

数量:						MAXCOUNT

支持数据类型: 		string,Blob,FixBlob


*/
extern char* StringStatic_table[];
extern int StringStatic_table_len;

int DataElementStringStatic_getcount(S_Element* pElement)
{
    ASSERT_NULL(pElement);
    int count = StringStatic_table_len;

    if (count == 0)
        return 0;

    return MAXCOUNT;
}

char* DataElementStringStatic_getvalue(S_Element* pElement, int pos)
{
    int count = StringStatic_table_len;
    int llvm_len;

    ASSERT_NULL(pElement);

    //库没有值，则空
    if (count == 0) {
        return set_ElementOriginalValue(pElement);
    }

    //库里随机抽取一个
    size_t Idx = RAND_32() % count;

    llvm_len = in_strlen(StringStatic_table[Idx]);

    if (llvm_len == 0) {
        return set_ElementOriginalValue(pElement);
    }

    //一半加上/0
    if (RAND_32() % 2)
        llvm_len = llvm_len + 1;

    int is_insert = RAND_32() % 3;

    // 0 Insert 1 Overwrite 2 replace
    magic_getvalue(pElement, StringStatic_table[Idx], llvm_len, is_insert);

    return pElement->para.value;
}

int DataElementStringStatic_getissupport(S_Element* pElement)
{
    ASSERT_NULL(pElement);
    // 对于fixblob,因为算法不会减小长度，对最大长度还有判断，所以可以支持
    if ((pElement->para.type == enum_String) || (pElement->para.type == enum_Blob) ||
        (pElement->para.type == enum_FixBlob))
        return enum_Yes;

    return enum_No;
}

const struct Mutater_group DataElementStringStatic_group = {"DataElementStringStatic",
    DataElementStringStatic_getcount,
    DataElementStringStatic_getvalue,
    DataElementStringStatic_getissupport,
    1};

void init_DataElementStringStatic(void)
{
    register_Mutater(&DataElementStringStatic_group, enum_DataElementStringStatic);
}
