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

简单的定制化blob类型变异算法，
只要在两个表中填入数据，你的数据就会出现在blob的测试例里

*/

extern char* CustomBlob_table[];
extern int CustomBlob_table_len[];
extern int CustomBlob_table_count;

int CustomBlob_getcount(S_Element* pElement)
{
    ASSERT_NULL(pElement);
    int count = CustomBlob_table_count;

    if (count == 0)
        return 0;

    return MAXCOUNT / 2;
}

char* CustomBlob_getvalue(S_Element* pElement, int pos)
{
    int count = CustomBlob_table_count;
    int llvm_len;

    ASSERT_NULL(pElement);

    //库没有值，不变异
    if (count == 0) {
        return set_ElementOriginalValue(pElement);
    }

    //库里随机抽取一个
    size_t Idx = RAND_32() % count;

    llvm_len = CustomBlob_table_len[Idx];

    //插入，覆盖，还是完整替换
    int is_insert = RAND_32() % 3;

    // 0 Insert 1 Overwrite 2 replace
    magic_getvalue(pElement, CustomBlob_table[Idx], llvm_len, is_insert);

    return pElement->para.value;
}

int CustomBlob_getissupport(S_Element* pElement)
{
    ASSERT_NULL(pElement);
    //只要是Blob就支持
    if ((pElement->para.type == enum_Blob) || (pElement->para.type == enum_FixBlob))
        return enum_Yes;

    return enum_No;
}

const struct Mutater_group CustomBlob_group = {
    "CustomBlob", CustomBlob_getcount, CustomBlob_getvalue, CustomBlob_getissupport, 1};

void init_CustomBlob(void)
{
    register_Mutater(&CustomBlob_group, enum_CustomBlob);
}
