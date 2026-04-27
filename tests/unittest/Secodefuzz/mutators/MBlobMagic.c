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

数量:					MAXCOUNT*4

支持数据类型: 	eBlob,FixBlob
*/

static int BlobMagic_getcount(S_Element* pElement)
{
    ASSERT_NULL(pElement);

    if ((llvmdata_mem_get_count() == 0) && (llvmdata_number_get_count() == 0))
        return 0;

    return MAXCOUNT * 4;
}

static char* mem_getvalue(S_Element* pElement, int pos)
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
static char* number_getvalue(S_Element* pElement, int pos)
{
    int llvm_len;

    ASSERT_NULL(pElement);

    u64 temp = llvmdata_number_get_value();

    //把值获取出来，放到对应的内存里
    int size = RAND_32() % 3;
    char* copy = NULL;
    llvm_len = 0;

    u16 c16;
    u32 c32;
    u64 c64;

    if (size == 0) {
        c16 = temp;
        copy = (char*)&c16;
        llvm_len = 2;

    } else if (size == 1) {
        c32 = temp;
        copy = (char*)&c32;
        llvm_len = 4;
    } else if (size == 2) {
        c64 = temp;
        copy = (char*)&c64;
        llvm_len = 8;
    }

    int is_insert = RAND_32() % 3;

    // 0 Insert 1 Overwrite 2 replace
    magic_getvalue(pElement, copy, llvm_len, is_insert);

    return pElement->para.value;
}

static char* BlobMagic_getvalue(S_Element* pElement, int pos)
{
    int temp = RAND_32() % 20;

    if (temp < 5) {
        return number_getvalue(pElement, pos);
    }

    return mem_getvalue(pElement, pos);
}

static int BlobMagic_getissupport(S_Element* pElement)
{
    ASSERT_NULL(pElement);
    //支持blob,  对于fixblob,因为算法不会减小长度，对最大长度还有判断，所以可以支持
    if ((pElement->para.type == enum_Blob) || (pElement->para.type == enum_FixBlob))
        return enum_Yes;

    return enum_No;
}

const struct Mutater_group BlobMagic_group = {
    "BlobMagic", BlobMagic_getcount, BlobMagic_getvalue, BlobMagic_getissupport, 1};

void init_BlobMagic(void)
{
    if (llvmhook_is_support() == 0)
        return;

    register_Mutater(&BlobMagic_group, enum_BlobMagic);
}
