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

原理:					利用hook来源数据替换

长度:

数量:					MAXCOUNT*2

支持数据类型: 	可变长数据类型
*/

int DataElementMagic_getcount(S_Element* pElement)
{
    if (llvmdata_mem_get_count() == 0)
        return 0;

    return MAXCOUNT * 2;
}

char* DataElementMagic_getvalue(S_Element* pElement, int pos)
{
    int llvm_len;
    char* value = NULL;
    ASSERT_NULL(pElement);

    value = llvmdata_mem_get_value(&llvm_len);

    //库没有值，则空
    if (llvm_len == 0) {
        return set_ElementOriginalValue(pElement);
    }

    if (llvm_len > pElement->para.max_len) {
        llvm_len = pElement->para.max_len;
    }

    set_ElementInitoutBuf_ex(pElement, llvm_len);
    hw_Memcpy(pElement->para.value, value, llvm_len);

    return (char*)pElement->para.value;
}

int DataElementMagic_getissupport(S_Element* pElement)
{
    //
    if ((pElement->para.type == enum_String) || (pElement->para.type == enum_Blob) ||
        (pElement->para.type == enum_FixBlob))
        return enum_Yes;

    return enum_No;
}

const struct Mutater_group DataElementMagic_group = {
    "DataElementMagic", DataElementMagic_getcount, DataElementMagic_getvalue, DataElementMagic_getissupport, 1};

void init_DataElementMagic(void)
{
    if (llvmhook_is_support() == 0)
        return;

    register_Mutater(&DataElementMagic_group, enum_DataElementMagic);
}
