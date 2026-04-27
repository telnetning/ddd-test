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

长度:					长度不变

数量:					MAXCOUNT*2

支持数据类型: 	整数类型
*/

//----------------------64

int NumberMagic_getcount(S_Element* pElement)
{
    if (llvmdata_number_get_count() == 0)
        return 0;

    return MAXCOUNT * 2;
}

char* NumberMagic_getvalue(S_Element* pElement, int pos)
{
    int len;

    ASSERT_NULL(pElement);

    //如果bit长度不被8整除，则加1
    len = (int)(pElement->inLen / 8) + IsAddone(pElement->inLen % 8);

    set_ElementInitoutBuf_ex(pElement, len);

    u64 temp = llvmdata_number_get_value();

    if (pElement->para.type == enum_NumberS) {
        switch (len) {
            case 1:
                *((s8*)pElement->para.value) = temp;
                break;
            case 2:
                *((s16*)pElement->para.value) = temp;
                break;
            case 4:
                *((s32*)pElement->para.value) = temp;
                break;
            case 8:
                *((s64*)pElement->para.value) = temp;
                break;
        }
    } else {
        switch (len) {
            case 1:
                *((u8*)pElement->para.value) = temp;
                break;
            case 2:
                *((u16*)pElement->para.value) = temp;
                break;
            case 4:
                *((u32*)pElement->para.value) = temp;
                break;
            case 8:
                *((u64*)pElement->para.value) = temp;
                break;
        }
    }

    return pElement->para.value;
}

int NumberMagic_getissupport(S_Element* pElement)
{
    //先不支持字符串，以后再加
    if ((pElement->para.type == enum_NumberU) || (pElement->para.type == enum_NumberS))
        return enum_Yes;

    return enum_No;
}

const struct Mutater_group NumberMagic_group = {
    "NumberMagic", NumberMagic_getcount, NumberMagic_getvalue, NumberMagic_getissupport, 1};

void init_NumberMagic(void)
{
    if (llvmhook_is_support() == 0)
        return;

    register_Mutater(&NumberMagic_group, enum_NumberMagic);
}