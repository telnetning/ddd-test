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

原理:					获取以初始值为中心，高斯变异的测试例

长度:					长度不变			(字符串的话最大33 或最大输出)

数量:					bit值乘4

支持数据类型: 	整数和有数字初始值的字符串

*/

int NumberVariance_getcount(S_Element* pElement)
{
    //字符串数字按照s64等同
    if (pElement->para.type == enum_String) {
        return 64 * 2 * 2;
    }

    return pElement->inLen * 2 * 2;
}

char* NumberVariance_getvalue(S_Element* pElement, int pos)
{
    int len;
    s64 sValue;

    ASSERT_NULL(pElement);

    if (pElement->para.type == enum_String) {
        set_ElementInitoutBuf_ex(pElement, String_Number_len);

        sValue = gaussRands64(pos % 64);

        s64 temp = (s64)sValue + *(s64*)pElement->numberValue;
        in_ltoa(temp, pElement->para.value, 10);

        len = in_strlen(pElement->para.value) + 1;
        if (len > pElement->para.max_len)
            len = pElement->para.max_len;

        pElement->para.value[len - 1] = 0;

        //重置长度为字符串实际长度
        pElement->para.len = len;

        return pElement->para.value;
    }

    //如果bit长度不被8整除，则加1
    len = (int)(pElement->inLen / 8) + IsAddone(pElement->inLen % 8);

    set_ElementInitoutBuf_ex(pElement, len);

    if (pElement->para.type == enum_NumberS) {
        switch (len) {
            case 1:
                *((s8*)pElement->para.value) = (s8)gaussRands64(pos % 8) + *(s8*)pElement->numberValue;
                break;
            case 2:
                *((s16*)pElement->para.value) = (s16)gaussRands64(pos % 16) + *(s16*)pElement->numberValue;
                break;
            case 4:
                *((s32*)pElement->para.value) = (s32)gaussRands64(pos % 32) + *(s32*)pElement->numberValue;
                break;
            case 8:
                *((s64*)pElement->para.value) = (s64)gaussRands64(pos % 64) + *(s64*)pElement->numberValue;
                break;
        }
    } else {
        switch (len) {
            case 1:
                *((u8*)pElement->para.value) = (s8)gaussRands64(pos % 8) + *(u8*)pElement->numberValue;
                break;
            case 2:
                *((u16*)pElement->para.value) = (s16)gaussRands64(pos % 16) + *(u16*)pElement->numberValue;
                break;
            case 4:
                *((u32*)pElement->para.value) = (s32)gaussRands64(pos % 32) + *(u32*)pElement->numberValue;
                break;
            case 8:
                *((u64*)pElement->para.value) = (s64)gaussRands64(pos % 64) + *(u64*)pElement->numberValue;
                break;
        }
    }

    return pElement->para.value;
}

int NumberVariance_getissupport(S_Element* pElement)
{
    ASSERT_NULL(pElement);
    if (((pElement->para.type == enum_NumberS) || (pElement->para.type == enum_NumberU)) &&
        (pElement->isHasInitValue == enum_Yes))
        return enum_Yes;

    if (in_StringIsNumber(pElement) == enum_Yes)
        return enum_Yes;

    return enum_No;
}

const struct Mutater_group NumberVariance_group = {
    "NumberVariance", NumberVariance_getcount, NumberVariance_getvalue, NumberVariance_getissupport, 1};

void init_NumberVariance(void)
{
    register_Mutater(&NumberVariance_group, enum_NumberVariance);
}
