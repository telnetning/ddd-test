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

简单的定制化number类型变异算法，
只要在表中填入数据，你的数据就会出现在number类型元素的测试例里

*/

extern u64 CustomNumber_table[];
extern int CustomNumber_table_count;

int CustomNumber_getcount(S_Element* pElement)
{
    ASSERT_NULL(pElement);
    int count = CustomNumber_table_count;

    if (count == 0)
        return 0;

    return MAXCOUNT / 2;
}

char* CustomNumber_getvalue(S_Element* pElement, int pos)
{
    int len;
    int count = CustomNumber_table_count;
    u64 value = 0;
    ASSERT_NULL(pElement);
    //如果bit长度不被8整除，则加1
    len = (int)(pElement->inLen / 8) + IsAddone(pElement->inLen % 8);

    //库没有值，则空
    if (count == 0) {
        value = 0;
    } else {
        value = CustomNumber_table[RAND_32() % count];
    }

    set_ElementInitoutBuf_ex(pElement, len);

    //有无符号分别对待
    if (pElement->para.type == enum_NumberS) {
        if (pElement->inLen <= 8)
            *((s8*)pElement->para.value) = (s8)value;
        else if (pElement->inLen <= 16)
            *((s16*)pElement->para.value) = (s16)value;
        else if (pElement->inLen <= 32)
            *((s32*)pElement->para.value) = (s32)value;
        else if (pElement->inLen <= 64)
            *((s64*)pElement->para.value) = value;
    } else if (pElement->para.type == enum_NumberU) {
        if (pElement->inLen <= 8)
            *((u8*)pElement->para.value) = (u8)value;
        else if (pElement->inLen <= 16)
            *((u16*)pElement->para.value) = (u16)value;
        else if (pElement->inLen <= 32)
            *((u32*)pElement->para.value) = (u32)value;
        else if (pElement->inLen <= 64)
            *((u64*)pElement->para.value) = value;
    }

    return pElement->para.value;
}

int CustomNumber_getissupport(S_Element* pElement)
{
    ASSERT_NULL(pElement);
    if ((pElement->para.type == enum_NumberS) || (pElement->para.type == enum_NumberU))
        return enum_Yes;

    return enum_No;
}

const struct Mutater_group CustomNumber_group = {
    "CustomNumber", CustomNumber_getcount, CustomNumber_getvalue, CustomNumber_getissupport, 1};

void init_CustomNumber(void)
{
    register_Mutater(&CustomNumber_group, enum_CustomNumber);
}
