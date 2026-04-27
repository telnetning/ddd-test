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

原理:					ipv4数据类型专有变异算法


长度:					长度不变

数量:					n个

支持数据类型: 	ipv4

*/

static u8 Ipv4_table[][4] =

    {
        {0, 0, 0, 0},
        {0, 0, 0, 255},
        {0, 0, 255, 255},
        {0, 255, 255, 255},
        {255, 255, 255, 255},
        {255, 255, 255, 0},
        {255, 255, 0, 0},
        {255, 0, 0, 0},
        {1, 0, 0, 0},
        {1, 0, 0, 1},
        {1, 0, 0, 255},
        {1, 255, 255, 255},
        {128, 0, 0, 0},
        {128, 0, 0, 1},
        {128, 0, 0, 255},
        {128, 0, 255, 255},
        {192, 0, 0, 0},
        {192, 0, 0, 1},
        {192, 0, 0, 255},
        {224, 0, 0, 0},
        {224, 0, 0, 1},
        {224, 0, 0, 255},
        {127, 0, 0, 1},
};

static u8 Ipv4_table2[][4] = {
    {255, 255, 255, 0},
    {255, 255, 0, 0},
    {255, 0, 0, 0},
};

static u8 Ipv4_table3[][4] = {
    {0, 0, 0, 255},
    {0, 0, 255, 255},
    {0, 255, 255, 255},
};

int Ipv4_getcount(S_Element* pElement)
{
    ASSERT_NULL(pElement);

    return sizeof(Ipv4_table) / 4 + sizeof(Ipv4_table2) / 4 + sizeof(Ipv4_table3) / 4;
}

char* Ipv4_getvalue(S_Element* pElement, int pos)
{

    ASSERT_NULL(pElement);
    int pos1 = sizeof(Ipv4_table) / 4;
    int pos2 = sizeof(Ipv4_table2) / 4 + pos1;
    int pos3 = sizeof(Ipv4_table3) / 4 + pos2;

    set_ElementInitoutBuf_ex(pElement, 4);

    if (pos < pos1)
        *((u32*)pElement->para.value) = *(u32*)Ipv4_table[pos];
    else if (pos < pos2)
        *((u32*)pElement->para.value) = (*(u32*)Ipv4_table2[pos - pos1]) & (*((u32*)pElement->inBuf));
    else if (pos < pos3)
        *((u32*)pElement->para.value) = (*(u32*)Ipv4_table3[pos - pos2]) | (*((u32*)pElement->inBuf));

    return pElement->para.value;
}

int Ipv4_getissupport(S_Element* pElement)
{
    ASSERT_NULL(pElement);
    if (pElement->para.type == enum_Ipv4)
        return enum_Yes;

    return enum_No;
}

const struct Mutater_group Ipv4_group = {"Ipv4", Ipv4_getcount, Ipv4_getvalue, Ipv4_getissupport, 1};

void init_Ipv4(void)
{
    register_Mutater(&Ipv4_group, enum_MIpv4);
}
