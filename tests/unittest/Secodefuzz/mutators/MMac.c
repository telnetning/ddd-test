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

原理:					mac数据类型专有变异算法


长度:					长度不变		,定值为6byte

数量:					n个

支持数据类型: 	mac

*/

static u8 Mac_table[][6] = {
    {0x01, 0x00, 0x5e, 0x00, 0x00, 0x00},
    {0x01, 0x00, 0x5e, 0x00, 0x00, 0x01},
    {0x01, 0x00, 0x5e, 0xff, 0xff, 0xff},
    {0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
    {0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
};

int Mac_getcount(S_Element* pElement)
{
    ASSERT_NULL(pElement);
    return sizeof(Mac_table) / 6 + 8;
}

char* Mac_getvalue(S_Element* pElement, int pos)
{

    ASSERT_NULL(pElement);
    int i = 0;

    set_ElementInitoutBuf_ex(pElement, 6);

    hw_Memcpy(pElement->para.value, pElement->inBuf, 6);

    //高位两个bit特置
    if (pos == 0) {
        FILL_BIT(pElement->para.value, 0);
        FILL_BIT(pElement->para.value, 1);
    }

    if (pos == 1) {
        FILL_BIT(pElement->para.value, 0);
        ZERO_BIT(pElement->para.value, 1);
    }

    if (pos == 2) {
        ZERO_BIT(pElement->para.value, 0);
        FILL_BIT(pElement->para.value, 1);
    }

    if (pos == 3) {
        ZERO_BIT(pElement->para.value, 0);
        ZERO_BIT(pElement->para.value, 1);
    }

    //组织标识符特置
    if (pos == 4) {
        for (i = 2; i <= 23; i++) {
            FILL_BIT(pElement->para.value, i);
        }
    }

    if (pos == 5) {
        for (i = 2; i <= 23; i++) {
            ZERO_BIT(pElement->para.value, i);
        }
    }

    //厂家id特置
    if (pos == 6) {
        for (i = 24; i <= 47; i++) {
            FILL_BIT(pElement->para.value, i);
        }
    }

    if (pos == 7) {
        for (i = 24; i <= 47; i++) {
            ZERO_BIT(pElement->para.value, i);
        }
    }
    if (pos >= 8) {
        // ip多播
        pos = pos - 8;
        for (i = 0; i < 6; i++)
            pElement->para.value[i] = Mac_table[pos][i];
    }

    //如有其他特殊的mac地址，加在这里

    return pElement->para.value;
}

int Mac_getissupport(S_Element* pElement)
{
    ASSERT_NULL(pElement);
    if (pElement->para.type == enum_Mac)
        return enum_Yes;

    return enum_No;
}

const struct Mutater_group Mac_group = {"Mac", Mac_getcount, Mac_getvalue, Mac_getissupport, 1};

void init_Mac(void)
{
    register_Mutater(&Mac_group, enum_MMac);
}
