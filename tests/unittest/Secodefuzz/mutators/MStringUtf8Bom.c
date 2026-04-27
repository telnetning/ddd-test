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

原理:					原字符串插入1-6个bom

支持数据类型: 	有初始值的字符串类型
*/

extern u8 bom[];
extern int bom_len;

int StringUtf8Bom_getcount(S_Element* pElement)
{
    ASSERT_NULL(pElement);

    //次数少点吧，这东西没啥大用
    return (MAXCOUNT / 50);
}

char* StringUtf8Bom_getvalue(S_Element* pElement, int pos)
{
    int i;
    int j;
    int out_len;

    ASSERT_NULL(pElement);

    //增加几个bom
    int bom_num = RAND_RANGE(1, 6);

    out_len = (int)(pElement->inLen / 8);

    if (((out_len + bom_num * bom_len) > pElement->para.max_len) || (out_len == 0)) {
        return set_ElementOriginalValue(pElement);
    }

    set_ElementInitoutBuf_ex(pElement, out_len + bom_num * bom_len);

    //拷贝剩下的
    hw_Memcpy(pElement->para.value, pElement->inBuf, out_len);

    for (i = 0; i < bom_num; i++) {
        //计算插入bom的位置
        int pos1 = RAND_RANGE(0, out_len);
        //先copy后边，在copy前边

        for (j = out_len; j > pos1; j--) {
            pElement->para.value[j - 1 + bom_len] = pElement->para.value[j - 1];
        }

        hw_Memcpy(pElement->para.value + pos1, bom, bom_len);

        out_len = out_len + bom_len;
    }

    ((char*)pElement->para.value)[out_len - 1] = 0;
    return pElement->para.value;
}

int StringUtf8Bom_getissupport(S_Element* pElement)
{
    ASSERT_NULL(pElement);

    //有初始值的字符串
    if ((pElement->para.type == enum_String) && (pElement->isHasInitValue == enum_Yes) &&
        ((int)(pElement->inLen / 8) > 1))
        return enum_Yes;

    return enum_No;
}

const struct Mutater_group StringUtf8Bom_group = {
    .name = "StringUtf8Bom",
    .getCount = StringUtf8Bom_getcount,
    .getValue = StringUtf8Bom_getvalue,
    .getIsSupport = StringUtf8Bom_getissupport,
};

void init_StringUtf8Bom(void)
{
    register_Mutater(&StringUtf8Bom_group, enum_StringUtf8Bom);
}
