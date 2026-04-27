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

原理:					测试用例中，Blob的随机数量的连续byte会被单独的改变，
                        用小套的替换值(从测试输出上看，小套替换值基本上是"01"，"00"，"FF"，"FE")。
                        变异发生的位置和byte变化的数量一样都是随机决定的。

长度:					长度不变

数量:					byte数量乘8与MAXCOUNT的最小值

支持数据类型: 	有初始值的blob，FixBlob元素

*/

const static u8 special[4] = {0x00, 0x01, 0xfe, 0xff};

int BlobChangeSpecial_getcount(S_Element* pElement)
{
    ASSERT_NULL(pElement);

    return MIN((int)pElement->inLen, MAXCOUNT);
}

char* BlobChangeSpecial_getvalue(S_Element* pElement, int pos)
{
    int i;
    int in_len;
    int count;
    int start, change_Len;

    ASSERT_NULL(pElement);

    in_len = (int)(pElement->inLen / 8);

    set_ElementInitoutBuf_ex(pElement, in_len);

    in_GetRegion(in_len, &start, &change_Len);

    hw_Memcpy(pElement->para.value, pElement->inBuf, start);

    count = sizeof(special) - 1;
    for (i = start; i < start + change_Len; i++) {
        pElement->para.value[i] = special[RAND_RANGE(0, count)];
    }

    hw_Memcpy(
        pElement->para.value + start + change_Len, pElement->inBuf + start + change_Len, in_len - start - change_Len);

    return pElement->para.value;
}

int BlobChangeSpecial_getissupport(S_Element* pElement)
{
    //只要是字符串就支持
    if (((pElement->para.type == enum_Blob) || (pElement->para.type == enum_FixBlob)) &&
        (pElement->isHasInitValue == enum_Yes))
        return enum_Yes;

    return enum_No;
}

const struct Mutater_group BlobChangeSpecial_group = {
    "BlobChangeSpecial", BlobChangeSpecial_getcount, BlobChangeSpecial_getvalue, BlobChangeSpecial_getissupport, 1};

void init_BlobChangeSpecial(void)
{
    register_Mutater(&BlobChangeSpecial_group, enum_BlobChangeSpecial);
}
