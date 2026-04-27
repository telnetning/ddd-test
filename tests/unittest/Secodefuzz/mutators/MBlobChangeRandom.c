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

原理:					测试用例中Blob的随机数量的连续的bytes会被改变。
                        变异发生的位置和byte变化的数量一样都是随机决定的。
                        使用随机byte填充

长度:					长度不变

数量:					bit值与MAXCOUNT的最小值

支持数据类型: 	有初始值的blob，FixBlob元素

*/

int BlobChangeRandom_getcount(S_Element* pElement)
{

    ASSERT_NULL(pElement);

    return MIN((int)pElement->inLen, MAXCOUNT);
}

char* BlobChangeRandom_getvalue(S_Element* pElement, int pos)
{
    int i;
    int in_len;
    int start;
    int change_Len;

    ASSERT_NULL(pElement);

    in_len = (int)(pElement->inLen / 8);

    set_ElementInitoutBuf_ex(pElement, in_len);

    in_GetRegion(in_len, &start, &change_Len);

    hw_Memcpy(pElement->para.value, pElement->inBuf, start);

    for (i = start; i < start + change_Len; i++) {
        pElement->para.value[i] = RAND_BYTE();
    }

    hw_Memcpy(
        pElement->para.value + start + change_Len, pElement->inBuf + start + change_Len, in_len - start - change_Len);

    return pElement->para.value;
}

int BlobChangeRandom_getissupport(S_Element* pElement)
{
    //只要是字符串就支持
    if (((pElement->para.type == enum_Blob) || (pElement->para.type == enum_FixBlob)) &&
        (pElement->isHasInitValue == enum_Yes))
        return enum_Yes;

    return enum_No;
}

const struct Mutater_group BlobChangeRandom_group = {
    "BlobChangeRandom", BlobChangeRandom_getcount, BlobChangeRandom_getvalue, BlobChangeRandom_getissupport, 1};

void init_BlobChangeRandom(void)
{
    register_Mutater(&BlobChangeRandom_group, enum_BlobChangeRandom);
}
