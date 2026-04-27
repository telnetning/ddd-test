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

原理:					测试用例中Blob会被插入随机数量的连续的bytes。
                        插入的位置采用纯随机
                        插入的连续byte的数量从1到255，使用高斯变异

                        第一个被插入的值是取值范围从0x00到0xff的随机选取的值
                        第二个被插入的值会在前一个值的基础上加1，
                        第三个被插入的值会继续再加1。
                        当0xff发生的时候，后面的值会回滚到0x00.
                        此后的值会持续加1，直到所有的插入值都有值了为止。

长度:					原始长度到最大长度之间

数量:					MAXCOUNT/5

支持数据类型: 	有初始值的blob元素

*/

static int BlobExpandSingleIncrementing_getcount(S_Element* pElement)
{
    ASSERT_NULL(pElement);

    return MAXCOUNT / 5;
}

static char* BlobExpandSingleIncrementing_getvalue(S_Element* pElement, int pos)
{
    int i;
    int out_len, in_len;
    int start;
    int change_Len;
    char value;
    ASSERT_NULL(pElement);

    in_len = (int)(pElement->inLen / 8);

    start = RAND_RANGE(0, in_len);

    //只插入1到255个随机byte
    change_Len = gaussRandu32(RAND_32() % 8);

    out_len = in_len + change_Len;

    if (out_len > maxOutputSize) {
        out_len = maxOutputSize;
        change_Len = maxOutputSize - in_len;
    }

    set_ElementInitoutBuf_ex(pElement, out_len);

    hw_Memcpy(pElement->para.value, pElement->inBuf, start);

    value = RAND_BYTE();
    for (i = start; i < start + change_Len; i++) {
        pElement->para.value[i] = value;
        value++;
    }

    hw_Memcpy(pElement->para.value + start + change_Len, pElement->inBuf + start, in_len - start);

    return pElement->para.value;
}

static int BlobExpandSingleIncrementing_getissupport(S_Element* pElement)
{
    //只要是字符串就支持
    if ((pElement->para.type == enum_Blob) || (pElement->para.type == enum_FixBlob))
        return enum_Yes;

    return enum_No;
}

const struct Mutater_group BlobExpandSingleIncrementing_group = {"BlobExpandSingleIncrementing",
    BlobExpandSingleIncrementing_getcount,
    BlobExpandSingleIncrementing_getvalue,
    BlobExpandSingleIncrementing_getissupport,
    1};

void init_BlobExpandSingleIncrementing(void)
{
    register_Mutater(&BlobExpandSingleIncrementing_group, enum_BlobExpandSingleIncrementing);
}
