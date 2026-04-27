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

原理:					将样本的一部分copy覆盖到另一部分

长度:					长度不变

数量:					MAXCOUNT

支持数据类型: 	blob ,FixBlob,String

*/

int DataElementCopyPartOf_getcount(S_Element* pElement)
{
    ASSERT_NULL(pElement);

    //爬分支变异手段
    return MAXCOUNT;
}

// Overwrites part of To[0,ToSize) with a part of From[0,FromSize).
// Returns ToSize.
size_t CopyPartOf(const uint8_t* From, size_t FromSize, uint8_t* To, size_t ToSize)
{
    // Copy From[FromBeg, FromBeg + CopySize) into To[ToBeg, ToBeg + CopySize).
    size_t ToBeg = RAND_RANGE(0, ToSize - 1);
    size_t CopySize = RAND_RANGE(0, ToSize - ToBeg - 1) + 1;
    CopySize = MIN(CopySize, FromSize);
    size_t FromBeg = RAND_RANGE(0, FromSize - CopySize);
    hw_Memmove(To + ToBeg, From + FromBeg, CopySize);
    return ToSize;
}

char* DataElementCopyPartOf_getvalue(S_Element* pElement, int pos)
{
    int in_len;
    ASSERT_NULL(pElement);

    in_len = (int)(pElement->inLen / 8);

    set_ElementInitoutBuf_ex(pElement, in_len);

    hw_Memcpy(pElement->para.value, pElement->inBuf, in_len);

    CopyPartOf((uint8_t*)pElement->para.value, in_len, (uint8_t*)pElement->para.value, in_len);

    return pElement->para.value;
}

int DataElementCopyPartOf_getissupport(S_Element* pElement)
{
    ASSERT_NULL(pElement);
    //目前仅支持blob,增强buf变异
    if (((pElement->para.type == enum_Blob) || (pElement->para.type == enum_FixBlob) ||
            (pElement->para.type == enum_String)) &&
        (pElement->isHasInitValue == enum_Yes))
        return enum_Yes;

    return enum_No;
}

const struct Mutater_group DataElementCopyPartOf_group = {"DataElementCopyPartOf",
    DataElementCopyPartOf_getcount,
    DataElementCopyPartOf_getvalue,
    DataElementCopyPartOf_getissupport,
    1};

void init_DataElementCopyPartOf(void)
{
    register_Mutater(&DataElementCopyPartOf_group, enum_DataElementCopyPartOf);
}
