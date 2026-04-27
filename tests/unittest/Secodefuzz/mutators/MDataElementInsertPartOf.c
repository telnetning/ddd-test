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

原理:					将样本的一部分随机插入到样本中

长度:					0到最大长度之间

数量:					MAXCOUNT

支持数据类型: 	blob ,FixBlob,String

*/

int DataElementInsertPartOf_getcount(S_Element* pElement)
{
    ASSERT_NULL(pElement);

    //爬分支变异手段
    return MAXCOUNT;
}

// Inserts part of From[0,ToSize) into To.
// Returns new size of To on success or 0 on failure.
size_t InsertPartOf(const uint8_t* From, size_t FromSize, uint8_t* To, size_t ToSize, size_t MaxToSize)
{

    size_t AvailableSpace;
    size_t MaxCopySize;
    size_t CopySize;
    size_t FromBeg;
    size_t ToInsertPos;
    size_t TailSize;

    if (ToSize >= MaxToSize)
        return 0;
    AvailableSpace = MaxToSize - ToSize;
    MaxCopySize = MIN(AvailableSpace, FromSize);
    CopySize = RAND_RANGE(0, MaxCopySize - 1) + 1;
    FromBeg = RAND_RANGE(0, FromSize - CopySize);
    ToInsertPos = RAND_RANGE(0, ToSize);

    TailSize = ToSize - ToInsertPos;
    if (To == From) {
        char* temp = hw_Malloc(MaxToSize);
        hw_Memcpy(temp, From + FromBeg, CopySize);
        hw_Memmove(To + ToInsertPos + CopySize, To + ToInsertPos, TailSize);
        hw_Memmove(To + ToInsertPos, temp, CopySize);
        hw_Free(temp);
    } else {
        hw_Memmove(To + ToInsertPos + CopySize, To + ToInsertPos, TailSize);
        hw_Memmove(To + ToInsertPos, From + FromBeg, CopySize);
    }
    return ToSize + CopySize;
}

char* DataElementInsertPartOf_getvalue(S_Element* pElement, int pos)
{
    int in_len;
    ASSERT_NULL(pElement);

    in_len = (int)(pElement->inLen / 8);

    set_ElementInitoutBuf_ex(pElement, pElement->para.max_len);

    hw_Memcpy(pElement->para.value, pElement->inBuf, in_len);

    pElement->para.len = InsertPartOf(
        (uint8_t*)pElement->para.value, in_len, (uint8_t*)pElement->para.value, in_len, pElement->para.max_len);

    return pElement->para.value;
}

int DataElementInsertPartOf_getissupport(S_Element* pElement)
{
    ASSERT_NULL(pElement);
    //目前仅支持blob,增强buf变异
    if (((pElement->para.type == enum_Blob) || (pElement->para.type == enum_FixBlob) ||
            (pElement->para.type == enum_String)) &&
        (pElement->isHasInitValue == enum_Yes))
        return enum_Yes;

    return enum_No;
}

const struct Mutater_group DataElementInsertPartOf_group = {"DataElementInsertPartOf",
    DataElementInsertPartOf_getcount,
    DataElementInsertPartOf_getvalue,
    DataElementInsertPartOf_getissupport,
    1};

void init_DataElementInsertPartOf(void)
{
    register_Mutater(&DataElementInsertPartOf_group, enum_DataElementInsertPartOf);
}
