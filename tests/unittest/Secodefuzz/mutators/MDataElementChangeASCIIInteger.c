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

原理:					改变数据里边的ascii为数字的部分，用数字随机

长度:					0到最大长度之间

数量:					MAXCOUNT

支持数据类型: 	blob ,FixBlob,String

*/

int DataElementChangeASCIIInteger_getcount(S_Element* pElement)
{
    ASSERT_NULL(pElement);

    //爬分支变异手段，因此变异数量有所倾向
    return MAXCOUNT;
}

size_t Mutate_ChangeASCIIInteger(uint8_t* Data, size_t Size)
{
    size_t i;
    size_t E;
    size_t B = RAND_RANGE(0, Size - 1);
    while (B < Size && !in_isdigit(Data[B]))
        B++;
    if (B == Size)
        return 0;
    E = B;
    while (E < Size && in_isdigit(Data[E]))
        E++;
    // now we have digits in [B, E).
    // strtol and friends don't accept non-zero-teminated data, parse it manually.
    uint64_t Val = Data[B] - '0';
    for (i = B + 1; i < E; i++)
        Val = Val * 10 + Data[i] - '0';

    // Mutate the integer value.
    switch (RAND_RANGE(0, 4)) {
        case 0:
            Val++;
            break;
        case 1:
            Val--;
            break;
        case 2:
            Val /= 2;
            break;
        case 3:
            Val *= 2;
            break;
        case 4:
            Val = RAND_RANGE(0, Val * Val);
            break;
        default:;
    }
    // Just replace the bytes with the new ones, don't bother moving bytes.
    for (i = B; i < E; i++) {
        size_t Idx = E + B - i - 1;
        Data[Idx] = (Val % 10) + '0';
        Val /= 10;
    }
    return Size;
}

char* DataElementChangeASCIIInteger_getvalue(S_Element* pElement, int pos)
{
    int in_len;
    ASSERT_NULL(pElement);

    in_len = (int)(pElement->inLen / 8);

    set_ElementInitoutBuf_ex(pElement, in_len);

    hw_Memcpy(pElement->para.value, pElement->inBuf, in_len);

    Mutate_ChangeASCIIInteger((uint8_t*)pElement->para.value, in_len);

    return pElement->para.value;
}

int DataElementChangeASCIIInteger_getissupport(S_Element* pElement)
{
    ASSERT_NULL(pElement);
    //目前仅支持blob,增强buf变异
    if (((pElement->para.type == enum_Blob) || (pElement->para.type == enum_FixBlob) ||
            (pElement->para.type == enum_String)) &&
        (pElement->isHasInitValue == enum_Yes))
        return enum_Yes;

    return enum_No;
}

const struct Mutater_group DataElementChangeASCIIInteger_group = {"DataElementChangeASCIIInteger",
    DataElementChangeASCIIInteger_getcount,
    DataElementChangeASCIIInteger_getvalue,
    DataElementChangeASCIIInteger_getissupport,
    1};

void init_DataElementChangeASCIIInteger(void)
{
    register_Mutater(&DataElementChangeASCIIInteger_group, enum_DataElementChangeASCIIInteger);
}
