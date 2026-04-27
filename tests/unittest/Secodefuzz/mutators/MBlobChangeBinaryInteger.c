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

原理:					改变内存，整数随机

长度:					0到最大长度之间

数量:					MAXCOUNT

支持数据类型: 	blob

*/

int BlobChangeBinaryInteger_getcount(S_Element* pElement)
{
    ASSERT_NULL(pElement);

    //爬分支变异手段
    return MAXCOUNT;
}

//大小端转换
uint8_t Bswap8(uint8_t x)
{
    return x;
}
uint16_t Bswap16(uint16_t x)
{
    uint16_t y;
    char* tempx = (char*)(&x);
    char* tempy = (char*)(&y);
    tempy[1] = tempx[0];
    tempy[0] = tempx[1];

    return y;
}
uint32_t Bswap32(uint32_t x)
{
    uint32_t y;
    char* tempx = (char*)(&x);
    char* tempy = (char*)(&y);
    tempy[3] = tempx[0];
    tempy[2] = tempx[1];
    tempy[1] = tempx[2];
    tempy[0] = tempx[3];

    return y;
}
uint64_t Bswap64(uint64_t x)
{
    uint64_t y;
    char* tempx = (char*)(&x);
    char* tempy = (char*)(&y);
    tempy[7] = tempx[0];
    tempy[6] = tempx[1];
    tempy[5] = tempx[2];
    tempy[4] = tempx[3];
    tempy[3] = tempx[4];
    tempy[2] = tempx[5];
    tempy[1] = tempx[6];
    tempy[0] = tempx[7];
    return y;
}

size_t ChangeBinaryInteger64(uint8_t* Data, size_t Size)
{
    size_t Off;
    uint64_t Val;
    if (Size < sizeof(uint64_t))
        return 0;
    Off = RAND_RANGE(0, (Size - sizeof(uint64_t)));
    if (Off + sizeof(uint64_t) > Size)
        return 0;
    if (Off < 64 && !RAND_RANGE(0, 3)) {
        Val = Size;
        if (RAND_BOOL())
            Val = Bswap64(Val);
    } else {
        hw_Memcpy(&Val, Data + Off, sizeof(Val));
        uint64_t Add = RAND_RANGE(0, 20);
        Add -= 10;
        if (RAND_BOOL())
            Val = Bswap64((uint64_t)(Bswap64(Val) + Add));  // Add assuming different endiannes.
        else
            Val = Val + Add;          // Add assuming current endiannes.
#ifndef _MSC_VER                      // vs编译不过
        if (Add == 0 || RAND_BOOL())  // Maybe negate.
            Val = -Val;
#endif
    }
    hw_Memcpy(Data + Off, &Val, sizeof(Val));
    return Size;
}

size_t ChangeBinaryInteger32(uint8_t* Data, size_t Size)
{
    size_t Off;
    uint32_t Val;
    if (Size < sizeof(uint32_t))
        return 0;
    Off = RAND_RANGE(0, (Size - sizeof(uint32_t)));
    if (Off + sizeof(uint32_t) > Size)
        return 0;
    if (Off < 64 && !RAND_RANGE(0, 3)) {
        Val = Size;
        if (RAND_BOOL())
            Val = Bswap32(Val);
    } else {
        hw_Memcpy(&Val, Data + Off, sizeof(Val));
        uint32_t Add = RAND_RANGE(0, 20);
        Add -= 10;
        if (RAND_BOOL())
            Val = Bswap32((uint32_t)(Bswap32(Val) + Add));  // Add assuming different endiannes.
        else
            Val = Val + Add;          // Add assuming current endiannes.
#ifndef _MSC_VER                      // vs编译不过
        if (Add == 0 || RAND_BOOL())  // Maybe negate.
            Val = -Val;
#endif
    }
    hw_Memcpy(Data + Off, &Val, sizeof(Val));
    return Size;
}

size_t ChangeBinaryInteger16(uint8_t* Data, size_t Size)
{
    size_t Off;
    uint16_t Val;
    if (Size < sizeof(uint16_t))
        return 0;
    Off = RAND_RANGE(0, (Size - sizeof(uint16_t)));
    if (Off + sizeof(uint16_t) > Size)
        return 0;
    if (Off < 64 && !RAND_RANGE(0, 3)) {
        Val = Size;
        if (RAND_BOOL())
            Val = Bswap16(Val);
    } else {
        hw_Memcpy(&Val, Data + Off, sizeof(Val));
        uint16_t Add = RAND_RANGE(0, 20);
        Add -= 10;
        if (RAND_BOOL())
            Val = Bswap16((uint16_t)(Bswap16(Val) + Add));  // Add assuming different endiannes.
        else
            Val = Val + Add;          // Add assuming current endiannes.
        if (Add == 0 || RAND_BOOL())  // Maybe negate.
            Val = -Val;
    }
    hw_Memcpy(Data + Off, &Val, sizeof(Val));
    return Size;
}

size_t ChangeBinaryInteger8(uint8_t* Data, size_t Size)
{
    size_t Off;
    uint8_t Val;
    if (Size < sizeof(uint8_t))
        return 0;
    Off = RAND_RANGE(0, (Size - sizeof(uint8_t)));
    if (Off + sizeof(uint8_t) > Size)
        return 0;
    if (Off < 64 && !RAND_RANGE(0, 3)) {
        Val = Size;
        if (RAND_BOOL())
            Val = Bswap8(Val);
    } else {
        hw_Memcpy(&Val, Data + Off, sizeof(Val));
        uint8_t Add = RAND_RANGE(0, 20);
        Add -= 10;
        if (RAND_BOOL())
            Val = Bswap8((uint8_t)(Bswap8(Val) + Add));  // Add assuming different endiannes.
        else
            Val = Val + Add;          // Add assuming current endiannes.
        if (Add == 0 || RAND_BOOL())  // Maybe negate.
            Val = -Val;
    }
    hw_Memcpy(Data + Off, &Val, sizeof(Val));
    return Size;
}

size_t Mutate_ChangeBinaryInteger_getvalue(uint8_t* Data, size_t Size)
{
    switch (RAND_RANGE(0, 3)) {
        case 3:
            return ChangeBinaryInteger64(Data, Size);
        case 2:
            return ChangeBinaryInteger32(Data, Size);
        case 1:
            return ChangeBinaryInteger16(Data, Size);
        case 0:
            return ChangeBinaryInteger8(Data, Size);
        default:;
    }
    return 0;
}

char* BlobChangeBinaryInteger_getvalue(S_Element* pElement, int pos)
{
    int in_len;
    ASSERT_NULL(pElement);

    in_len = (int)(pElement->inLen / 8);

    set_ElementInitoutBuf_ex(pElement, in_len);

    hw_Memcpy(pElement->para.value, pElement->inBuf, in_len);

    Mutate_ChangeBinaryInteger_getvalue((uint8_t*)pElement->para.value, in_len);

    return pElement->para.value;
}

int BlobChangeBinaryInteger_getissupport(S_Element* pElement)
{
    ASSERT_NULL(pElement);
    //目前仅支持blob,增强buf变异
    if (((pElement->para.type == enum_Blob) || (pElement->para.type == enum_FixBlob)) &&
        (pElement->isHasInitValue == enum_Yes))
        return enum_Yes;

    return enum_No;
}

const struct Mutater_group BlobChangeBinaryInteger_group = {"BlobChangeBinaryInteger",
    BlobChangeBinaryInteger_getcount,
    BlobChangeBinaryInteger_getvalue,
    BlobChangeBinaryInteger_getissupport,
    1};

void init_BlobChangeBinaryInteger(void)
{
    register_Mutater(&BlobChangeBinaryInteger_group, enum_BlobChangeBinaryInteger);
}
