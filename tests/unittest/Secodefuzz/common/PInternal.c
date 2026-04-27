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
/*

模块之间公用的接口放在这里

*/
#include "PCommon.h"

u64 EdgeCase_table[] = {
    // 0
    // 0x0,		0x0,
    // 1
    0x0,
    0x1,
    // 2
    0x2,
    0x3,
    // 3
    0x4,
    0x7,
    // 4
    0x8,
    0xF,
    // 5
    0x10,
    0x1F,
    // 6
    0x20,
    0x3F,
    // 7
    0x40,
    0x7F,
    // 8
    0x80,
    0xFF,
    // 9
    0x100,
    0x1FF,
    // 10
    0x200,
    0x3FF,
    // 11
    0x400,
    0x7FF,
    // 12
    0x800,
    0xFFF,
    // 13
    0x1000,
    0x1FFF,
    // 14
    0x2000,
    0x3FFF,
    // 15
    0x4000,
    0x7FFF,
    // 16
    0x8000,
    0xFFFF,
    // 17
    0x10000,
    0x1FFFF,
    // 18
    0x20000,
    0x3FFFF,
    // 19
    0x40000,
    0x7FFFF,
    // 20
    0x80000,
    0xFFFFF,
    // 21
    0x100000,
    0x1FFFFF,
    // 22
    0x200000,
    0x3FFFFF,
    // 23
    0x400000,
    0x7FFFFF,
    // 24
    0x800000,
    0xFFFFFF,
    // 25
    0x1000000,
    0x1FFFFFF,
    // 26
    0x2000000,
    0x3FFFFFF,
    // 27
    0x4000000,
    0x7FFFFFF,
    // 28
    0x8000000,
    0xFFFFFFF,
    // 29
    0x10000000,
    0x1FFFFFFF,
    // 30
    0x20000000,
    0x3FFFFFFF,
    // 31
    0x40000000,
    0x7FFFFFFF,
    // 32
    0x80000000,
    0xFFFFFFFF,
    // 33
    0x100000000,
    0x1FFFFFFFF,
    // 34
    0x200000000,
    0x3FFFFFFFF,
    // 35
    0x400000000,
    0x7FFFFFFFF,
    // 36
    0x800000000,
    0xFFFFFFFFF,
    // 37
    0x1000000000,
    0x1FFFFFFFFF,
    // 38
    0x2000000000,
    0x3FFFFFFFFF,
    // 39
    0x4000000000,
    0x7FFFFFFFFF,
    // 40
    0x8000000000,
    0xFFFFFFFFFF,
    // 41
    0x10000000000,
    0x1FFFFFFFFFF,
    // 42
    0x20000000000,
    0x3FFFFFFFFFF,
    // 43
    0x40000000000,
    0x7FFFFFFFFFF,
    // 44
    0x80000000000,
    0xFFFFFFFFFFF,
    // 45
    0x100000000000,
    0x1FFFFFFFFFFF,
    // 46
    0x200000000000,
    0x3FFFFFFFFFFF,
    // 47
    0x400000000000,
    0x7FFFFFFFFFFF,
    // 48
    0x800000000000,
    0xFFFFFFFFFFFF,
    // 49
    0x1000000000000,
    0x1FFFFFFFFFFFF,
    // 50
    0x2000000000000,
    0x3FFFFFFFFFFFF,
    // 51
    0x4000000000000,
    0x7FFFFFFFFFFFF,
    // 52
    0x8000000000000,
    0xFFFFFFFFFFFFF,
    // 53
    0x10000000000000,
    0x1FFFFFFFFFFFFF,
    // 54
    0x20000000000000,
    0x3FFFFFFFFFFFFF,
    // 55
    0x40000000000000,
    0x7FFFFFFFFFFFFF,
    // 56
    0x80000000000000,
    0xFFFFFFFFFFFFFF,
    // 57
    0x100000000000000,
    0x1FFFFFFFFFFFFFF,
    // 58
    0x200000000000000,
    0x3FFFFFFFFFFFFFF,
    // 59
    0x400000000000000,
    0x7FFFFFFFFFFFFFF,
    // 60
    0x800000000000000,
    0xFFFFFFFFFFFFFFF,
    // 61
    0x100000000000000,
    0x1FFFFFFFFFFFFFFF,
    // 62
    0x200000000000000,
    0x3FFFFFFFFFFFFFFF,
    // 63
    0x400000000000000,
    0x7FFFFFFFFFFFFFFF,
    // 64
    0x800000000000000,
    0xFFFFFFFFFFFFFFFF,
};

char* typeStr[] = {"UnsignedInteger",
    "SignedInteger",
    "EnumInteger",
    "RangeInteger",
    "String",
    "StringEnum",
    "Blob",
    "BlobEnum",
    "FixBlob",
    "AFL",
    "IPV4",
    "IPV6",
    "MAC",
    "MAX"};

//变异算法生成值的临时变量
char* g_valueBuf = NULL;
int is_has_init = 0;

//得到数字的bit宽度
u32 in_GetBitNumber(u32 n)
{
    u32 c;
    if (n == 0)
        return 0;

    c = 32;

    if (!(n & 0xffff0000)) {
        c -= 16;
        n <<= 16;
    }
    if (!(n & 0xff000000)) {
        c -= 8;
        n <<= 8;
    }
    if (!(n & 0xf0000000)) {
        c -= 4;
        n <<= 4;
    }
    if (!(n & 0xc0000000)) {
        c -= 2;
        n <<= 2;
    }
    if (!(n & 0x80000000)) {
        c -= 1;
    }

    return c;
}

char* in_GetStringFromType(int type)
{
    int index = sizeof(typeStr) / sizeof(typeStr[0]) - 1;

    if (type < index)
        index = type;
    return typeStr[index];
}

int in_GetTypeFromString(char* type_name)
{
    int count = sizeof(typeStr) / sizeof(char*);
    int i = 0;
    for (i = 0; i < count; i++) {
        if (hw_Strcmp(typeStr[i], type_name) == 0)
            break;
    }
    return i;
}

//判断字符串能否转化为数字
int in_StringIsNumber(S_Element* pElement)
{
    if (pElement->para.type == enum_String) {
        if (pElement->isHasInitValue == enum_Yes) {
            s64 temp_s64 = 0;
            temp_s64 = in_atol(pElement->inBuf);
            if ((temp_s64 != 0) && (temp_s64 != -1)) {
                return enum_Yes;
            }
        }
    }

    return enum_No;
}

//将inbuf copy给outbuf,如果有初始值的话，没有则置空
char* set_ElementOriginalValue(S_Element* pElement)
{
    if (pElement->inLen) {
        pElement->isNeedFreeOutBuf = enum_Yes;
        pElement->para.len = (pElement->inLen) >> 3;
        pElement->para.value = hw_Malloc((pElement->inLen) >> 3);
        hw_Memcpy(pElement->para.value, pElement->inBuf, pElement->para.len);
    } else {
        pElement->isNeedFreeOutBuf = enum_No;
        pElement->para.len = 0;
        pElement->para.value = NULL;
    }

    return pElement->para.value;
}

//为outbuf分配内存，设置长度
char* set_ElementInitoutBuf(S_Element* pElement, int len)
{
    pElement->isNeedFreeOutBuf = enum_Yes;
    pElement->para.len = len;
    pElement->para.value = hw_Malloc(len);

    return pElement->para.value;
}

//为outbuf分配内存，设置长度
char* set_ElementInitoutBuf2(S_Element* pElement, int len)
{
    if (is_has_init == 0) {
        is_has_init = 1;
        g_valueBuf = hw_Malloc(MAX_valueBuf);
    }
    pElement->isNeedFreeOutBuf = enum_No;
    pElement->para.len = len;
    pElement->para.value = g_valueBuf;

    return pElement->para.value;
}

char* set_ElementInitoutBuf_ex(S_Element* pElement, int len)
{
    if (Is_Use_Global_Malloc == 0)
        return set_ElementInitoutBuf(pElement, len);
    else
        return set_ElementInitoutBuf2(pElement, len);
}

char* magic_getvalue(S_Element* pElement, char* data, int len, int type)
{
    int in_len;
    int start;

    ASSERT_NULL(pElement);

    //找到要插入或者覆盖的起始位置
    in_len = (int)(pElement->inLen / 8);
    start = RAND_RANGE(0, in_len);

    if (type == enum_Insert)  // Insert
    {
        if ((len + in_len) > maxOutputSize) {
            return set_ElementOriginalValue(pElement);
        }

        set_ElementInitoutBuf_ex(pElement, len + in_len);

        hw_Memcpy(pElement->para.value, pElement->inBuf, in_len);
        hw_Memcpy(pElement->para.value + start, data, len);
        hw_Memcpy(pElement->para.value + start + len, pElement->inBuf + start, in_len - start);

    } else if (type == enum_Overwrite)  // Overwrite
    {
        if ((len + start) > maxOutputSize) {
            return set_ElementOriginalValue(pElement);
        }

        set_ElementInitoutBuf_ex(pElement, MAX(len + start, in_len));

        hw_Memcpy(pElement->para.value, pElement->inBuf, in_len);
        hw_Memcpy(pElement->para.value + start, data, len);

    } else if (type == enum_replace)  // replace
    {
        if (len > maxOutputSize)
            len = maxOutputSize;

        set_ElementInitoutBuf_ex(pElement, len);

        hw_Memcpy(pElement->para.value, data, len);
    }
    return pElement->para.value;
}

//得到buf 内所有byte中0的数量
int in_GetBufZeroNumber(char* string, int len)
{
    int count = 0;
    int i = 0;
    for (i = 0; i < len; i++) {
        if (string[i] == 0)
            count++;
    }

    return count;
}

void in_GetRegion(int length, int* outStart, int* outLength)
{
    int value;

    ASSERT_ZERO(length);
    ASSERT_NULL(outStart);
    ASSERT_NULL(outLength);

    value = RAND_RANGE(1, length);
    *outStart = RAND_RANGE(0, length - value);
    *outLength = value;
}

//得到字符串里英文字母的数量
int in_GetLetterNumber(char* string)
{
    int count = 0;
    int i = 0;
    int k = in_strlen(string);
    for (i = 0; i < k; i++) {

        count = count + in_IsLetter(string[i]);
    }

    return count;
}

int in_toupper(int c)
{
    return (c >= 'a' && c <= 'z') ? (c + 'A' - 'a') : c;
}

// tolower
int in_tolower(int c)
{
    return (c >= 'A' && c <= 'Z') ? (c - 'A' + 'a') : c;
}

int in_IsLetter(char c)
{
    if (c >= 'a' && c <= 'z')
        return 1;

    if (c >= 'A' && c <= 'Z')
        return 1;

    return 0;
}

int in_isascii(char c)
{
    return (((c) & ~0x7f) == 0);
}

int in_isprint(char c)
{
    // return (((c) & ~0x60) == 0);

    return ((31 < c) && (c < 127));
}
int in_isspace(char c)
{
    if (c == '\t' || c == '\n' || c == ' ')
        return 1;
    else
        return 0;
}

int in_isxdigit(char c)
{
    if ((c >= '0') && (c <= '9'))
        return 1;

    if ((c >= 'a') && (c <= 'f'))
        return 1;

    if ((c >= 'A') && (c <= 'F'))
        return 1;

    return 0;
}

int in_isdigit(char c)
{
    if ((c >= '0') && (c <= '9'))
        return 1;

    return 0;
}

u32 in_strlen(const char* s)
{
    int i;
    for (i = 0; s[i]; i++)
        ;
    return i;
}

u64 in_sqrt(u64 x)
{
    u64 a, b;
    if (x <= 0)
        return 0;
    a = (x >> 3) + 1;
    for (;;) {
        b = ((a + 1) >> 1) + ((x / a) >> 1);
        if (a - b < 2)
            return b - 1 + ((x - b * b + (b << 2)) / b >> 2);
        a = b;
    }
}

/*长整形转字符型*/
// num要转换的数值，*str字符串，radix转换的进制
char* in_ltoa(s64 value, char* string, int radix)
{
    char tmp[33];
    char* tp = tmp;
    s64 i;
    s64 v;
    int sign;
    char* sp;

    if (radix > 36 || radix <= 1) {
        return 0;
    }

    sign = (radix == 10 && value < 0);
    if (sign)
        v = -value;
    else
        v = (s64)value;
    while (v || tp == tmp) {
        i = v % radix;
        v = v / radix;
        if (i < 10)
            *tp++ = i + '0';
        else
            *tp++ = i + 'a' - 10;
    }

    if (string == 0)
        string = (char*)hw_Malloc((tp - tmp) + sign + 1);
    sp = string;

    if (sign)
        *sp++ = '-';
    while (tp > tmp)
        *sp++ = *--tp;
    *sp = 0;
    return string;
}

// 把一个数字字符串转换为一个整数。
s64 in_atol(char* string)
{
    s64 value = 0;
    s64 f = 1;

    // 去掉前边的空格，最后增加代码，注意可能有bug
    while (*string == ' ') {
        string++;
    }

    if (*string == '-') {
        string++;
        f = -1;
    }

    // 逐个把字符串的字符转换为数字。
    while (*string >= '0' && *string <= '9') {
        value *= 10;
        value += *string - '0';
        string++;
    }

    value = f * value;

    //错误检查：如杲由于遇到一个非数字字符而终止，把结果设置为0
    //这个暂时注释掉，感觉也没啥用	，取数字部分就好了
    // if( *string != '\0' )
    //     value = 0;

    return value;
}

//将十六进制的字符串转换成整数
s64 in_htol(char* s)
{
    s64 i;
    s64 n = 0;
    if (s[0] == '0' && (s[1] == 'x' || s[1] == 'X')) {
        i = 2;
    } else {
        i = 0;
    }
    for (; (s[i] >= '0' && s[i] <= '9') || (s[i] >= 'a' && s[i] <= 'z') || (s[i] >= 'A' && s[i] <= 'Z'); ++i) {
        if (in_tolower(s[i]) > '9') {
            n = 16 * n + (10 + in_tolower(s[i]) - 'a');
        } else {
            n = 16 * n + (in_tolower(s[i]) - '0');
        }
    }
    return n;
}

void in_delSpace(char* str)
{
    char* p = str;
    char* t = p;
    while (*p != '\0') {
        if (*p == ' ') {
            t++;
            if (*t != ' ') {
                *p = *t;
                *t = ' ';
            }
        } else {
            p++;
            t = p;
        }
    }
}

int in_parse_string_to_bin(char* Str, char* buf)
{
    int temp = 0;
    size_t Pos;

    size_t L = 0, R = in_strlen(Str) - 1;  // We are parsing the range [L,R].
    // Skip spaces from both sides.
    while (L < R && in_isspace(Str[L]))
        L++;
    while (R > L && in_isspace(Str[R]))
        R--;
    if (R - L < 2)
        return 0;
    // Check the closing "
    if (Str[R] != '"')
        return 0;
    R--;
    // Find the opening "
    while (L < R && Str[L] != '"')
        L++;
    if (L >= R)
        return 0;

    L++;

    for (Pos = L; Pos <= R; Pos++) {
        uint8_t V = (uint8_t)Str[Pos];
        if (!in_isprint(V) && !in_isspace(V))
            return 0;
        if (V == '\\') {
            // Handle '\\'
            if (Pos + 1 <= R && (Str[Pos + 1] == '\\' || Str[Pos + 1] == '"')) {
                buf[temp++] = Str[Pos + 1];
                Pos++;
                continue;
            }
            // Handle '\xAB'
            if (Pos + 3 <= R && Str[Pos + 1] == 'x' && in_isxdigit(Str[Pos + 2]) && in_isxdigit(Str[Pos + 3])) {
                char Hex[] = "0xAA";
                Hex[2] = Str[Pos + 2];
                Hex[3] = Str[Pos + 3];
                buf[temp++] = (hw_Strtol(Hex, NULL, 16));
                Pos += 3;
                continue;
            }
            return 0;  // Invalid escape.
        } else {
            // Any other character.
            buf[temp++] = V;
        }
    }
    return temp;
}

int in_parse_bin_to_string(char* Str, char* buf, int len)
{
    int size = 0;
    int k = 0;

    size += hw_sprintf(Str + size, "value\t\t=\"");

    for (k = 0; k < len; k++) {
        uint8_t Byte = buf[k];
        if (Byte == '\\')
            size += hw_sprintf(Str + size, "\\\\");
        else if (Byte == '"')
            size += hw_sprintf(Str + size, "\\\"");
        // else if (Byte == '%')
        // size += hw_sprintf(Str + size,"\\x%02x", Byte);
        else if (Byte >= 32 && Byte < 127)
            size += hw_sprintf(Str + size, "%c", Byte);
        else
            size += hw_sprintf(Str + size, "\\x%02x", Byte);
    }
    size += hw_sprintf(Str + size, "\"\r\n");
    return size;
}
