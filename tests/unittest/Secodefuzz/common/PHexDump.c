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
#include "PCommon.h"

const char hex_asc[] = "0123456789abcdef";
#define hex_asc_lo(x) hex_asc[((x)&0x0f)]
#define hex_asc_hi(x) hex_asc[((x)&0xf0) >> 4]

void hex_dump_to_buffer1(
    const u8* buf, size_t len, unsigned int rowsize, int groupsize, char* linebuf, size_t linebuflen, int ascii)
{
    const u8* ptr = buf;
    u8 ch;
    unsigned int j, lx = 0;
    unsigned int ascii_column;
    // 每行固定16或者32个字节，符合国际审美观
    if (rowsize != 16 && rowsize != 32)
        rowsize = 16;

    if (!len)
        goto nil;
    if (len > rowsize) /* limit to one line at a time */
        len = rowsize;
    if ((len % groupsize) != 0) /* no mixed size output */
        groupsize = 1;

    switch (groupsize) {
        default:
            //  转换成ASCii 形式
            for (j = 0; (j < len) && (lx + 3) <= linebuflen; j++) {
                ch = ptr[j];
                linebuf[lx++] = hex_asc_hi(ch);
                linebuf[lx++] = hex_asc_lo(ch);
                linebuf[lx++] = ' ';
            }
            if (j)
                lx--;

            ascii_column = 3 * rowsize + 2;
            break;
    }
    if (!ascii)
        goto nil;
    // 加上对应的ASCII字符串，区别如下：
    //  0009ab42: 40 41 42 43 44 45 46 47 48 49 4a 4b 4c 4d 4e 4f  @ABCDEFGHIJKLMNO
    //  0009ab42: 40 41 42 43 44 45 46 47 48 49 4a 4b 4c 4d 4e 4f
    while (lx < (linebuflen - 1) && lx < (ascii_column - 1))
        linebuf[lx++] = ' ';
    for (j = 0; (j < len) && (lx + 2) < linebuflen; j++) {
        ch = ptr[j];
        linebuf[lx++] = ((ch) && in_isascii(ch) && in_isprint(ch)) ? ch : '.';
    }
nil:
    // 记得加上结束符号，有可能导致printk --> kernel panic
    linebuf[lx++] = '\0';
}

void print_hex_dump1(unsigned int rowsize, int groupsize, const u8* buf, size_t len, int ascii)
{
    const u8* ptr = buf;

    unsigned int i, linelen, remaining = len;
    /* 每行数据类似下面
       40 41 42 43 44 45 46 47 48 49 4a 4b 4c 4d 4e 4f  @ABCDEFGHIJKLMNO
       |                                          |  |                 |
       -----------------------------------------------
                16*3 or 32*3                       2      32+1
    */
    unsigned char linebuf[32 * 3 + 2 + 32 + 1];

    if (rowsize != 16 && rowsize != 32)
        rowsize = 16;

    for (i = 0; i < len; i += rowsize) {
        linelen = MIN(remaining, rowsize);
        remaining -= rowsize;
        //  linebuf 返回需要打印的字符串
        hex_dump_to_buffer1(ptr + i, linelen, rowsize, groupsize, (char*)linebuf, sizeof(linebuf), ascii);
        hw_printf("%s\n", linebuf);
    }
}

// 一般在kernel代码中，调用此接口，许多参数用默认的，不需要了解太多细节
void HEX_Dump(u8* buf, u32 len)
{
    print_hex_dump1(16, 1, buf, len, 1);
}