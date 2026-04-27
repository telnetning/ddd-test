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

用户在这个文件里编辑字典

*/

// blob
char* CustomBlob_table[] = {
    "CustomBlob",
    "\x00\x01\x02\x00\xff",
    "\x00\x01\x02\x00\xff\x00\x01\x02\x00\xff\x00\x01\x02\x00\xff",
    "\xff\x33\x00\x01\x02\x00\xff",
};
int CustomBlob_table_len[] = {11, 5, 15, 7};

int CustomBlob_table_count = sizeof(CustomBlob_table_len) / sizeof(int);

// number
u64 CustomNumber_table[] = {
    0x8,
    0x88,
};

int CustomNumber_table_count = sizeof(CustomNumber_table) / sizeof(CustomNumber_table[0]);

// string
char* CustomString_table[] = {
    "CustomString",
};

int CustomString_table_count = sizeof(CustomString_table) / sizeof(char*);