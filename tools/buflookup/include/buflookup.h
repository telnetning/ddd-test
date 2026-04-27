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

#ifndef TOOL_BUF_LOOKUP_H
#define TOOL_BUF_LOOKUP_H

#include "common/algorithm/dstore_dynahash.h"

namespace DSTORE {

/* Color codes for terminal output */
constexpr char BLACK[] =       "\033[30m";
constexpr char RED[] =         "\033[31m";
constexpr char GREEN[] =       "\033[32m";
constexpr char YELLOW[] =      "\033[33m";
constexpr char BLUE[] =        "\033[34m";
constexpr char MAGENTA[] =     "\033[35m";
constexpr char CYAN[] =        "\033[36m";
constexpr char WHITE[] =       "\033[37m";
constexpr char RESET[] =       "\033[0m";
constexpr char BOLDBLACK[] =   "\033[1m\033[30m";
constexpr char BOLDRED[] =     "\033[1m\033[31m";
constexpr char BOLDGREEN[] =   "\033[1m\033[32m";
constexpr char BOLDYELLOW[] =  "\033[1m\033[33m";
constexpr char BOLDBLUE[] =    "\033[1m\033[34m";
constexpr char BOLDMAGENTA[] = "\033[1m\033[35m";
constexpr char BOLDCYAN[] =    "\033[1m\033[36m";
constexpr char BOLDWHITE[] =   "\033[1m\033[37m";

/* For calculating hash table low mask and high mask */
constexpr uint32 KB_PER_GB = 1048576;
constexpr uint32 SHARED_BUF_ELEM_PER_KB = 8;

} // namespace DSTORE

#endif
