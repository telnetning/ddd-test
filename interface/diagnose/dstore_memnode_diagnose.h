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

#ifndef DSTORE_MEMNODE_DIAGNOSE_H
#define DSTORE_MEMNODE_DIAGNOSE_H

namespace DSTORE {

const size_t MAX_ARG_NUMBER = 6;

struct MemInputParam {
    char *name;
    uint32_t narg;
    char *args[MAX_ARG_NUMBER];
};

struct MemOutputParam {
    long error;
    char *errInfo;
    uint32_t num;
    char **results;
};
}


#endif