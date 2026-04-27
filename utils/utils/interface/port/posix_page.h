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
 *
 * ---------------------------------------------------------------------------------------
 *
 * posix_page.h
 *
 * Description:
 * Posix OS memory allocation interface.
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef UTILS_POSIX_PAGE_H
#define UTILS_POSIX_PAGE_H

#include "defines/err_code.h"
#include "types/data_types.h"
#include "port/port_page.h"
GSDB_BEGIN_C_CODE_DECLS
/*
 * The output parameters of MMAP on different platform are different. Therefore,
 * the structure of the output parameters is defined in the header file of each platform.
 */
typedef struct MemMapBuffer MemMapBuffer;
struct MemMapBuffer {
    void *addr;
    size_t length;
};

/*
 * Creates a new mapping in the virtual address space of the calling process.
 */
ErrorCode MemMap(MemMapAttribute *memMapAttribute, MemMapBuffer *memMapBuffer);
/*
 * Removes the mappings for pages.
 */
void MemUnmap(MemMapBuffer *memMapBuffer);

GSDB_END_C_CODE_DECLS

#endif /* UTILS_POSIX_PAGE_H */
