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
 * memory_allocator.h
 *
 * Description:
 * the common memory allocation manager.
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef UTILS_MEMORY_ALLOCATOR_H
#define UTILS_MEMORY_ALLOCATOR_H

#include "types/data_types.h"

GSDB_BEGIN_C_CODE_DECLS

typedef struct MemAllocator MemAllocator;
struct MemAllocator {
    void *context;
    void *(*alloc)(MemAllocator *self, uint64_t size);
    void (*free)(MemAllocator *self, void *ptr);
};

/**
 * Allocate memory of specified size.
 *
 * @param[in] self: the MemAllocator.
 * @param[in] size: Applied memory size.
 * @return the Assign Address.
 */
void *AllocatorAlloc(MemAllocator *self, uint64_t size);

/**
 * Releases the memory of a specified address.
 *
 * @param[in] self: the MemAllocator.
 * @param[in] ptr: the address of the memory to be released.
 */
void AllocatorFree(MemAllocator *self, void *ptr);

GSDB_END_C_CODE_DECLS

#endif /* UTILS_MEMORY_ALLOCATOR_H */
