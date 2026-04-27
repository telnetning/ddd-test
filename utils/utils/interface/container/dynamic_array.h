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
 * ---------------------------------------------------------------------------------
 *
 * dynamic_array.h
 *
 * Description:
 * 1. This file is a simple dynamic array implementation
 *
 * ---------------------------------------------------------------------------------
 */

#ifndef UTILS_DYNAMIC_ARRAY_H
#define UTILS_DYNAMIC_ARRAY_H

#include "defines/common.h"
#include "defines/err_code.h"
#include "types/data_types.h"

GSDB_BEGIN_C_CODE_DECLS

#define DEFAULT_ARRAY_CAPACITY 10

typedef struct DynamicArray DynamicArray;
typedef void *ElementType;
struct DynamicArray {
    uint32_t length;
    uint32_t capacity;
    ElementType *array;
    ElementType initCap[DEFAULT_ARRAY_CAPACITY];
};

/**
 * Init a new dynamic array.
 * @param[in] dArray The array to init.
 * @param[in] capacity The initial capacity of the dynamic array, it will use DEFAULT_ARRAY_CAPACITY
 * as initial size when capacity is less than DEFAULT_ARRAY_CAPACITY.
 * @return a errCode
 */
ErrorCode ArrayInit(DynamicArray *dArray, uint32_t capacity);

/**
 * Create a new dynamic array.
 * @param[in] capacity The initial capacity of the dynamic array, it will use DEFAULT_ARRAY_CAPACITY
 * as initial size when capacity is less than DEFAULT_ARRAY_CAPACITY.
 * @return A dynamic array pointer
 */
DynamicArray *ArrayNew(uint32_t capacity);

/**
 * Clear a dynamic array.
 * @param[in] array A dynamic array pointer.
 */
void ArrayClear(DynamicArray *dArray);

/**
 * Destroy a dynamic array.
 * @param[in] dArray A dynamic array pointer.
 */
void ArrayFree(DynamicArray *dArray);

/**
 * Move a dynamic array.
 * @param[in] from A dynamic array pointer.
 * @param[in] to A dynamic array pointer.
 */
void ArrayMove(DynamicArray *from, DynamicArray *to);

/**
 * Reduce the capacity of array to fit its size.
 * @param[in] dArray A dynamic array pointer.
 */
ErrorCode ArrayShrink(DynamicArray *dArray);

/**
 * Insert the element at the specified location in the dynamic array.
 * Return Error if the index not in the range[0,length]
 * @param dArray[in] A dynamic array pointer
 * @param index[in] Index of pointer to insert
 * @param element[in] Element to be inserted
 * @return The errCode.
 */
ErrorCode ArrayInsert(DynamicArray *dArray, uint32_t index, const ElementType element);

/**
 * Replace the element at the specified location in the dynamic array. The old
 * element at index is returned. The index must bein the range, (index >= 0 && index < Vector_size()).
 * @param dArray[in] A dynamic array pointer
 * @param index[in] Index of pointer to replace
 * @param element[in] Element to be inserted
 * @param errCode[out] ERROR_SYS_OK if success.
 * @return The previous pointer at this location
 */
ElementType ArraySet(DynamicArray *dArray, uint32_t index, const ElementType newElement, ErrorCode *errCode);

/**
 * Returns the element at the specified position. The index should
 * in the range [0,length)
 * @param dArray[in] A dynamic array pointer
 * @param index[in] Index of pointer to return
 * @param errCode[out] the errCode returned. (can be null)
 * @return The element returned.
 */
ElementType ArrayGet(DynamicArray *dArray, uint32_t index, ErrorCode *errCode);

/**
 * Remove the element at the specified position in the dynamic array.
 * Error code is set when index is out of bound.
 * @param dArray[in] A dynamic array pointer
 * @param index[in] Index of element to remove
 * @return errCode[out] ERROR_SYS_OK if success.
 */
ErrorCode ArrayRemove(DynamicArray *dArray, uint32_t index);

/**
 * Append the element to the end of this dynamic array increasing it's size with 1
 * @param dArray[in] A dynamic array pointer
 * @param element[in] Element to be appended
 * @return ERROR_SYS_OK if success.
 */
ErrorCode ArrayPushBack(DynamicArray *dArray, const ElementType element);

/**
 * Remove the last element from the dynamic array. The pointer removed is returned.
 * Error code is set if dynamic array is empty.
 * @param dArray[in] A dynamic array pointer
 * @param errCode[out] ERROR_SYS_OK if success.
 * @return The element that was removed
 */
ErrorCode ArrayPopBack(DynamicArray *dArray, ElementType *element);

/**
 * Get the element number in array.
 * If array is null, return 0;
 * @param dArray[in] A dynamic array pointer
 * @return Current element number.
 */
uint32_t GetArrayLength(DynamicArray *dArray);

/**
 * Get the current maximum number(memory allocated)
 * of the array.
 * If array is null, return 0;
 * @param dArray[in] A dynamic array pointer
 * @return Current array capacity.
 */
uint32_t GetArrayCapacity(DynamicArray *dArray);

/* Fault injection definitions */
enum FaultInjectionDynamicArrayPoint {
    FI_ARRAY_ALLOC_MEMORY_FAILED,
    FI_ARRAY_MEMCPY_FAILED,
};

GSDB_END_C_CODE_DECLS

#endif
