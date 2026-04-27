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
 * dynamic_array.c
 *
 * Description:
 * 1. This file is a simple dynamic array implementation
 *
 * ---------------------------------------------------------------------------------
 */

#include "securec.h"
#include "memory/memory_allocator.h"
#include "fault_injection/fault_injection.h"

#include "container/dynamic_array.h"

#undef ARRAY_DEBUG_PRINT

static MemAllocator g_memAllocator = {0};
static void *ArrayAllocMemZero(uint64_t size)
{
    ASSERT(size != 0);

    FAULT_INJECTION_RETURN(FI_ARRAY_ALLOC_MEMORY_FAILED, NULL);

    void *ptr = AllocatorAlloc(&g_memAllocator, size);
    if (unlikely(ptr == NULL)) {
#ifdef ARRAY_DEBUG_PRINT
        (void)fprintf(stderr, "zero alloc failed, system out of memory, size:%lu.\n", size);
#endif
        return NULL;
    }
    errno_t rc = memset_s(ptr, size, 0, size);
    if (unlikely(rc != EOK)) {
#ifdef ARRAY_DEBUG_PRINT
        (void)fprintf(stderr, "Failed to memset_s, errno is %d.\n", rc);
#endif
        AllocatorFree(&g_memAllocator, ptr);
        return NULL;
    }

    return ptr;
}

static void ArrayFreeMem(void *ptr)
{
    ASSERT(ptr != NULL);
    AllocatorFree(&g_memAllocator, ptr);
}

static inline bool ArrayIsFull(DynamicArray *dArray)
{
    ASSERT(dArray != NULL);
    return (dArray->length >= dArray->capacity);
}

static ErrorCode ArrayExpand(DynamicArray *dArray)
{
    ASSERT(dArray != NULL);

    uint32_t capacity = 2 * dArray->length;
    ElementType *newArray = (ElementType *)ArrayAllocMemZero(capacity * sizeof(ElementType));
    if (unlikely(newArray == NULL)) {
#ifdef ARRAY_DEBUG_PRINT
        (void)fprintf(stderr, "Expand failed when allocate memory, size is %lu.\n", capacity * sizeof(ElementType));
#endif
        return ERROR_UTILS_COMMON_NOENOUGH_MEMORY;
    }

    // NOTICE, the 'ret' is initialized to an error code to test the scenario where the memcpy copy fails.
    int ret = EINVAL;
    FAULT_INJECTION_CALL_REPLACE(FI_ARRAY_MEMCPY_FAILED, NULL)
    ret = memcpy_s(newArray, capacity * sizeof(ElementType), dArray->array, dArray->length * sizeof(ElementType));
    FAULT_INJECTION_CALL_REPLACE_END;
    if (unlikely(ret != EOK)) {
        ArrayFreeMem(newArray);
#ifdef ARRAY_DEBUG_PRINT
        (void)fprintf(stderr, "Expand failed when copy memory.\n");
#endif
        return ERROR_UTILS_COMMON_SECURE_C_FUNCTION_FAILED;
    }

    dArray->capacity = capacity;
    if (dArray->array != dArray->initCap) {
        ArrayFreeMem(dArray->array);
    }
    dArray->array = newArray;

    return ERROR_SYS_OK;
}

ErrorCode ArrayShrink(DynamicArray *dArray)
{
    if (unlikely(dArray == NULL)) {
        return ERROR_UTILS_COMMON_NOT_NULL_VIOLATION;
    }

    if (dArray->capacity <= DEFAULT_ARRAY_CAPACITY || dArray->length == dArray->capacity) {
        return ERROR_SYS_OK;
    }

    ElementType *newArray = NULL;
    uint32_t capacity = dArray->length;
    if (capacity <= DEFAULT_ARRAY_CAPACITY) {
        newArray = dArray->initCap;
    } else {
        newArray = (ElementType *)ArrayAllocMemZero(capacity * sizeof(ElementType));
        if (unlikely(newArray == NULL)) {
#ifdef ARRAY_DEBUG_PRINT
            (void)fprintf(stderr, "Shrink failed when allocate memory, size is %lu.\n", capacity * sizeof(ElementType));
#endif
            return ERROR_UTILS_COMMON_NOENOUGH_MEMORY;
        }
    }

    // NOTICE, the 'ret' is initialized to an error code to test the scenario where the memcpy copy fails.
    int ret = EINVAL;
    FAULT_INJECTION_CALL_REPLACE(FI_ARRAY_MEMCPY_FAILED, NULL)
    ret = memcpy_s(newArray, capacity * sizeof(ElementType), dArray->array, dArray->length * sizeof(ElementType));
    FAULT_INJECTION_CALL_REPLACE_END;
    if (unlikely(ret != EOK)) {
        if (newArray != dArray->initCap) {
            ArrayFreeMem(newArray);
        }
#ifdef ARRAY_DEBUG_PRINT
        (void)fprintf(stderr, "Shrink dynamic array failed when copy memory.\n");
#endif
        return ERROR_UTILS_COMMON_SECURE_C_FUNCTION_FAILED;
    }

    ASSERT(dArray->array != dArray->initCap);
    ArrayFreeMem(dArray->array);
    dArray->array = newArray;
    if (newArray == dArray->initCap) {
        dArray->capacity = DEFAULT_ARRAY_CAPACITY;
    } else {
        dArray->capacity = capacity;
    }
    return ERROR_SYS_OK;
}

ErrorCode ArrayInit(DynamicArray *dArray, uint32_t capacity)
{
    if (unlikely(dArray == NULL)) {
        return ERROR_UTILS_COMMON_NOT_NULL_VIOLATION;
    }

    dArray->length = 0;
    dArray->capacity = capacity;
    if (capacity <= DEFAULT_ARRAY_CAPACITY) {
        dArray->capacity = DEFAULT_ARRAY_CAPACITY;
        dArray->array = dArray->initCap;
        return ERROR_SYS_OK;
    }
    dArray->array = (ElementType *)ArrayAllocMemZero(dArray->capacity * sizeof(ElementType));
    if (unlikely(dArray->array == NULL)) {
#ifdef ARRAY_DEBUG_PRINT
        (void)fprintf(stderr, "Init failed when allocate memory, size is %lu.\n",
                      dArray->capacity * sizeof(ElementType));
#endif
        return ERROR_UTILS_COMMON_NOENOUGH_MEMORY;
    }
    return ERROR_SYS_OK;
}

DynamicArray *ArrayNew(uint32_t capacity)
{
    DynamicArray *array = (DynamicArray *)ArrayAllocMemZero(sizeof(DynamicArray));
    if (unlikely(array == NULL)) {
#ifdef ARRAY_DEBUG_PRINT
        (void)fprintf(stderr, "New failed when allocate memory, size is %lu.\n", sizeof(DynamicArray));
#endif
        return NULL;
    }
    ErrorCode err = ArrayInit(array, capacity);
    if (unlikely(err != ERROR_SYS_OK)) {
        ArrayFreeMem(array);
        return NULL;
    }
    return array;
}

void ArrayClear(DynamicArray *dArray)
{
    if (unlikely(dArray == NULL)) {
        return;
    }

    if (dArray->array && dArray->array != dArray->initCap) {
        ArrayFreeMem(dArray->array);
        dArray->array = dArray->initCap;
    }
    dArray->capacity = DEFAULT_ARRAY_CAPACITY;
    dArray->length = 0;
}

void ArrayFree(DynamicArray *dArray)
{
    if (unlikely(dArray == NULL)) {
        return;
    }

    ArrayClear(dArray);
    ArrayFreeMem(dArray);
}

void ArrayMove(DynamicArray *from, DynamicArray *to)
{
    if (unlikely(from == NULL || to == NULL)) {
        return;
    }

    *to = *from;
    if (from->array == from->initCap) {
        to->array = to->initCap;
    }
    from->length = 0;
    from->capacity = DEFAULT_ARRAY_CAPACITY;
    from->array = from->initCap;
}

static bool ArrayIsValid(DynamicArray *dArray, uint32_t index, ErrorCode *errCode)
{
    if (unlikely(dArray == NULL)) {
        if (errCode) {
            *errCode = ERROR_UTILS_COMMON_NOT_NULL_VIOLATION;
        }
        return false;
    }

    if (unlikely(index >= dArray->length)) {
        if (errCode) {
            *errCode = ERROR_UTILS_COMMON_INDEX_OUT_OF_RANGE;
        }
        return false;
    }

    return true;
}

ErrorCode ArrayInsert(DynamicArray *dArray, uint32_t index, const ElementType element)
{
    if (unlikely(dArray == NULL)) {
        return ERROR_UTILS_COMMON_NOT_NULL_VIOLATION;
    }

    if (unlikely(index > dArray->length)) {
        return ERROR_UTILS_COMMON_INDEX_OUT_OF_RANGE;
    }

    if (unlikely(ArrayIsFull(dArray))) {
        ErrorCode ret = ArrayExpand(dArray);
        if (ret != ERROR_SYS_OK) {
            return ret;
        }
    }

    for (uint32_t i = dArray->length++; i > index; i--) {
        dArray->array[i] = dArray->array[i - 1];
    }
    dArray->array[index] = element;
    return ERROR_SYS_OK;
}

ElementType ArraySet(DynamicArray *dArray, uint32_t index, const ElementType newElement, ErrorCode *errCode)
{
    if (!ArrayIsValid(dArray, index, errCode)) {
        return NULL;
    }
    ElementType prev = dArray->array[index];
    dArray->array[index] = newElement;
    if (errCode) {
        *errCode = ERROR_SYS_OK;
    }
    return prev;
}

ElementType ArrayGet(DynamicArray *dArray, uint32_t index, ErrorCode *errCode)
{
    if (!ArrayIsValid(dArray, index, errCode)) {
        return NULL;
    }
    if (errCode) {
        *errCode = ERROR_SYS_OK;
    }
    return dArray->array[index];
}

ErrorCode ArrayRemove(DynamicArray *dArray, uint32_t index)
{
    if (unlikely(dArray == NULL)) {
        return ERROR_UTILS_COMMON_NOT_NULL_VIOLATION;
    }

    if (unlikely(index >= dArray->length)) {
        return ERROR_UTILS_COMMON_INDEX_OUT_OF_RANGE;
    }
    uint32_t lengthAfterRemove = dArray->length - 1;
    for (uint32_t i = index; i < lengthAfterRemove; i++) {
        dArray->array[i] = dArray->array[i + 1];
    }
    dArray->length = lengthAfterRemove;
    return ERROR_SYS_OK;
}

ErrorCode ArrayPushBack(DynamicArray *dArray, const ElementType element)
{
    if (unlikely(dArray == NULL)) {
        return ERROR_UTILS_COMMON_NOT_NULL_VIOLATION;
    }

    if (unlikely(ArrayIsFull(dArray))) {
        ErrorCode ret = ArrayExpand(dArray);
        if (ret != ERROR_SYS_OK) {
            return ret;
        }
    }

    dArray->array[dArray->length++] = element;
    return ERROR_SYS_OK;
}

ErrorCode ArrayPopBack(DynamicArray *dArray, ElementType *element)
{
    if (unlikely(dArray == NULL || element == NULL)) {
        return ERROR_UTILS_COMMON_NOT_NULL_VIOLATION;
    }

    if (unlikely(dArray->length == 0)) {
        *element = NULL;
        return ERROR_UTILS_COMMON_DYNAMIC_ARRAY_IS_EMPTY;
    }
    *element = dArray->array[--dArray->length];
    return ERROR_SYS_OK;
}

uint32_t GetArrayLength(DynamicArray *dArray)
{
    if (unlikely(dArray == NULL)) {
        return 0;
    }

    return dArray->length;
}

uint32_t GetArrayCapacity(DynamicArray *dArray)
{
    if (unlikely(dArray == NULL)) {
        return 0;
    }

    return dArray->capacity;
}
