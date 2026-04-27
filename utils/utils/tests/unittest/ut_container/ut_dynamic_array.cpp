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
 * Description: test dynamic array
 */

#include "gtest/gtest.h"
#include <mockcpp/mokc.h>
#include "defines/utils_errorcode.h"
#include "container/dynamic_array.h"
#include "memory/memory_allocator.h"
#include "fault_injection/fault_injection.h"

// Fault injection is used for exception tests because mockcpp can't be work on the ARM platform.
#undef TEST_BY_MOCKCPP

class DynamicArrayTest: public testing::Test {
public:
    static void SetUpTestSuite()
    {
        FaultInjectionEntry entries[] = {
            FAULT_INJECTION_ENTRY(FI_ARRAY_ALLOC_MEMORY_FAILED, false, nullptr),
            FAULT_INJECTION_ENTRY(FI_ARRAY_MEMCPY_FAILED, false, nullptr),
        };
        ASSERT_EQ(RegisterFaultInjection(entries, sizeof(entries) / sizeof(entries[0]), FI_GLOBAL), ERROR_SYS_OK);
    }

    static void TearDownTestSuite()
    {
        DestroyFaultInjectionHash(FI_GLOBAL);
    }

    void SetUp() override
    {
    };

    void TearDown() override
    {
    };
};

TEST_F(DynamicArrayTest, TestAllInterfaceWhenArrayHandleIsNull)
{
    ErrorCode ret = ERROR_SYS_OK;

    ret = ArrayShrink(NULL);
    ASSERT_EQ(ret, ERROR_UTILS_COMMON_NOT_NULL_VIOLATION);

    ret = ArrayInit(NULL, 100);
    ASSERT_EQ(ret, ERROR_UTILS_COMMON_NOT_NULL_VIOLATION);

    ArrayClear(NULL);
    ArrayFree(NULL);

    DynamicArray from;
    DynamicArray to;
    ArrayMove(NULL, &to);
    ArrayMove(&from, NULL);

    ElementType newValue = (ElementType)(uintptr_t)0x5678;
    ret = ArrayInsert(NULL, 0, newValue);
    ASSERT_EQ(ret, ERROR_UTILS_COMMON_NOT_NULL_VIOLATION);

    ElementType oldValue = ArraySet(NULL, 0, newValue, &ret);
    ASSERT_EQ(ret, ERROR_UTILS_COMMON_NOT_NULL_VIOLATION);
    ASSERT_TRUE(oldValue == NULL);

    oldValue = ArrayGet(NULL, 0, &ret);
    ASSERT_EQ(ret, ERROR_UTILS_COMMON_NOT_NULL_VIOLATION);
    ASSERT_TRUE(oldValue == NULL);

    ret = ArrayRemove(NULL, 0);
    ASSERT_EQ(ret, ERROR_UTILS_COMMON_NOT_NULL_VIOLATION);

    ret = ArrayPushBack(NULL, newValue);
    ASSERT_EQ(ret, ERROR_UTILS_COMMON_NOT_NULL_VIOLATION);

    ret = ArrayPopBack(NULL, &oldValue);
    ASSERT_EQ(ret, ERROR_UTILS_COMMON_NOT_NULL_VIOLATION);

    DynamicArray array;
    ret = ArrayPopBack(&array, NULL);
    ASSERT_EQ(ret, ERROR_UTILS_COMMON_NOT_NULL_VIOLATION);

    uint32_t length = GetArrayLength(NULL);
    ASSERT_EQ(length, 0);

    uint32_t capacity = GetArrayCapacity(NULL);
    ASSERT_EQ(capacity, 0);
}

TEST_F(DynamicArrayTest, TestInitWithObjectWhenCapcityIsSmall)
{
    DynamicArray array;
    ErrorCode ret = ArrayInit(&array, DEFAULT_ARRAY_CAPACITY);

    ASSERT_EQ(ret, ERROR_SYS_OK);

    ArrayClear(&array);
}

TEST_F(DynamicArrayTest, TestInitWithObjectWhenCapcityIsBig)
{
    DynamicArray array;
    ErrorCode ret = ArrayInit(&array, DEFAULT_ARRAY_CAPACITY+1);

    ASSERT_EQ(ret, ERROR_SYS_OK);

    ArrayClear(&array);
}

TEST_F(DynamicArrayTest, TestNewWhenCapcityIsSmall)
{
    DynamicArray *array = ArrayNew(DEFAULT_ARRAY_CAPACITY);

    ASSERT_TRUE(array != NULL);

    ArrayFree(array);
}

TEST_F(DynamicArrayTest, TestNewWhenCapcityIsBig)
{
    DynamicArray *array = ArrayNew(DEFAULT_ARRAY_CAPACITY+1);

    ASSERT_TRUE(array != NULL);

    ArrayFree(array);
}

TEST_F(DynamicArrayTest, TestMoveWhenSrcArrayIsSmall)
{
    DynamicArray from;
    ErrorCode ret = ArrayInit(&from, DEFAULT_ARRAY_CAPACITY);
    ASSERT_EQ(ret, ERROR_SYS_OK);

    DynamicArray to;
    ArrayMove(&from, &to);

    ASSERT_EQ(from.length, 0);
    ASSERT_EQ(from.capacity, DEFAULT_ARRAY_CAPACITY);

    ArrayClear(&to);
}

TEST_F(DynamicArrayTest, TestMoveWhenSrcArrayIsBig)
{
    DynamicArray from;
    ErrorCode ret = ArrayInit(&from, DEFAULT_ARRAY_CAPACITY+1);
    ASSERT_EQ(ret, ERROR_SYS_OK);

    DynamicArray to;
    ArrayMove(&from, &to);

    ASSERT_EQ(from.length, 0);
    ASSERT_EQ(from.capacity, DEFAULT_ARRAY_CAPACITY);

    ArrayClear(&to);
}

TEST_F(DynamicArrayTest, TestShrinkWhenCapacityIsSmall)
{
    DynamicArray array;
    ErrorCode ret = ArrayInit(&array, DEFAULT_ARRAY_CAPACITY-1);
    ASSERT_EQ(ret, ERROR_SYS_OK);

    uint32_t capacity = GetArrayCapacity(&array);
    ASSERT_EQ(capacity, DEFAULT_ARRAY_CAPACITY);

    ret = ArrayShrink(&array);
    ASSERT_EQ(ret, ERROR_SYS_OK);

    ArrayClear(&array);
}

TEST_F(DynamicArrayTest, TestShrinkWhenFull)
{
    DynamicArray array;
    ErrorCode ret = ArrayInit(&array, DEFAULT_ARRAY_CAPACITY+1);
    ASSERT_EQ(ret, ERROR_SYS_OK);

    uint32_t capacity = GetArrayCapacity(&array);
    ASSERT_EQ(capacity, DEFAULT_ARRAY_CAPACITY+1);
    uint32_t length = GetArrayLength(&array);
    ASSERT_EQ(length, 0);

    for (int i = 0; i < capacity; i++) {
        ret = ArrayPushBack(&array, NULL);
        ASSERT_EQ(ret, ERROR_SYS_OK);
    }

    capacity = GetArrayCapacity(&array);
    length = GetArrayLength(&array);
    ASSERT_EQ(length, capacity);

    ret = ArrayShrink(&array);
    ASSERT_EQ(ret, ERROR_SYS_OK);

    ArrayClear(&array);
}

TEST_F(DynamicArrayTest, TestShrinkToDefaultCapacity)
{
    DynamicArray array;
    ErrorCode ret = ArrayInit(&array, DEFAULT_ARRAY_CAPACITY + 20);
    ASSERT_EQ(ret, ERROR_SYS_OK);

    uint32_t capacity = GetArrayCapacity(&array);
    ASSERT_EQ(capacity, DEFAULT_ARRAY_CAPACITY + 20);
    uint32_t length = GetArrayLength(&array);
    ASSERT_EQ(length, 0);

    int pushCount = DEFAULT_ARRAY_CAPACITY - 1;
    for (int i = 0; i < pushCount; i++) {
        ret = ArrayPushBack(&array, NULL);
        ASSERT_EQ(ret, ERROR_SYS_OK);
    }

    length = GetArrayLength(&array);
    ASSERT_EQ(length, pushCount);

    ret = ArrayShrink(&array);
    ASSERT_EQ(ret, ERROR_SYS_OK);

    capacity = GetArrayCapacity(&array);
    ASSERT_EQ(capacity, DEFAULT_ARRAY_CAPACITY);

    ArrayClear(&array);
}

TEST_F(DynamicArrayTest, TestShrinkToDefaultCapacityWhenMemcpyFail)
{
    DynamicArray array;
    ErrorCode ret = ArrayInit(&array, DEFAULT_ARRAY_CAPACITY + 20);
    ASSERT_EQ(ret, ERROR_SYS_OK);

    uint32_t capacity = GetArrayCapacity(&array);
    ASSERT_EQ(capacity, DEFAULT_ARRAY_CAPACITY + 20);
    uint32_t length = GetArrayLength(&array);
    ASSERT_EQ(length, 0);

    int pushCount = DEFAULT_ARRAY_CAPACITY - 1;
    for (int i = 0; i < pushCount; i++) {
        ret = ArrayPushBack(&array, NULL);
        ASSERT_EQ(ret, ERROR_SYS_OK);
    }

    length = GetArrayLength(&array);
    ASSERT_EQ(length, pushCount);

    FAULT_INJECTION_ACTIVE(FI_ARRAY_MEMCPY_FAILED, FI_GLOBAL);

    ret = ArrayShrink(&array);
    ASSERT_EQ(ret, ERROR_UTILS_COMMON_SECURE_C_FUNCTION_FAILED);

    FAULT_INJECTION_INACTIVE(FI_ARRAY_MEMCPY_FAILED, FI_GLOBAL);

    ArrayClear(&array);
}

TEST_F(DynamicArrayTest, TestShrinkToRealLength)
{
    DynamicArray array;
    ErrorCode ret = ArrayInit(&array, DEFAULT_ARRAY_CAPACITY + 20);
    ASSERT_EQ(ret, ERROR_SYS_OK);

    int pushCount = DEFAULT_ARRAY_CAPACITY + 10;
    for (int i = 0; i < pushCount; i++) {
        ret = ArrayPushBack(&array, NULL);
        ASSERT_EQ(ret, ERROR_SYS_OK);
    }

    ret = ArrayShrink(&array);
    ASSERT_EQ(ret, ERROR_SYS_OK);

    uint32_t capacity = GetArrayCapacity(&array);
    ASSERT_EQ(capacity, pushCount);

    ArrayClear(&array);
}

TEST_F(DynamicArrayTest, TestInsertWhenIndexIsValidThenSucceed)
{
    DynamicArray array;
    ErrorCode ret = ArrayInit(&array, DEFAULT_ARRAY_CAPACITY);
    ASSERT_EQ(ret, ERROR_SYS_OK);

    // insert at the end
    uint32_t length = GetArrayLength(&array);
    ASSERT_EQ(length, 0);
    int count = 50;
    for (int i = 0; i < count; i++) {
        ret = ArrayInsert(&array, i, NULL);
        ASSERT_EQ(ret, ERROR_SYS_OK);
    }

    // insert at the begin
    length = GetArrayLength(&array);
    ASSERT_EQ(length, count);
    ret = ArrayInsert(&array, 0, NULL);
    ASSERT_EQ(ret, ERROR_SYS_OK);

    // insert at the middle
    length = GetArrayLength(&array);
    ASSERT_EQ(length, count+1);
    ret = ArrayInsert(&array, count/2, NULL);
    ASSERT_EQ(ret, ERROR_SYS_OK);

    ArrayClear(&array);
}

TEST_F(DynamicArrayTest, TestInsertWhenIndexIsInValidThenFail)
{
    DynamicArray array;
    ErrorCode ret = ArrayInit(&array, DEFAULT_ARRAY_CAPACITY);
    ASSERT_EQ(ret, ERROR_SYS_OK);

    // index should less than length
    uint32_t length = GetArrayLength(&array);
    ret = ArrayInsert(&array, length+1, NULL);
    ASSERT_NE(ret, ERROR_SYS_OK);

    ret = ArrayInsert(&array, length+2, NULL);
    ASSERT_NE(ret, ERROR_SYS_OK);

    ret = ArrayInsert(&array, length, NULL);
    ASSERT_EQ(ret, ERROR_SYS_OK);

    ArrayClear(&array);
}

TEST_F(DynamicArrayTest, TestInsertWhenFullThenSucceed)
{
    DynamicArray array;
    ErrorCode ret = ArrayInit(&array, DEFAULT_ARRAY_CAPACITY);
    ASSERT_EQ(ret, ERROR_SYS_OK);

    int count = DEFAULT_ARRAY_CAPACITY;
    for (int i = 0; i < count; i++) {
        ret = ArrayInsert(&array, i, NULL);
        ASSERT_EQ(ret, ERROR_SYS_OK);
    }

    uint32_t length = GetArrayLength(&array);
    ASSERT_EQ(length, DEFAULT_ARRAY_CAPACITY);

    uint32_t capacity = GetArrayCapacity(&array);
    ASSERT_EQ(capacity, DEFAULT_ARRAY_CAPACITY);

    ret = ArrayInsert(&array, length, NULL);
    ASSERT_EQ(ret, ERROR_SYS_OK);

    ArrayClear(&array);
}

TEST_F(DynamicArrayTest, TestGetInsertAtTheEnd)
{
    DynamicArray array;
    ErrorCode ret = ArrayInit(&array, DEFAULT_ARRAY_CAPACITY);
    ASSERT_EQ(ret, ERROR_SYS_OK);

    int count = 50;
    uint32_t start = 0x1234;
    for (int i = 0; i < count; i++) {
        ElementType element = (ElementType)(uintptr_t)(start + i);
        ret = ArrayInsert(&array, i, element);
        ASSERT_EQ(ret, ERROR_SYS_OK);
    }

    int length = GetArrayLength(&array);
    ASSERT_EQ(length, count);

    for (int i = 0; i < length; i++) {
        ElementType elementExpect = (ElementType)(uintptr_t)(start + i);
        ElementType elementReal = ArrayGet(&array, i, &ret);
        ASSERT_TRUE( elementReal ==  elementExpect);
    }

    ArrayClear(&array);
}

TEST_F(DynamicArrayTest, TestSetWhenIndexIsValidThenSucceed)
{
    DynamicArray array;
    ErrorCode ret = ArrayInit(&array, DEFAULT_ARRAY_CAPACITY);
    ASSERT_EQ(ret, ERROR_SYS_OK);

    ElementType element = (ElementType)(uintptr_t)0x1234;
    ret = ArrayInsert(&array, 0, element);
    ASSERT_EQ(ret, ERROR_SYS_OK);
    uint32_t length = GetArrayLength(&array);
    ASSERT_EQ(length, 1);

    ElementType newValue = (ElementType)(uintptr_t)0x5678;
    ElementType oldValue = ArraySet(&array, 0, &newValue, &ret);

    ASSERT_EQ(oldValue, element);
    ASSERT_EQ(ret, ERROR_SYS_OK);

    ArrayClear(&array);
}

TEST_F(DynamicArrayTest, TestSetWhenIndexIsInValidThenFail)
{
    DynamicArray array;
    ErrorCode ret = ArrayInit(&array, DEFAULT_ARRAY_CAPACITY);
    ASSERT_EQ(ret, ERROR_SYS_OK);

    ElementType element = (ElementType)(uintptr_t)0x1234;
    ret = ArrayInsert(&array, 0, element);
    ASSERT_EQ(ret, ERROR_SYS_OK);
    uint32_t length = GetArrayLength(&array);
    ASSERT_EQ(length, 1);

    ElementType oldValue = ArraySet(&array, 1, NULL, &ret);

    ASSERT_TRUE(oldValue == NULL);
    ASSERT_NE(ret, ERROR_SYS_OK);

    ArrayClear(&array);
}

TEST_F(DynamicArrayTest, TestGetWhenIndexIsOutOfRangeThenFail)
{
    DynamicArray array;
    ErrorCode ret = ArrayInit(&array, DEFAULT_ARRAY_CAPACITY);
    ASSERT_EQ(ret, ERROR_SYS_OK);

    // index is out of range
    ElementType element = ArrayGet(&array, 0, &ret);
    ASSERT_NE(ret, ERROR_SYS_OK);
    ASSERT_TRUE(element == NULL);

    element = ArrayGet(&array, 1, &ret);
    ASSERT_NE(ret, ERROR_SYS_OK);
    ASSERT_TRUE(element == NULL);

    ArrayClear(&array);
}

TEST_F(DynamicArrayTest, TestGetWhenIndexIsInRangeThenSucceed)
{
    DynamicArray array;
    ErrorCode ret = ArrayInit(&array, DEFAULT_ARRAY_CAPACITY);
    ASSERT_EQ(ret, ERROR_SYS_OK);

    // push back a element
    ElementType element = (ElementType)(uintptr_t)0x1234;
    ret = ArrayPushBack(&array, element);
    ASSERT_EQ(ret, ERROR_SYS_OK);

    // index is in the range
    element = ArrayGet(&array, 0, &ret);
    ASSERT_EQ(ret, ERROR_SYS_OK);
    ASSERT_TRUE(element == (ElementType)(uintptr_t)0x1234);

    // index is out of range, fail
    element = ArrayGet(&array, 1, &ret);
    ASSERT_NE(ret, ERROR_SYS_OK);
    ASSERT_TRUE(element == NULL);

    ArrayClear(&array);
}

TEST_F(DynamicArrayTest, TestGetWhenErrCodeIsNullThenSucceed)
{
    DynamicArray array;
    ErrorCode ret = ArrayInit(&array, DEFAULT_ARRAY_CAPACITY);
    ASSERT_EQ(ret, ERROR_SYS_OK);

    // push back a element
    ElementType element = (ElementType)(uintptr_t)0x1234;
    ret = ArrayPushBack(&array, element);
    ASSERT_EQ(ret, ERROR_SYS_OK);

    // index is in the range
    element = ArrayGet(&array, 0, NULL);
    ASSERT_TRUE(element == (ElementType)(uintptr_t)0x1234);

    // index is out of range
    element = ArrayGet(&array, 1, NULL);
    ASSERT_TRUE(element == NULL);

    ArrayClear(&array);
}

TEST_F(DynamicArrayTest, TestRemoveWhenIndexOutOfRangeThenFail)
{
    DynamicArray array;
    ErrorCode ret = ArrayInit(&array, DEFAULT_ARRAY_CAPACITY);
    ASSERT_EQ(ret, ERROR_SYS_OK);

    ret = ArrayRemove(&array, 0);
    ASSERT_NE(ret, ERROR_SYS_OK);

    ArrayClear(&array);
}

TEST_F(DynamicArrayTest, TestRemoveWhenIndexInRangeThenSucceed)
{
    DynamicArray array;
    ErrorCode ret = ArrayInit(&array, DEFAULT_ARRAY_CAPACITY);
    ASSERT_EQ(ret, ERROR_SYS_OK);

    ElementType element = (ElementType)(uintptr_t)0x1234;
    ret = ArrayPushBack(&array, element);
    ASSERT_EQ(ret, ERROR_SYS_OK);

    ret = ArrayRemove(&array, 0);
    ASSERT_EQ(ret, ERROR_SYS_OK);

    ArrayClear(&array);
}

TEST_F(DynamicArrayTest, TestPushBackSucceed)
{
    DynamicArray array;
    ErrorCode ret = ArrayInit(&array, DEFAULT_ARRAY_CAPACITY);
    ASSERT_EQ(ret, ERROR_SYS_OK);

    ElementType element = (ElementType)(uintptr_t)0x1234;
    ret = ArrayPushBack(&array, element);
    ASSERT_EQ(ret, ERROR_SYS_OK);
    ASSERT_TRUE(element != NULL);

    ArrayClear(&array);
}

TEST_F(DynamicArrayTest, TestPopBackWhenEmptyThenFail)
{
    DynamicArray array;
    ErrorCode ret = ArrayInit(&array, DEFAULT_ARRAY_CAPACITY);
    ASSERT_EQ(ret, ERROR_SYS_OK);

    uint32_t length = GetArrayLength(&array);
    ASSERT_EQ(length, 0);

    ElementType element = NULL;
    ret = ArrayPopBack(&array, &element);
    ASSERT_NE(ret, ERROR_SYS_OK);
    ASSERT_TRUE(element == NULL);

    ArrayClear(&array);
}

TEST_F(DynamicArrayTest, TestPopBackWhenNotEmptyThenSucceed)
{
    DynamicArray array;
    ErrorCode ret = ArrayInit(&array, DEFAULT_ARRAY_CAPACITY);
    ASSERT_EQ(ret, ERROR_SYS_OK);

    ElementType elementPush = (ElementType)(uintptr_t)0x1234;
    ret = ArrayPushBack(&array, elementPush);
    ASSERT_EQ(ret, ERROR_SYS_OK);

    ElementType elementPop = NULL;
    ret = ArrayPopBack(&array, &elementPop);
    ASSERT_EQ(ret, ERROR_SYS_OK);
    ASSERT_TRUE(elementPop == elementPush);

    ArrayClear(&array);
}

TEST_F(DynamicArrayTest, TestGetLength)
{
    DynamicArray array;
    ErrorCode ret = ArrayInit(&array, DEFAULT_ARRAY_CAPACITY);
    ASSERT_EQ(ret, ERROR_SYS_OK);

    uint32_t length = GetArrayLength(&array);
    ASSERT_EQ(length, 0);

    for (int i = 0; i < DEFAULT_ARRAY_CAPACITY; i++) {
        ret = ArrayPushBack(&array, NULL);
        ASSERT_EQ(ret, ERROR_SYS_OK);

        uint32_t length = GetArrayLength(&array);
        ASSERT_EQ(length, i+1);
    }

    ArrayClear(&array);
}

TEST_F(DynamicArrayTest, TestGetCapacity)
{
    DynamicArray array;
    ErrorCode ret = ArrayInit(&array, DEFAULT_ARRAY_CAPACITY);
    ASSERT_EQ(ret, ERROR_SYS_OK);

    uint32_t capacity = GetArrayCapacity(&array);
    ASSERT_EQ(capacity, DEFAULT_ARRAY_CAPACITY);

    for (int i = 0; i < DEFAULT_ARRAY_CAPACITY; i++) {
        ret = ArrayPushBack(&array, NULL);
        ASSERT_EQ(ret, ERROR_SYS_OK);

        capacity = GetArrayCapacity(&array);
        ASSERT_EQ(capacity, DEFAULT_ARRAY_CAPACITY);
    }

    ret = ArrayPushBack(&array, NULL);
    ASSERT_EQ(ret, ERROR_SYS_OK);

    capacity = GetArrayCapacity(&array);
    ASSERT_EQ(capacity, DEFAULT_ARRAY_CAPACITY * 2);

    ArrayClear(&array);
}

TEST_F(DynamicArrayTest, TestExpandCapacityFromSmall)
{
    DynamicArray array;
    uint32_t initCapacity = DEFAULT_ARRAY_CAPACITY - 1;
    ErrorCode ret = ArrayInit(&array, initCapacity);
    ASSERT_EQ(ret, ERROR_SYS_OK);

    uint32_t capacity = GetArrayCapacity(&array);
    ASSERT_EQ(capacity, DEFAULT_ARRAY_CAPACITY);

    for (int i = 0; i < DEFAULT_ARRAY_CAPACITY; i++) {
        ret = ArrayPushBack(&array, NULL);
        ASSERT_EQ(ret, ERROR_SYS_OK);

        capacity = GetArrayCapacity(&array);
        ASSERT_EQ(capacity, DEFAULT_ARRAY_CAPACITY);
    }

    // expand first time
    ret = ArrayPushBack(&array, NULL);
    ASSERT_EQ(ret, ERROR_SYS_OK);

    capacity = GetArrayCapacity(&array);
    ASSERT_EQ(capacity, DEFAULT_ARRAY_CAPACITY * 2);

    ArrayClear(&array);
}

TEST_F(DynamicArrayTest, TestExpandCapacityFromBig)
{
    uint32_t initCapacity = DEFAULT_ARRAY_CAPACITY + 1;
    DynamicArray *array = ArrayNew(initCapacity);
    ASSERT_TRUE(array != NULL);

    uint32_t capacity = GetArrayCapacity(array);
    ASSERT_EQ(capacity, initCapacity);

    ErrorCode ret;
    for (int i = 0; i < capacity; i++) {
        ret = ArrayPushBack(array, NULL);
        ASSERT_EQ(ret, ERROR_SYS_OK);

        capacity = GetArrayCapacity(array);
        ASSERT_EQ(capacity, initCapacity);
    }

    // expand first time
    ret = ArrayPushBack(array, NULL);
    ASSERT_EQ(ret, ERROR_SYS_OK);

    capacity = GetArrayCapacity(array);
    ASSERT_EQ(capacity, initCapacity * 2);

    // expand second time
    for (int i = 1; i < capacity - initCapacity; i++) {
        ret = ArrayPushBack(array, NULL);
        ASSERT_EQ(ret, ERROR_SYS_OK);

        capacity = GetArrayCapacity(array);
        ASSERT_EQ(capacity, initCapacity * 2);
    }

    ret = ArrayPushBack(array, NULL);
    ASSERT_EQ(ret, ERROR_SYS_OK);

    capacity = GetArrayCapacity(array);
    ASSERT_EQ(capacity, initCapacity * 2 * 2);

    ArrayFree(array);
}

TEST_F(DynamicArrayTest, TestPushBackWhenMemcpyFailInExpandThenFail)
{
    DynamicArray array;
    uint32_t initCapacity = DEFAULT_ARRAY_CAPACITY - 1;
    ErrorCode ret = ArrayInit(&array, initCapacity);
    ASSERT_EQ(ret, ERROR_SYS_OK);

    uint32_t capacity = GetArrayCapacity(&array);
    ASSERT_EQ(capacity, DEFAULT_ARRAY_CAPACITY);

    for (int i = 0; i < DEFAULT_ARRAY_CAPACITY; i++) {
        ret = ArrayPushBack(&array, NULL);
        ASSERT_EQ(ret, ERROR_SYS_OK);

        capacity = GetArrayCapacity(&array);
        ASSERT_EQ(capacity, DEFAULT_ARRAY_CAPACITY);
    }

    FAULT_INJECTION_ACTIVE(FI_ARRAY_MEMCPY_FAILED, FI_GLOBAL);

    // expand first time
    ret = ArrayPushBack(&array, NULL);
    ASSERT_EQ(ret, ERROR_UTILS_COMMON_SECURE_C_FUNCTION_FAILED);

    capacity = GetArrayCapacity(&array);
    ASSERT_EQ(capacity, DEFAULT_ARRAY_CAPACITY);

    FAULT_INJECTION_INACTIVE(FI_ARRAY_MEMCPY_FAILED, FI_GLOBAL);

    ArrayClear(&array);
}

#ifdef TEST_BY_MOCKCPP
void *StubAllocatorAlloc(MemAllocator *self, uint64_t size)
{
    return NULL;
}
#endif

TEST_F(DynamicArrayTest, TestInitWhenAllocFailThenFail)
{
#ifdef TEST_BY_MOCKCPP
    MOCKER(AllocatorAlloc)
        .stubs()
        .will(invoke(StubAllocatorAlloc));
#else
    FAULT_INJECTION_ACTIVE(FI_ARRAY_ALLOC_MEMORY_FAILED, FI_GLOBAL);
#endif

    DynamicArray array;
    ErrorCode ret = ArrayInit(&array, DEFAULT_ARRAY_CAPACITY + 1);
    ASSERT_EQ(ret, ERROR_UTILS_COMMON_NOENOUGH_MEMORY);

#ifdef TEST_BY_MOCKCPP
    GlobalMockObject::verify();
#else
    FAULT_INJECTION_INACTIVE(FI_ARRAY_ALLOC_MEMORY_FAILED, FI_GLOBAL);
#endif
}

TEST_F(DynamicArrayTest, TestNewWhenAllocFailThenFail)
{
    FAULT_INJECTION_ACTIVE(FI_ARRAY_ALLOC_MEMORY_FAILED, FI_GLOBAL);

    DynamicArray *array = ArrayNew(DEFAULT_ARRAY_CAPACITY);
    ASSERT_TRUE(array == NULL);

    FAULT_INJECTION_INACTIVE(FI_ARRAY_ALLOC_MEMORY_FAILED, FI_GLOBAL);
}

TEST_F(DynamicArrayTest, TestPushBackWhenAllocFailThenFail)
{
    DynamicArray array;
    uint32_t initCapacity = DEFAULT_ARRAY_CAPACITY - 1;
    ErrorCode ret = ArrayInit(&array, initCapacity);
    ASSERT_EQ(ret, ERROR_SYS_OK);

    for (int i = 0; i < DEFAULT_ARRAY_CAPACITY; i++) {
        ret = ArrayPushBack(&array, NULL);
        ASSERT_EQ(ret, ERROR_SYS_OK);
    }

    FAULT_INJECTION_ACTIVE(FI_ARRAY_ALLOC_MEMORY_FAILED, FI_GLOBAL);

    // expand first time
    ret = ArrayPushBack(&array, NULL);
    ASSERT_EQ(ret, ERROR_UTILS_COMMON_NOENOUGH_MEMORY);

    FAULT_INJECTION_INACTIVE(FI_ARRAY_ALLOC_MEMORY_FAILED, FI_GLOBAL);

    ArrayClear(&array);
}

TEST_F(DynamicArrayTest, TestShrinkWhenAllocFailThenFail)
{
    DynamicArray array;
    ErrorCode ret = ArrayInit(&array, DEFAULT_ARRAY_CAPACITY + 20);
    ASSERT_EQ(ret, ERROR_SYS_OK);

    int pushCount = DEFAULT_ARRAY_CAPACITY + 1;
    for (int i = 0; i < pushCount; i++) {
        ret = ArrayPushBack(&array, NULL);
        ASSERT_EQ(ret, ERROR_SYS_OK);
    }

    FAULT_INJECTION_ACTIVE(FI_ARRAY_ALLOC_MEMORY_FAILED, FI_GLOBAL);

    ret = ArrayShrink(&array);
    ASSERT_EQ(ret, ERROR_UTILS_COMMON_NOENOUGH_MEMORY);

    FAULT_INJECTION_INACTIVE(FI_ARRAY_ALLOC_MEMORY_FAILED, FI_GLOBAL);

    ArrayClear(&array);
}

