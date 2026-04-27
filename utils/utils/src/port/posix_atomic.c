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
 * posix_atomic.c
 *
 * Description:
 * 1. Implementation of the atomic interface wrapper.
 *
 * ---------------------------------------------------------------------------------
 */
#include "port/posix_time.h"
#include "port/posix_atomic.h"

#define ATOMICU8_INSTANCE_INITIALIZING                   1
#define ATOMICU8_INSTANCE_INITIALIZED_SUCCESS            2
#define ATOMICU8_INSTANCE_INITIALIZED_FAIL               3
#define ATOMICU8_INSTANCE_WAIT_INITIALIZED_INTERVAL_TIME 1
#define ATOMICU8_INSTANCE_WAIT_INITIALIZED_TRY_COUNT     100
static ErrorCode InitializedOnlyOnceCallback(AtomicU8 *atomic8, ErrorCode (*initializedCallback)(void *), void *arg)
{
    ErrorCode errorCode;
    uint8_t expected;
    uint8_t desired;
    bool result = false;
    errorCode = initializedCallback(arg);
    if (errorCode == ERROR_SYS_OK) {
        desired = ATOMICU8_INSTANCE_INITIALIZED_SUCCESS;
    } else {
        desired = ATOMICU8_INSTANCE_INITIALIZED_FAIL;
    }
    expected = ATOMICU8_INSTANCE_INITIALIZING;
    result = AtomicU8CompareExchangeStrong(atomic8, &expected, desired, MEMORY_ORDER_SEQ_CST, MEMORY_ORDER_SEQ_CST);
    if (result) {
        return errorCode;
    } else {
        return ERROR_UTILS_COMMON_FAILED;
    }
}

static ErrorCode InitializedOnlyOnceWaitResult(AtomicU8 *atomic8)
{
    uint32_t tryCount;
    uint8_t instanceInitialState = ATOMICU8_INSTANCE_INITIALIZING;
    tryCount = 0;
    while (instanceInitialState == ATOMICU8_INSTANCE_INITIALIZING) {
        Usleep(ATOMICU8_INSTANCE_WAIT_INITIALIZED_INTERVAL_TIME);
        instanceInitialState = AtomicU8Get(atomic8, MEMORY_ORDER_SEQ_CST);
        tryCount++;
        if (tryCount >= ATOMICU8_INSTANCE_WAIT_INITIALIZED_TRY_COUNT) {
            return ERROR_UTILS_COMMON_FAILED;
        }
    }
    if (instanceInitialState == ATOMICU8_INSTANCE_INITIALIZED_SUCCESS) {
        return ERROR_SYS_OK;
    } else {
        return ERROR_UTILS_COMMON_FAILED;
    }
}
UTILS_EXPORT ErrorCode InitializedOnlyOnce(AtomicU8 *atomic8, ErrorCode (*initializedCallback)(void *), void *arg)
{
    if (atomic8 == NULL || initializedCallback == NULL) {
        return ERROR_UTILS_COMMON_INVALID_PARAMETER;
    }
    uint8_t expected = ATOMICU8_INSTANCE_INITIAL_VALUE;
    uint8_t desired = ATOMICU8_INSTANCE_INITIALIZING;
    bool result = false;
    result = AtomicU8CompareExchangeStrong(atomic8, &expected, desired, MEMORY_ORDER_SEQ_CST, MEMORY_ORDER_SEQ_CST);
    if (result) {
        return InitializedOnlyOnceCallback(atomic8, initializedCallback, arg);
    } else {
        if (expected == ATOMICU8_INSTANCE_INITIALIZED_SUCCESS) {
            return ERROR_SYS_OK;
        } else if (expected == ATOMICU8_INSTANCE_INITIALIZING) {
            return InitializedOnlyOnceWaitResult(atomic8);
        } else {
            return ERROR_UTILS_COMMON_FAILED;
        }
    }
}
