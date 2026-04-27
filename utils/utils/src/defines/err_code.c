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
 * Description: Implement for error code transfer to error infomation
 */

#include <string.h>
#include <stdint.h>
#include "defines/err_code.h"

static uint32_t g_errorCodeCountMap[UINT8_MAX][UINT8_MAX] = {0};
const char **g_errorCodeMap[UINT8_MAX][UINT8_MAX]; /* avoid assign initial value, take advantage of COW */

void RegisterErrorDescInfo(uint8_t componentId, uint8_t moduleId, const char **array, size_t size)
{
    if ((array == NULL) || (size >= UINT32_MAX)) {
        return;
    }
    ASSERT(g_errorCodeCountMap[componentId][moduleId] == 0);
    g_errorCodeCountMap[componentId][moduleId] = (uint32_t)size;
    g_errorCodeMap[componentId][moduleId] = array;
}

void RegisterErrorDescInfoCallback(uint8_t componentId, uint8_t moduleId, ErrorDescInfoCallback callback)
{
    if (callback == NULL) {
        return;
    }
    ASSERT(g_errorCodeCountMap[componentId][moduleId] == 0);
    g_errorCodeCountMap[componentId][moduleId] = UINT32_MAX;
    g_errorCodeMap[componentId][moduleId] = (const char **)(void *)callback;
}

const char *GetErrorCodeInfo(uint64_t errorCode)
{
    uint8_t componentId = ERROR_GET_COMPONENT(errorCode);
    uint8_t moduleId = ERROR_GET_MODULE(errorCode);
    const char **map = g_errorCodeMap[componentId][moduleId];
    uint32_t count = g_errorCodeCountMap[componentId][moduleId];
    if ((count == UINT32_MAX)) {
        return ((ErrorDescInfoCallback)map)(errorCode);
    }
    uint32_t errorNo = ERROR_GET_CODE(errorCode);
    if (errorNo < count) {
        return map[errorNo];
    }
    return "NOTFOUNT:undefined error code infomation";
}
