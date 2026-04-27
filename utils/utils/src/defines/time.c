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
 * time.c
 *
 * ---------------------------------------------------------------------------------
 */

#include <time.h>
#include "defines/time.h"

#define MSECS_PER_SECOND 1000
#define NSECS_PER_MSEC   1000000

UTILS_EXPORT uint64_t GetTimeMsec(TimeType timeType, ErrorCode *errorCode)
{
    struct timespec tp;
    switch (timeType) {
        case EPOCH_TIME:
            (void)clock_gettime(CLOCK_REALTIME, &tp);
            break;
        case BOOT_TIME:
            (void)clock_gettime(CLOCK_BOOTTIME, &tp);
            break;
        default:
            if (errorCode != NULL) {
                *errorCode = UTILS_ERROR_TIME_UNKNOWN_TIMETYPE;
            }
            return INVALID_TIME_VALUE;
    }

    if (errorCode != NULL) {
        *errorCode = ERROR_SYS_OK;
    }

    uint64_t retTime = (uint64_t)(tp.tv_sec * MSECS_PER_SECOND + (tp.tv_nsec / NSECS_PER_MSEC));
    return retTime;
}
