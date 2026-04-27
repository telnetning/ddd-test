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
 * time.h
 *
 * ---------------------------------------------------------------------------------
 */
#ifndef UTILS_TIME_H
#define UTILS_TIME_H

#include "common.h"
#include "types/data_types.h"
#include "defines/utils_errorcode.h"

GSDB_BEGIN_C_CODE_DECLS

#define INVALID_TIME_VALUE 0

typedef enum TimeType { EPOCH_TIME = 0, BOOT_TIME } TimeType;

/**
 * GetTimeMsec - Get the milliseconds value of the time type specified by the input parameter.
 *
 * @param[in] timeType: specifies the time type.
 * @param[out] errorCode: ERROR_SYS_OK if success.
 * @return the milliseconds value, or return INVALID_TIME_VALUE if timeType is invalid.
 */
uint64_t GetTimeMsec(TimeType timeType, ErrorCode *errorCode);

GSDB_END_C_CODE_DECLS

#endif /* UTILS_TIME_H */
