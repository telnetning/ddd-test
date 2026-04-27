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
 * Description: portable safe random function header in win32 platform
 */
#ifndef UTILS_WIN32_RANDOM_H
#define UTILS_WIN32_RANDOM_H

#include <stdint.h>
#include "defines/common.h"

GSDB_BEGIN_C_CODE_DECLS

/**
 * Generate safe random value in range [0, UINT32_MAX]
 * @return random value
 */
uint32_t GetSafeRandomValue(void);

GSDB_END_C_CODE_DECLS

#endif /* UTILS_WIN32_RANDOM_H */
