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
 * common_file.h
 *
 * Description:
 * This file defines the file-related macros
 *
 * ---------------------------------------------------------------------------------
 */
#ifndef COMMON_DEFS_H
#define COMMON_DEFS_H

#include "types/data_types.h"

GSDB_BEGIN_C_CODE_DECLS

/*
 * Maximum bytes of a file path, the definition is from VFS
 */
#define FILE_PATH_MAX_BYTE 1024
#define BLCKSZ             8192

GSDB_END_C_CODE_DECLS

#endif /* COMMON_DEFS_H */