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
 * posix_io.h
 *
 * Description:Defines the io external interfaces wrapper for linux platform file io.
 *
 * ---------------------------------------------------------------------------------
 */

#ifndef UTILS_POSIX_IO_H
#define UTILS_POSIX_IO_H

#include <stdio.h>
#include "defines/common.h"

GSDB_BEGIN_C_CODE_DECLS

#define FILE_IO_MODE_TEXT   0
#define FILE_IO_MODE_BINARY 1

/**
 * Sets the file translation mode. This interface is used only to keep the same as the Windows platform
 * and does not need to be implemented..
 */
void SetFileIOMode(FILE *fh, int mode);

/**
 * set file permission
 * @param file FILE
 * @param mode permission mode
 * @return int
 */
int SetFilePermission(FILE *file, unsigned int mode);

GSDB_END_C_CODE_DECLS
#endif /* UTILS_POSIX_IO_H */
