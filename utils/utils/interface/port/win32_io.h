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
 * win32_io.h
 *
 * Description:Defines the io external interfaces wrapper for windows platform file io.
 *
 * ---------------------------------------------------------------------------------
 */

#ifndef UTILS_WIN32_IO_H
#define UTILS_WIN32_IO_H

#include <fcntl.h>
#include <io.h>

GSDB_BEGIN_C_CODE_DECLS

#define FILE_IO_MODE_TEXT   0
#define FILE_IO_MODE_BINARY 1

/**
 * Sets the file translation mode. Passing FILE_IO_MODE_TEXT as mode sets text (that is, translated) mode.
 * Carriage return-line feed (CR-LF) combinations are translated into a single line feed character on input.
 * Line feed characters are translated into CR-LF combinations on output. Passing FILE_IO_MODE_BINARY sets
 * binary (untranslated) mode, in which these translations are suppressed.
 */
void SetFileIOMode(FILE *fh, int mode);

/**
 * set file permission
 * @param file FILE
 * @param mode permission mode
 */
int SetFilePermission(FILE *file, unsigned int mode);

GSDB_END_C_CODE_DECLS
#endif /* UTILS_WIN32_IO_H */
