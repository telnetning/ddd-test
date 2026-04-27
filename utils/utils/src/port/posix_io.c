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
 * posix_io.c
 *
 * Description:
 * 1. Implementation of the linux io interface wrapper.
 *
 * ---------------------------------------------------------------------------------
 */

#include <sys/stat.h>
#include "port/posix_io.h"

UTILS_EXPORT void SetFileIOMode(SYMBOL_UNUSED FILE *fh, SYMBOL_UNUSED int mode)
{
    return;
}

UTILS_EXPORT int SetFilePermission(FILE *file, unsigned int mode)
{
    return fchmod(file->_fileno, mode);
}
