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
 * win32_io.c
 *
 * Description:
 * 1. Implementation of the windows io interface wrapper
 *
 * ---------------------------------------------------------------------------------
 */
#include "port/win32_io.h"

/**
 * Sets the file translation mode.
 * @param file : FILE.
 * @param mode : FILE_IO_MODE_TEXT or FILE_IO_MODE_BINARY.
 */
void SetFileIOMode(FILE *file, int mode)
{
    switch (mode) {
        case FILE_IO_MODE_TEXT:
            _setmode(_fileno(file), _O_TEXT);
            break;
        case FILE_IO_MODE_BINARY:
        default:
            _setmode(_fileno(file), _O_BINARY);
            break;
    }
}

int SetFilePermission(FILE *file, unsigned int mode)
{
    return _setmode(_fileno(file), mode);
}
