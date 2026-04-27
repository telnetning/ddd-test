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
 * ---------------------------------------------------------------------------------------
 *
 * IDENTIFICATION
 *        include/common/algorithm/fakefile/dstore_fakefile.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_GAUSSKERNEL_INCLUDE_FAKEFILE_DSTORE_FAKEFILE_H_
#define SRC_GAUSSKERNEL_INCLUDE_FAKEFILE_DSTORE_FAKEFILE_H_

#include "common/dstore_datatype.h"

namespace DSTORE {
    inline bool IsFdValid(int fd)
    {
        return fd >= 0;
    }
    int FileOpen(const char* fileName, int fileFlags, int fileMode);
    void FileUnlink(const char* tempFilePath);
    extern void FileClose(int file);
    extern int FilePRead(int file, char* buffer, int amount, off_t offset);
    extern int FilePWrite(int file, const char* buffer, int amount, off_t offset);
    extern off_t FileSeek(int file, off_t offset, int whence);
}

#endif /* SRC_GAUSSKERNEL_INCLUDE_FAKEFILE_STORAGE_FAKEFILE_H_ */
