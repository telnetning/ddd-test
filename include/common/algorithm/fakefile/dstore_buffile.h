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
 *        include/common/algorithm/fakefile/dstore_buffile.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_GAUSSKERNEL_INCLUDE_COMMON_DSTORE_BUFFILE_H_
#define SRC_GAUSSKERNEL_INCLUDE_COMMON_DSTORE_BUFFILE_H_

#include "common/dstore_datatype.h"

namespace DSTORE {

    struct BufFile;

    extern BufFile* BufFileCreateTemp(bool interXact, const char* baseTmpFileName);
    extern void BufFileClose(BufFile* file) noexcept;
    extern size_t BufFileRead(BufFile* file, void* ptr, size_t size);
    extern RetStatus BufFileWrite(BufFile* file, void* ptr, size_t size);
    extern int BufFileSeek(BufFile* file, int fileno, off_t offset, int whence);
    extern void BufFileTell(BufFile* file, int* fileno, off_t* offset);
    extern int BufFileSeekBlock(BufFile* file, long blknum);
    extern int64 BufFileSize(BufFile *file);
    extern long BufFileAppend(BufFile *target, BufFile *source);
    extern long BufFileAppend(BufFile *target, BufFile *source);
    /* check if buffile is dirty, can check if file is dumpped sucessfully */
    extern bool IsBufFileDirty(BufFile *file);
    void GetFileName(BufFile *file, char *fileName);

}
#endif /* SRC_GAUSSKERNEL_INCLUDE_COMMON_STORAGE_BUFFILE_H_ */
