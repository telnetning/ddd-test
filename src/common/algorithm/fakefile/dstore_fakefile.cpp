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
 * IDENTIFICATION
 *        src/common/algorithm/fakefile/dstore_fakefile.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include <sys/file.h>
#include <unistd.h>
#include "securec.h"
#include "common/dstore_datatype.h"
#include "common/algorithm/fakefile/dstore_fakefile.h"


namespace DSTORE {

/*
 * create file and open it.
 * return -1 if failed, else return the fd (0 or positive integer).
 */
int FileOpen(const char* fileName, int fileFlags, int fileMode)
{
    int fd = -1;

tryAgain:
    fd = open(fileName, fileFlags, fileMode);

    if (fd >= 0) {
        return fd; /* success! */
    }

    if (errno == EMFILE || errno == ENFILE) {
        errno = 0;
        goto tryAgain;
    }

    return -1; /* failure */
}

void FileClose(int file)
{
    (void)close(file);
}

void FileUnlink(const char* tempFilePath)
{
    (void)unlink(tempFilePath);
}

// FilePRead
//      Read from a file at a given offset , using pread() for multithreading safe
//      NOTE: The file offset is not changed.
int FilePRead(int file, char* buffer, int amount, off_t offset)
{
    int returnCode;

retry:
    returnCode = static_cast<int>(pread(file, buffer, static_cast<size_t>(static_cast<unsigned int>(amount)), offset));
    if (returnCode < 0) {
        /*
         * Windows may run out of kernel buffers and return "Insufficient
         * system resources" error.  Wait a bit and retry to solve it.
         *
         * It is rumored that EINTR is also possible on some Unix filesystems,
         * in which case immediate retry is indicated.
         */
        /* OK to retry if interrupted */
        if (errno == EINTR) {
            goto retry;
        }
    }

    return returnCode;
}


off_t FileSeek(int file, off_t offset, int whence)
{
    return lseek(file, offset, whence);
}

// FilePWrite
//      Write to a file at a given offset, using pwrite() for multithreading safe
//      NOTE: The file offset is not changed.
int FilePWrite(int file, const char* buffer, int amount, off_t offset)
{
    ssize_t returnCode;

retry:
    errno = 0;
    returnCode = pwrite(file, buffer, static_cast<size_t>(static_cast<unsigned int>(amount)), offset);
    /* if write didn't set errno, assume problem is no disk space */
    if (returnCode != amount && errno == 0) {
        errno = ENOSPC;
    }

    if (returnCode < 0) {
        /* OK to retry if interrupted */
        if (errno == EINTR) {
            goto retry;
        }
    }

    return static_cast<int>(returnCode);
}

}
