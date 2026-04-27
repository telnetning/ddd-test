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
 * Description: portable safe random function implementation in linux platform
 */
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <stdbool.h>
#include "fault_injection/fault_injection.h"
#include "port/posix_random.h"

static int PrepareRandomSource(void)
{
    FAULT_INJECTION_RETURN(FI_OPEN_RANDOM_FILE_FAILED, -1);
    int fd = open("/dev/random", O_RDONLY);
    return fd;
}

static bool ReadRandomSource(int fd, void *buf, size_t len)
{
    ASSERT(fd >= 0);
    ASSERT(buf != NULL);
    size_t remainLen = len;
    char *ptr = buf;
    while (remainLen > 0) {
        ssize_t res = read(fd, ptr, remainLen);
        FAULT_INJECTION_ACTION(FI_READ_RANDOM_FILE_FAILED, (res = -1, errno = EINVAL));
        if (res <= 0) {
            if (errno == EINTR) {
                continue; /* interrupted by signal, just retry */
            }
            return false;
        }
        ptr += (uintptr_t)res;
        remainLen -= (size_t)res;
    }
    return true;
}

static void ReleaseRandomSource(int fd)
{
    (void)close(fd);
}

UTILS_EXPORT uint32_t GetSafeRandomValue(void)
{
    int fd = PrepareRandomSource();
    if (fd < 0) {
        goto ERROR_EXIT;
    }
    uint32_t randomVal = UINT32_MAX;
    bool res = ReadRandomSource(fd, &randomVal, sizeof(uint32_t));
    ReleaseRandomSource(fd);
    if (!res) {
        goto ERROR_EXIT;
    }
    return randomVal;

ERROR_EXIT:
    return UINT32_MAX;
}

UTILS_EXPORT bool GenerateSecureRandomByteArray(size_t size, unsigned char *out)
{
    int fd = PrepareRandomSource();
    if (fd < 0) {
        return false;
    }
    if (!ReadRandomSource(fd, out, size)) {
        ReleaseRandomSource(fd);
        return false;
    }
    ReleaseRandomSource(fd);
    return true;
}