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
 */
#ifndef TOOL_HTAB_LOOKUP_H
#define TOOL_HTAB_LOOKUP_H

#include "types/data_types.h"
#include "buffer/dstore_buf.h"

namespace DSTORE {
enum class Buildmode {
    BUILD_MODE_UNSET,
    BUILD_MODE_DEBUG,
    BUILD_MODE_RELEASE
};

struct HTABContext {
    Buildmode mode;
    BufferTag bufTag;
    uint32 maxBucket;
    uint32 highMask;
    uint32 lowMask;
    int sshift;
    long ssize;
};

inline void HTABLookupHelp()
{
    (void)fprintf(stderr,
        "Usage:\n\thtablookup [OPTIONS]\n"
        "\t\t-h\t--help\tShow this help and exit\n"
        "\t\t-m,\t--mode\tBuild mode: release or debug (default)\n"
        "\t\t-p,\t--pdbid\tInput buffer tag pdbId\n"
        "\t\t-f,\t--fileid\tInput buffer tag fileId\n"
        "\t\t-b,\t--blockid\tInput buffer tag blockId\n"
        "\t\t-m,\t--max-bucket\tInput the max bucket\n"
        "\t\t-H,\t--high-mask\tInput the high mask\n"
        "\t\t-l,\t--low-mask\tInput the low mask\n"
        "\t\t-s,\t--sshift\tInput the sshift\n"
        "\t\t-S,\t--sshift\tInput the ssize\n"
        "\t\t-M,\t--mode\tBuild mode: release (default) or debug\n");
}

} /* namespace DSTORE */

#endif
