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
#include "htablookup.h"

#include <string>
#include <unistd.h>
#include <getopt.h>

#include "common/algorithm/dstore_hsearch.h"
#include "common/algorithm/dstore_dynahash.h"

using namespace DSTORE;

static bool ParseArgs(int argc, char **argv, HTABContext *context)
{
    int opt = 0;
    int index = 0;
    const struct option longopts[] = {
        {"help", no_argument, nullptr, 'h'},
        {"pdbid", required_argument, nullptr, 'p'},
        {"fileid", required_argument, nullptr, 'f'},
        {"blockid", required_argument, nullptr, 'b'},
        {"max-bucket", required_argument,  nullptr, 'm'},
        {"high-mask", required_argument, nullptr, 'H'},
        {"low-mask", required_argument, nullptr, 'l'},
        {"sshift", required_argument, nullptr, 's'},
        {"ssize", required_argument, nullptr, 'S'},
        {"mode", required_argument, nullptr, 'M'},
        {nullptr, 0, nullptr, 0},
    };
    while ((opt = getopt_long(argc, argv, ":hp:f:b:m:H:l:s:S:M:", longopts, &index)) != -1) {
        switch (opt) {
            case 0: {
                break;
            }
            case 'p': {
                context->bufTag.pdbId = strtoul(optarg, nullptr, 0);
                break;
            }
            case 'f': {
                context->bufTag.pageId.m_fileId = strtoul(optarg, nullptr, 0);
                break;
            }
            case 'b': {
                context->bufTag.pageId.m_blockId = strtoul(optarg, nullptr, 0);
                break;
            }
            case 'm': {
                context->maxBucket = strtoul(optarg, nullptr, 0);
                break;
            }
            case 'H': {
                context->highMask = strtoul(optarg, nullptr, 0);
                break;
            }
            case 'l': {
                context->lowMask = strtoul(optarg, nullptr, 0);
                break;
            }
            case 's': {
                context->sshift = strtoul(optarg, nullptr, 0);
                break;
            }
            case 'S': {
                context->ssize = strtoul(optarg, nullptr, 0);
                break;
            }
            case 'M': {
                if (!strcmp("debug", optarg)) {
                    context->mode = Buildmode::BUILD_MODE_DEBUG;
                } else if (!strcmp("release", optarg)) {
                    context->mode = Buildmode::BUILD_MODE_RELEASE;
                } else {
                    printf("Invalid mode parameter!\n");
                    return false;
                }
                break;
            }
            case ':': {
                (void)fprintf(stderr, "Option - %c needs a value\n", optopt);
                [[fallthrough]];
            }
            case 'h':
            default:{
                if (optopt != 'h' && optopt != ':') {
                    (void)fprintf(stderr, "Unknown option %c\n", optopt);
                }
                return false;
            }
        }
    }
    return true;
}


int main(int argc, char **argv)
{
    HTABContext context = {
        .mode = Buildmode::BUILD_MODE_UNSET,
        .bufTag = {},
        .maxBucket = 0,
        .highMask = 0,
        .lowMask = 0,
        .sshift = 0,
        .ssize = 0
    };

    /* Parse the command line arguments */
    if (!ParseArgs(argc, argv, &context)) {
        printf("Failed parse\n");
        HTABLookupHelp();
        return -1;
    }

    void *key = static_cast<void *>(&context.bufTag);
    uint32 hash;
    switch (context.mode) {
        case Buildmode::BUILD_MODE_UNSET:
            [[fallthrough]];
        case Buildmode::BUILD_MODE_RELEASE:
            hash = DatumGetUInt32(hash_any(static_cast<unsigned char *>(key), sizeof(BufferTag)));
            break;
        case Buildmode::BUILD_MODE_DEBUG:
            hash = hashquickany(0xFFFFFFFF, static_cast<unsigned char *>(key), sizeof(BufferTag));
            break;
        default:
            HTABLookupHelp();
            return -1;
    }

    uint32 bucket = hash & context.highMask;
    if (bucket > context.maxBucket) {
        bucket = hash & context.lowMask;
    }
    long segmentNum = static_cast<long>(bucket) >> context.sshift;
    /* Fast mod for powers of 2 */
    long segmentNdx = (bucket) & ((context.ssize) - 1);

    uint32 alignHashElement = MAXALIGN(sizeof(HASHELEMENT));
    printf("hash = %u\nbucket = %u\nsegNum = %ld\nsegNdx = %ld\nalign = %u\n",
        hash, bucket, segmentNum, segmentNdx, alignHashElement);
    return 0;
}
