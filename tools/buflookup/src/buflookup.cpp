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

#include <iostream>
#include <cstdlib>
#include <getopt.h>
#include "buffer/dstore_buf_table.h"
#include "buflookup.h"

/* Known hash table mask values for different shared_buffer sizes
 * 10GB: high_mask = 4194303, low_mask = 2097151
 * 20GB, 30GB: high_mask = 8388607, low_mask = 4194303
 * 40GB, 50GB, 60GB: high_mask = 16777215, low_mask = 8388607
 * 70GB, 80GB, 90GB, 100GB: high_mask = 33554431, low_mask = 16777215
 */

using namespace DSTORE;

uint32 g_lowMask = 0, g_highMask = 0, g_maxBucket = 0;
static constexpr uint32 DEFAULT_SHARED_BUFFERS = 30;
static constexpr int DIR_PATH_NUM = 3;
static const char *g_progName = "buflookup";

/* equivalent to my_log2 from storage_dynahash.cpp */
static int LongLog2(long num)
{
    const long logBase = 2;
    int i;
    unsigned long limit;
    if (num > LONG_MAX / logBase) {
        num = LONG_MAX / logBase;
    }
    for (i = 0, limit = 1; limit < static_cast<unsigned long>(num); i++, limit <<= 1) {
    }
    return i;
}

/* taken from storage_dynahash.cpp because it is not exposed */
static int NextPow2Long(long num)
{
    const int powBase = 2;
    if (num > INT_MAX / powBase) {
        num = INT_MAX / powBase;
    }
    return 1 << static_cast<unsigned int>(LongLog2(num));
}

static void CalcHashMasks(uint32 sharedBufSize)
{
    printf("\n%sCalculating masks for shared buffer hash table%s", BOLDGREEN, RESET);

    /* Based on init_htab in storage_dynahash.cpp */
    uint32 numElem = sharedBufSize * KB_PER_GB / SHARED_BUF_ELEM_PER_KB;
    uint32 numBuckets = NextPow2Long(numElem - 1);
    g_lowMask = numBuckets - 1;
    g_maxBucket = g_lowMask;
    g_highMask = (numBuckets << 1) - 1;

    printf("\nCalculated hash table masks for shared_buffers = %uGB\n", sharedBufSize);
    printf("high_mask = %s%u%s\nlow_mask = max_bucket = %s%u%s\n",
           BOLDCYAN, g_highMask, RESET, BOLDCYAN, g_lowMask, RESET);
}

static uint32 TagHashDebug(const void* key)
{
    int keysize = static_cast<int>(sizeof(BufferTag));
    return DatumGetUInt32(hash_any(static_cast<const unsigned char*>(key), keysize));
}

static uint32 TagHashRelease(const void* key)
{
    int keysize = static_cast<int>(sizeof(BufferTag));
    return hashquickany(0xFFFFFFFF, static_cast<const unsigned char *>(key), keysize);
}

static uint64 CalcHashBucket(uint32 hashcode)
{
    uint64 bucket;

    bucket = hashcode & g_highMask;
    if (bucket > g_maxBucket) {
        bucket = bucket & g_lowMask;
    }
    return bucket;
}

static inline uint64 SegMod(uint64 x, uint64 y)
{
    return ((x) & ((y) - 1));
}

static void CalcSharedBufLocation(uint32 hashcode)
{
    /* Default values from storage_dynahash.cpp are always used for shared buf hash */
    const uint32 ssize = 256;
    const uint32 sshift = 8;

    uint64 hashbucket = CalcHashBucket(hashcode);
    uint64 segNum = hashbucket >> sshift;
    uint64 segNdx = SegMod(hashbucket, static_cast<uint64>(ssize));

    const char* bufDirPaths[] = {"g_instance->storage_instance->m_bufMgr->m_buftable->m_bufHash->dir",
                                 "g_storageInstance->m_bufMgr->m_buftable->m_bufHash->dir",
                                 "m_self->m_buftable->m_bufHash->dir"};

    printf("Hash Bucket: %s%lu%s\n", BOLDCYAN, hashbucket, RESET);
    printf("SegNum: %s%lu%s\n", BOLDCYAN, segNum, RESET);
    printf("SegNdx: %s%lu%s\n", BOLDCYAN, segNdx, RESET);

    printf("\n%sVariable paths for gdb%s", BOLDGREEN, RESET);
    printf("\n1) Verify HashValue matches key in shared buffer hash table (pick one)\n");
    for (int i = 0; i < DIR_PATH_NUM; i++) {
        std::cout<<" • "<<" "<<BOLDBLUE<<"p "<<bufDirPaths[i]<<"["
            <<BOLDCYAN<<segNum<<BOLDBLUE<<"]["<<BOLDCYAN<<segNdx<<BOLDBLUE<<"].hashvalue"<<RESET<<" matches "
            <<BOLDCYAN<<hashcode<<std::endl<<RESET;
    }
    printf("\n2) Verify BufferDesc is valid and buffertag matches input (pick one)\n");
    for (int i = 0; i < DIR_PATH_NUM; i++) {
        std::cout<<" • "<<" "<<BOLDBLUE<<"p* ((BufferLookupEnt*)((char*)&"<<bufDirPaths[i]<<
        "["<<BOLDCYAN<<segNum<<BOLDBLUE<<"]["<<BOLDCYAN<<segNdx<<BOLDBLUE<<"].hashvalue + 8))->buffer"
        <<RESET<<std::endl;
    }
    printf("Note: If debugging somewhere outside of DSTORE, remember to prepend the DSTORE namespace"
           "Example: (BufferLookupEnt*) to (DSTORE::BufferLookupEnt*) \n");
}

static void PrintHelp()
{
    printf("\n%sHelp:%s\n", BOLDYELLOW, RESET);
    printf("\n%s maps BufferTag to BufferDesc stored in shared buffer hash table for debugging\n", g_progName);

    printf("\n%sUsage:\n%s", BOLDYELLOW, RESET);
    printf("  %s [OPTION]...\n", g_progName);

    printf("\n%sOptions:\n%s", BOLDYELLOW, RESET);
    printf("  -p, --pdbid          input buffer tag pdbId\n");
    printf("  -f, --fileid         input buffer tag fileId\n");
    printf("  -b, --blockid        input buffer tag blockId\n");
    printf("  -s, --sharedbuffers  instance shared_buffers in GB (default 30GB)\n");
    printf("  -m, --mode           build mode: release or debug (default)\n");
    printf("  -h, --help           show this help and exit\n");

    printf("\n%sExamples:\n%s", BOLDYELLOW, RESET);
    printf("  ./buflookup --pdbid 3 --fileid 2 --blockid 2 --mode debug\n");
    printf("  ./buflookup -p 3 -f 2 -b 2\n");
    printf("  ./buflookup -p 3 -f 2 -b 2 -m release -s 30\n");

    printf("\n%sAdditional Help:\n%s", BOLDYELLOW, RESET);
    printf("• Connect gdb to a live gaussdb instance or a coredump and run: set print object on\n"
           "• Verify values are correct for low_mask and high_mask by checking "
           "g_instance->storage_instance->m_bufMgr->m_buftable->m_bufHash->hctl "
           "these values change with size of shared_buffers\n"
           "• Follow instructions to lookup a BufferTag in bufferpool hash table for RELEASE or DEBUG build\n"
           "• Replace BufferDesc with DSTORE::BufferDesc if accessing shared buf from outside DSTORE\n"
           "• If gdb returns cannot access memory then the bucket does not exist, check input BufferTag\n");
}

namespace DSTORE {
enum class Buildmode {
    BUILD_MODE_RELEASE,
    BUILD_MODE_DEBUG,
    BUILD_MODE_UNSET
};
}

static int ParseCmdLineInput(int argc, char **argv, BufferTag& bufTag, Buildmode& mode)
{
    int argcount = 0, option = 0, optIndex = 0;
    bool havePdbId = false, haveFileId = false, haveBlockId = false;
    const char *shortOptions = "p:f:b:m:s:h";
    struct option longOptions[] = {{"pdbid", required_argument, nullptr, 'c'},
                                   {"fileid", required_argument, nullptr, 'f'},
                                   {"blockid", required_argument, nullptr, 'b'},
                                   {"mode", required_argument, nullptr, 'm'},
                                   {"sharedbuffers", required_argument, nullptr, 's'},
                                   {"help", no_argument, nullptr, 'h'},
                                   {nullptr, 0, nullptr, 0}};

    printf("\n%sInput Parameters%s\n", BOLDGREEN, RESET);
    while ((option = getopt_long(argc, argv, shortOptions, longOptions, &optIndex)) != -1) {
        argcount++;
        switch (option) {
            case 'p': {
                printf("pdbid = %s\n", optarg);
                bufTag.pdbId = std::stoi(optarg);
                havePdbId = true;
                break;
            }
            case 'f': {
                printf("fileid = %s\n", optarg);
                bufTag.pageId.m_fileId = std::stoi(optarg);
                haveFileId = true;
                break;
            }
            case 'b': {
                printf("blockid = %s\n", optarg);
                bufTag.pageId.m_blockId = std::stoi(optarg);
                haveBlockId = true;
                break;
            }
            case 'm': {
                printf("mode = %s\n", optarg);
                if (!strcmp("debug", optarg)) {
                    mode = Buildmode::BUILD_MODE_DEBUG;
                } else if (!strcmp("release", optarg)) {
                    mode = Buildmode::BUILD_MODE_RELEASE;
                } else {
                    printf("Invalid mode parameter!\n");
                    PrintHelp();
                    return 0;
                }
                break;
            }
            case 's': {
                printf("shared_buffers = %s\n", optarg);
                CalcHashMasks(std::stoi(optarg));
                break;
            }
            case 'h': {
                PrintHelp();
                return 0;
            }
            default: {
                PrintHelp();
                return 0;
            }
        }
    }
    /* Input parameters must include a complete buffertag */
    if (havePdbId && haveFileId && haveBlockId) {
        return 1;
    } else {
        printf("Insufficient parameters: Please provide pdbId, fileId, and blockId\n");
    }

    PrintHelp();
    return 0;
}

static int CalcSharedBufHash(BufferTag* bufTag, Buildmode mode, uint32 &hashcode)
{
    printf("\n%sCalculating buffertag hash%s\n", BOLDGREEN, RESET);
    if (mode == Buildmode::BUILD_MODE_DEBUG) {
        hashcode = TagHashDebug(bufTag);
        printf("Debug Build Hash Value: %s%u%s\n", BOLDCYAN, hashcode, RESET);
        return 1;
    } else if (mode == Buildmode::BUILD_MODE_RELEASE) {
        hashcode = TagHashRelease(bufTag);
        printf("Release Build Hash Value: %s%u%s\n", BOLDCYAN, hashcode, RESET);
        return 1;
    }

    printf("\nInvalid mode parameter\n");
    PrintHelp();
    return 0;
}

int main(int argc, char* argv[])
{
    BufferTag bufTag;
    Buildmode mode = Buildmode::BUILD_MODE_UNSET;
    uint32 hashcode = 0;

    if (!ParseCmdLineInput(argc, argv, bufTag, mode)) {
        return EXIT_FAILURE;
    }

    if (mode == Buildmode::BUILD_MODE_UNSET) {
        printf("%s--mode parameter not provided, defaulting to: mode = debug%s\n", RED, RESET);
        mode = Buildmode::BUILD_MODE_DEBUG;
    }

    if (g_highMask == 0) {
        printf("%s--sharedbuffers parameter not provided, defaulting to: sharedbuffers = %u%s\n",
               RED, DEFAULT_SHARED_BUFFERS, RESET);
        CalcHashMasks(DEFAULT_SHARED_BUFFERS);
    }

    if (!CalcSharedBufHash(&bufTag, mode, hashcode)) {
        return EXIT_FAILURE;
    }

    CalcSharedBufLocation(hashcode);

    return EXIT_SUCCESS;
}
