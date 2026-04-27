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

#ifndef DSTORE_BUF_MGR_DIAGNOSE
#define DSTORE_BUF_MGR_DIAGNOSE
#include "common/dstore_common_utils.h"

namespace DSTORE {

struct BufferDesc;
struct BufDescPrintInfo;
struct PageDirectoryInfo;
struct IoInfo;
struct LruInfo;
#pragma GCC visibility push(default)

class BufMgrDiagnose {
public:
    /**
     * Get the item in the lru hot list.
     */
    static size_t GetLruHotList(uint32_t queueIdx, char **items);

    /**
     * Get the item in the lru list.
     */
    static size_t GetLruList(uint32_t queueIdx, char **items);

    /**
     * Get the item in the candidate list.
     */
    static size_t GetCandidateList(uint32_t queueIdx, char **items);

    /**
     * Get the summary info of the buffer lru list.
     * This method will return the list size of each list.
     */
    static size_t GetBufLruSummaryInfo(char **items);

    /**
     * Get the info of the buffer desc by {pdbId, fileId, blockId}.
     */
    static char *PrintBufferDesc(PdbId pdbId, FileId fileId, BlockNumber blockId);

    /**
     * Get the info of the buffer desc by the memory address.
     */
    static char *PrintBufferDesc(BufferDesc *selectBufferDesc);

    /*
     * Get the buffer mgr size.
     */
    static size_t GetBufMgrSize();

    /*
     * Get the number of lru paritions.
     */
    static size_t GetLruPartition();

    /*
     * Get the max size of the hot lru list.
     */
    static size_t GetHotListSize();

    /**
     * Get the buffer mgr statistics info.
     */
    static char *PrintBufMgrStatistics();

    /**
     * Reset the buffer mgr statistics info.
     */
    static char *ResetBufMgrStatistics();

    /**
     * Get the mem chuk statistics info.
     */
    static char *PrintMemChunkStatistic();

    /**
     * Get the distributed bufferpool info after recovery.
     */
    static char *PrintPdRecoveryInfo();

    /**
     * Get the AntiCache info.
     *
     * @param allPartition need colloct info across all partitions
     * @param partitionId indicate which partition to colloct info
     * @return the AntiCache memory usage info in the form of a string to be printed out
     */
    static char *PrintAntiCacheInfo(bool allPartition, uint32_t partitionId);

    static DSTORE::RetStatus GetBufDescPrintInfo(size_t *length, char **errInfo,
                                                       DSTORE::BufDescPrintInfo **bufferDescArr);

    static void FreeBufferDescArr(DSTORE::BufDescPrintInfo *bufferDescArr);

    static DSTORE::RetStatus GetPageDirectoryInfo(size_t *length, char **errInfo,
                                                  DSTORE::PageDirectoryInfo **pageDirectoryArr);

    static void FreePageDirectoryArr(DSTORE::PageDirectoryInfo *pageDirectoryArr);

    static void FreePageDirectoryContextErrInfo(char *errInfo);

    static DSTORE::RetStatus GetPDBucketInfo(size_t *length, char ***chashBucketInfo,
        uint32_t startBucket, uint32_t endBucket);

    static char *GetClusterBufferInfo(PdbId pdbId, FileId fileId, BlockNumber blockId);

    static DSTORE::RetStatus GetIOStatistics(DSTORE::IoInfo *ioInfo, long time);

    static DSTORE::RetStatus GetLruListInfo(DSTORE::LruInfo *lruInfo);

    static char *GetPdBucketLockInfo();
};
#pragma GCC visibility pop
}  // namespace DSTORE

#endif
