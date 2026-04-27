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

#include "diagnose/dstore_buf_mgr_diagnose.h"
#include "framework/dstore_instance.h"
#include "common/algorithm/dstore_string_info.h"
#include "buffer/dstore_buf_mgr.h"
#include "buffer/dstore_buf_perf_unit.h"

namespace DSTORE {

size_t BufMgrDiagnose::GetLruHotList(uint32 queueIdx, char** items)
{
    BufMgr* bufferPool = dynamic_cast<BufMgr*>(g_storageInstance->GetBufferMgr());
    BufLruList* bufLruList = bufferPool->GetBufLruList(queueIdx);
    LruHotList* hotList = bufLruList->GetHotList();

    Size itemSize = 0;
    hotList->LockList();
    ConcurrentDList::ForwardIterator iter = hotList->begin();
    while (iter != hotList->end()) {
        BufferDesc *bufferDesc = BufLruList::GetNode(*iter)->GetValue<BufferDesc>();
        bufferDesc->Pin();
        items[itemSize] = bufferPool->PrintLruInfo(bufferDesc);
        bufferDesc->Unpin();
        itemSize++;
        ++iter;
    }

    hotList->UnlockList();
    return itemSize;
}

size_t BufMgrDiagnose::GetLruList(uint32 queueIdx, char** items)
{
    BufMgr* bufferPool = dynamic_cast<BufMgr*>(g_storageInstance->GetBufferMgr());
    BufLruList* bufLruList = bufferPool->GetBufLruList(queueIdx);
    LruList* lruList = bufLruList->GetLruList();

    Size itemSize = 0;
    lruList->LockList();
    ConcurrentDList::ForwardIterator iter = lruList->begin();
    while (iter != lruList->end()) {
        BufferDesc *bufferDesc = BufLruList::GetNode(*iter)->GetValue<BufferDesc>();
        bufferDesc->Pin();
        items[itemSize] = bufferPool->PrintLruInfo(bufferDesc);
        bufferDesc->Unpin();
        itemSize++;
        ++iter;
    }

    lruList->UnlockList();
    return itemSize;
}

size_t BufMgrDiagnose::GetCandidateList(uint32 queueIdx, char** items)
{
    BufMgr* bufferPool = dynamic_cast<BufMgr*>(g_storageInstance->GetBufferMgr());
    BufLruList* bufLruList = bufferPool->GetBufLruList(queueIdx);
    LruCandidateList *candidateList = bufLruList->GetCandidateList();

    StringInfoData itemInfo;
    if (unlikely(!itemInfo.init())) {
        ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("cannot allocate memory for CandidateList dump info."));
        items[0] = nullptr;
        return 0;
    }
    itemInfo.append("candidate list size:%lu", candidateList->Length());
    items[0] = itemInfo.data;
    return 1u;
}

size_t BufMgrDiagnose::GetBufLruSummaryInfo(char** items)
{
    BufMgr* bufferPool = dynamic_cast<BufMgr*>(g_storageInstance->GetBufferMgr());
    Size lruPartiton = bufferPool->GetLruPartition();

    for (uint32 i = 0; i < lruPartiton; i++) {
        BufLruList* bufLruList = bufferPool->GetBufLruList(i);
        items[i] = bufLruList->DumpSummaryInfo();
    }

    return lruPartiton;
}

char *BufMgrDiagnose::PrintBufferDesc(PdbId pdbId, FileId fileId, BlockNumber blockId)
{
    BufMgr* bufferPool = dynamic_cast<BufMgr*>(g_storageInstance->GetBufferMgr());
    return bufferPool->PrintBufferDescByTag(pdbId, fileId, blockId);
}

char *BufMgrDiagnose::PrintBufferDesc(BufferDesc *selectBufferDesc)
{
    selectBufferDesc->Pin();
    char* info = selectBufferDesc->PrintBufferDesc();
    selectBufferDesc->Unpin();
    return info;
}

size_t BufMgrDiagnose::GetBufMgrSize()
{
    BufMgr* bufferPool = dynamic_cast<BufMgr*>(g_storageInstance->GetBufferMgr());
    return bufferPool->GetBufMgrSize();
}

size_t BufMgrDiagnose::GetLruPartition()
{
    BufMgr* bufferPool = dynamic_cast<BufMgr*>(g_storageInstance->GetBufferMgr());
    return bufferPool->GetLruPartition();
}

size_t BufMgrDiagnose::GetHotListSize()
{
    BufMgr* bufferPool = dynamic_cast<BufMgr*>(g_storageInstance->GetBufferMgr());
    if (bufferPool->GetLruPartition() == 0) {
        return 0;
    }
    return bufferPool->GetBufLruList(0)->GetHotList()->GetMaxSize();
}

char *BufMgrDiagnose::PrintBufMgrStatistics()
{
    return g_storageInstance->GetBufferMgr()->PrintBufferpoolStatistic();
}

char *BufMgrDiagnose::ResetBufMgrStatistics()
{
    return g_storageInstance->GetBufferMgr()->ResetBufferpoolStatistic();
}

char *BufMgrDiagnose::PrintMemChunkStatistic()
{
    BufMgr* bufferPool = dynamic_cast<BufMgr*>(g_storageInstance->GetBufferMgr());
    StringInfoData str;
    if (unlikely(!str.init())) {
        ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("cannot allocate memory for MemChunkStatistic dump info."));
        return nullptr;
    }
    bufferPool->PrintMemChunkStatistics(str);
    return str.data;
}

RetStatus BufMgrDiagnose::GetBufDescPrintInfo(size_t *length, char **errInfo, DSTORE::BufDescPrintInfo **bufferDescArr)
{
    return g_storageInstance->GetBufferMgr()->GetBufDescPrintInfo(length, errInfo, bufferDescArr);
}

void BufMgrDiagnose::FreeBufferDescArr(BufDescPrintInfo *bufferDescArr)
{
    DstorePfreeExt(bufferDescArr);
}

RetStatus BufMgrDiagnose::GetPageDirectoryInfo(size_t *length, char **errInfo, PageDirectoryInfo **pageDirectoryArr)
{
    return g_storageInstance->GetBufferMgr()->GetPageDirectoryInfo(length, errInfo, pageDirectoryArr);
}

RetStatus BufMgrDiagnose::GetPDBucketInfo(size_t *length, char ***chashBucketInfo,
    uint32_t startBucket, uint32_t endBucket)
{
    return g_storageInstance->GetBufferMgr()->GetPDBucketInfo(length, chashBucketInfo, startBucket, endBucket);
}

void BufMgrDiagnose::FreePageDirectoryArr(PageDirectoryInfo *pageDirectoryArr)
{
    ErrLog(DSTORE_LOG, MODULE_BUFMGR, ErrMsg("Free memory for PageDirectoryInfo."));
    DstorePfreeExt(pageDirectoryArr);
}

void BufMgrDiagnose::FreePageDirectoryContextErrInfo(char *errInfo)
{
    ErrLog(DSTORE_LOG, MODULE_BUFMGR, ErrMsg("Free memory for PageDirectoryContext errInfo."));
    DstorePfreeExt(errInfo);
}

char *BufMgrDiagnose::PrintPdRecoveryInfo()
{
    BufMgr* bufferPool = dynamic_cast<BufMgr*>(g_storageInstance->GetBufferMgr());
    return bufferPool->PrintPdRecoveryInfo();
}

char *BufMgrDiagnose::PrintAntiCacheInfo(bool allPartition, uint32_t partitionId)
{
    return g_storageInstance->GetBufferMgr()->PrintAntiCacheInfo(allPartition, partitionId);
}

char *BufMgrDiagnose::GetClusterBufferInfo(PdbId pdbId, FileId fileId, BlockNumber blockId)
{
    return g_storageInstance->GetBufferMgr()->GetClusterBufferInfo(pdbId, fileId, blockId);
}

RetStatus BufMgrDiagnose::GetIOStatistics(DSTORE::IoInfo *ioInfo, long time)
{
    BufMgr* bufferPool = dynamic_cast<BufMgr*>(g_storageInstance->GetBufferMgr());
    std::unique_lock<std::mutex> lock(bufferPool->m_mutex);
    bufferPool->m_isCount.store(true);

    bufferPool->m_readCount.store(0);
    bufferPool->m_writeCount.store(0);
    GaussUsleep(time * 1000000L);

    ioInfo->readCount = bufferPool->m_readCount;
    ioInfo->avgReadLatency = BufPerfUnit::GetInstance().m_readStat.GetAverage() / NANOSECONDS_ONE_MICROSECONDS;
    ioInfo->maxReadLatency = BufPerfUnit::GetInstance().m_readStat.GetMax() / NANOSECONDS_ONE_MICROSECONDS;
    ioInfo->minReadLatency = ioInfo->maxReadLatency == 0 ?
            0 : BufPerfUnit::GetInstance().m_readStat.GetMin() / NANOSECONDS_ONE_MICROSECONDS;
    ioInfo->writeCount = bufferPool->m_writeCount;
    ioInfo->avgWriteLatency = BufPerfUnit::GetInstance().m_writeStat.GetAverage() / NANOSECONDS_ONE_MICROSECONDS;
    ioInfo->maxWriteLatency = BufPerfUnit::GetInstance().m_writeStat.GetMax() / NANOSECONDS_ONE_MICROSECONDS;
    ioInfo->minWriteLatency = ioInfo->maxWriteLatency == 0 ?
            0 : BufPerfUnit::GetInstance().m_writeStat.GetMin() / NANOSECONDS_ONE_MICROSECONDS;

    BufPerfUnit::GetInstance().m_readStat.Reset();
    BufPerfUnit::GetInstance().m_writeStat.Reset();
    bufferPool->m_isCount.store(false);

    return DSTORE_SUCC;
}

RetStatus BufMgrDiagnose::GetLruListInfo(DSTORE::LruInfo *lruInfo)
{
    BufMgr* bufferPool = dynamic_cast<BufMgr*>(g_storageInstance->GetBufferMgr());
    bufferPool->GetLruSummary(lruInfo);

    return DSTORE_SUCC;
}

char *BufMgrDiagnose::GetPdBucketLockInfo()
{
    return g_storageInstance->GetBufferMgr()->GetPdBucketLockInfo();
}

}
