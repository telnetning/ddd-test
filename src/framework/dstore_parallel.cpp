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
 * dstore_parallel.cpp
 *    ParallelWorkControlBlock provides all functions to access work in parallel workload.
 *
 * IDENTIFICATION
 *        storage/src/framework/dstore_parallel.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "framework/dstore_parallel.h"
#include "heap/dstore_heap_scan_parallel.h"
#include "framework/dstore_instance.h"

namespace DSTORE {

ParallelWorkController::ParallelWorkController(int smpNum)
    : m_mainThread(thrd), m_workSource(nullptr), m_smpNum(smpNum), m_seqscanHistory(nullptr)
{
}

ParallelWorkController::~ParallelWorkController()
{
    ResetWorkSource();
    m_mainThread = nullptr;
    ClearHistory();
}

void ParallelWorkController::ResetWorkSource()
{
    if (Done()) {
        std::lock_guard<std::mutex> lk(m_mutex);
        if (m_workSource) {
            delete m_workSource;
            m_workSource = nullptr;
        }
    }
}

RetStatus ParallelWorkController::InitWorkload()
{
    ParallelWorkloadInfoInterface *workload = m_workSource->GetWork();
    uint64 workloadIdx = 0;
    while (workload != nullptr) {
        if (STORAGE_FUNC_FAIL(
            m_seqscanHistory[workloadIdx % m_smpNum].AddWorkload(dynamic_cast<ParallelHeapScanWorkloadInfo*>(workload)))) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("failed to AddWorkload."));
            return DSTORE_FAIL;
        }
        workloadIdx++;
        delete workload;
        workload = m_workSource->GetWork();
    }
    return DSTORE_SUCC;
}

RetStatus ParallelWorkController::InitParallelHeapScan(PdbId pdbId, PageId segmentMetaPageId)
{
    std::lock_guard<std::mutex> lk(m_mutex);
    if (m_workSource == nullptr) {
        m_workSource = DstoreNew(g_storageInstance->GetMemoryMgr()->
                                 GetGroupContext(MemoryGroupType::MEMORY_CONTEXT_QUERY))
                        ParallelHeapScanWorkSource(pdbId, segmentMetaPageId, g_storageInstance->GetBufferMgr());
        if (unlikely(m_workSource == nullptr)) {
            storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
            ErrLog(DSTORE_LOG, MODULE_HEAP,
                ErrMsg("InitParallelHeapScan fail, m_workSource is nullptr, segmentMetaPageId(%hu, %u).",
                segmentMetaPageId.m_fileId, segmentMetaPageId.m_blockId));
            return DSTORE_FAIL;
        }
        if (unlikely(STORAGE_FUNC_FAIL(m_workSource->Init()))) {
            storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
            ErrLog(DSTORE_LOG, MODULE_HEAP,
                ErrMsg("InitParallelHeapScan fail, m_workSource init fail, segmentMetaPageId(%hu, %u).",
                segmentMetaPageId.m_fileId, segmentMetaPageId.m_blockId));
            delete m_workSource;
            m_workSource = nullptr;
            return DSTORE_FAIL;
        }
    }
    if (m_seqscanHistory == nullptr) {
        m_seqscanHistory = DstoreNew(g_storageInstance->GetMemoryMgr()->
                                     GetGroupContext(MemoryGroupType::MEMORY_CONTEXT_QUERY))
                                     ParallelSeqscanHistory[m_smpNum];
        if (m_seqscanHistory == nullptr) {
            storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
            ErrLog(DSTORE_LOG, MODULE_HEAP, ErrMsg("Acquire SeqscanHistory memory fail."));
            return DSTORE_FAIL;
        }
        if (unlikely(STORAGE_FUNC_FAIL(InitWorkload()))) {
            delete m_seqscanHistory;
            m_seqscanHistory = nullptr;
            delete m_workSource;
            m_workSource = nullptr;
            return DSTORE_FAIL;
        }
    }
    m_txid = thrd->GetCurrentXid();
    return DSTORE_SUCC;
}

bool ParallelWorkController::IsInitialized()
{
    std::lock_guard<std::mutex> lk(m_mutex);
    bool res = m_workSource != nullptr;
    return res;
}

bool ParallelWorkController::Done()
{
    if (!IsInitialized()) {
        return true;
    }
    std::lock_guard<std::mutex> lk(m_mutex);
    for (int smpId = 0; smpId < m_smpNum; smpId++) {
        if (m_seqscanHistory && !m_seqscanHistory[smpId].Done()) {
            return false;
        }
    }

    return true;
}

ParallelWorkloadInfoInterface *ParallelWorkController::GetWork(int smpId)
{
    ParallelWorkloadInfoInterface *workload = nullptr;
    std::lock_guard<std::mutex> lk(m_mutex);
        if (m_seqscanHistory != nullptr) {
            workload = m_seqscanHistory[smpId].GetWork();
        } else {
            ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
                ErrMsg("streamId: %d, m_seqscanHistory is invalid", smpId));
        }

    return workload;
}

ParallelWorkloadInfoInterface *ParallelSeqscanHistory::GetWork()
{
    if (m_arraySize > 0 && m_scannedIdx < m_hisNum) {
        ParallelWorkloadInfoInterface *ret = &(m_workloadArray[m_scannedIdx]);
        ++m_scannedIdx;
        return ret;
    } else {
        return nullptr;
    }
}

bool ParallelSeqscanHistory::Done()
{
    return !(m_arraySize > 0 && m_scannedIdx < m_hisNum);
}

RetStatus ParallelSeqscanHistory::AddWorkload(ParallelHeapScanWorkloadInfo *workload)
{
    if (unlikely(++m_hisNum > m_arraySize)) {
        ParallelHeapScanWorkloadInfo *newArray = DstoreNew(g_storageInstance->GetMemoryMgr()->
                GetGroupContext(MemoryGroupType::MEMORY_CONTEXT_QUERY))
                ParallelHeapScanWorkloadInfo[m_arraySize + INIT_EXT_ARRAY_SIZE];
        if (newArray == nullptr) {
            storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("failed to create parallel heap scan array."));
            return DSTORE_FAIL;
        }
        if (m_arraySize > 0) {
            errno_t rc = memcpy_s(newArray, sizeof(ParallelHeapScanWorkloadInfo) * m_arraySize,
                        m_workloadArray, sizeof(ParallelHeapScanWorkloadInfo) * m_arraySize);
            storage_securec_check(rc, "\0", "\0");
        }
        delete[] m_workloadArray;
        m_workloadArray = newArray;
        m_arraySize += INIT_EXT_ARRAY_SIZE;
    }

    errno_t rc = memcpy_s(&m_workloadArray[m_hisNum - 1], sizeof(ParallelHeapScanWorkloadInfo),
                          workload, sizeof(ParallelHeapScanWorkloadInfo));
    storage_securec_check(rc, "\0", "\0");
    return DSTORE_SUCC;
}

void ParallelSeqscanHistory::Reset()
{
    m_scannedIdx = 0;
}

ParallelSeqscanHistory::~ParallelSeqscanHistory()
{
    delete[] m_workloadArray;
}

}