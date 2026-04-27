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
 * dstore_wal_utils.cpp
 *
 * IDENTIFICATION
 * src/wal/dstore_wal_utils.cpp
 *
 * ---------------------------------------------------------------------------------------
 *
 */

#include <csignal>
#include "common/concurrent/dstore_atomic.h"
#include "framework/dstore_thread_autobinder_interface.h"
#include "framework/dstore_instance.h"
#include "wal/dstore_wal_redo_manager.h"
#include "wal/dstore_wal_struct.h"
#include "wal/dstore_wal_utils.h"

namespace DSTORE {
#define WAL_BIND_CPU_MAX_TRY_TIMES 30
static const int MAX_NAME_LEN = 16;
template <bool isEnd>
uint64 WalUtils::GetRecordPlsn(uint64 groupStartPlsn, uint64 recordOffset, uint64 walFileSize)
{
    if (unlikely(walFileSize <= WAL_FILE_HDR_SIZE)) {
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("Invalid param walFileSize."));
    }
    uint64 bytePos = WalPlsnToBytePos(walFileSize, groupStartPlsn) + recordOffset;
    return WalBytePosToPlsn<isEnd>(walFileSize, bytePos);
}

uint64 WalUtils::GetWalGroupStartPlsn(uint64 groupEndPlsn, uint64 recordLen, uint64 walFileSize)
{
    if (unlikely(walFileSize <= WAL_FILE_HDR_SIZE)) {
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("Invalid param walFileSize."));
    }
    uint64 bytePos = WalPlsnToBytePos(walFileSize, groupEndPlsn) - recordLen - sizeof(WalRecordAtomicGroup);
    return WalUtils::WalBytePosToPlsn<false>(walFileSize, bytePos);
}
/*
 * Calculate the corresponding plsn for the specified bytepos.
 * BytePos means sequence of wal data bytes.
 * plsn means sequence of wal data bytes and file header size.
 */
template <bool isEnd>
uint64 WalUtils::WalBytePosToPlsn(uint64 walFileSize, uint64 bytepos)
{
    uint32 offset = (isEnd && (bytepos % WalUtils::GetWalFileUsableBytes(walFileSize, WAL_FILE_HDR_SIZE) == 0))
                        ? 0
                        : WAL_FILE_HDR_SIZE;
    return bytepos / WalUtils::GetWalFileUsableBytes(walFileSize, WAL_FILE_HDR_SIZE) * walFileSize +
           bytepos % WalUtils::GetWalFileUsableBytes(walFileSize, WAL_FILE_HDR_SIZE) + offset;
}

/*
 * Calculate the corresponding bytepos for the specified plsn.
 * BytePos means sequence of wal data bytes.
 * plsn means sequence of wal data bytes and file header size.
 */
uint64 WalUtils::WalPlsnToBytePos(uint64 walFileSize, uint64 plsn)
{
    /* Step1: Calculate bytepos of the beginning of wal file corresponding to specified plsn */
    uint64 bytepos = plsn / walFileSize * WalUtils::GetWalFileUsableBytes(walFileSize, WAL_FILE_HDR_SIZE);

    /*
     * Step2: Calculate offset of specified plsn within the wal file, offset includes
     * WAL_FILE_HDR_SIZE bytes of wal file header.
     */
    uint64 offset = plsn % walFileSize;
    if (offset < WAL_FILE_HDR_SIZE) {
        /*
         * Step3: if plsn points to bytes inside the wal file header, return bytepos of
         * the beginning of wal file corresponding to specified plsn
         */
        return bytepos;
    }
    /* Step4: increase the offset but skip bytes of wal file's header. */
    return bytepos + offset - WAL_FILE_HDR_SIZE;
}

bool WalUtils::TryAtomicSetBiggerU64(volatile uint64 *target, uint64 newVal)
{
    TsAnnotateBenignRaceSized(target, sizeof(uint64));
    uint64 nowVal = GsAtomicReadU64(target);
    if (newVal <= nowVal) {
        return false;
    }
    do {
        if (GsAtomicCompareExchangeU64(target, &nowVal, newVal)) {
            return true;
        }
    } while (newVal > nowVal);
    return false;
}

bool WalUtils::IsOvertimed(const timespec &start, const timespec &end, const timespec &target)
{
    if ((end.tv_sec < start.tv_sec) || (end.tv_sec == start.tv_sec && end.tv_nsec < start.tv_nsec)) {
        ErrLog(DSTORE_WARNING, MODULE_WAL, ErrMsg("start time is after end time."));
        return true;
    }
    if ((end.tv_sec - start.tv_sec) > target.tv_sec) {
        return true;
    }
    if ((end.tv_sec - start.tv_sec) == target.tv_sec && (end.tv_nsec - start.tv_nsec) > target.tv_nsec) {
        return true;
    }
    return false;
}

uint64 WalUtils::TimeDiffInMicroseconds(const timespec& start, const timespec& end)
{
    if ((end.tv_sec < start.tv_sec) || (end.tv_sec == start.tv_sec && end.tv_nsec < start.tv_nsec)) {
        ErrLog(DSTORE_WARNING, MODULE_WAL, ErrMsg("start time is after end time."));
        return 0;
    }

    time_t secDiff = 0;
    uint64 nsecDiff = 0;

    if (end.tv_nsec < start.tv_nsec) {
        secDiff = end.tv_sec - start.tv_sec - 1;
        nsecDiff = 1000000000 + end.tv_nsec - start.tv_nsec;
    } else {
        secDiff = end.tv_sec - start.tv_sec;
        nsecDiff = end.tv_nsec - start.tv_nsec;
    }

    return secDiff * 1000000 + nsecDiff / 1000;
}

void WalUtils::SignalBlock()
{
    sigset_t intMask;
    storage_securec_check(sigfillset(&intMask), "\0", "\0");
    storage_securec_check(sigdelset(&intMask, SIGPROF), "\0", "\0");
    storage_securec_check(sigdelset(&intMask, SIGSEGV), "\0", "\0");
    storage_securec_check(sigdelset(&intMask, SIGBUS), "\0", "\0");
    storage_securec_check(sigdelset(&intMask, SIGFPE), "\0", "\0");
    storage_securec_check(sigdelset(&intMask, SIGILL), "\0", "\0");
    storage_securec_check(sigdelset(&intMask, SIGSYS), "\0", "\0");

    storage_securec_check(pthread_sigmask(SIG_SETMASK, &intMask, NULL), "\0", "\0");
}

RetStatus WalUtils::SetThreadAffinity(size_t targetCpu, const char* threadName)
{
    cpu_set_t cpuSet;
    CPU_ZERO(&cpuSet);
    CPU_SET(targetCpu, &cpuSet);
    int rc = sched_setaffinity(0, sizeof(cpu_set_t), &cpuSet);
    if (rc == -1) {
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("%s bind cpu %zu failed, ErrorCode:%d.", threadName, targetCpu, rc));
        return DSTORE_FAIL;
    }
    CPU_ZERO(&cpuSet);
    rc = pthread_getaffinity_np(pthread_self(), sizeof(cpuSet), &cpuSet);
    if (rc != 0) {
        ErrLog(DSTORE_WARNING, MODULE_WAL, ErrMsg("%s get affinity failed, ErrorCode:%d.", threadName, rc));
        return DSTORE_FAIL;
    }
    if (!CPU_ISSET(targetCpu, &cpuSet)) {
        ErrLog(DSTORE_WARNING, MODULE_WAL, ErrMsg("%s bind cpu:%lu failed.", threadName, targetCpu));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

void WalUtils::HandleWalThreadCpuBind(const char* threadName)
{
    char threadGroupName[] = "WalGroup";
    if (STORAGE_FUNC_FAIL(
        RegisterThreadToBind(pthread_self(), BindType::NUMA_CPU_BIND, CoreBindLevel::HIGH, threadGroupName))) {
        ErrLog(DSTORE_WARNING, MODULE_WAL, ErrMsg("%s RegisterThreadToBind fail", threadName));
    }
    uint32 targetCpu;
    int retryTimes = 0;
    while (retryTimes < WAL_BIND_CPU_MAX_TRY_TIMES) {
        if (WalRedoManager::GetNextAwailableCpu(&targetCpu) == DSTORE_SUCC) {
            /* if bind target cpu failed, change this thread to auto bind */
            if (WalUtils::SetThreadAffinity(targetCpu, threadName) == DSTORE_FAIL) {
                ErrLog(DSTORE_WARNING, MODULE_WAL, ErrMsg("%s SetThreadAffinity with cpu:%u fail, try next",
                    threadName, targetCpu));
                continue;
            } else {
                ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("%s SetThreadAffinity with cpu:%u success",
                    threadName, targetCpu));
                break;
            }
        }
        retryTimes++;
    }
}

void WalUtils::HandleWalThreadCpuUnbind(const char* threadName)
{
    char threadGroupName[] = "WalGroup";
    if (STORAGE_FUNC_FAIL(UnRegisterThreadToBind(pthread_self(), BindType::NUMA_CPU_BIND, threadGroupName))) {
        ErrLog(DSTORE_WARNING, MODULE_WAL, ErrMsg("%s UnRegisterThreadToBind fail", threadName));
    }
}

uint64 WalUtils::GetFileVersion(const PdbId pdbId, const FileId fileId)
{
    StoragePdb *pdb = g_storageInstance->GetPdb(pdbId);
    if (STORAGE_VAR_NULL(pdb)) {
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("storagePdb is nullptr."));
    }
    TablespaceMgr *tablespaceMgr = pdb->GetTablespaceMgr();
    if (STORAGE_VAR_NULL(tablespaceMgr)) {
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("tablespaceMgr is nullptr."));
    }
    uint64 version = INVALID_FILE_VERSION;
    if (tablespaceMgr->GetFileVersion(fileId, &version) != DSTORE_SUCC) {
        ErrLog(DSTORE_ERROR, MODULE_WAL,
               ErrMsg("Get file version from tbs failed, try to get info from control file."));
        ControlFile *controlFile = pdb->GetControlFile();
        if (STORAGE_VAR_NULL(controlFile)) {
            ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("controlFile is nullptr."));
        }
        ControlDataFilePageItemData datafileItem;
        if (STORAGE_FUNC_FAIL(controlFile->GetDataFilePageItemData(fileId, &datafileItem))) {
            ErrLog(DSTORE_PANIC, MODULE_WAL,
                   ErrMsg("Get datafile page item failed, pdbId %u, fileId %hu.", pdbId, fileId));
        }
        version = datafileItem.version;
    }
    return version;
}

uint64 WalUtils::GetTbsVersion(const PdbId pdbId, const TablespaceId tablespaceId)
{
    StoragePdb *pdb = g_storageInstance->GetPdb(pdbId);
    if (STORAGE_VAR_NULL(pdb)) {
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE, ErrMsg("storagePdb is nullptr."));
    }
    ControlFile *controlFile = pdb->GetControlFile();
    if (STORAGE_VAR_NULL(controlFile)) {
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE, ErrMsg("controlFile is nullptr."));
    }
    ControlTablespacePageItemData tbsItem;
    if (STORAGE_FUNC_FAIL(controlFile->GetTbsPageItemData(tablespaceId, &tbsItem))) {
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("Get tbs version from control file failed."));
    }
    return tbsItem.reuseVersion;
}

template uint64 WalUtils::GetRecordPlsn<true>(uint64, uint64, uint64);
template uint64 WalUtils::GetRecordPlsn<false>(uint64, uint64, uint64);

template uint64 WalUtils::WalBytePosToPlsn<true>(uint64, uint64);
template uint64 WalUtils::WalBytePosToPlsn<false>(uint64, uint64);
}  // namespace DSTORE
