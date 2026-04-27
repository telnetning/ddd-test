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
 * Description: The performance monitoring unit of UpdateFile.
 */

#ifndef DSTORE_BUF_PERF_UNIT_H
#define DSTORE_BUF_PERF_UNIT_H

#include "perfcounter/dstore_perf_catalog.h"

#define PERF_COUNTER_BUF_PERF_UNIT(_def)                                                                       \
    _def(DSTORE::LatencyStat, m_pdRecovery, pdRecovery, DSTORE::PerfLevel::RELEASE);                           \
    _def(DSTORE::LatencyStat, m_collectLostBuckets, collectLostBuckets, DSTORE::PerfLevel::RELEASE);           \
    _def(DSTORE::LatencyStat, m_claimRipBuckets, claimRipBuckets, DSTORE::PerfLevel::RELEASE);                 \
    _def(DSTORE::LatencyStat, m_bufferpoolScan, bufferpoolScan, DSTORE::PerfLevel::RELEASE);                   \
    _def(DSTORE::LatencyStat, m_batchInsert, batchInsert, DSTORE::PerfLevel::RELEASE);                         \
    _def(DSTORE::LatencyStat, m_setBucketValid, setBucketValid, DSTORE::PerfLevel::PERF_DEBUG);                \
    _def(DSTORE::LatencyStat, m_bufferPoolRelocation, bufferPoolRelocation, DSTORE::PerfLevel::PERF_DEBUG);    \
    _def(DSTORE::LatencyStat, m_getInvalidBuckets, getInvalidBuckets, DSTORE::PerfLevel::RELEASE);             \
    _def(DSTORE::LatencyStat, m_resourceCleanup, resourceCleanup, DSTORE::PerfLevel::PERF_DEBUG);              \
    _def(DSTORE::LatencyStat, m_readFromRemote, readFromRemote, DSTORE::PerfLevel::RELEASE);                   \
    _def(DSTORE::LatencyStat, m_read, read, DSTORE::PerfLevel::PERF_DEBUG);                                    \
    _def(DSTORE::LatencyStat, m_lookupBuffer, lookupBuffer, DSTORE::PerfLevel::PERF_DEBUG);                    \
    _def(DSTORE::LatencyStat, m_getLatestPageVer, getLatestPageVer, DSTORE::PerfLevel::PERF_DEBUG);            \
    _def(DSTORE::LatencyStat, m_recoveryRead, recoveryRead, DSTORE::PerfLevel::PERF_DEBUG);                    \
    _def(DSTORE::LatencyStat, m_loadFromDisk, loadFromDisk, DSTORE::PerfLevel::RELEASE);                       \
    _def(DSTORE::LatencyStat, m_loadBlock, loadBlock, DSTORE::PerfLevel::RELEASE);                             \
    _def(DSTORE::LatencyStat, m_tryFlush, tryFlush, DSTORE::PerfLevel::RELEASE);                               \
    _def(DSTORE::LatencyStat, m_requestAtPo, requestAtPo, DSTORE::PerfLevel::RELEASE);                         \
    _def(DSTORE::LatencyStat, m_acquirePdEntry, acquirePdEntry, DSTORE::PerfLevel::RELEASE);                   \
    _def(DSTORE::LatencyStat, m_revokeReadAuth, revokeReadAuth, DSTORE::PerfLevel::RELEASE);                   \
    _def(DSTORE::LatencyStat, m_aioFsync, aioFsync, DSTORE::PerfLevel::RELEASE);                               \
    _def(DSTORE::LatencyStat, m_readStat, readStat, DSTORE::PerfLevel::RELEASE);                \
    _def(DSTORE::LatencyStat, m_writeStat, writeStat, DSTORE::PerfLevel::RELEASE);                \
    _def(DSTORE::LatencyStat, m_validatePageOwner, validatePageOwner, DSTORE::PerfLevel::RELEASE);             \
    _def(DSTORE::LatencyStat, m_handleResponseAtPdOwner, handleResponseAtPdOwner, DSTORE::PerfLevel::RELEASE); \
    _def(DSTORE::LatencyStat, m_readCrFromRemote, readCrFromRemote, DSTORE::PerfLevel::RELEASE);               \
    _def(DSTORE::LatencyStat, m_asyncBatchEvictPd, asyncBatchEvictPd, DSTORE::PerfLevel::RELEASE);             \
    _def(DSTORE::LatencyStat, m_batchEcictPd, batchEcictPd, DSTORE::PerfLevel::RELEASE);                       \
    _def(DSTORE::LatencyStat, m_removPdEntry, removPdEntry, DSTORE::PerfLevel::RELEASE);                       \
    _def(DSTORE::LatencyStat, m_removPdEntryBroadcast, removPdEntryBroadcast, DSTORE::PerfLevel::RELEASE);     \
    _def(DSTORE::LatencyStat, m_tryAbandonAllPoShip, tryAbandonAllPoShip, DSTORE::PerfLevel::RELEASE);         \
    _def(DSTORE::LatencyStat, m_scanHotAndTransfer, scanHotAndTransfer, DSTORE::PerfLevel::RELEASE);           \
    _def(DSTORE::LatencyStat, m_broadcastAbandonMsg, broadcastAbandonMsg, DSTORE::PerfLevel::RELEASE);         \
    _def(DSTORE::LatencyStat, m_scanHotPageThread, scanHotPageThread, DSTORE::PerfLevel::RELEASE);             \
    _def(DSTORE::LatencyStat, m_backendTryFlush, backendTryFlush, DSTORE::PerfLevel::RELEASE)

namespace DSTORE {
class BufPerfUnit : public DstorePerfUnit {
public:
    static BufPerfUnit &GetInstance()
    {
        static BufPerfUnit perfUnit{};

        return perfUnit;
    };

    ~BufPerfUnit() override = default;

    bool Init(DstoreMemoryContext memCtxPtr)
    {
        if (!InitUnit("BufPerfUnit", memCtxPtr, PERF_UNIT_DEFAULT_TASK_INDEX)) {
            return false;
        }

        PERF_COUNTER_BUF_PERF_UNIT(REGIST_PERFITEM);
        return true;
    }

    PERF_COUNTER_BUF_PERF_UNIT(DECLARE_PERF_ITEM);

private:
    BufPerfUnit() = default;
};
}  // namespace DSTORE
#endif /* STORAGE_BUF_PERF_UNIT_H */
