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

#ifndef DSTORE_RECOVERY_PERF_UNIT_H
#define DSTORE_RECOVERY_PERF_UNIT_H

#include "perfcounter/dstore_perf_catalog.h"

#define PERF_COUNTER_RECOVERY_PERF_UNIT(_def) \
    _def(DSTORE::LatencyStat, m_redoWalForSinglePage, redoWalForSinglePage, DSTORE::PerfLevel::RELEASE); \
    _def(DSTORE::LatencyStat, m_collectWalRecords, collectWalRecords, DSTORE::PerfLevel::RELEASE); \
    _def(DSTORE::LatencyStat, m_collectWalRecordsLocally, collectWalRecordsLocally, DSTORE::PerfLevel::RELEASE); \
    _def(DSTORE::LatencyStat, m_redoQueueEnqueue, redoQueueEnqueue, DSTORE::PerfLevel::RELEASE); \
    _def(DSTORE::LatencyStat, m_walStreamRedoQueueEnqueue, walStreamRedoQueueEnqueue, DSTORE::PerfLevel::RELEASE); \
    _def(DSTORE::LatencyStat, m_redoAndDequeue, redoAndDequeue, DSTORE::PerfLevel::RELEASE); \
    _def(DSTORE::LatencyStat, m_walStreamRedoQueueDequeue, walStreamRedoQueueDequeue, DSTORE::PerfLevel::RELEASE); \
    _def(DSTORE::LatencyStat, m_collectWalRecordsRemotely, collectWalRecordsRemotely, DSTORE::PerfLevel::RELEASE)

namespace DSTORE {
/********************************
 * Module Specific Interfaces
 ********************************/
enum RecoveryRpcMessageType : uint16 {
    RPC_RECOVERY_REQUEST_WAL_RECORDS = 0,
    RPC_RECOVERY_REQUEST_COLLECTED_WAL_RECORDS,
    RPC_RECOVERY_REQUEST_WAL_CANCEL,

    RPC_RECOVERY_RESPONSE_LOCAL, /* for local error process */
    RPC_RECOVERY_RESPONSE_WAL_RECORDS,
    RPC_RECOVERY_RESPONSE_COLLECT_WAL_RECORDS,

    RPC_BACKUP_REQUEST_OPERAT_PDB,
    RPC_BACKUP_REQUEST_GET_WAL_INFO,
    RPC_BACKUP_RESPONSE_OPERAT_PDB,
    RPC_BACKUP_RESPONSE_GET_WAL_INFO,

    RPC_CHECKPOINT_REQUEST,
    RPC_CHECKPOINT_RESPONSE,
    RPC_CHECKPOINT_STAT_REQUEST,
    RPC_CHECKPOINT_STAT_RESPONSE,

    RPC_RECOVERY_MSG_MAX
};
class RecoveryPerfUnit : public DstorePerfUnit {
public:
    static RecoveryPerfUnit &GetInstance()
    {
        static RecoveryPerfUnit perfUnit{};

        return perfUnit;
    };

    ~RecoveryPerfUnit() override = default;

    bool Init(DstoreMemoryContext memCtxPtr)
    {
        if (!InitUnit("RecoveryPerfUnit", memCtxPtr, PERF_UNIT_DEFAULT_TASK_INDEX)) {
            return false;
        }

        PERF_COUNTER_RECOVERY_PERF_UNIT(REGIST_PERFITEM);
        return true;
    }

    PERF_COUNTER_RECOVERY_PERF_UNIT(DECLARE_PERF_ITEM);

private:
    RecoveryPerfUnit() = default;
};
}  // namespace DSTORE
#endif /* STORAGE_RECOVERY_PERF_UNIT_H */
