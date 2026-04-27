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

#ifndef DSTORE_TRANSACTION_PERF_UNIT_H
#define DSTORE_TRANSACTION_PERF_UNIT_H

#include "perfcounter/dstore_perf_catalog.h"

#define PERF_COUNTER_TRANSACTION_PERF_UNIT(_def)                                                   \
    _def(DSTORE::LatencyStat, m_trxCompleteLatency, trxCompleteLatency, DSTORE::PerfLevel::RELEASE); \
    _def(DSTORE::LatencyStat, m_csnBroadcastLatency, csnBroadcastLatency, DSTORE::PerfLevel::PERF_DEBUG); \
    _def(DSTORE::LatencyStat, m_pendingCommitLatency, pendingCommitLatency, DSTORE::PerfLevel::RELEASE); \
    _def(DSTORE::LatencyStat, m_committingTrxLatency, committingTrxLatency, DSTORE::PerfLevel::RELEASE); \
    _def(DSTORE::LatencyStat, m_allocTrxSlotLatency, allocTrxSlotLatency, DSTORE::PerfLevel::RELEASE); \
    _def(DSTORE::LatencyStat, m_batchFaaLatency, batchFaaLatency, DSTORE::PerfLevel::RELEASE)
namespace DSTORE {
class TransactionPerfUnit : public DstorePerfUnit {
public:
    static TransactionPerfUnit &GetInstance()
    {
        static TransactionPerfUnit perfUnit{};

        return perfUnit;
    };

    ~TransactionPerfUnit() override = default;

    bool Init(DstoreMemoryContext memCtxPtr)
    {
        if (!InitUnit("TransactionPerfUnit", memCtxPtr, PERF_UNIT_DEFAULT_TASK_INDEX)) {
            return false;
        }

        PERF_COUNTER_TRANSACTION_PERF_UNIT(REGIST_PERFITEM);
        return true;
    }

    PERF_COUNTER_TRANSACTION_PERF_UNIT(DECLARE_PERF_ITEM);

private:
    TransactionPerfUnit() = default;
};
}  // namespace DSTORE
#endif /* STORAGE_TRANSACTION_PERF_UNIT_H */
