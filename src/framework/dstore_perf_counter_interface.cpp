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
 * dstore_perf_counter_interface.cpp
 *
 * IDENTIFICATION
 *        src/framework/dstore_perf_counter_interface.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "common/memory/dstore_mctx.h"
#include "framework/dstore_external_perf_unit.h"
#include "framework/dstore_perf_counter_interface.h"

namespace PerfCounterInterface {
using namespace DSTORE;

PerfItem *CreatePerfItems(const char *groupName, uint8_t num)
{
    if (unlikely(groupName == nullptr || num == 0)) {
        return nullptr;
    }
    PerfItem *perfItems =
        reinterpret_cast<PerfItem *>(ExternalPerfUnit::GetInstance().CreateGroupPerfItems(groupName, num));
    return perfItems;
}

Timer Start(PerfItem item)
{
    if (unlikely(!item)) {
        return nullptr;
    }
    LatencyStat::Timer *dstoreTimer = static_cast<LatencyStat::Timer *>(
        DstoreMemoryContextAllocZero(g_dstoreCurrentMemoryContext, sizeof(LatencyStat::Timer)));
    if (unlikely(!dstoreTimer)) {
        ErrLog(DSTORE_PANIC, MODULE_FRAMEWORK, ErrMsg("alloc memory for timer failed"));
    }
    if (STORAGE_VAR_NULL(dstoreTimer)) {
        ErrLog(DSTORE_PANIC, MODULE_FRAMEWORK, ErrMsg("DstoreTimer is nullptr."));
    }
    ::new (dstoreTimer) LatencyStat::Timer(static_cast<LatencyStat *>(item), false);
    dstoreTimer->Start();
    return dstoreTimer;
}

void End(Timer timer)
{
    if (unlikely(!timer)) {
        return;
    }
    LatencyStat::Timer *dstoreTimer = static_cast<LatencyStat::Timer *>(timer);
    dstoreTimer->End();
    DstorePfree(dstoreTimer);
}
}  // namespace PerfCounterInterface
