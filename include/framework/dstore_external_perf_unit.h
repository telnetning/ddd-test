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
#ifndef DSTORE_EXTERNAL_PERF_UNIT_H
#define DSTORE_EXTERNAL_PERF_UNIT_H

#include "perfcounter/dstore_perf_unit.h"
#include "common/algorithm/dstore_hsearch.h"

namespace DSTORE {

constexpr long PERF_COUNTER_MAX_GROUP_ELEM = 64;

struct PerfGroupEntry {
    char groupName[PERF_MAX_NAME_LEN];
    uint8 perfItemNum;
    LatencyStat **perfItems;
};

class ExternalPerfUnit : public DstorePerfUnit {
public:
    static ExternalPerfUnit &GetInstance()
    {
        static ExternalPerfUnit perfUnit{};

        return perfUnit;
    };

    bool Destroy()
    {
        if (!DestroyUnit()) {
            return false;
        }
        HASH_SEQ_STATUS hashSeq;
        hash_seq_init(&hashSeq, m_perfGroupHash);
        PerfGroupEntry *perfGroupEntry;
        while ((perfGroupEntry = (PerfGroupEntry *)hash_seq_search(&hashSeq)) != nullptr) {
            for (uint8 i = 0; i < perfGroupEntry->perfItemNum; i++) {
                UnRegisterPerfItem(perfGroupEntry->perfItems[i], PerfLevel::PERF_DEBUG);
            }
            DstorePfreeExt(perfGroupEntry->perfItems);
        }
        hash_destroy(m_perfGroupHash);
        m_perfGroupHash = nullptr;
        m_isInit = false;
        return true;
    }

    bool Init(DstoreMemoryContext memCtxPtr)
    {
        if (!InitUnit("ExternalPerfUnit", memCtxPtr, PERF_UNIT_DEFAULT_TASK_INDEX)) {
            return false;
        }
        m_slock.Init();
        HASHCTL hashCtl;
        hashCtl.keysize = PERF_MAX_NAME_LEN;
        hashCtl.entrysize = sizeof(PerfGroupEntry);
        hashCtl.hash = string_hash;
        hashCtl.hcxt = memCtxPtr;
        int flags = (HASH_ELEM | HASH_FUNCTION | HASH_SHRCTX);
        m_perfGroupHash = hash_create("PerfCounterHash", PERF_COUNTER_MAX_GROUP_ELEM, &hashCtl, flags);
        if (!m_perfGroupHash) {
            ErrLog(DSTORE_PANIC, MODULE_FRAMEWORK, ErrMsg("crete perfGroupHash failed"));
        }
        m_isInit = true;
        return true;
    }

    LatencyStat **CreateGroupPerfItems(const char *groupName, uint8 num, PerfLevel level = PerfLevel::RELEASE)
    {
        if (unlikely(!m_isInit)) {
            ErrLog(DSTORE_PANIC, MODULE_FRAMEWORK,
                   ErrMsg("externalPerfUnit not init"));
        }
        if (unlikely(strlen(groupName) > PERF_MAX_NAME_LEN)) {
            ErrLog(DSTORE_PANIC, MODULE_FRAMEWORK, ErrMsg("groupName length greater than %u", PERF_MAX_NAME_LEN));
        }
        bool found = false;
        m_slock.Acquire();
        PerfGroupEntry *perfGroupEntry =
            static_cast<PerfGroupEntry *>(hash_search(m_perfGroupHash, groupName, HASH_FIND, &found));
        if (unlikely(found)) {
            m_slock.Release();
            ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK, ErrMsg("%s groupPerfItems has been created.", groupName));
            return perfGroupEntry->perfItems;
        }
        LatencyStat **perfItems = CreateGroupPerfItem(groupName, num, level);
        if (unlikely(!perfItems)) {
            ErrLog(DSTORE_PANIC, MODULE_FRAMEWORK, ErrMsg("%s groupPerfItems alloc memory failed.", groupName));
        }
        perfGroupEntry = static_cast<PerfGroupEntry *>(hash_search(m_perfGroupHash, groupName, HASH_ENTER, &found));
        if (unlikely(found || perfGroupEntry == nullptr)) {
            ErrLog(DSTORE_PANIC, MODULE_FRAMEWORK,
                   ErrMsg("%s perfCounterHashEntry enter failed.", groupName));
        }
        perfGroupEntry->perfItems = perfItems;
        perfGroupEntry->perfItemNum = num;
        m_slock.Release();
        return perfItems;
    }

private:
    ExternalPerfUnit() = default;
    bool m_isInit = false;
    DstoreSpinLock m_slock;
    HTAB *m_perfGroupHash = nullptr; /*  key:groupName, value:perfGroupEntry */
};
}  // namespace DSTORE

#endif
