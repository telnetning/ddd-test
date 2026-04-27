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

#ifndef DSTORE_LOCK_PERF_UNIT_H
#define DSTORE_LOCK_PERF_UNIT_H

#include "perfcounter/dstore_perf_catalog.h"

namespace DSTORE {
class LockPerfUnit : public DstorePerfUnit {
public:
    static LockPerfUnit &GetInstance()
    {
        static LockPerfUnit perfUnit{};

        return perfUnit;
    };

    ~LockPerfUnit() override = default;

    bool Init(DstoreMemoryContext memCtxPtr)
    {
        if (!InitUnit("LockPerfUnit", memCtxPtr, PERF_UNIT_DEFAULT_TASK_INDEX)) {
            return false;
        }
        return true;
    }

private:
    LockPerfUnit() = default;
};
}  // namespace DSTORE
#endif /* STORAGE_LOCK_PERF_UNIT_H */
