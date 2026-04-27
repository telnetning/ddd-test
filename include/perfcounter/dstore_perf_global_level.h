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
 * Description: The PerfCounter perform level.
 */

#ifndef DSTORE_PERF_GLOBAL_LEVEL_H
#define DSTORE_PERF_GLOBAL_LEVEL_H
#include "framework/dstore_instance_interface.h"

namespace DSTORE {

class PerfGlobalLevel {
public:
    static PerfGlobalLevel &GetInstance()
    {
        static PerfGlobalLevel instance;
        return instance;
    }
    ~PerfGlobalLevel() = default;

    inline bool SetPerfLevel(PerfLevel level)
    {
        if (level <= PerfLevel::RELEASE || level == PerfLevel::OFF) {
            m_level = level;
            return true;
        }

        return false;
    }

    inline PerfLevel GetPerfLevel() const
    {
        return m_level;
    }

private:
    PerfGlobalLevel() = default;

private:
    PerfLevel m_level{PerfLevel::RELEASE};
};
}  // namespace DSTORE
#endif /* STORAGE_PERF_GLOBAL_LEVEL_H */
