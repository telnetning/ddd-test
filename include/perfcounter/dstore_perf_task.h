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
 * Description: The perf counter task for performance monitor.
 */

#ifndef DSTORE_PERF_COUNTER_TASK_H
#define DSTORE_PERF_COUNTER_TASK_H

#include <functional>
#include "common/dstore_datatype.h"

namespace DSTORE {
constexpr uint8 PERF_COUNTER_MAX_TASK_NUM = 2; /* cannot be 0xFF, 0xFF is used by PERF_COUNTER_ALL_UNIT_TASK_INDEX */
constexpr uint8 PERF_COUNTER_DEFAULT_TIMER_THREAD_NUM = 1;
constexpr uint64 PERF_COUNTER_TIMER_TICK_UNIT = 1000;

using PerfSchedulerTask = std::function<bool(uint8 taskIndex)>;

class PerfCounterTask : public BaseObject {
public:
    PerfCounterTask(uint8 taskIndex, const PerfSchedulerTask &callbackTask)
        : m_taskIndex(taskIndex), m_callbackTask(callbackTask)
    {}
    ~PerfCounterTask() = default;

    bool Call()
    {
        if (unlikely(m_callbackTask == nullptr)) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                   ErrMsg("Perf scheduler has no callback task to run, task index:%hhu", m_taskIndex));
            return false;
        }
        return m_callbackTask(m_taskIndex);
    }

private:
    uint8 m_taskIndex{0};
    PerfSchedulerTask m_callbackTask{nullptr};
};
}  // namespace DSTORE
#endif /* STORAGE_PERF_COUNTER_TASK_H */
