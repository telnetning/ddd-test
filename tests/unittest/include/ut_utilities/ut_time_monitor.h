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
 */
#ifndef DSTORE_UT_TIME_MONITOR_H
#define DSTORE_UT_TINE_MONITOR_H

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

namespace DSTORE {

class UTTimeoutMonitor {
public:
    static UTTimeoutMonitor *GetInstance();

    static void Destroy();

    void Start();

    void ResetCountDown();

    void Interrupt();

private:
    /* The monitor checks the test case every 1ms. */
    static constexpr int MAX_SECONDS_ALLOWED{240};
    static constexpr int CHECK_TIMES{MAX_SECONDS_ALLOWED * 1000};
    static constexpr std::chrono::milliseconds CHECK_DURATION_MILLIS{1};

    static UTTimeoutMonitor *m_self;

    static void MonitorLoop(void *arg);

    UTTimeoutMonitor();

    std::mutex m_monitorMut;
    std::condition_variable m_monitorCv;
    std::unique_ptr<std::thread> m_monitorThrd;
    int m_timeCounter;
    std::atomic_bool m_interrupted;
};

} /* namespace DSTORE */
#endif
