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
#include "common/dstore_datatype.h"

#include "ut_utilities/ut_time_monitor.h"

#include <memory>

using namespace DSTORE;

constexpr int UTTimeoutMonitor::MAX_SECONDS_ALLOWED;
constexpr int UTTimeoutMonitor::CHECK_TIMES;
constexpr std::chrono::milliseconds UTTimeoutMonitor::CHECK_DURATION_MILLIS;

UTTimeoutMonitor *UTTimeoutMonitor::m_self{nullptr};

UTTimeoutMonitor::UTTimeoutMonitor() : m_monitorThrd{nullptr}, m_timeCounter{0}, m_interrupted{false}
{
    m_self = this;
}

UTTimeoutMonitor *UTTimeoutMonitor::GetInstance()
{
    if (m_self == nullptr) {
        m_self = new UTTimeoutMonitor{};
    }
    return m_self;
}

void UTTimeoutMonitor::Destroy()
{
    if (m_self != nullptr) {
        delete m_self;
        m_self = nullptr;
    }
}

void UTTimeoutMonitor::Start()
{
    m_timeCounter = 0;
    m_interrupted.store(false, std::memory_order_release);
    m_monitorThrd = std::unique_ptr<std::thread>(new std::thread{MonitorLoop, nullptr});
    pthread_setname_np(m_monitorThrd->native_handle(), "TimeMonitor");
}

void UTTimeoutMonitor::ResetCountDown()
{
    std::lock_guard<std::mutex> lock{m_monitorMut};
    m_timeCounter = 0;
}

void UTTimeoutMonitor::Interrupt()
{
    {
        std::lock_guard<std::mutex> lock{m_monitorMut};
        m_timeCounter = 0;
        m_interrupted.store(true, std::memory_order_release);
    }
    if (m_monitorThrd->joinable()) {
        m_monitorThrd->join();
    } else {
        StorageReleasePanic(true, MODULE_FRAMEWORK, ErrMsg("m_monitorThrd force destoryed"));
    }

}

void UTTimeoutMonitor::MonitorLoop(void *arg)
{
    int &counter = m_self->m_timeCounter;
    while (!m_self->m_interrupted.load(std::memory_order_acquire)) {
        std::unique_lock<std::mutex> lock{m_self->m_monitorMut};
        m_self->m_monitorCv.wait_for(lock, CHECK_DURATION_MILLIS);
        counter++;
#ifndef ENABLE_THREAD_CHECK
        if (counter >= CHECK_TIMES) {
            printf("Current single testcase limit time is %d s. Time Out!\n", MAX_SECONDS_ALLOWED);
            StorageReleasePanic(true, MODULE_FRAMEWORK,
                                ErrMsg("Current single testcase limit time is %d s. Time Out!\n", MAX_SECONDS_ALLOWED));
        }
#endif
    }
}
