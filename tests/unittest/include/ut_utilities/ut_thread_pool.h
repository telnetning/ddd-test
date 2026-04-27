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
#ifndef DSTORE_UT_THREAD_POOL_H
#define DSTORE_UT_THREAD_POOL_H

#include <chrono>
#include <mutex>
#include <queue>
#include <vector>
#include <thread>
#include <condition_variable>
#include <future>
#include "ut_dstore_framework.h"

enum UTThreadMode : uint8 {
    UT_STORAGE_KERNEL_THREAD = 0,
    UT_COMMUNICATOR_THREAD,
    /* Add more thread mode as needed. */
};

class UTThreadPool {
public:
    UTThreadPool()
    {}
    ~UTThreadPool()
    {}

    void Start(int threadsNum = 3, UTThreadMode threadMode = UT_STORAGE_KERNEL_THREAD)
    {
        m_shutdown = false;
        m_threads.reserve(threadsNum);
        std::function<void()> threadFunc = SelectThreadMainFunc(threadMode);
        for (int i = 0; i < threadsNum; i++) {
            m_threads.emplace_back(threadFunc);
        }
        m_freeThdNum = threadsNum;
    }

    void Shutdown()
    {
        {
            std::unique_lock<std::mutex> lock(m_mutex);
            m_shutdown = true;
        }
        m_condition.notify_all();
        for (std::thread &thread : m_threads) {
            if (thread.joinable()) {
                thread.join();
            } else {
                StorageAssert(0);
            }
        }
    }

    template <typename F, typename... Args>
    void AddTask(F &&f, Args &&...args)
    {
        std::function<decltype(f(args...))()> func = std::bind(std::forward<F>(f), std::forward<Args>(args)...);
        auto taskPtr = std::make_shared<std::packaged_task<decltype(f(args...))()>>(func);
        std::function<void()> wrapper = [taskPtr]() { (*taskPtr)(); };
        {
            std::unique_lock<std::mutex> lock(m_mutex);
            m_tasks.push(wrapper);
        }
        m_condition.notify_one();
    }

    void WaitAllTaskFinish()
    {
        while (1) {
            std::unique_lock<std::mutex> queueLock(m_mutex);
            /* Wait until no pending task and no running thread */
            if (m_tasks.empty() && m_freeThdNum == m_threads.size()) {
                break;
            }
            queueLock.unlock();
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }

private:
    void InnerTaskFetchLoop()
    {
        while (!m_shutdown) {
            std::function<void()> task;
            {
                std::unique_lock<std::mutex> lock(m_mutex);
                m_condition.wait(lock, [this] { return (!m_tasks.empty() || m_shutdown); });
                if (m_shutdown) {
                    break;
                }
                task = m_tasks.front();
                --m_freeThdNum;
                m_tasks.pop();
            }

            task();
            ++m_freeThdNum;
        }
    }

    void StorageKernelThreadRun()
    {
        UtInstance *instance = (UtInstance *)g_storageInstance;
        instance->ThreadSetupAndRegister();
        InnerTaskFetchLoop();
        instance->ThreadUnregisterAndExit();
    }

    std::function<void()> SelectThreadMainFunc(UTThreadMode threadMode)
    {
        std::function<void()> threadFunc{nullptr};
        switch (threadMode) {
            case UT_STORAGE_KERNEL_THREAD:
                threadFunc = [this] { StorageKernelThreadRun(); };
                break;
                
            default:
                StorageAssert(false);
                break;
        }
        StorageAssert(threadFunc);
        return threadFunc;
    }

    bool m_shutdown;
    std::mutex m_mutex;
    std::condition_variable m_condition;
    std::vector<std::thread> m_threads;
    std::queue<std::function<void()>> m_tasks;
    std::atomic_int64_t m_freeThdNum;
};

#endif  // DSTORE_UT_THREAD_POOL_H
