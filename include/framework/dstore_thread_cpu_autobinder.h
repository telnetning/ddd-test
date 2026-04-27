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
 * Description:
 *    ThreadCpuAutoBinder is a class  that can implement automatic thread core binding.
 *    ThreadCpuAutoBinder provides RegisterThreadToCpuBind()/UnRegisterThreadToCpuBind()
 *        to bind cpu core automatically,
 *        and UpdateCpuRes() to update cpu resouce for process.
 *    Core Bindind/Unbinding will be triggered at Register, Unregister and UpdateCpuRes.
 *
 */
#ifndef DSTORE_THREAD_CPU_AUTOBINDER_H
#define DSTORE_THREAD_CPU_AUTOBINDER_H

#include <mutex>
#include <pthread.h>
#include "common/dstore_datatype.h"
#include "common/algorithm/dstore_ilist.h"
#include "common/memory/dstore_mctx.h"
#include "common/dstore_common_utils.h"
#include "lock/dstore_lwlock.h"
#include "framework/dstore_instance_interface.h"
#include "framework/dstore_thread_autobinder_interface.h"


namespace DSTORE {

constexpr uint16 INVALID_CPU_CORE = UINT16_MAX;
constexpr uint32 INVALID_NUMA_ID = UINT32_MAX;
constexpr uint32 MAX_THRDGROUP_SIZE = 256;
constexpr uint32 MAX_THRDGROUP_COUNT = 16;
constexpr uint32 MAX_THRDGROUP_NAME_LEN = 128;

struct CpuInfo {
    uint32 numaId;
    uint16 cpuId;
    bool isSetAffinity = false;

    bool operator==(const CpuInfo &cpuInfo) const
    {
        return cpuId == cpuInfo.cpuId && numaId == cpuInfo.numaId;
    }

    bool operator!=(const CpuInfo &cpuInfo) const
    {
        return !(*this == cpuInfo);
    }
};

struct ThreadBindInfo {
    ThreadId tid;
    CpuInfo *bindCpu = nullptr;
    uint8 weight; /* Reserved */
};
struct ThrdBindCpuListNode {
    dlist_node listNode;
    ThreadBindInfo threadBindInfo;
};

struct ThrdGroupBindInfo {
    char groupName[MAX_THRDGROUP_NAME_LEN];
    uint32 bindNumaId = INVALID_NUMA_ID;
    uint32 threadCount = 0;
    ThreadBindInfo threadGroup[MAX_THRDGROUP_SIZE];
};

struct NumaNodeInfo {
    uint32 numaId;
    uint32 cpuCount;
    uint32 availableCpuCount;
    CpuInfo cpuArr[MAX_CPU_NUM_PER_NUMA];

    bool operator==(const NumaNodeInfo &numaNodeInfo) const
    {
        if ((numaId != numaNodeInfo.numaId) || (cpuCount != numaNodeInfo.cpuCount)) {
            return false;
        }
        for (uint32 i = 0; i < cpuCount; ++i) {
            if (cpuArr[i] != numaNodeInfo.cpuArr[i]) {
                return false;
            }
        }
        return true;
    }

    bool operator!=(const NumaNodeInfo &numaNodeInfo) const
    {
        return !(*this == numaNodeInfo);
    }
};

class ThreadCpuAutoBinder : public BaseObject {
public:
    ThreadCpuAutoBinder() : m_minCpuCount(0), m_maxCpuCount(0), m_numaCount(0), m_numaNodeInfos(nullptr),
        m_lastNumaNodeInfos(nullptr), m_cpuCoreNum(0), m_threadNum(0), m_binderTail(nullptr), m_thrdGroupNum(0),
        m_isActive(false)
    {
        LWLockInitialize(&m_registeredThreadLock, LWLOCK_GROUP_REGISTERED_THREAD);
        DListInit(&m_registeredThread);
        CPU_ZERO(&m_procCpuSet);
        for (uint32 i = 0; i < MAX_THRDGROUP_COUNT; ++i) {
            InitThrdGroupBindInfo(i);
        }
    }
    void Initialize(bool activeAutoBinder = false, uint32 numaCount = 0, NumaCpuInfo *numaCpuInfos = nullptr);
    void Destroy();

    RetStatus RegisterThreadToCpuBind(ThreadId tid, CoreBindLevel level = CoreBindLevel::LOW);

    RetStatus UnRegisterThreadToCpuBind(ThreadId tid);

    RetStatus RegisterThreadToNumaCpuBind(ThreadId tid, char *thrdGroupName);

    RetStatus UnRegisterThreadToNumaCpuBind(ThreadId tid, char *thrdGroupName);

     /* when cpu res changed, rebind all registered thread */
    RetStatus UpdateCpuRes(uint32 minCpuCount, uint32 maxCpuCount, uint32 numaCount, NumaCpuInfo *numaCpuInfos);

    uint16 GetThreadBindCore(ThreadId tid, char *groupName = nullptr);

    uint32 GetThreadNum() const
    {
        return m_threadNum;
    }

    void SetProcAffinityToCPUSet(pid_t pid, cpu_set_t *srcCpuSet, cpu_set_t *dstCpuSet) const;

    bool IsActive() const
    {
        return m_isActive;
    }

    /* just for UT */
    bool IsThreadRegistered(ThreadId tid, char *groupName = nullptr);

private:
    void InitThrdGroupBindInfo(uint32 index);
    void BindThread(ThreadBindInfo *thread, uint32 numaId = INVALID_NUMA_ID);
    void UnbindThread(ThreadBindInfo *thread);
    void UpdateBinderTailAfterUnbindThrd(ThrdBindCpuListNode *unboundThrd = nullptr);
    void UpdateProcCpuSet();
    CpuInfo *GetAvailableCpuCore(uint32 numaId = INVALID_NUMA_ID);
    NumaNodeInfo *GetAvailableNumaNodeInfo(uint32 numaId);
    NumaNodeInfo *GetNumaNodeInfo(uint32 numaId);
    uint32 GetUnusedThrdIndexInGroup(ThrdGroupBindInfo* groupBindInfo) const;
    uint32 GetUnusedGroupIndex() const;
    uint32 GetGroupIndex(char *groupName) const;
    uint32 GetThrdIndexInGroup(ThreadId tid, uint32 groupIndex) const;
    uint32 AddNewThreadGroup(char *groupName);

    void SetCpuCountRange(uint32 minCpuCount, uint32 maxCpuCount)
    {
        m_minCpuCount = minCpuCount;
        m_maxCpuCount = maxCpuCount;
    }
    RetStatus UpdateNumaNodeInfos(uint32 numaCount, NumaCpuInfo *numaCpuInfos);
    NumaNodeInfo *ParseNumaInfos(uint32 numaCount, NumaCpuInfo *numaCpuInfos, uint32 *cpuCoreCnt) const;
    uint32 ParseCpuId(NumaNodeInfo *infos, char *cpuIdStr) const;
    void PrintNumaNodeInfo() const;
    void ReBindAllThreads();
    void ReBindThrdGroup(ThrdGroupBindInfo *thrdGroup);
    uint32 m_minCpuCount;       /* min size of vCores(cpu cores/thread) size */
    uint32 m_maxCpuCount;       /* max size of vCores */
    uint32 m_numaCount;
    NumaNodeInfo *m_numaNodeInfos;
    NumaNodeInfo *m_lastNumaNodeInfos;
    cpu_set_t m_procCpuSet;     /* cpu set for whole gaussDb proc */
    uint32 m_cpuCoreNum;        /* Available CPU cores number */
    uint32 m_threadNum;         /* registered thread number */
    dlist_head m_registeredThread;
    dlist_node* m_binderTail;
    uint32 m_thrdGroupNum;
    ThrdGroupBindInfo m_thrdGroupBindInfos[MAX_THRDGROUP_COUNT];
    std::atomic<bool> m_isActive;
    LWLock m_registeredThreadLock;
};
} /* namespace DSTORE */

#endif