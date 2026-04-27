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
 *    ThreadCpuAutoBinder is a class that can implement automatic thread core binding.
 *    ThreadCpuAutoBinder provides RegisterThreadToCpuBind()/UnRegisterThreadToCpuBind()
 *        to bind cpu core automatically,
 *        and UpdateCpuRes() to update cpu resouce for process.
 *    Core Bindind/Unbinding will be triggered at Register, Unregister and UpdateCpuRes.
 *
 */

#include "framework/dstore_thread_cpu_autobinder.h"
#include <cstdlib>
#include <dirent.h>
#include "framework/dstore_instance.h"
#include "common/log/dstore_log.h"

namespace DSTORE {
static constexpr uint16 MAX_NUMA_INFOSTR_LEN = 4096;
static constexpr uint16 MAX_CPUID_INFOSTR_LEN = 16;
static constexpr uint16 MAX_THREAD_NUM = 4096;
static constexpr uint16 MAX_TASKID_STR_LEN = 256;
static constexpr uint16 MAX_BIND_LOOP_COUNT = 5;
static constexpr int32 INVALID_BIND_COUNT = -1;
void ThreadCpuAutoBinder::Initialize(bool activeAutoBinder, uint32 numaCount, NumaCpuInfo *numaCpuInfos)
{
    ErrLog(DSTORE_INFO, MODULE_FRAMEWORK,
        ErrMsg("ThreadCpuAutoBinder begin to initialize. activeAutoBinder: %s", activeAutoBinder ? "on" : "off"));
    (void)UpdateNumaNodeInfos(numaCount, numaCpuInfos);
    m_binderTail = &(m_registeredThread.head);
    m_isActive = activeAutoBinder;
}

void ThreadCpuAutoBinder::Destroy()
{
    DstorePfreeExt(m_numaNodeInfos);
    DstorePfreeExt(m_lastNumaNodeInfos);
    m_binderTail = nullptr;
    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("[DS_SHUTDOWN]thread cpu auto binder destroy success!"));
}

void ThreadCpuAutoBinder::InitThrdGroupBindInfo(uint32 index)
{
    errno_t rc = memset_s(m_thrdGroupBindInfos[index].groupName, MAX_THRDGROUP_NAME_LEN, 0, MAX_THRDGROUP_NAME_LEN);
    storage_securec_check(rc, "\0", "\0");
    m_thrdGroupBindInfos[index].bindNumaId = INVALID_NUMA_ID;
    for (uint32 i = 0; i < MAX_THRDGROUP_SIZE; ++i) {
        m_thrdGroupBindInfos[index].threadGroup[i].tid = 0;
        m_thrdGroupBindInfos[index].threadGroup[i].bindCpu = nullptr;
    }
    m_thrdGroupBindInfos[index].threadCount = 0;
}

RetStatus ThreadCpuAutoBinder::RegisterThreadToCpuBind(ThreadId tid, CoreBindLevel level)
{
    AutoMemCxtSwitch autoSwitch{g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE)};
    ErrLog(DSTORE_INFO, MODULE_FRAMEWORK, ErrMsg("RegisterThreadToCpuBind %lu.", tid));
    ThrdBindCpuListNode *thrdInfo = static_cast<ThrdBindCpuListNode *>(DstorePalloc0(sizeof(ThrdBindCpuListNode)));
    if (!thrdInfo) {
        return DSTORE_FAIL;
    }
    thrdInfo->threadBindInfo.tid = tid;
    DstoreLWLockAcquire(&m_registeredThreadLock, LW_EXCLUSIVE);
    if (level == CoreBindLevel::HIGH) {
        /* when level is HIGH, push the thread in the head. */
        DListPushHead(&m_registeredThread, &(thrdInfo->listNode));

        /* if cpu cores are not enough, Unbind m_binderTail->thread and bind the new one */
        if (m_cpuCoreNum > 0) {
            if (m_cpuCoreNum <= m_threadNum) {
                ThrdBindCpuListNode *lastThrdInfo = dlist_container(ThrdBindCpuListNode, listNode, m_binderTail);
                UnbindThread(&lastThrdInfo->threadBindInfo);
                m_binderTail = m_binderTail->prev;
            }
            BindThread(&thrdInfo->threadBindInfo);
            if (m_binderTail == &m_registeredThread.head) {
                m_binderTail = &thrdInfo->listNode;
            }
        }
    } else if (level == CoreBindLevel::LOW) {
        /* when level is LOW, push the thread in the tail. */
        DListPushTail(&m_registeredThread, &(thrdInfo->listNode));
        /* if cpu cores are enough, then bind the thread */
        if (m_cpuCoreNum > m_threadNum) {
            BindThread(&thrdInfo->threadBindInfo);
            m_binderTail = &(thrdInfo->listNode);
        }
    }
    m_threadNum++;
    LWLockRelease(&m_registeredThreadLock);
    return DSTORE_SUCC;
}

RetStatus ThreadCpuAutoBinder::UnRegisterThreadToCpuBind(ThreadId tid)
{
    RetStatus ret = DSTORE_FAIL;
    ThrdBindCpuListNode *curThrd = nullptr;
    DstoreLWLockAcquire(&m_registeredThreadLock, LW_EXCLUSIVE);
    dlist_mutable_iter iter;
    dlist_foreach_modify(iter, &m_registeredThread) {
        curThrd = dlist_container(ThrdBindCpuListNode, listNode, iter.cur);
        if (curThrd->threadBindInfo.tid != tid) {
            continue;
        }
        if (curThrd->threadBindInfo.bindCpu != nullptr) {
            UnbindThread(&curThrd->threadBindInfo);
            UpdateBinderTailAfterUnbindThrd(curThrd);
        }
        DListDelete(iter.cur);
        m_threadNum--;
        DstorePfreeExt(curThrd);
        ret = DSTORE_SUCC;
        break;
    }
    LWLockRelease(&m_registeredThreadLock);
    return ret;
}

RetStatus ThreadCpuAutoBinder::RegisterThreadToNumaCpuBind(ThreadId tid, char *thrdGroupName)
{
    RetStatus ret = DSTORE_FAIL;
    ErrLog(DSTORE_INFO, MODULE_FRAMEWORK,
           ErrMsg("RegisterThreadToNumaCpuBind tid:%lu, thrdGroupName:%s.", tid, thrdGroupName));
    if (thrdGroupName == nullptr || strlen(thrdGroupName) == 0 || strlen(thrdGroupName) >= MAX_THRDGROUP_NAME_LEN) {
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK, ErrMsg("RegisterThreadToNumaCpuBind failed:thrdGroupName err."));
        return ret;
    }
    DstoreLWLockAcquire(&m_registeredThreadLock, LW_EXCLUSIVE);

    uint32 groupIndex = GetGroupIndex(thrdGroupName);
    uint32 thrdIndex = MAX_THRDGROUP_SIZE;
    if (groupIndex == MAX_THRDGROUP_COUNT) {
        groupIndex = AddNewThreadGroup(thrdGroupName);
        if (groupIndex == MAX_THRDGROUP_COUNT) {
            goto EXIT;
        }
    }

    thrdIndex = GetUnusedThrdIndexInGroup(&m_thrdGroupBindInfos[groupIndex]);
    if (thrdIndex == MAX_THRDGROUP_SIZE) {
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
            ErrMsg("RegisterThreadToNumaCpuBind failed:thrdNum is max.tid:%lu, groupName:%s.", tid, thrdGroupName));
        goto EXIT;
    }
    m_thrdGroupBindInfos[groupIndex].threadGroup[thrdIndex].tid = tid;
    
    if (m_thrdGroupBindInfos[groupIndex].bindNumaId != INVALID_NUMA_ID) {
        BindThread(&m_thrdGroupBindInfos[groupIndex].threadGroup[thrdIndex],
            m_thrdGroupBindInfos[groupIndex].bindNumaId);
        m_threadNum++;
    }
    m_thrdGroupBindInfos[groupIndex].threadCount++;
    ret = DSTORE_SUCC;
EXIT:
    LWLockRelease(&m_registeredThreadLock);
    return ret;
}

RetStatus ThreadCpuAutoBinder::UnRegisterThreadToNumaCpuBind(ThreadId tid, char *thrdGroupName)
{
    RetStatus ret = DSTORE_FAIL;
    if (thrdGroupName == nullptr || strlen(thrdGroupName) == 0) {
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
            ErrMsg("UnRegisterThreadToNumaCpuBind failed:thrdGroupName err."));
        return ret;
    }
    DstoreLWLockAcquire(&m_registeredThreadLock, LW_EXCLUSIVE);
    uint32 groupIndex = GetGroupIndex(thrdGroupName);
    if (groupIndex >= MAX_THRDGROUP_COUNT) {
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
            ErrMsg("UnRegisterThreadToNumaCpuBind failed, can't find thrdGroupName %s.", thrdGroupName));
        LWLockRelease(&m_registeredThreadLock);
        return ret;
    }
    for (uint32 i = 0; i < MAX_THRDGROUP_SIZE; ++i) {
        if (m_thrdGroupBindInfos[groupIndex].threadGroup[i].tid == tid) {
            if (m_thrdGroupBindInfos[groupIndex].threadGroup[i].bindCpu != nullptr) {
                UnbindThread(&m_thrdGroupBindInfos[groupIndex].threadGroup[i]);
                m_threadNum--;
                UpdateBinderTailAfterUnbindThrd();
            }
            m_thrdGroupBindInfos[groupIndex].threadGroup[i].tid = 0;
            m_thrdGroupBindInfos[groupIndex].threadCount--;
            break;
        }
    }
    if (m_thrdGroupBindInfos[groupIndex].threadCount == 0) {
        InitThrdGroupBindInfo(groupIndex);
        m_thrdGroupNum--;
    }
    LWLockRelease(&m_registeredThreadLock);
    return DSTORE_SUCC;
}


uint32 ThreadCpuAutoBinder::AddNewThreadGroup(char *groupName)
{
    uint32 groupIndex = GetUnusedGroupIndex();
    if (groupIndex == MAX_THRDGROUP_COUNT) {
        return MAX_THRDGROUP_COUNT;
    }
    errno_t rc = strcpy_s(m_thrdGroupBindInfos[groupIndex].groupName, MAX_THRDGROUP_NAME_LEN, groupName);
    storage_securec_check(rc, "\0", "\0");
    if (m_thrdGroupNum < m_numaCount) {
        m_thrdGroupBindInfos[groupIndex].bindNumaId = m_numaNodeInfos[groupIndex].numaId;
    }
    m_thrdGroupNum++;
    return groupIndex;
}

uint32 ThreadCpuAutoBinder::GetGroupIndex(char *groupName) const
{
    for (uint32 i = 0; i < MAX_THRDGROUP_COUNT; ++i) {
        if (strcmp(groupName, m_thrdGroupBindInfos[i].groupName) == 0) {
            return i;
        }
    }
    return MAX_THRDGROUP_COUNT;
}

uint32 ThreadCpuAutoBinder::GetUnusedGroupIndex() const
{
    for (uint32 i = 0; i < MAX_THRDGROUP_COUNT; ++i) {
        if (strlen(m_thrdGroupBindInfos[i].groupName) == 0) {
            return i;
        }
    }
    return MAX_THRDGROUP_COUNT;
}

uint32 ThreadCpuAutoBinder::GetUnusedThrdIndexInGroup(ThrdGroupBindInfo* groupBindInfo) const
{
    if (groupBindInfo->threadCount == MAX_THRDGROUP_SIZE) {
        return MAX_THRDGROUP_SIZE;
    }
    uint32 index = groupBindInfo->threadCount;
    while (index != groupBindInfo->threadCount - 1) {
        if (groupBindInfo->threadGroup[index].tid == 0) {
            return index;
        }
        if (++index == MAX_THRDGROUP_SIZE) {
            index = 0;
        }
    }
    return index;
}

uint32 ThreadCpuAutoBinder::GetThrdIndexInGroup(ThreadId tid, uint32 groupIndex) const
{
    if (groupIndex >= MAX_THRDGROUP_COUNT) {
        return MAX_THRDGROUP_SIZE;
    }
    for (uint32 i = 0; i < MAX_THRDGROUP_SIZE; ++i) {
        if (m_thrdGroupBindInfos[groupIndex].threadGroup[i].tid == tid) {
            return i;
        }
    }
    return MAX_THRDGROUP_SIZE;
}

void ThreadCpuAutoBinder::BindThread(ThreadBindInfo *thread, uint32 numaId)
{
    StorageAssert(thread != nullptr);
    cpu_set_t cpuSet;
    CPU_ZERO(&cpuSet);
    /* get avliable cpu core */
    CpuInfo *bindCpu = GetAvailableCpuCore(numaId);
    if (bindCpu == nullptr) {
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
            ErrMsg("Bind thread %lu failed: no available Cpu", thread->tid));
        return;
    }
    CPU_SET(bindCpu->cpuId, &cpuSet);
    int ret = pthread_setaffinity_np(thread->tid, sizeof(cpu_set_t), &cpuSet);
    if (ret != 0) {
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
            ErrMsg("Bind thread %lu to core %hu failed: %d", thread->tid, bindCpu->cpuId, ret));
        return;
    }
    ErrLog(DSTORE_INFO, MODULE_FRAMEWORK,
        ErrMsg("Bind thread %lu to core %hu.", thread->tid, bindCpu->cpuId));
    bindCpu->isSetAffinity = true;
    thread->bindCpu = bindCpu;
}

void ThreadCpuAutoBinder::UnbindThread(ThreadBindInfo *thread)
{
    StorageAssert(thread != nullptr);

    /* Restore the thread affinity to the CPU resources of the entire process. */
    int ret = pthread_setaffinity_np(thread->tid, sizeof(cpu_set_t), &m_procCpuSet);
    if (ret != 0) {
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
            ErrMsg("UnBind thread %lu failed: %d", thread->tid, ret));
        return;
    }
    ErrLog(DSTORE_INFO, MODULE_FRAMEWORK, ErrMsg("UnBind thread %lu.", thread->tid));
    if (thread->bindCpu != nullptr) {
        NumaNodeInfo *numaNodeInfo = GetNumaNodeInfo(thread->bindCpu->numaId);
        if (numaNodeInfo) {
            numaNodeInfo->availableCpuCount++;
        }
        thread->bindCpu->isSetAffinity = false;
        thread->bindCpu = nullptr;
    }
}

NumaNodeInfo *ThreadCpuAutoBinder::GetNumaNodeInfo(uint32 numaId)
{
    for (uint32 i = 0; i < m_numaCount; ++i) {
        if (numaId == m_numaNodeInfos[i].numaId) {
            return &m_numaNodeInfos[i];
        }
    }
    return nullptr;
}

NumaNodeInfo *ThreadCpuAutoBinder::GetAvailableNumaNodeInfo(uint32 numaId)
{
    NumaNodeInfo *numaNodeInfo = nullptr;
    /* When numaId is specified, select numaNodeInfo. */
    if (numaId != INVALID_NUMA_ID) {
        return GetNumaNodeInfo(numaId);
    }
    /* To avoid binding cores on one numa, Select the numaNodeInfo with the largest availableCpuCount. */
    uint32 maxAvailableCpuCount = 0;
    for (uint32 i = 0; i < m_numaCount; ++i) {
        if (maxAvailableCpuCount < m_numaNodeInfos[i].availableCpuCount) {
            maxAvailableCpuCount = m_numaNodeInfos[i].availableCpuCount;
            numaNodeInfo = &m_numaNodeInfos[i];
        }
    }
    return numaNodeInfo;
}

CpuInfo *ThreadCpuAutoBinder::GetAvailableCpuCore(uint32 numaId)
{
    NumaNodeInfo *numaNodeInfo = GetAvailableNumaNodeInfo(numaId);
    if (numaNodeInfo == nullptr) {
        return nullptr;
    }
    for (uint32 i = 0; i < numaNodeInfo->cpuCount; ++i) {
        if (!(numaNodeInfo->cpuArr[i].isSetAffinity)) {
            numaNodeInfo->availableCpuCount--;
            return &(numaNodeInfo->cpuArr[i]);
        }
    }
    return nullptr;
}

void ThreadCpuAutoBinder::ReBindAllThreads()
{
    if (!m_isActive) {
        return;
    }
    /* Rebind threads with cpu bind type */
    ThrdBindCpuListNode *curThrd = nullptr;
    uint32_t cpuBindCount = 0;
    m_binderTail = &m_registeredThread.head;
    dlist_iter iter;
    dlist_foreach(iter, &m_registeredThread) {
        curThrd = dlist_container(ThrdBindCpuListNode, listNode, iter.cur);
        if (cpuBindCount < m_cpuCoreNum) {
            BindThread(&curThrd->threadBindInfo);
            cpuBindCount++;
            m_binderTail = iter.cur;
        } else if (curThrd->threadBindInfo.bindCpu) {
            UnbindThread(&curThrd->threadBindInfo);
        }
    }
    /* Rebind threads with numa cpu bind type */
    uint32 groupCount = 0;
    for (uint32 i = 0; i < MAX_THRDGROUP_COUNT; ++i) {
        if (strlen(m_thrdGroupBindInfos[i].groupName) == 0 || m_thrdGroupBindInfos[i].threadCount == 0) {
            continue;
        }
        if (groupCount < m_numaCount) {
            m_thrdGroupBindInfos[i].bindNumaId = m_numaNodeInfos[groupCount].numaId;
        } else {
            m_thrdGroupBindInfos[i].bindNumaId = INVALID_NUMA_ID;
        }
        ReBindThrdGroup(&m_thrdGroupBindInfos[i]);
        groupCount++;
    }
}

void ThreadCpuAutoBinder::ReBindThrdGroup(ThrdGroupBindInfo *thrdGroup)
{
    uint32 bindThreadCount = 0;
    for (uint32 i = 0; i < MAX_THRDGROUP_SIZE; ++i) {
        if (thrdGroup->threadGroup[i].tid == 0) {
            continue;
        }
        if (bindThreadCount < thrdGroup->threadCount && thrdGroup->bindNumaId != INVALID_NUMA_ID) {
            BindThread(&thrdGroup->threadGroup[i], thrdGroup->bindNumaId);
        } else if (thrdGroup->threadGroup[i].bindCpu) {
            UnbindThread(&thrdGroup->threadGroup[i]);
        }
    }
}

void ThreadCpuAutoBinder::UpdateBinderTailAfterUnbindThrd(ThrdBindCpuListNode *unboundThrd)
{
    /* thread-nodes after m_binderTail are not bound to a core, m_binderTail->next needs to be bound */
    if (DListHasNext(&m_registeredThread, m_binderTail)) {
        ThrdBindCpuListNode *binderTailThrd = dlist_container(ThrdBindCpuListNode, listNode,
                                                            DListNextNode(&m_registeredThread, m_binderTail));
        BindThread(&binderTailThrd->threadBindInfo);
        m_binderTail = &(binderTailThrd->listNode);
    /* when m_binderTail pointers to unregistered thread, m_binderTail needs to be moved forward */
    } else if (unboundThrd != nullptr && m_binderTail == &(unboundThrd->listNode)) {
        if (DListHasPrev(&m_registeredThread, m_binderTail)) {
            m_binderTail = DListPrevNode(&m_registeredThread, m_binderTail);
        } else {
            m_binderTail = &(m_registeredThread.head);
        }
    }
}

RetStatus ThreadCpuAutoBinder::UpdateCpuRes(uint32 minCpuCount, uint32 maxCpuCount,
                                            uint32 numaCount, NumaCpuInfo *numaCpuInfos)
{
    RetStatus ret = DSTORE_FAIL;
    if (!m_isActive) {
        return ret;
    }
    DstoreLWLockAcquire(&m_registeredThreadLock, LW_EXCLUSIVE);
    if (STORAGE_FUNC_SUCC(UpdateNumaNodeInfos(numaCount, numaCpuInfos))) {
        SetCpuCountRange(minCpuCount, maxCpuCount);
        ReBindAllThreads();
        DstorePfreeExt(m_lastNumaNodeInfos);
        ret = DSTORE_SUCC;
    }
    LWLockRelease(&m_registeredThreadLock);
    return ret;
}

RetStatus ThreadCpuAutoBinder::UpdateNumaNodeInfos(uint32 numaCount, NumaCpuInfo *numaCpuInfos)
{
    AutoMemCxtSwitch autoSwitch{g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE)};
    if (numaCount == 0 || numaCpuInfos == nullptr) {
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK, ErrMsg("UpdateNumaNodeInfos parameter is incorrect."));
        return DSTORE_FAIL;
    }
    m_lastNumaNodeInfos = m_numaNodeInfos;
    uint32 cpuCoreCount = 0;
    m_numaNodeInfos = ParseNumaInfos(numaCount, numaCpuInfos, &cpuCoreCount);
    if (m_numaNodeInfos == nullptr || cpuCoreCount == 0) {
        m_numaNodeInfos = m_lastNumaNodeInfos;
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK, ErrMsg("ParseNumaInfos failed."));
        return DSTORE_FAIL;
    }

    m_numaCount = numaCount;
    m_cpuCoreNum = cpuCoreCount;
    PrintNumaNodeInfo();
    UpdateProcCpuSet();
    return DSTORE_SUCC;
}

NumaNodeInfo *ThreadCpuAutoBinder::ParseNumaInfos(uint32 numaCount, NumaCpuInfo *numaCpuInfos, uint32 *cpuCoreCnt) const
{
    size_t numaCpuInfoSize = sizeof(NumaNodeInfo) * numaCount;
    NumaNodeInfo *infos = static_cast<NumaNodeInfo *>(DstorePalloc0(numaCpuInfoSize));
    if (infos == nullptr) {
        return nullptr;
    }

    char buf[MAX_DSTORE_CPU_LIST_LEN];
    uint32 cpuCount = 0;
    for (uint32 i = 0; i < numaCount; ++i) {
        errno_t rc = strcpy_s(buf, MAX_DSTORE_CPU_LIST_LEN, numaCpuInfos[i].cpuList);
        storage_securec_check(rc, "\0", "\0");
        char *context = nullptr;
        char *target = strtok_s(buf, ",", &context);
        if (target == nullptr) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                ErrMsg("Numa cpu info %s parse error.", numaCpuInfos[i].cpuList));
            DstorePfree(infos);
            return nullptr;
        }
        infos[i].numaId = numaCpuInfos[i].numaId;
        while (target != nullptr) {
            cpuCount += ParseCpuId(&infos[i], target);
            target = strtok_s(nullptr, ",", &context);
        }
        infos[i].availableCpuCount = infos[i].cpuCount;
    }
    *cpuCoreCnt = cpuCount;
    return infos;
}

uint32 ThreadCpuAutoBinder::ParseCpuId(NumaNodeInfo *infos, char *cpuIdStr) const
{
    uint32 cpuCount = 0;
    char *cursor = strchr(cpuIdStr, '-');
    if (cursor) {
        *cursor++ = '\0';
        uint16 startId = static_cast<uint16>(strtol(cpuIdStr, nullptr, 0));
        uint16 endId = static_cast<uint16>(strtol(cursor, nullptr, 0));
        while (startId <= endId) {
            infos->cpuArr[infos->cpuCount].cpuId = startId;
            infos->cpuArr[infos->cpuCount].numaId = infos->numaId;
            infos->cpuArr[infos->cpuCount].isSetAffinity = false;
            infos->cpuCount++;
            cpuCount++;
            startId++;
        }
    } else {
        uint16 cpuId = static_cast<uint16>(strtol(cpuIdStr, nullptr, 0));
        infos->cpuArr[infos->cpuCount].cpuId = cpuId;
        infos->cpuArr[infos->cpuCount].numaId = infos->numaId;
        infos->cpuArr[infos->cpuCount].isSetAffinity = false;
        cpuCount++;
        infos->cpuCount++;
    }
    return cpuCount;
}

void ThreadCpuAutoBinder::PrintNumaNodeInfo() const
{
    ErrLog(DSTORE_INFO, MODULE_FRAMEWORK, ErrMsg("NumaCount:%u.", m_numaCount));
    size_t bufLen = MAX_NUMA_INFOSTR_LEN;
    char *buf = static_cast<char *>(DstorePalloc0(bufLen));
    if (unlikely(buf == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("PrintDstoreNumaInfos alloc mem failed."));
        return;
    }
    for (uint32 i = 0; i < m_numaCount; i++) {
        int rc = sprintf_s(buf, bufLen, "NumaId: %u, CpuCnt: %u, CpuArr:",
                           m_numaNodeInfos[i].numaId, m_numaNodeInfos[i].cpuCount);
        storage_securec_check_ss(rc);
        for (uint32 j = 0; j < m_numaNodeInfos[i].cpuCount; j++) {
            char cpuIdStr[MAX_CPUID_INFOSTR_LEN];
            rc = sprintf_s(cpuIdStr, MAX_CPUID_INFOSTR_LEN, " %hu", m_numaNodeInfos[i].cpuArr[j].cpuId);
            storage_securec_check_ss(rc);
            rc = strcat_s(buf, bufLen, cpuIdStr);
            storage_securec_check(rc, "\0", "\0");
        }
        ErrLog(DSTORE_INFO, MODULE_FRAMEWORK, ErrMsg("%s.", buf));
    }
    DstorePfree(buf);
}

void ThreadCpuAutoBinder::UpdateProcCpuSet()
{
    CPU_ZERO(&m_procCpuSet);
    for (uint32 i = 0; i < m_numaCount; ++i) {
        for (uint32 j = 0; j < m_numaNodeInfos[i].cpuCount; ++j) {
            CPU_SET(m_numaNodeInfos[i].cpuArr[j].cpuId, &m_procCpuSet);
        }
    }
}

uint16 ThreadCpuAutoBinder::GetThreadBindCore(ThreadId tid, char *groupName)
{
    DstoreLWLockAcquire(&m_registeredThreadLock, LW_SHARED);
    ThrdBindCpuListNode *curThrd = nullptr;
    uint16 cpuId = INVALID_CPU_CORE;
    dlist_iter iter;
    dlist_foreach(iter, &m_registeredThread) {
        curThrd = dlist_container(ThrdBindCpuListNode, listNode, iter.cur);
        if (curThrd->threadBindInfo.tid == tid) {
            if (curThrd->threadBindInfo.bindCpu != nullptr) {
                cpuId = curThrd->threadBindInfo.bindCpu->cpuId;
            }
            break;
        }
    }
    if (groupName != nullptr) {
        uint32 groupIndex = GetGroupIndex(groupName);
        uint32 threadIndex = GetThrdIndexInGroup(tid, groupIndex);
        if (groupIndex < MAX_THRDGROUP_COUNT && threadIndex < MAX_THRDGROUP_SIZE) {
            if (m_thrdGroupBindInfos[groupIndex].threadGroup[threadIndex].bindCpu != nullptr) {
                cpuId = m_thrdGroupBindInfos[groupIndex].threadGroup[threadIndex].bindCpu->cpuId;
            }
        }
    }
    LWLockRelease(&m_registeredThreadLock);
    return cpuId;
}

bool ThreadCpuAutoBinder::IsThreadRegistered(ThreadId tid, char *groupName)
{
    DstoreLWLockAcquire(&m_registeredThreadLock, LW_SHARED);
    ThrdBindCpuListNode *curThrd = nullptr;
    bool isThreadRegistered = false;
    dlist_iter iter;
    dlist_foreach(iter, &m_registeredThread) {
        curThrd = dlist_container(ThrdBindCpuListNode, listNode, iter.cur);
        if (curThrd->threadBindInfo.tid == tid) {
            isThreadRegistered = true;
            break;
        }
    }
    if (groupName != nullptr) {
        if (GetThrdIndexInGroup(tid, GetGroupIndex(groupName)) != MAX_THRDGROUP_SIZE) {
            isThreadRegistered = true;
        }
    }
    LWLockRelease(&m_registeredThreadLock);
    return isThreadRegistered;
}

static int GetAllThreadsId(pid_t pid, pid_t *threadIds)
{
    char taskpath[MAX_TASKID_STR_LEN];
    int rc = sprintf_s(taskpath, MAX_TASKID_STR_LEN, "/proc/%d/task", pid);
    storage_securec_check_ss(rc);
    DIR *dir = opendir(taskpath);
    if (dir == nullptr) {
        return 0;
    }
    struct dirent *entry;
    int count = 0;
    while ((entry = readdir(dir)) != nullptr) {
        if (entry->d_name[0] == '.') {
            continue;
        }
        pid_t tid = static_cast<pid_t>(strtol(entry->d_name, nullptr, 0));
        if (tid <= 0) {
            continue;
        }
        threadIds[count++] = tid;
        if (unlikely(count >= MAX_THREAD_NUM)) {
            ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
                   ErrMsg("GetAllThreadsId failed, threadNum is over MAX_THREAD_NUM."));
            break;
        }
    }
    rc = closedir(dir);
    if (unlikely(rc != 0)) {
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK, ErrMsg("closedir %s failed: %d", taskpath, rc));
    }
    return count;
}

static int SetChildThreadsToCPUSet(pid_t pid, cpu_set_t *srcCpuSet, cpu_set_t *dstCpuSet)
{
    pid_t *threads = static_cast<pid_t *>(DstorePalloc0(MAX_THREAD_NUM * sizeof(pid_t)));
    if (unlikely(threads == nullptr)) {
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK, ErrMsg("SetChildThreadsToCPUSet alloc mem failed."));
        return INVALID_BIND_COUNT;
    }
    int threadsNum = GetAllThreadsId(pid, threads);
    int bindThreadCount = 0;
    for (int i = 0; i < threadsNum; ++i) {
        cpu_set_t threadCpuSet;
        CPU_ZERO(&threadCpuSet);
        int ret = sched_getaffinity(threads[i], sizeof(cpu_set_t), &threadCpuSet);
        /* if the thread have been bound to the specific cpu, it doesn't need to be rebound here. */
        if (ret != 0 || !CPU_EQUAL(&threadCpuSet, srcCpuSet)) {
            continue;
        }
        bindThreadCount++;
        ret = sched_setaffinity(threads[i], sizeof(cpu_set_t), dstCpuSet);
        if (ret != 0) {
            ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
                ErrMsg("Bind thread %d failed: %d", threads[i], ret));
            continue;
        }
    }
    DstorePfree(threads);
    return bindThreadCount;
}

void ThreadCpuAutoBinder::SetProcAffinityToCPUSet(pid_t pid, cpu_set_t *srcCpuSet, cpu_set_t *dstCpuSet) const
{
    /* Set the CPU affinity to threads. */
    int loopCount = 0;
    /* loop until bindThreadNum is 0 in case that new thread is created when SetChildThreadsToCPUSet() */
    while (SetChildThreadsToCPUSet(pid, srcCpuSet, dstCpuSet) != 0) {
        loopCount++;
        if (loopCount >= MAX_BIND_LOOP_COUNT) {
            ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
                ErrMsg("bind threads over MAX_BIND_LOOP_COUNT, there may be threads that are not set to cpuset"));
            break;
        }
    }
}

} /* namespace DSTORE */