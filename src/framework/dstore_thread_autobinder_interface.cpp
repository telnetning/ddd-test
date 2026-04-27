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
 *    autobinder_interface advides interface to register/unregister thread to thread_cpu_autobinder
 *    which is init in Dstore.
 */

#include "framework/dstore_thread_autobinder_interface.h"
#include "framework/dstore_thread_cpu_autobinder.h"
#include "framework/dstore_instance.h"
#include "common/log/dstore_log.h"

namespace DSTORE {
RetStatus RegisterThreadToBind(ThreadId tid, BindType bindType, CoreBindLevel level, char *thrdGroupName)
{
    if (unlikely(!g_storageInstance || !(g_storageInstance->GetThreadCpuAutoBinder()))) {
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
            ErrMsg("RegisterThreadToCpuBind %lu failed: Dstore is not Init.", tid));
        return DSTORE_FAIL;
    }
    if (!g_storageInstance->GetThreadCpuAutoBinder()->IsActive()) {
        ErrLog(DSTORE_LOG, MODULE_FRAMEWORK,
            ErrMsg("RegisterThreadToCpuBind %lu failed: ThreadCpuAutoBinder is not active.", tid));
        return DSTORE_SUCC;
    }
    switch (bindType) {
        case BindType::CPU_BIND:
            return g_storageInstance->GetThreadCpuAutoBinder()->RegisterThreadToCpuBind(tid, level);
        case BindType::NUMA_CPU_BIND:
            return g_storageInstance->GetThreadCpuAutoBinder()->RegisterThreadToNumaCpuBind(tid, thrdGroupName);
        case BindType::NUMA_BIND:
        default:
            /* to be done when needed  */
            break;
    }
    return DSTORE_FAIL;
}

RetStatus UnRegisterThreadToBind(ThreadId tid, BindType bindType, char *thrdGroupName)
{
    if (unlikely(!g_storageInstance || !(g_storageInstance->GetThreadCpuAutoBinder()))) {
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
            ErrMsg("RegisterThreadToCpuBind %lu failed: Dstore is not Init.", tid));
        return DSTORE_FAIL;
    }
    if (!g_storageInstance->GetThreadCpuAutoBinder()->IsActive()) {
        return DSTORE_SUCC;
    }
    switch (bindType) {
        case BindType::CPU_BIND:
            return g_storageInstance->GetThreadCpuAutoBinder()->UnRegisterThreadToCpuBind(tid);
        case BindType::NUMA_CPU_BIND:
            return g_storageInstance->GetThreadCpuAutoBinder()->UnRegisterThreadToNumaCpuBind(tid, thrdGroupName);
        case BindType::NUMA_BIND:
        default:
            /* to be done when needed  */
            break;
    }
    return DSTORE_FAIL;
}

}