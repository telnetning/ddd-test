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
#ifndef DSTORE_AUTOBINDER_INTERFACE_H
#define DSTORE_AUTOBINDER_INTERFACE_H
#include "common/dstore_common_utils.h"

namespace DSTORE {
#pragma GCC visibility push(default)
/* If the number of registered threads is greater than the number of CPUs, high-level threads will be bound first. */
enum class CoreBindLevel {
    LOW,
    HIGH
};

enum class BindType {
    CPU_BIND,
    NUMA_CPU_BIND,
    NUMA_BIND
};

/* note: the thread calling RegisterThreadToBind must call UnRegisterThreadToBind before exiting!! */
RetStatus RegisterThreadToBind(ThreadId tid, BindType bindType = BindType::CPU_BIND,
                               CoreBindLevel level = CoreBindLevel::LOW, char *thrdGroupName = nullptr);

RetStatus UnRegisterThreadToBind(ThreadId tid, BindType bindType = BindType::CPU_BIND, char *thrdGroupName = nullptr);

#pragma GCC visibility pop
}

#endif