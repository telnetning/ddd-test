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
 * ---------------------------------------------------------------------------------------
 *
 * dstore_perf_counter_interface.h
 *
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/dstore/interface/framework/dstore_perf_counter_interface.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_PERF_COUNTER_INTERFACE_H
#define DSTORE_PERF_COUNTER_INTERFACE_H

#include <cstdint>

namespace PerfCounterInterface {
#pragma GCC visibility push(default)

using PerfItem = void *;

using Timer = void *;

PerfItem *CreatePerfItems(const char *groupName, uint8_t num);

Timer Start(PerfItem item);

void End(Timer timer);

#pragma GCC visibility pop
}  // namespace PerfCounterInterface

#endif