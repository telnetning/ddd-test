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
 * Memory barrier operations.
 */
#ifndef DSTORE_BARRIER_H
#define DSTORE_BARRIER_H


#include <atomic>
#include "lock/dstore_s_lock.h"

namespace DSTORE {
/*
 * A load operation with this memory order performs an acquire operation,
 * a store performs a release operation, and read-modify-write performs both
 * an acquire operation and a release operation, plus a single total order exists
 * in which all threads observe all modifications in the same order
 */
#define GS_MEMORY_BARRIER() std::atomic_thread_fence(std::memory_order_seq_cst)
/*
 * A load operation with this memory order performs the acquire operation
 * on the affected memory location: no reads or writes in the current thread
 * can be reordered before this load. All writes in other threads that
 * release the same atomic variable are visible in the current thread.
 */
#define GS_READ_BARRIER() std::atomic_thread_fence(std::memory_order_acquire)
/*
 * A store operation with this memory order performs the release operation:
 * no reads or writes in the current thread can be reordered after this store.
 * All writes in the current thread are visible in other threads that
 * acquire the same atomic variable.
 */
#define GS_WRITE_BARRIER() std::atomic_thread_fence(std::memory_order_release)

}
#endif /* DSTORE_BARRIER_H */