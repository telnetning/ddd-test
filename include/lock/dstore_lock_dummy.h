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

#ifndef DSTORE_LOCK_DUMMY_H
#define DSTORE_LOCK_DUMMY_H

#include <stddef.h>
#include <cstdlib>

#include "common/dstore_datatype.h"
#include "common/instrument/dstore_stat.h"
#include "lock/dstore_lwlock.h"

namespace DSTORE {

#define HOLD_INTERRUPTS()
#define RESUME_INTERRUPTS()
#define CHECK_FOR_INTERRUPTS()

#define TRACE_POSTGRESQL_LWLOCK_WAIT_START(a, b)
#define TRACE_POSTGRESQL_LWLOCK_WAIT_DONE(a, b)
#define TRACE_POSTGRESQL_LWLOCK_ACQUIRE(a, b)
#define TRACE_POSTGRESQL_LWLOCK_CONDACQUIRE_FAIL(a, b)
#define TRACE_POSTGRESQL_LWLOCK_CONDACQUIRE(a, b)
#define TRACE_POSTGRESQL_LWLOCK_WAIT_UNTIL_FREE_FAIL(a, b)
#define TRACE_POSTGRESQL_LWLOCK_WAIT_UNTIL_FREE(a, b)
#define TRACE_POSTGRESQL_LWLOCK_RELEASE(a)

}

#endif
