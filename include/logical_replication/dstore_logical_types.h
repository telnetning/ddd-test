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
 *
 *
 * IDENTIFICATION
 *        storage/include/logical_replication/dstore_logical_types.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_LOGICAL_TYPES_H
#define DSTORE_LOGICAL_TYPES_H

#include "common/dstore_datatype.h"

namespace DSTORE {

/* distribute lock id used for logical meta operation */
constexpr uint32 LOGICAL_META_LOCK_ID = 1;

/* distribute lock id used for logical slots operation */
constexpr uint32 LOGICAL_SLOT_LOCK_ID = 2;

/* tmp, Max parallel decode number, will move to GUC */
constexpr uint32 MAX_DECODE_WORKER_NUM = 5;

const uint16 LOGICAL_DECODE_OUTPUT = 8;

}
#endif