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
 * ---------------------------------------------------------------------------------
 *
 * cluster_defs.h
 *
 * Description:
 * This file defines the cluster-related macros
 *
 * ---------------------------------------------------------------------------------
 */
#ifndef CLUSTER_DEFS_H
#define CLUSTER_DEFS_H

#include "types/data_types.h"

GSDB_BEGIN_C_CODE_DECLS

/* Ensure that each module uses the same definition. */
#define INVALID_CLUSTER_ID UINT32_MAX

/* We support up to 5 regional clusters. */
#define MAX_CLUSTER_COUNT 5

/* We support up to 4 dr clusters. */
#define MAX_DR_CLUSTER_COUNT (MAX_CLUSTER_COUNT - 1)

GSDB_END_C_CODE_DECLS

#endif /* CLUSTER_DEFS_H */