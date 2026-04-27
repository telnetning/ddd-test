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
 * This file defines the data for the core system tables.
 */
#ifndef DSTORE_SYSTABLE_PRESET_H
#define DSTORE_SYSTABLE_PRESET_H

#include <cstdint>
#include "common/dstore_common_utils.h"
#include "systable/dstore_systable_struct.h"
#include "systable/systable_attribute.h"
#include "systable/systable_relation.h"
namespace DSTORE {

extern const char* PRESET_SYS_ATTR_ROWS[][NATTS_SYS_ATTRIBUTE] ;
extern const int PRESET_SYS_ATTR_CNT;
extern const char* PRESET_SYS_RELATION_ROWS[][NATTS_SYS_REL + 1];
extern const int PRESET_SYS_RELATION_CNT;
}  // namespace DSTORE
#endif