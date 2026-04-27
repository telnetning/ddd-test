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
 * systable_callback_param.h
 *
 * IDENTIFICATION
 *        interface/systable/systable_callback_param.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SYSTABLE_CALLBACK_PARAM_H
#define SYSTABLE_CALLBACK_PARAM_H

#include <functional>
#include "page/dstore_page_struct.h"
#include "tuple/dstore_tuple_struct.h"

namespace DSTORE {
using PreSetFunc = std::function<void(TupleDesc &, HeapTuple **)>;
using CountFunc = std::function<int(void)>;
using BuildRelCache = std::function<void(const char *const &, const Oid &, TupleDesc &, PageId &)>;
struct SysTablePageIdMap;
using IndexRebuilFunc = std::function<void (TablespaceId, SysTablePageIdMap *, unsigned int)>;
}  // namespace DSTORE

#endif