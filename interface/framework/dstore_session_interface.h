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
 * dstore_session_interface.h
 *
 *
 *
 * IDENTIFICATION
 *        interface/framework/dstore_session_interface.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_SESSION_INTERFACE_H
#define DSTORE_SESSION_INTERFACE_H

#include "systable/dstore_relation.h"

namespace DSTORE {

#pragma GCC visibility push(default)
class StorageSession;
StorageSession *CreateStorageSession(uint64_t sessionId, int32_t bufNums = 0);
void CleanUpSession(StorageSession *session);
#pragma GCC visibility pop
}  /* namespace DSTORE */
#endif