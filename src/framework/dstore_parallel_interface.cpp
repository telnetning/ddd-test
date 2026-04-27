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
 * dstore_parallel_interface.cpp
 *
 * IDENTIFICATION
 *        storage/src/framework/dstore_parallel_interface.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "framework/dstore_parallel.h"
#include "framework/dstore_thread.h"
#include "common/memory/dstore_mctx.h"
#include "framework/dstore_instance.h"

namespace ParallelInterface {
using namespace DSTORE;

ParallelWorkController *CreateParallelWorkController(int smpNum)
{
    return DstoreNew(g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_QUERY))
        ParallelWorkController(smpNum);
}

void DestroyParallelWorkController(ParallelWorkController *controller)
{
    if (controller != nullptr) {
        delete controller;
        controller = nullptr;
    }
}

void ResetParallelWorkController(ParallelWorkController *controller)
{
    if (controller != nullptr) {
        controller->ResetWorkSource();
    }
}
}