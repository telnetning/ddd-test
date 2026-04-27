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
 * dstore_parallel_interface.h
 *
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/dstore/interface/framework/dstore_parallel_interface.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_PARALLEL_INTERFACE_H
#define DSTORE_PARALLEL_INTERFACE_H


namespace DSTORE {
class ParallelWorkController;
} // namespace DSTORE

namespace ParallelInterface {
#pragma GCC visibility push(default)

DSTORE::ParallelWorkController *CreateParallelWorkController(int smpNum);
void DestroyParallelWorkController(DSTORE::ParallelWorkController *controller);
void ResetParallelWorkController(DSTORE::ParallelWorkController *controller);

#pragma GCC visibility pop
} // namespace ParallelInterface
#endif
