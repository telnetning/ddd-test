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
 * dstore_wal_fault_injection.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_WAL_FAULT_INJECTION_H
#define DSTORE_WAL_FAULT_INJECTION_H

namespace DSTORE {
enum class DstoreWalFI {
    WAIT_PLSN_SLOT,
    WAIT_ITER_CUR_STREAM,
    WAIT_ITER_NEXT_STREAM,
    WAIT_DELETE_CUR_STREAM,
    WAIT_DELETE_NEXT_STREAM,
};

}

#endif  // STORAGE_WAL_FAULT_INJECTION_H