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
 * dstore_heap_fault_injection.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_HEAP_FAULT_INJECTION_H
#define DSTORE_HEAP_FAULT_INJECTION_H

namespace DSTORE {
enum class DstoreHeapFI {
    CONSTRUCT_CR_BEFORE_READ_BASE_PAGE,
    CONSTRUCT_CR_AFTER_READ_BASE_PAGE,
    CONSTRUCT_CR_PAGE,
    READY_TO_WAIT_TRX_END,
};

}

#endif  // STORAGE_HEAP_FAULT_INJECTION_H