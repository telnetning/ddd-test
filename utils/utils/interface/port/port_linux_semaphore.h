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
 * port_linux_semaphore.h
 *
 * Description:
 * Compile selection header file for Posix semaphore and System v semaphore on Linux.
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef UTILS_PORT_LINUX_SEMAPHORE_H
#define UTILS_PORT_LINUX_SEMAPHORE_H

#if defined(USE_POSIX_SEMAPHORES)
#include "port/posix_semaphore.h"
#else
#include "port/posix_system_v_semaphore.h"
#endif

GSDB_BEGIN_C_CODE_DECLS
/*
 * On the Linux platform, semaphore has two implementation mechanisms: Posix and System v.
 * You can specify the compilation macro during compilation to select the implementation.
 * The system v implementation is preferred by default.
 */
GSDB_END_C_CODE_DECLS

#endif /* UTILS_PORT_LINUX_SEMAPHORE_H */
