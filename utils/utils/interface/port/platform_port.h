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
 * platform_port.h
 *
 * Description:
 * Defines the cross platform porting interfaces.
 *
 * ---------------------------------------------------------------------------------
 */
#ifndef UTILS_PLATFORM_PORT_H
#define UTILS_PLATFORM_PORT_H

#if defined(_WIN32) || defined(_WIN64) || defined(__CYGWIN__) || defined(__MINGW32__) || defined(__BORLANDC__)
#define WINDOWS_PLATFORM
#endif

#if defined(WINDOWS_PLATFORM)
#include "win32_spin.h"
#include "win32_rwlock.h"
#include "win32_errcode.h"
#include "win32_execinfo.h"
#include "win32_io.h"
#include "win32_mutex.h"
#include "win32_path.h"
#include "win32_random.h"
#include "win32_syslog.h"
#include "win32_thread.h"
#include "win32_time.h"
#include "win32_atomic.h"
#include "win32_page.h"
#include "win32_semaphore.h"
#else

#include "posix_spin.h"
#include "posix_rwlock.h"
#include "posix_errcode.h"
#include "posix_execinfo.h"
#include "posix_io.h"
#include "posix_mutex.h"
#include "posix_path.h"
#include "posix_random.h"
#include "posix_signal.h"
#include "posix_syslog.h"
#include "posix_thread.h"
#include "posix_time.h"
#include "posix_atomic.h"
#include "posix_page.h"
#include "port_linux_semaphore.h"

#endif

#endif /* UTILS_PLATFORM_PORT_H */
