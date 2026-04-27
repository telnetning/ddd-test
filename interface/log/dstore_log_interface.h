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
 * dstore_log_interface.h
 *
 *
 *
 * IDENTIFICATION
 *        interface/log/dstore_log_interface.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_LOG_INTERFACE
#define DSTORE_LOG_INTERFACE

namespace StorageLogInterface {
#pragma GCC visibility push(default)

bool IsLogStarted();
void SetLogLevelAndFoldParam(int logLevel, int foldPeriod, int foldThreshold, int foldLevel);
void InitLogAdapterInstance(int logLevel, const char* logDir, int foldPeriod, int foldThreshold, int foldLevel);
void StopLogAdapterInstance();
void OpenLoggerThread();
void CloseLoggerThread();

#pragma GCC visibility pop
}

#endif