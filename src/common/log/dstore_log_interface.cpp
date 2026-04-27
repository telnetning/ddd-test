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
 * dstore_log_interface.cpp
 *
 *
 * IDENTIFICATION
 *        src/common/log/dstore_log_interface.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "common/log/dstore_log.h"
#include "log/dstore_log_interface.h"

namespace StorageLogInterface {

bool IsLogStarted()
{
    return DSTORE::IsLogStarted();
}

void SetLogLevelAndFoldParam(int logLevel, int foldPeriod, int foldThreshold, int foldLevel)
{
    DSTORE::SetLogLevelAndFoldParam(logLevel, foldPeriod, foldThreshold, foldLevel);
}

void InitLogAdapterInstance(int logLevel, const char* logDir, int foldPeriod, int foldThreshold, int foldLevel)
{
    DSTORE::InitLogAdapterInstance(logLevel, logDir, foldPeriod, foldThreshold, foldLevel);
}

void StopLogAdapterInstance()
{
    DSTORE::StopLogAdapterInstance();
}

void OpenLoggerThread()
{
    DSTORE::OpenLoggerThread();
}

void CloseLoggerThread()
{
    DSTORE::CloseLoggerThread();
}

}  // namespace StorageLogInterface
