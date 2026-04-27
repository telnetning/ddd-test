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
 */

#ifndef UTILS_UT_ERR_LOG_COMMON_H
#define UTILS_UT_ERR_LOG_COMMON_H

#include "vfs/vfs_interface.h"

#define LOCAL_MODULE_NAME "ut_err_log"

bool IsFileContainedString(VirtualFileSystem *vfs, const char *fileFullPath, const char *substr, bool isRemoteErrLog);

void RemoveErrorLogFileFromLocalDirectory(VirtualFileSystem *vfs, const char *dirName);

ErrorCode GetErrorLogFileFullPath(char *fileFullPath, Size len, void (*getLogDirectory)(char *logDirectory, Size len),
                                  char *fileSuffixStr, char *fileName);

int GetLogFileLineNum(const char *fileName);

#endif /* UTILS_UT_ERR_LOG_COMMON_H */
