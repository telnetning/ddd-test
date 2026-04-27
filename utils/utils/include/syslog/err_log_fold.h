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

#ifndef UTILS_COMMON_ERR_LOG_FOLD_H
#define UTILS_COMMON_ERR_LOG_FOLD_H

#include <stdbool.h>

#include "defines/common.h"
#include "port/platform_port.h"
#include "memory/memory_ctx.h"

GSDB_BEGIN_C_CODE_DECLS

#define FILE_NAME_MAX_LEN 60

typedef struct LogIdentifier LogIdentifier;
struct LogIdentifier {
    uint64_t threadId;
    int lineNum;
    int logLevel;
    time_t timeSecond;
    char fileName[FILE_NAME_MAX_LEN];
};

typedef struct LogFoldContent LogFoldContent;
struct LogFoldContent {
    uint64_t foldCount;
    char *msgBuf;
    size_t msgLen;
};

typedef struct LogFoldContext LogFoldContext;

LogFoldContext *LogFoldAllocContext(MemoryContext memoryContext, int foldLevel, uint32_t foldThreshold,
                                    uint32_t foldPeriod);

void LogFoldFreeContext(LogFoldContext *context);

bool LogFoldSetRule(LogFoldContext *context, int foldLevel, uint32_t foldThreshold, uint32_t foldPeriod);

bool LogFoldIsNeedFold(LogFoldContext *context, const LogIdentifier *logIdentifier, char *msgBuf, size_t msgLen,
                       LogFoldContent *logFoldContent);

uint64_t FoldLogGetContent(LogFoldContext *context, bool checkTime, time_t curTime, LogFoldContent *contentArray,
                           size_t arrayLen);

GSDB_END_C_CODE_DECLS

#endif /* UTILS_COMMON_ERR_LOG_FOLD_H */
