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

#ifndef UTILS_COMMON_ERR_LOG_FLOW_CONTROL_H
#define UTILS_COMMON_ERR_LOG_FLOW_CONTROL_H

#include <stdbool.h>

#include "defines/common.h"
#include "port/platform_port.h"
#include "syslog/err_log_fold.h"
#include "syslog/err_log.h"

GSDB_BEGIN_C_CODE_DECLS

#define MSEC_PRE_SEC 1000

typedef struct LogFlowContext LogFlowContext;
struct LogFlowContext {
    int filterLevel;
    uint64_t flowControlThreshold;
    uint64_t logStatPeriod;
};

LogFlowContext *LogFlowInitContext(uint64_t flowControlThreshold, int filterLevel, uint64_t logStatPeriod);

void LogFlowFreeContext(LogFlowContext *context);

bool LogFlowSetRule(LogFlowContext *context, uint64_t flowControlThreshold, int filterLevel, uint64_t logStatPeriod);

bool IsNeededLogFlowControl(LogFlowContext *context, const LogIdentifier *logIdentifier,
                            struct LogStatTime *logStatTime, uint32_t *count);

bool IsNeedLogFilter(LogFlowContext *context, const LogIdentifier *logIdentifier);

GSDB_END_C_CODE_DECLS

#endif /* UTILS_COMMON_ERR_LOG_FLOW_CONTROL_H */