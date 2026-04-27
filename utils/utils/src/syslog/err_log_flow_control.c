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
 * Description: Error log flow control implementation
 */

#include "syslog/err_log_flow_control.h"

LogFlowContext *LogFlowInitContext(uint64_t flowControlThreshold, int filterLevel, uint64_t logStatPeriod)
{
    if (filterLevel > WARNING || filterLevel <= 0) {
        return NULL;
    }
    LogFlowContext *context = (LogFlowContext *)malloc(sizeof(LogFlowContext));
    if (unlikely(context == NULL)) {
        return NULL;
    }
    context->flowControlThreshold = flowControlThreshold;
    context->filterLevel = filterLevel;
    context->logStatPeriod = logStatPeriod;
    return context;
}

void LogFlowFreeContext(LogFlowContext *context)
{
    if (context != NULL) {
        free(context);
    }
}

bool LogFlowSetRule(LogFlowContext *context, uint64_t flowControlThreshold, int filterLevel, uint64_t logStatPeriod)
{
    if (context == NULL || filterLevel > WARNING || filterLevel <= 0) {
        return false;
    }
    context->flowControlThreshold = flowControlThreshold;
    context->filterLevel = filterLevel;
    context->logStatPeriod = logStatPeriod;
    return true;
}

/*
 * Log flow control.
 */
bool IsNeededLogFlowControl(LogFlowContext *context, const LogIdentifier *logIdentifier,
                            struct LogStatTime *logStatTime, uint32_t *count)
{
    if (context == NULL || context->logStatPeriod == 0 || context->flowControlThreshold == 0 || logIdentifier == NULL) {
        /* Log flow control is not active or parameters invalid, do not log flow contorl */
        return false;
    }
    (*count)++;
    uint64_t accuTime = logStatTime->curStatTime - logStatTime->preStatTime;
    if (context->logStatPeriod != 0 && accuTime >= context->logStatPeriod) {
        uint32_t tmpCount = *count;
        *count = 0;

        if (tmpCount * MSEC_PRE_SEC > context->flowControlThreshold * accuTime) {
            return true;
        }
    }
    return false;
}

bool IsNeedLogFilter(LogFlowContext *context, const LogIdentifier *logIdentifier)
{
    if (logIdentifier == NULL || context == NULL) {
        return false;
    }
    if (logIdentifier->logLevel <= context->filterLevel) {
        return true;
    }
    return false;
}