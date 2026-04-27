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
 * Description: Inter-service call chain tracing
 */

#ifndef UTILS_TRACE_H
#define UTILS_TRACE_H

#include "types/data_types.h"

GSDB_BEGIN_C_CODE_DECLS

typedef struct TraceId TraceId;
struct TraceId {
    uint64_t chainId;
} __attribute__((packed));

/*
 * Begin trace. If the user passes in a valid TraceId, it will be used directly.
 * Otherwise, the system will generate a unique ID for Trace.
 * When the tracing operation is not required, invoke TraceChainEnd to end the tracing.
 * @param id[in], the trace ID.
 *   If the id is NULL, we will generate a unique TraceID by random.
 *   If the id is no NULL, we will try to use it directly.
 */
UTILS_EXPORT void TraceChainBegin(const TraceId *id);

/*
 * End trace.
 * It is used together with the TraceChainBegin to end the tracing.
 */
UTILS_EXPORT void TraceChainEnd(void);

/*
 * Get trace ID
 * @return, a valid trace ID if success, otherwise invalid
 */
UTILS_EXPORT TraceId TraceChainGetId(void);

/*
 * Set a trace ID
 * @param id[in], the trace ID
 */
UTILS_EXPORT void TraceChainSetId(const TraceId *id);

/*
 * Clear trace ID of thread local
 * This interface is used together with TraceChainSetId to end the tracing.
 */
UTILS_EXPORT void TraceChainClearId(void);

// internal interface
/*
 * Check the trace ID is valid or not
 * @param id[in], the trace ID
 * @return, true if valid, otherwise false.
 */
bool TraceChainIsTraceIdValid(const TraceId *id);

/*
 * Generate UID
 * @return, an 8-byte unique ID
 */
uint64_t TraceChainGenerateUID(void);

GSDB_END_C_CODE_DECLS

#endif
