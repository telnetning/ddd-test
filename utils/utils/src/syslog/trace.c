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
 * Description: implement call chain tracing throughout a service process.
 */

#include "defines/time.h"
#include "port/posix_random.h"
#include "securec.h"

#include "syslog/trace.h"

typedef enum TraceIdValid {
    TRACE_ID_INVALID = 0,
    TRACE_ID_VALID = 1,
} TraceIdValid;

typedef struct TraceData {
    TraceId id;
    TraceIdValid valid;
} TraceData;

static THR_LOCAL TraceData g_traceData = {0};

static inline TraceData *GetThreadTraceData(void)
{
    return &g_traceData;
}

static inline bool TraceChainIsSameChainId(const TraceId *id1, const TraceId *id2)
{
    ASSERT(id1 != NULL);
    ASSERT(id2 != NULL);

    return (memcmp(id1, id2, sizeof(TraceId)) == 0);
}

static inline void TraceChainInitChainId(TraceId *id)
{
    ASSERT(id != NULL);
    id->chainId = 0;
}

static inline uint32_t TraceChainGenerateRandomNodeId(void)
{
    static uint32_t nodeId = 0;
    if (nodeId == 0) {
        nodeId = GetSafeRandomValue();
    }

    return nodeId;
}

static inline uint64_t TraceChainGetSequence(void)
{
    static uint64_t sequence = 0;
    if (sequence == 0) {
        sequence = GetTimeMsec(EPOCH_TIME, NULL);
    }
    __atomic_fetch_add(&sequence, 1, __ATOMIC_RELAXED);
    return sequence;
}

uint64_t TraceChainGenerateUID(void)
{
    // UID format: Node ID + Sequence ID
    // Node ID, highest 24 bit
    // Sequence ID, lowest  40 bit, which start with a times tamp
#define BIT_WIDTH_SEQUENCE 44
    uint64_t uid = TraceChainGetSequence();
    uid &= (1L << BIT_WIDTH_SEQUENCE) - 1;

    uint64_t nodeId = TraceChainGenerateRandomNodeId();
    uid |= nodeId << BIT_WIDTH_SEQUENCE;

    return uid;
}

void TraceChainBegin(const TraceId *id)
{
    TraceData *data = GetThreadTraceData();

    if (TraceChainIsTraceIdValid(id)) {
        data->id = *id;
    } else {
        TraceId newId;
        TraceChainInitChainId(&newId);
        newId.chainId = TraceChainGenerateUID();
        data->id = newId;
    }
    data->valid = TRACE_ID_VALID;
}

void TraceChainEnd(void)
{
    TraceData *data = GetThreadTraceData();
    TraceChainInitChainId(&data->id);
    data->valid = TRACE_ID_INVALID;
}

TraceId TraceChainGetId(void)
{
    TraceData *data = GetThreadTraceData();
    if (data->valid == TRACE_ID_VALID) {
        return data->id;
    }

    TraceId id;
    TraceChainInitChainId(&id);
    return id;
}

void TraceChainSetId(const TraceId *id)
{
    if (TraceChainIsTraceIdValid(id)) {
        TraceData *data = GetThreadTraceData();
        data->id = *id;
        data->valid = TRACE_ID_VALID;
    }
}

void TraceChainClearId(void)
{
    TraceData *data = GetThreadTraceData();
    TraceChainInitChainId(&data->id);
    data->valid = TRACE_ID_INVALID;
}

bool TraceChainIsTraceIdValid(const TraceId *id)
{
    if (id == NULL) {
        return false;
    }

    TraceId idInvalid;
    TraceChainInitChainId(&idInvalid);
    return (!TraceChainIsSameChainId(&idInvalid, id));
}
