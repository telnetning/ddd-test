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

#ifndef DSTORE_TRACE_H
#define DSTORE_TRACE_H

#include <cstddef>
#include <cstdint>
#include "common/dstore_common_utils.h"

namespace DSTORE {

#ifdef DSTORE_USE_ASSERT_CHECKING
#define storage_trace_entry Trace::Entry
#define storage_trace_exit  Trace::Exit
#define storage_trace_data  Trace::Data
#else
#define storage_trace_entry(arg1)
#define storage_trace_exit(arg1)
#define storage_trace_data(...)
#endif
#define STORAGE_TRACE_ID_PACK(_module, _func) ((_func) | ((static_cast<int>(_module)) << 0x10))


enum struct TraceType {
    TRACE_ENTRY,
    TRACE_EXIT,
    TRACE_DATA
};

enum struct TraceDataFmt {
    TRC_DATA_FMT_NONE,  /* no data in this trace record */
    TRC_DATA_FMT_DFLT,  /* data is stored with HEX format */
    TRC_DATA_FMT_UINT32 /* data is stored as uint32 */
};

using TraceFunc = void (*)(const TraceType type, const uint32_t probe, const uint32_t recId,
                           const TraceDataFmt fmtType, const char *data, size_t dataLen);

/**
 * This is a trace utility class that allows the storage engine
 * to hook up to the existing GS Trace facility by accepting
 * a function pointer to gstrace_internal().
 *
 * Usage:
 * 1. Call Trace::Initialize(&GstraceStorage) once as part of storage engine initialization.
 * 2. Each function in the storage engine must call storage_trace_entry()/storage_trace_exit(),
 *    optionally, storage_trace_data(). These functions are equivalent to the existing
 *    gstrace_entry()/gstrace_exit()/gstrace_data().
 * 3. All the valid values for rec_id are generated from
 *    <repo>/src/gausskernel/dstore/src/trace/storage_*.in,
 *    which need to be edited when a new function or component is added.
 */
class Trace {
    Trace() = delete;
    ~Trace() = default;

    DISALLOW_COPY_AND_MOVE(Trace);

public:
    static void Initialize(TraceFunc func);
    static void Entry(const uint32_t recId);
    static void Exit(const uint32_t recId);
    static void Data(const uint32_t probe, const uint32_t recId, const TraceDataFmt fmtType, const char *data,
                     size_t dataLen);

    static TraceFunc m_traceFunc;
};
constexpr uint32_t TRACE_PROBE_ID_1 = 1;
constexpr uint32_t TRACE_PROBE_ID_2 = 2;
constexpr uint32_t TRACE_PROBE_ID_3 = 3;
}

/* This is the signature to be matched by the gstrace function outside of the storage engine */
#ifdef DECOUPLE_STORAGE_ENGINE
extern void gstrace_storage(const DSTORE::TraceType type, const uint32_t probe, const uint32_t recId,
                            const DSTORE::TraceDataFmt fmtType, const char *pData, size_t dataLen);
#endif

#endif  // STORAGE_TRACE_H
