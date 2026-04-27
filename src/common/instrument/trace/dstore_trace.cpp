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

#include <cstdlib>
#include <cstdint>

#include "common/instrument/trace/dstore_trace.h"


namespace DSTORE {

TraceFunc Trace::m_traceFunc = nullptr;

/**
 * Initialize trace by passing in a function pointer from the SQL engine.
 *
 * @param[in] func  Trace function pointer.
 */
void Trace::Initialize(TraceFunc func)
{
    Trace::m_traceFunc = func;
}

/**
 * Trace the entry of the given function
 *
 * @param[in] rec_id  The unique id for the given function.
 */
void Trace::Entry(const uint32_t recId)
{
    if (Trace::m_traceFunc != nullptr) {
        Trace::m_traceFunc(TraceType::TRACE_ENTRY, 0, recId, TraceDataFmt::TRC_DATA_FMT_NONE, nullptr, 0);
    }
}

/**
 * Trace the exit of the given function
 *
 * @param[in] rec_id  The unique id for the given function.
 */
void Trace::Exit(const uint32_t recId)
{
    if (Trace::m_traceFunc != nullptr) {
        Trace::m_traceFunc(TraceType::TRACE_EXIT, 0, recId, TraceDataFmt::TRC_DATA_FMT_NONE, nullptr, 0);
    }
}

/**
 * Trace data of the given function.
 *
 * @param[in] probe     A number that is used to identity the trace data record is from within the function.
 * @param[in] recId    The unique id for the given function
 * @param[in] fmtType  The display format of the data in the trace
 * @param[in] data      Data to be traced
 * @param[in] dataLen  Length of data to be traced
 */
void Trace::Data(const uint32_t probe, const uint32_t recId, const TraceDataFmt fmtType, const char *data,
                 size_t dataLen)
{
    if (Trace::m_traceFunc != nullptr) {
        Trace::m_traceFunc(TraceType::TRACE_DATA, probe, recId, fmtType, data, dataLen);
    }
}
}
