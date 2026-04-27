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
#include "ut_trace/ut_trace.h"

using namespace DSTORE;

static char trace_buffer[1024] = "";

const char * ut_trace_get_type_string(TraceType type) {
    const char *type_str = nullptr;

    switch(type) {
        case TraceType::TRACE_ENTRY:
           type_str = "TRACE_ENTRY";
           break;

        case TraceType::TRACE_EXIT:
           type_str = "TRACE_EXIT";
           break;

        case TraceType::TRACE_DATA:
           type_str = "TRACE_DATA";
           break;

        default:
           type_str = "ERROR";
           break;
    }

    return type_str;
}

const char * ut_trace_get_format_string(TraceDataFmt fmt) {
    const char *fmt_str = nullptr;

    switch(fmt) {
        case TraceDataFmt::TRC_DATA_FMT_NONE:   /* no data in this trace record */
           fmt_str = "TRC_DATA_FMT_NONE";
           break;

        case TraceDataFmt::TRC_DATA_FMT_DFLT:   /* data is stored with HEX format */
           fmt_str = "TRC_DATA_FMT_DFLT";
           break;

        case TraceDataFmt::TRC_DATA_FMT_UINT32: /* data is stored as uint32 */
           fmt_str = "TRC_DATA_FMT_UINT32";
           break;

        default:
           fmt_str = "ERROR";
           break;
    }

    return fmt_str;
}

void ut_trace(const TraceType type, const uint32_t probe, const uint32_t rec_id, const TraceDataFmt fmt_type, const char* data, size_t data_len) {
    sprintf(trace_buffer,
        "type = %s, probe = %d, rec_id = %d, fmt_type = %s, data = %*s",
        ut_trace_get_type_string(type), probe, rec_id, ut_trace_get_format_string(fmt_type), data_len, data);
}

TEST_F(TraceTest, TraceEntryTest) {
    Trace::Initialize(&ut_trace);
    storage_trace_entry(123);
    ASSERT_STREQ(trace_buffer, "type = TRACE_ENTRY, probe = 0, rec_id = 123, fmt_type = TRC_DATA_FMT_NONE, data = (null)");
}

TEST_F(TraceTest, TraceExitTest) {
    Trace::Initialize(&ut_trace);
    storage_trace_exit(123);
    ASSERT_STREQ(trace_buffer, "type = TRACE_EXIT, probe = 0, rec_id = 123, fmt_type = TRC_DATA_FMT_NONE, data = (null)");
}

TEST_F(TraceTest, TraceDataTest) {
    Trace::Initialize(&ut_trace);
    storage_trace_data(100, 123, TraceDataFmt::TRC_DATA_FMT_DFLT, "abc", strlen("abc"));
    ASSERT_STREQ(trace_buffer, "type = TRACE_DATA, probe = 100, rec_id = 123, fmt_type = TRC_DATA_FMT_DFLT, data = abc");
}
