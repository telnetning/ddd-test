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

#ifndef DSTORE_WAL_ERROR_CODE_MAP_H
#define DSTORE_WAL_ERROR_CODE_MAP_H

#include "errorcode/dstore_wal_error_code.h"

namespace DSTORE {

static ErrorDetails g_wal_error_code_map[] = {
    [STORAGE_OK] = {"STORAGE_OK", ""},
    [ERROR_GET_CODE(WAL_ERROR_INIT_ERROR)] =
        {"WAL_ERROR_INIT_ERROR", "Wal module initialize failed"},
    [ERROR_GET_CODE(WAL_ERROR_INIT_ALLOC_OOM)] =
        {"WAL_ERROR_INIT_ALLOC_OOM", "Wal module init with allocate space OOM"},
    [ERROR_GET_CODE(WAL_ERROR_NOT_INITED)] =
        {"WAL_ERROR_NOT_INITED", "Wal module don't work for not inited"},
    [ERROR_GET_CODE(WAL_ERROR_INVALID_DATA)] =
        {"WAL_ERROR_INVALID_DATA", "invaild data in atomic Wal write process"},
    [ERROR_GET_CODE(WAL_ERROR_WRITER_DROP_NOT_ENDED_DATA)] =
        {"WAL_ERROR_WRITER_DROP_NOT_ENDED_DATA", "Wal atomic writer drop not ended atomic log "},
    [ERROR_GET_CODE(WAL_ERROR_FLUSH_INVALID_DATA)] =
        {"WAL_ERROR_FLUSH_INVALID_DATA", "Wal bgwriter get invalid data from buffer"},
    [ERROR_GET_CODE(WAL_ERROR_INVALID_PARAM)] =
        {"WAL_ERROR_INVALID_PARAM", "Wal Module get invalid param"},
    [ERROR_GET_CODE(WAL_ERROR_STRING_OPS)] =
        {"WAL_ERROR_STRING_OPS", "Wal File operate string fail"},
    [ERROR_GET_CODE(WAL_ERROR_INTERNAL_ERROR)] =
        {"WAL_ERROR_INTERNAL_ERROR", "Wal internal error"},
    [ERROR_GET_CODE(WAL_ERROR_PANIC_INTERNAL_ERROR)] =
        {"WAL_ERROR_PANIC_INTERNAL_ERROR", "Wal internal panic"}, // todo actual this should be panic error level
    [ERROR_GET_CODE(WAL_ERROR_BUFFER_INIT_MEMORY_NOT_ENOUGH)] =
        {"WAL_ERROR_BUFFER_INIT_MEMORY_NOT_ENOUGH", "Wal buffer memory not enough when init"},
    [ERROR_GET_CODE(WAL_ERROR_STREAM_COUNT_EXCEED_LIMIT)] =
        {"WAL_ERROR_STREAM_COUNT_EXCEED_LIMIT", "Wal stream count exceed limit"},
    [ERROR_GET_CODE(WAL_ERROR_STREAM_NOT_FOUND)] =
        {"WAL_ERROR_STREAM_NOT_FOUND", "The corresponding Wal Stream cannot be found based on the specified WalId."},
    [ERROR_GET_CODE(WAL_ERROR_UNREACHABLE_CODE)] =
        {"WAL_ERROR_UNREACHABLE_CODE", "Wal Module reached an unreachable location."},
    [ERROR_GET_CODE(WAL_ERROR_WALID_INVALID)] =
        {"WAL_ERROR_WALID_INVALID", "The given WalId is invalid, there is no corresponding Wal stream."},
    [ERROR_GET_CODE(WAL_ERROR_READ_ERROR)] =
        {"WAL_ERROR_READ_ERROR", "The read content of target Wal group is incorrect."},
    [ERROR_GET_CODE(WAL_ERROR_REJECT_REDO_TASK)] =
        {"WAL_ERROR_REJECT_REDO_TASK", "Reject execute redo task"},
    [ERROR_GET_CODE(WAL_ERROR_FORCE_STOP)] =
        {"WAL_ERROR_FORCE_STOP", "Standby redo force exit."},
    [ERROR_GET_CODE(WAL_ERROR_STANDBY_TAKE_OVER)] =
        {"WAL_ERROR_STANDBY_TAKE_OVER", "Standby take over wal stream."},
    [ERROR_GET_CODE(WAL_ERROR_STOP_STREAM)] =
        {"WAL_ERROR_STOP_STREAM", "Standby stop one wal stream."},
};

static_assert(sizeof(g_wal_error_code_map) != WAL_ERROR_MAX, "Error code map size doesn't match");

} /* namespace DSTORE */

#endif
