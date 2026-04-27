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

#ifndef DSTORE_LOGICAL_REPLICATION_ERROR_CODE_MAP_H
#define DSTORE_LOGICAL_REPLICATION_ERROR_CODE_MAP_H

#include "errorcode/dstore_logical_replication_error_code.h"

namespace DSTORE {

static ErrorDetails g_logical_replication_error_code_map[] = {
    [STORAGE_OK] = {"STORAGE_OK", ""},
    [ERROR_GET_CODE(DECODE_INFO_PLUGIN_LOAD_ERROR)] =
        {"DECODE_INFO_PLUGIN_LOAD_ERROR", "plugin load error"},
    [ERROR_GET_CODE(DECODE_ERROR_DICT_NOT_FOUND)] =
        {"DECODE_ERROR_DICT_NOT_FOUND", "decode dict found null. trx: (xid: %lu, snapshotCsn: &lu)"},
    [ERROR_GET_CODE(DECODE_ERROR_UNSUPPORT_TYPE)] =
        {"DECODE_ERROR_UNSUPPORT_TYPE", "decode unsurpported type (only decode dml)"},
    [ERROR_GET_CODE(DECODE_ERROR_EMPTY_TRX)] =
        {"DECODE_ERROR_EMPTY_TRX", "decode an empty commit trx"},
    [ERROR_GET_CODE(DECODE_ERROR_UNSUPPORTED_ATTR_TYPE)] =
        {"DECODE_ERROR_UNSUPPORTED_ATTR_TYPE", "decode an unsurpported attr type, typeOid: u%"},
    [ERROR_GET_CODE(DECODE_ERROR_CONTEXT_CREATE_ERROR)] =
        {"DECODE_ERROR_CONTEXT_CREATE_ERROR", "decode context create error"},
    [ERROR_GET_CODE(DECODE_ERROR_DUPLICATE_SLOT)] =
        {"DECODE_ERROR_DUPLICATE_SLOT", "replication slot \"%s\" already exists"},
    [ERROR_GET_CODE(DECODE_ERROR_SLOT_EXCEED_MAX_NUM)] =
        {"DECODE_ERROR_SLOT_EXCEED_MAX_NUM", "all replication slots are in use"},
    [ERROR_GET_CODE(DECODE_ERROR_SLOT_NOT_FOUND)] =
        {"DECODE_ERROR_SLOT_NOT_FOUND", "replication slot \"%s\" does not exist"},
    [ERROR_GET_CODE(DECODE_ERROR_SLOT_IS_ACTIVE)] =
        {"DECODE_ERROR_SLOT_IS_ACTIVE", "replication slot is in active"},
    [ERROR_GET_CODE(DECODE_ERROR_DECODE_HANDLER_NO_MEM)] =
        {"DECODE_ERROR_DECODE_HANDLER_NO_MEM", "no memory for logical decode handler"},
    [ERROR_GET_CODE(DECODE_ERROR_DECODE_CTX_INIT_FAIL)] =
        {"DECODE_ERROR_DECODE_CTX_INIT_FAIL", "logical decode context init failed"},
    [ERROR_GET_CODE(DECODE_ERROR_DISTRIBUTE_DECODE_HANDLER_EMPTY)] =
        {"DECODE_ERROR_DISTRIBUTE_DECODE_HANDLER_EMPTY", "logical decode handler empty when get start/stop signal"},
};

static_assert(sizeof(g_logical_replication_error_code_map) != DECODE_ERROR_MAX, "Error code map size doesn't match");

} /* namespace DSTORE */

#endif
