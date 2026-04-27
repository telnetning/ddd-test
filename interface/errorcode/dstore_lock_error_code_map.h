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

#ifndef DSTORE_LOCK_ERROR_CODE_MAP_H
#define DSTORE_LOCK_ERROR_CODE_MAP_H

#include "errorcode/dstore_lock_error_code.h"

namespace DSTORE {

static ErrorDetails g_lock_error_code_map[] = {
    [STORAGE_OK] = {"STORAGE_OK", ""},
    [ERROR_GET_CODE(LOCK_INFO_NOT_AVAIL)] =
        {"LOCK_INFO_NOT_AVAIL", "Lock is not available to be granted immediately"},
    [ERROR_GET_CODE(LOCK_ERROR_DEADLOCK)] =
        {"LOCK_ERROR_DEADLOCK", "Lock acquire fail due to deadlock occurrence"},
    [ERROR_GET_CODE(LOCK_ERROR_OUT_OF_MEMORY)] =
        {"LOCK_ERROR_OUT_OF_MEMORY", "Lock manager runs out of memory"},
    [ERROR_GET_CODE(LOCK_ERROR_MEMORY_OPERATE)] =
        {"LOCK_ERROR_MEMORY_OPERATE", "Lock manager memory operation failure"},
    [ERROR_GET_CODE(LOCK_INFO_WAITING)] =
        {"LOCK_INFO_WAITING", "Lock is waiting to be granted"},
    [ERROR_GET_CODE(LOCK_ERROR_INVALID_PARAMETER)] =
        {"LOCK_ERROR_INVALID_PARAMETER", "Input locking parameter is invalid"},
    [ERROR_GET_CODE(LOCK_ERROR_ADVISORY_NOT_HELD)] =
        {"LOCK_ERROR_ADVISORY_NOT_HELD", "Advisory lock is not held by current resource owner"},
    [ERROR_GET_CODE(LOCK_ERROR_WAIT_TIMEOUT)] =
        {"LOCK_ERROR_WAIT_TIMEOUT", "Lock has waited too long"},
    [ERROR_GET_CODE(LOCK_ERROR_WAIT_CANCELED)] =
        {"LOCK_ERROR_WAIT_CANCELED", "Lock is asked to cancel waiting immediately"},
    [ERROR_GET_CODE(LOCK_ERROR_THREAD_NOT_FOUND)] =
        {"LOCK_ERROR_THREAD_NOT_FOUND", "Unable to find the thread"},
    [ERROR_GET_CODE(XACTLOCK_ERROR_TRANSFER_LOCK_NOT_HELD)] =
        {"XACTLOCK_ERROR_TRANSFER_LOCK_NOT_HELD", "Xact lock is not held by anyone"},
    [ERROR_GET_CODE(LWLOCK_ERROR_NOT_AVAIL_NAME)] =
        {"LWLOCK_ERROR_NOT_AVAIL_NAME", "lock %s is not held"},
    [ERROR_GET_CODE(RWLOCK_WARNING_TOO_MANY_READERS_WRITERS)] =
        {"RWLOCK_WARNING_TOO_MANY_READERS_WRITERS",
         "CrossThreadRWLock has got too many readers/waiters, there may be unattended requests, parent mcxt: %s, "
         "grandparent mcxt: %s, readers = %d, waiters = %d"},
    [ERROR_GET_CODE(LOCK_WARNING_FREEZING)] =
        {"LOCK_WARNING_FREEZING", "Lock manager is in freeze state"},
    [ERROR_GET_CODE(LOCK_INFO_RELOCATING)] =
        {"LOCK_INFO_RELOCATING", "Lock manager is in relocating state"},
    [ERROR_GET_CODE(LOCK_INFO_LOCK_NOT_LOCAL)] =
        {"LOCK_INFO_LOCK_NOT_LOCAL", "Lock is not on local node"},
    [ERROR_GET_CODE(LOCK_INFO_RELOCATION_VALID_BUCKET)] =
        {"LOCK_INFO_RELOCATION_VALID_BUCKET", "Relocation receiver node has valid bucket %u"},
    [ERROR_GET_CODE(LOCK_INFO_ZONELOCK_TRANSFER_REJECTED)] =
        {"LOCK_INFO_ZONELOCK_TRANSFER_REJECTED", "Zone lock transfer was rejected. "},
    [ERROR_GET_CODE(LOCK_ERROR_NODE_FAIL)] =
        {"LOCK_ERROR_NODE_FAIL", "Lock acquire failed due to node (id=%u) failure"},
    [ERROR_GET_CODE(LOCK_ERROR_BUCKET_RELOCATION_FAILURE)] =
        {"LOCK_ERROR_BUCKET_RELOCATION_FAILURE", "Failed to relocate bucket %u"},
    [ERROR_GET_CODE(LOCK_ERROR_BUCKET_RECOVERY_IN_PROGRESS_FAILURE)] =
        {"LOCK_ERROR_BUCKET_RECOVERY_IN_PROGRESS_FAILURE",
         "Failed to recover bucket %u due to RecoveringInProgress check"},
    [ERROR_GET_CODE(LOCK_ERROR_BUCKET_RECOVERY_VALID_FAILURE)] =
        {"LOCK_ERROR_BUCKET_RECOVERY_VALID_FAILURE", "Failed to recover bucket %d due to bucket is valid"},
    [ERROR_GET_CODE(LOCK_ERROR_RPC_MESSAGE_PARSER_FAILURE)] =
        {"LOCK_ERROR_RPC_MESSAGE_PARSER_FAILURE", "Lock RPC message failed to parse."},
    [ERROR_GET_CODE(LOCK_ERROR_RECOVERY_FAILURE)] =
        {"LOCK_ERROR_RECOVERY_FAILURE", "Lock recovery failed."},
    [ERROR_GET_CODE(LOCK_ERROR_NOT_SUPPORTED)] =
        {"LOCK_ERROR_NOT_SUPPORTED", "Lock functionality not supported."},
    [ERROR_GET_CODE(LOCK_ERROR_SCAN_PASSED)] =
        {"LOCK_ERROR_SCAN_PASSED",
         "Lock recovery scanned node before lock or unlock was recorded. Need to retry request"},
    [ERROR_GET_CODE(LOCK_ERROR_WAIT_SPECIFY_TIMEOUT)] =
        {"LOCK_ERROR_WAIT_SPECIFY_TIMEOUT", "The lock wait time exceeds the timeout specified by the caller"},
};

static_assert(sizeof(g_lock_error_code_map) != LOCK_ERROR_MAX, "Error code map size doesn't match");

} /* namespace DSTORE */

#endif
