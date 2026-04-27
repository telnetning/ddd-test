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
 * Description: storage stat interface file.
 */

#ifndef DSTORE_STAT_INTERFACE_H
#define DSTORE_STAT_INTERFACE_H

#include <stdint.h>
#include <functional>
#include "dstore_instr_time.h"
#include "framework/dstore_stat_pgstat.h"
#include "framework/dstore_wait_state.h"
#include "framework/dstore_io_event.h"
#include "systable/dstore_relation.h"

namespace DSTORE {
#define OPTUTIL_GSSTAT_WAIT_EVENT_END 0x00000000U
#define OPTUTIL_GSSTAT_WAIT_LWLOCK 0x01000000U
#define OPTUTIL_GSSTAT_WAIT_LOCK 0x03000000U
#define OPTUTIL_GSSTAT_WAIT_IO 0x0A000000U
#define OPTUTIL_GSSTAT_WAIT_SQL 0x0B000000U
#define OPTUTIL_GSSTAT_WAIT_STATE 0x0C000000U

typedef struct StmtLockTag {
    uint32_t field1;      /* a 32-bit ID field */
    uint32_t field2;      /* a 32-bit ID field */
    uint32_t field3;      /* a 32-bit ID field */
    uint32_t field4;      /* a 32-bit ID field */
    uint32_t field5;      /* a 16-bit ID field */
    uint16_t type;        /* see enum LockTagType */
} StmtLockTag;

using ReportWaitEventCallback = std::function<void(uint32_t)>;
using ReportWaitStatusCallback = std::function<uint32_t(uint32_t)>;
using ReportXactTimestampCallback = std::function<void(int64_t)>;
using ReportCurrentTopXidCallback = std::function<void(uint64_t)>;
using ReportCurrentXidCallback = std::function<void(uint64_t)>;
using ReportXlogLenCallback = std::function<void(uint32_t)>;
using ReportXactInfoCallback = std::function<void(bool)>;
using ReportCountBufferReadCallback = std::function<void(unsigned int, unsigned int)>;
using ReportCountBufferHitCallback = std::function<void(unsigned int, unsigned int)>;
using ReportCountBufferCallback = std::function<void(StorageRelation, unsigned int, unsigned int)>;
using ReportLockStatCallback = std::function<void(uint16_t, int, StmtLockTag*)>;
using ReportLWLockStatCallback = std::function<void(uint16_t, int, uint16_t)>;
using ReportSendBgwriterCallback = std::function<void(GsStatMsgBgWriter*)>;
using ReportBufferReadTimeCallback = std::function<void(instr_time)>;
using ReportBufferWriteTimeCallback = std::function<void(instr_time)>;
using ReportDeadLockTagCallback = std::function<void(int, uint8_t, StmtLockTag*)>;
using ReportSendTabstatCallback = std::function<void(bool)>;
using ReportDataIOTimeRecordCallback = std::function<void(int64_t*, bool)>;
} // namespace DSTORE

#endif
