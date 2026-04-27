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
 * ---------------------------------------------------------------------------------------
 *
 * dstore_stat.h
 *
 * Created on: May 10, 2024
 * For hook callback function
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_STAT_H
#define DSTORE_STAT_H

#include "framework/dstore_stat_interface.h"
#include "framework/dstore_pdb.h"
#include "buffer/dstore_buf_mgr.h"
#include "systable/dstore_relation.h"
namespace DSTORE {

enum class StmtDetailType {
    LOCK_START = 1,
    LOCK_END,
    LOCK_WAIT_START,
    LOCK_WAIT_END,
    LOCK_RELEASE,
    LWLOCK_START,
    LWLOCK_END,
    LWLOCK_WAIT_START,
    LWLOCK_WAIT_END,
    LWLOCK_RELEASE,
    TYPE_INVALID
};

class StorageStat {
public:
    StorageStat() = default;
    ~StorageStat() = default;
    void ReportXactTimestamp(int64_t tstamp);
    void ReportCurrentTopXid(uint64_t st_top_xid);
    void ReportCurrentXid(uint64_t st_current_xid);
    void ReportXlogLen(uint64_t len);
    void ReportXactInfo(bool ic_commit);
    void ReportCountBufferRead(unsigned int rel, unsigned int count_buffer);
    void ReportCountBufferHit(unsigned int rel, unsigned int count_buffer);
    void ReportCountBuffer(DSTORE::StorageRelation relation, unsigned int count_buffer, unsigned int read_hit);
    void ReportLockStat(DSTORE::StmtDetailType stmt_detail_type, int lock_mode = -1,
        const struct LockTag *tag = NULL);
    void ReportLWLockStat(DSTORE::StmtDetailType stmt_detail_type, int lock_mode = -1,
        uint16_t lwlockId = 0);

    void ReportSendBgwriter(GsStatMsgBgWriter* bgwriter);
    void ReportBufferReadTime(instr_time read_time);
    void ReportBufferWriteTime(instr_time write_time);
    void ReportDeadLockTag(int lock_mode, const struct LockTag *tag);
    void ReportSendTabstat(bool force);
    void RegisterReportWaitEventCallback(ReportWaitEventCallback callback);
    void RegisterReportWaitEventFailedCallback(ReportWaitEventCallback callback);
    void RegisterReportWaitStatusCallback(ReportWaitStatusCallback callback);
    void RegisterReportXactTimestampCallback(ReportXactTimestampCallback callback);
    void RegisterReportCurrentTopXidCallback(ReportCurrentTopXidCallback callback);
    void RegisterReportCurrentXidCallback(ReportCurrentXidCallback callback);
    void RegisterReportXlogLenCallback(ReportXlogLenCallback callback);
    void RegisterReportXactInfoCallback(ReportXactInfoCallback callback);
    void RegisterReportCountBufferReadCallback(ReportCountBufferReadCallback callback);
    void RegisterReportCountBufferHitCallback(ReportCountBufferHitCallback callback);
    void RegisterReportCountBufferCallback(ReportCountBufferCallback callback);
    void RegisterReportLockStatCallback(DSTORE::ReportLockStatCallback callback);
    void RegisterReportLWLockStatCallback(DSTORE::ReportLWLockStatCallback callback);
    void RegisterReportSendBgwriterCallback(ReportSendBgwriterCallback callback);
    void RegisterReportBufferReadTimeCallback(ReportBufferReadTimeCallback callback);
    void RegisterReportBufferWriteTimeCallback(ReportBufferWriteTimeCallback callback);
    void RegisterReportDeadLockTagCallback(ReportDeadLockTagCallback callback);
    void RegisterReportSendTabstatCallback(ReportSendTabstatCallback callback);
    ReportWaitEventCallback m_reportWaitEvent = [](uint32_t waitEvent) {
        (void)waitEvent;
    };
    ReportWaitEventCallback m_reportWaitEventFailed = [](uint32_t waitEvent) {
        (void)waitEvent;
    };
    ReportWaitStatusCallback m_reportWaitStatus = [](uint32_t waitStatus) {
        (void)waitStatus;
        return static_cast<uint32_t>(GsStatWaitState::STATE_WAIT_UNDEFINED);
    };
    void ReportDataIOTimeRecord(int64_t*, bool);
    void RegisterReportDataIOTimeRecordCallback(ReportDataIOTimeRecordCallback callback);
private:
    ReportXactTimestampCallback m_reportXactTimestamp = nullptr;
    ReportCurrentTopXidCallback m_reportCurrentTopXid = nullptr;
    ReportCurrentXidCallback m_reportReportCurrentXid = nullptr;
    ReportXlogLenCallback m_reportReportXlogLen = nullptr;
    ReportXactInfoCallback m_reportXactInfo = nullptr;
    ReportCountBufferReadCallback m_countBufferRead = nullptr;
    ReportCountBufferHitCallback m_countBufferHit = nullptr;
    ReportCountBufferCallback m_countBuffer = nullptr;
    ReportLockStatCallback m_reportLock = nullptr;
    ReportLWLockStatCallback m_reportLWLock = nullptr;
    ReportSendBgwriterCallback m_sendBgwriter = nullptr;
    ReportBufferReadTimeCallback m_readTime = nullptr;
    ReportBufferWriteTimeCallback m_writeTime = nullptr;
    ReportDeadLockTagCallback m_deadLockTag = nullptr;
    ReportSendTabstatCallback m_sendTabstat = nullptr;
    ReportDataIOTimeRecordCallback m_reportDataIOTimeRecord = nullptr;
};

}

#endif
