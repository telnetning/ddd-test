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
 * Description: storage stats.
 */
#include "common/instrument/dstore_stat.h"
#include "common/dstore_datatype.h"
#include "lock/dstore_lock_datatype.h"
#include "framework/dstore_instance.h"
#include "buffer/dstore_checkpointer.h"

namespace DSTORE {
void StorageStat::RegisterReportWaitEventCallback(ReportWaitEventCallback callback)
{
    m_reportWaitEvent = callback;
}

void StorageStat::RegisterReportWaitEventFailedCallback(ReportWaitEventCallback callback)
{
    m_reportWaitEventFailed = callback;
}

void StorageStat::RegisterReportWaitStatusCallback(ReportWaitStatusCallback callback)
{
    m_reportWaitStatus = callback;
}

void StorageStat::RegisterReportXactTimestampCallback(ReportXactTimestampCallback callback)
{
    m_reportXactTimestamp = callback;
}

void StorageStat::RegisterReportCurrentTopXidCallback(ReportCurrentTopXidCallback callback)
{
    m_reportCurrentTopXid = callback;
}

void StorageStat::RegisterReportCurrentXidCallback(ReportCurrentXidCallback callback)
{
    m_reportReportCurrentXid = callback;
}

void StorageStat::RegisterReportXlogLenCallback(ReportXlogLenCallback callback)
{
    m_reportReportXlogLen = callback;
}

void StorageStat::RegisterReportXactInfoCallback(ReportXactInfoCallback callback)
{
    m_reportXactInfo = callback;
}

void StorageStat::RegisterReportCountBufferReadCallback(ReportCountBufferReadCallback callback)
{
    m_countBufferRead = callback;
}

void StorageStat::RegisterReportCountBufferHitCallback(ReportCountBufferHitCallback callback)
{
    m_countBufferHit = callback;
}

void StorageStat::RegisterReportCountBufferCallback(ReportCountBufferCallback callback)
{
    m_countBuffer = callback;
}
void StorageStat::RegisterReportLockStatCallback(ReportLockStatCallback callback)
{
    m_reportLock = callback;
}
void StorageStat::RegisterReportLWLockStatCallback(ReportLWLockStatCallback callback)
{
    m_reportLWLock = callback;
}

void StorageStat::RegisterReportSendBgwriterCallback(ReportSendBgwriterCallback callback)
{
    m_sendBgwriter = callback;
}

void StorageStat::RegisterReportBufferReadTimeCallback(ReportBufferReadTimeCallback callback)
{
    m_readTime = callback;
}

void StorageStat::RegisterReportBufferWriteTimeCallback(ReportBufferWriteTimeCallback callback)
{
    m_writeTime = callback;
}
void StorageStat::RegisterReportDeadLockTagCallback(ReportDeadLockTagCallback callback)
{
    m_deadLockTag = callback;
}
void StorageStat::RegisterReportSendTabstatCallback(ReportSendTabstatCallback callback)
{
    m_sendTabstat = callback;
}

void StorageStat::ReportXactTimestamp(int64_t tstamp)
{
    if (m_reportXactTimestamp == nullptr) {
        return;
    }

    m_reportXactTimestamp(tstamp);
}

void StorageStat::ReportCurrentTopXid(uint64_t st_top_xid)
{
    if (m_reportCurrentTopXid == nullptr) {
        return;
    }

    m_reportCurrentTopXid(st_top_xid);
}

void StorageStat::ReportCurrentXid(uint64_t st_current_xid)
{
    if (m_reportReportCurrentXid == nullptr) {
        return;
    }

    m_reportReportCurrentXid(st_current_xid);
}

void StorageStat::ReportXlogLen(uint64_t len)
{
    if (m_reportReportXlogLen == nullptr) {
        return;
    }

    m_reportReportXlogLen(len);
}

void StorageStat::ReportXactInfo(bool is_commit)
{
    if (m_reportXactInfo == nullptr) {
        return;
    }

    m_reportXactInfo(is_commit);
}

void StorageStat::ReportCountBufferRead(unsigned int rel, unsigned int count_buffer)
{
    if (m_countBufferRead == nullptr) {
        return;
    }

    m_countBufferRead(rel, count_buffer);
}

void StorageStat::ReportCountBufferHit(unsigned int rel, unsigned int count_buffer)
{
    if (m_countBufferHit == nullptr) {
        return;
    }

    m_countBufferHit(rel, count_buffer);
}

void StorageStat::ReportCountBuffer(StorageRelation relation, unsigned int count_buffer, unsigned int read_hit)
{
    if (m_countBuffer == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("m_countBuffer is null."));
        return;
    }
 
    m_countBuffer(relation, count_buffer, read_hit);
}

void StorageStat::ReportLockStat(DSTORE::StmtDetailType stmt_detail_type, int lock_mode,
    const struct LockTag *tag)
{
    if (likely(!g_storageInstance->GetGuc()->enableStmtTrack) || m_reportLock == nullptr) {
        return;
    }
    if (tag == nullptr) {
        m_reportLock((uint16_t)stmt_detail_type, lock_mode, NULL);
        return;
    }
    StmtLockTag stmttag  = {tag->field1, tag->field2, tag->field3, tag->field4, tag->field5, tag->lockTagType};
    m_reportLock((uint16_t)stmt_detail_type, lock_mode, &stmttag);
}
void StorageStat::ReportLWLockStat(DSTORE::StmtDetailType stmt_detail_type, int lock_mode, uint16_t lwlockId)
{
    if (likely(!g_storageInstance->GetGuc()->enableStmtTrack) || m_reportLWLock == nullptr) {
        return;
    }
    m_reportLWLock((uint16_t)stmt_detail_type, lock_mode, lwlockId);
}

void StorageStat::ReportSendBgwriter(GsStatMsgBgWriter* bgwriter)
{
    if (m_sendBgwriter == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("m_sendBgwriter is null."));
        return;
    }
 
    m_sendBgwriter(bgwriter);
}

void StorageStat::ReportBufferReadTime(instr_time read_time)
{
    if (m_readTime == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("m_readTime is null."));
        return;
    }
 
    m_readTime(read_time);
}

void StorageStat::ReportBufferWriteTime(instr_time write_time)
{
    if (m_writeTime == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("m_writeTime is null."));
        return;
    }
 
    m_writeTime(write_time);
}

void StorageStat::ReportDeadLockTag(int lock_mode, const LockTag *tag)
{
    if (m_deadLockTag == nullptr) {
        return;
    }
    if (!g_storageInstance->GetGuc()->enableTrackActivities) {
        return;
    }
    StmtLockTag lock_tag = {tag->field1, tag->field2, tag->field3, tag->field4, tag->field5, tag->lockTagType};
    m_deadLockTag(lock_mode, tag->lockMethodId, &lock_tag);
}

void StorageStat::ReportSendTabstat(bool force)
{
    if (m_sendTabstat == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("m_sendTabstat is null."));
        return;
    }
 
    m_sendTabstat(force);
}

void StorageStat::RegisterReportDataIOTimeRecordCallback(ReportDataIOTimeRecordCallback callback)
{
    m_reportDataIOTimeRecord = callback;
}

void StorageStat::ReportDataIOTimeRecord(int64_t* startTime, bool startRecording)
{
    if (likely(!g_storageInstance->GetGuc()->enableStmtTrack) || m_reportDataIOTimeRecord == nullptr) {
        return;
    }
    m_reportDataIOTimeRecord(startTime, startRecording);
}

}  // namespace DSTORE
