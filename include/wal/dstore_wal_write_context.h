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
 * dstore_wal_write_context.h
 *
 * Description:
 * Wal writer interface header file, contains all Wal writer definition exposed to caller.
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_WAL_WRITE_CONTEXT_H
#define DSTORE_WAL_WRITE_CONTEXT_H

#include "common/dstore_datatype.h"
#include "dstore_wal_struct.h"
#include "dstore_wal.h"

namespace DSTORE {

/*
 * Interface for atomic Wal write Objects.
 */
class AtomicWalWriterContext : public BaseObject {
public:
    AtomicWalWriterContext(DstoreMemoryContext memoryContext, PdbId pdbId, WalManager *walManager);
    ~AtomicWalWriterContext() noexcept;
    DISALLOW_COPY_AND_MOVE(AtomicWalWriterContext)

    /*
     * Init wal writer and inner data structure.
     *
     * @return: OK if success, detail error info otherwise
     */
    RetStatus Init();

    /*
     * Start an atomic Wal write operation.
     * Notice that if former Atomic write not ended, e.g., call BeginAtomicLog twice, the second call do nothing but
     * record warning log, and not influence log entry inserted after the first begin_atomic_log.
     *
     * @param: xid is transaction id, is 0 when not exits
     * @param: needPersist is false when no need to persist wal, these log don't write to buffer and occupy plsn
     */
    void BeginAtomicWal(Xid xid);

    /*
     * Specifies the walId to be inserted by the user who inserts the Wal.
     *
     * @param: walId is walStream Id.
     */
    void SetWalStream(WalId walId);

    WalId GetWalId();

    PdbId GetPdbId();

    void AllocWalId();

    /*
     * End an atomic Wal write operation.
     * Do nothing if there is no valid log entry.
     */
    WalGroupLsnInfo EndAtomicWal();

    /*
     * Add one WalRecord to WalRecordGroup, whole WalRecord copyed to this context's private buffer
     *
     * @param: record is log to append
     */
    void PutNewWalRecord(WalRecord *record);

    /*
     * Append data after the former log entry.
     * Notice that if no valid log entry in this log group, e.g., never call PutNewWalRecord before; or not begin
     * normally(refer to put_new_log_entry annotation), append do nothing but record warning log.
     *
     * @param: buf: a pointer to the data to be appended
     * @param: size: size of the data to be appended
     */
    void Append(void *buf, uint16 size);

    /*
     * Insert a log group with only one log entry, automatically finish insert operation in end_atomic_log.
     * Convenient when only need to insert one log entry.
     * Notice that former log group not ended normally, e.g., call BeginAtomicLog never but not call
     * end_atomic_log/abort_atomic_log, insert_one_log do nothing but give warning log.
     *
     * @param: record is log to append
     * @param: xid is transaction id, is 0 when not exits
     */
    WalGroupLsnInfo AtomicInsertOneWal(WalRecord *record, Xid xid = INVALID_XID);

    /*
     * Flush all Wal in walId before plsn to disk.
     *
     * @param: WalId is target stream's Wal id
     * @param: plsn is target stream's plsn
     *
     * @return: OK if success, detail error info otherwise
     */
    void WaitTargetPlsnPersist(WalId walId, uint64 plsn);

    /*
     * This is an override version to flush all Wal before groupPtr.m_endPlsn to disk in groupPtr.walId.
     *
     * @param: groupPtr contains WalId and endPlsn to be flushed.
     *
     * @return: OK if success, detail error info otherwise
     */
    void WaitTargetPlsnPersist(const WalGroupLsnInfo groupPtr);

    /*
     * Mark the page so that the glsn、plsn and WalId can be updated later before generate redo log for a page.
     *
     * @param: bufDesc indicates the description of the page in the buffer pool.
     */
    void RememberPageNeedWal(BufferDesc *bufDesc);

    /*
     * Return true if we are in logging, which means called BeginAtomicWal and not EndAtomicLog.
     */
    inline bool HasAlreadyBegin() const
    {
        return m_bufUsed != 0;
    }

    void ResetForAbort();

    inline void* GetTempWalBuf()
    {
        return m_tempWalBuf;
    }

    inline void ThrottleIfNeed()
    {
        m_walStream->ThrottleIfNeed();
    }

#ifndef UT
private:
#endif
    void CompressProcess(WalRecord *srcRecord, WalRecord *destRecord, uint32 destBufSize,
                         const WalRecordCompressAndDecompressItem *walRecordItem);
private:
    /*
     * Return true if page is dirty
     */
    static bool IsPageDirty(BufferDesc *bufDesc);

    /*
     * Clear the wal write status of the page after the page redo log is inserted to the wal buffer.
     */
    void SetPagesEndWriteWal();

    void SetPagesLSN(const WalGroupLsnInfo insertResult);

    inline void FindTargetPage(PageId pageId, Page **targetPage)
    {
        for (uint16 rememberPageIndex = 0; rememberPageIndex < m_numPagesNeedWal; rememberPageIndex++) {
            if (m_pagesNeedWal[rememberPageIndex]->GetPageId() == pageId) {
                *targetPage = m_pagesNeedWal[rememberPageIndex]->GetPage();
                break;
            }
        }
    }

    void CheckPageForEndWriteWal() const;

    /*
     * Reset member variables when redo log insertion end or abort.
     */
    void ClearState();

    /*
     * Reallocate memory if the memory space is insufficient during redo log insertion.
     */
    bool ReallocateBuf(uint32 *remBufSize, uint32 size, WalRecordAtomicGroup **logGroup);

    bool CheckPageHasRemembered(BufferDesc *bufDesc);

    void BuildRememberedPageIdHtab();
    void *m_tempWalBuf;  /* temp wal buffer which is used for memory of generate wal */
    uint8 *m_buf; /* Pointer to WalGroup head */
    uint32 m_bufSize; /* total len of m_buf */
    uint32 m_bufUsed; /* len of log data in m_buf */
    uint8 *m_bufOrigin; /* save the initialize m_buf */
    WalRecord *m_curLogEntry; /* shift of active WalRecord head from m_buf */

    DstoreMemoryContext m_memoryContext;
    PdbId m_pdbId;
    WalManager *m_walManager; /* wal manager process instance */
    WalId m_walId;
    WalStream *m_walStream;
    BufferDesc *m_pagesNeedWal[MAX_PAGES_COUNT_PER_WAL_GROUP];
    bool m_alreadySetPageIsWritingWal[MAX_PAGES_COUNT_PER_WAL_GROUP];
    uint16 m_numPagesNeedWal;
    HTAB* m_rememberedPageIdHtab;
    STATIC_ASSERT_VAL_IN_RANGE(MAX_PAGES_COUNT_PER_WAL_GROUP, m_numPagesNeedWal);
};
} /* The end of namespace DSTORE */

#endif
