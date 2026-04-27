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
 * dstore_wal_write_context.cpp
 *
 * IDENTIFICATION
 * src/wal/dstore_wal_write_context.cpp
 *
 * ---------------------------------------------------------------------------------------
 *
 */

#include "wal/dstore_wal_write_context.h"
#include "wal/dstore_wal.h"
#include "wal/dstore_wal_perf_statistic.h"
#include "wal/dstore_wal_utils.h"
#include "wal/dstore_wal_perf_unit.h"
#include "tablespace/dstore_tablespace_wal.h"
#include "page/dstore_page.h"
#include "securec.h"
#include "common/log/dstore_log.h"
#include "undo/dstore_undo_wal.h"

namespace DSTORE {
constexpr uint32 ATOMICLOG_BUF_INIT_SIZE = 4096;
constexpr uint32 SEQ_FIND_MAX_LEN = 8;

struct RememberedPagesHtabEntry {
    PageId pageId;
    BufferDesc *bufferDesc;
};

AtomicWalWriterContext::AtomicWalWriterContext(DstoreMemoryContext memoryContext, PdbId pdbId, WalManager *walManager)
    : m_tempWalBuf(nullptr),
      m_buf(nullptr),
      m_bufSize(ATOMICLOG_BUF_INIT_SIZE),
      m_bufUsed(0),
      m_bufOrigin(nullptr),
      m_curLogEntry(nullptr),
      m_memoryContext(memoryContext),
      m_pdbId(pdbId),
      m_walManager(walManager),
      m_walId(INVALID_WAL_ID),
      m_walStream(nullptr),
      m_numPagesNeedWal(0),
      m_rememberedPageIdHtab(nullptr)
{
    for (uint16 i = 0; i < MAX_PAGES_COUNT_PER_WAL_GROUP; i++) {
        m_pagesNeedWal[i] = nullptr;
        m_alreadySetPageIsWritingWal[i] = false;
    }
}

AtomicWalWriterContext::~AtomicWalWriterContext() noexcept
{
    DstorePfreeExt(m_buf);
    DstorePfreeExt(m_tempWalBuf);
    if (m_rememberedPageIdHtab != nullptr) {
        hash_destroy(m_rememberedPageIdHtab);
        m_rememberedPageIdHtab = nullptr;
    }
    m_bufOrigin = nullptr;
    m_memoryContext = nullptr;
    m_curLogEntry = nullptr;
    m_walStream = nullptr;
    m_walManager = nullptr;
}

PdbId AtomicWalWriterContext::GetPdbId()
{
    return m_pdbId;
}

RetStatus AtomicWalWriterContext::Init()
{
    if (m_memoryContext == nullptr || m_walManager == nullptr) {
        storage_set_error(WAL_ERROR_UNREACHABLE_CODE);
        ErrLog(DSTORE_PANIC, MODULE_WAL,
            ErrMsg("AtomicWalWriterContext Init fail, cause WalManager is nullptr or not inited for pdb %u.", m_pdbId));
        return DSTORE_FAIL;
    }
    if (m_buf == nullptr) {
        m_buf = (uint8 *)DstoreMemoryContextAllocZero(m_memoryContext, m_bufSize);
        if (unlikely(m_buf == nullptr)) {
            storage_set_error(WAL_ERROR_UNREACHABLE_CODE);
            ErrLog(DSTORE_ERROR, MODULE_WAL,
                ErrMsg("Allocate space OOM, allocate m_buf fail when AtomicWalWriterContext Init."));
            return DSTORE_FAIL;
        }
    }
    if (m_tempWalBuf == nullptr) {
        m_tempWalBuf = DstoreMemoryContextAllocZero(m_memoryContext, BLCKSZ);
        if (unlikely(m_tempWalBuf == nullptr)) {
            storage_set_error(WAL_ERROR_UNREACHABLE_CODE);
            ErrLog(DSTORE_ERROR, MODULE_WAL,
                   ErrMsg("Allocate space for undo OOM, allocate m_tempWalBuf fail when AtomicWalWriterContext Init."));
            return DSTORE_FAIL;
        }
    }

    return DSTORE_SUCC;
}

void AtomicWalWriterContext::SetWalStream(WalId walId)
{
    if (!HasAlreadyBegin()) {
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("The atomic Wal write have not be started."));
    }

    if (m_walId != INVALID_WAL_ID) {
        ErrLog(DSTORE_WARNING, MODULE_WAL,
            ErrMsg("You are attempting to modify the previously specified WalId."));
    }

    m_walId = walId;
}

void AtomicWalWriterContext::AllocWalId()
{
    if (unlikely(m_walManager == nullptr || m_walManager->GetWalStreamManager() == nullptr)) {
        m_walId = INVALID_WAL_ID;
        m_walStream = nullptr;
        return;
    }

    uint32 streamCount = m_walManager->GetWalStreamManager()->GetTotalWalStreamsCount();
    if (streamCount != 0) {
        WalStream *walStream = m_walManager->GetWalStreamManager()->GetWritingWalStream();
        if (walStream != nullptr) {
            m_walStream = walStream;
            m_walId = walStream->GetWalId();
            return;
        }
    }

    m_walId = INVALID_WAL_ID;
    m_walStream = nullptr;
}

WalId AtomicWalWriterContext::GetWalId()
{
    if (m_walId != INVALID_WAL_ID) {
        return m_walId;
    }

    AllocWalId();
    return m_walId;
}

void AtomicWalWriterContext::BeginAtomicWal(Xid xid)
{
    if (unlikely(m_buf == nullptr)) {
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("Not Init before BeginAtomicWal."));
        return;
    }
    /* Step 1:check if last WalRecordGroup insert finish */

    if (unlikely(HasAlreadyBegin())) {
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("The BeginAtomicWal interface is incorrectly invoked."));
    }

    if (STORAGE_VAR_NULL(g_storageInstance->GetPdb(m_pdbId)) ||
        (g_storageInstance->GetPdb(m_pdbId)->GetPdbRoleMode() != PdbRoleMode::PDB_PRIMARY &&
        !g_storageInstance->GetPdb(m_pdbId)->GetNeedRollbackBarrierInFailover())) {
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("Invalid pdb (%u), begin atomic wal failed.", m_pdbId));
    }

    /* Step 2:reset inner data struct */
    WalRecordAtomicGroup *logGroup = reinterpret_cast<WalRecordAtomicGroup *>(m_buf);
    logGroup->groupLen = sizeof(WalRecordAtomicGroup);
    m_bufUsed = logGroup->groupLen;
    logGroup->recordNum = 0;

    /* Step 3:init data structure and objects */
    logGroup->xid = xid;
    m_numPagesNeedWal = 0;
}

void AtomicWalWriterContext::ClearState()
{
    m_numPagesNeedWal = 0; /* So we don't need to clear m_pagesNeedWal, m_alreadySetPageIsWritingWal because len is 0 */
    if (m_rememberedPageIdHtab != nullptr) {
        hash_destroy(m_rememberedPageIdHtab);
        m_rememberedPageIdHtab = nullptr;
    }
    if (m_bufSize != ATOMICLOG_BUF_INIT_SIZE) {
        StorageAssert(m_buf != m_bufOrigin);
        DstorePfreeExt(m_buf);
        m_buf = m_bufOrigin;
        m_bufSize = ATOMICLOG_BUF_INIT_SIZE;
    }

    m_curLogEntry = nullptr;
    m_bufUsed = 0;
}

WalGroupLsnInfo AtomicWalWriterContext::EndAtomicWal()
{
    LatencyStat::Timer timer(&WalPerfUnit::GetInstance().m_endAtomicWalLatency);

    WalGroupLsnInfo insertResult = {INVALID_WAL_ID, INVALID_PLSN, INVALID_PLSN};

    WalRecordAtomicGroup *logGroup = reinterpret_cast<WalRecordAtomicGroup *>(m_buf);

    if (unlikely(!HasAlreadyBegin())) {
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("The EndAtomicWal interface is incorrectly invoked."));
    }

    if (unlikely(m_walManager == nullptr || !m_walManager->IsInited())) {
        storage_set_error(WAL_ERROR_NOT_INITED);
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("EndAtomicWal fail, cause WalManager is nullptr or not inited."));
    }

    StoragePdb *pdb = g_storageInstance->GetPdb(m_pdbId);
    StorageReleasePanic(pdb == nullptr, MODULE_WAL, ErrMsg("EndAtomicWal get pdb failed, pdbId(%u).", m_pdbId));
    if (unlikely(pdb->GetPdbRoleMode() != PdbRoleMode::PDB_PRIMARY && !pdb->GetNeedRollbackBarrierInFailover())) {
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("Not primary pdb, end atomic wal failed."));
    }

    CheckPageForEndWriteWal();

    /* Wal Manager init not complete */
    if (unlikely(m_walManager->GetWalStreamManager()->GetTotalWalStreamsCount() == 0)) {
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("EndAtomicWal failed, no walstream."));
        goto END;
    }

    /* no log to be insert, just quit and clear memory */
    if (unlikely(logGroup->groupLen <= sizeof(WalRecordAtomicGroup))) {
        goto END;
    }

    /* Step 1: user data has already been linked during PutNewLogEntry and append, just allocate WalStream */
    if (unlikely((m_walId == INVALID_WAL_ID && GetWalId() == INVALID_WAL_ID) || m_walStream == nullptr)) {
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("EndAtomicWal failed, cannot get valid walid."));
    }

    /* Step 2: using WalStream::append to insert WalRecordGroup into buffer */
    insertResult = m_walStream->Append(m_buf, m_bufUsed);
    /* Step 3: set glsn、plsn and WalId of page */
    SetPagesLSN(insertResult);
END:
    /* Step 4: Reset page finish wal write */
    SetPagesEndWriteWal();

    /* Step 5: Reset RedoLogGroup */
    ClearState();
    return insertResult;
}

void AtomicWalWriterContext::SetPagesLSN(const WalGroupLsnInfo insertResult)
{
    uint8 *group = m_buf;
    uint32 groupLen = m_bufUsed;
    uint32 offset = sizeof(WalRecordAtomicGroup);
    WalRecord *record = nullptr;
    PageId firstPageId;
    uint16 pageCount = 0;
    uint64 recordEndPlsn;
    if (STORAGE_VAR_NULL(m_walStream)) {
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("Invalid WalStream in SetPagesLSN."));
    }
    uint64 walFileSize = m_walStream->GetWalFileSize();
    uint32 walGroupPageIndex = 0;

    while (offset < groupLen) {
        record = STATIC_CAST_PTR_TYPE(group + offset, WalRecord *);
        switch (record->m_type) {
            case WAL_TBS_INIT_MULTIPLE_DATA_PAGES:
                STATIC_CAST_PTR_TYPE(record, WalRecordTbsInitDataPages *)->GetPageIdRange(firstPageId, pageCount);
                break;
            case WAL_TBS_INIT_BITMAP_PAGES:
                STATIC_CAST_PTR_TYPE(record, WalRecordTbsInitBitmapPages *)->GetPageIdRange(firstPageId, pageCount);
                break;
            case WAL_TBS_CREATE_TABLESPACE:
            case WAL_TBS_CREATE_DATA_FILE:
            case WAL_TBS_ADD_FILE_TO_TABLESPACE:
            case WAL_TBS_DROP_TABLESPACE:
            case WAL_TBS_DROP_DATA_FILE:
            case WAL_TBS_ALTER_TABLESPACE:
            case WAL_NEXT_CSN:
            case WAL_BARRIER_CSN:
            case WAL_SYSTABLE_WRITE_BUILTIN_RELMAP:
                pageCount = 0;
                break;
            default:
                firstPageId = STATIC_CAST_PTR_TYPE(record, WalRecordForPageOnDisk *)->GetCompressedPageId();
                pageCount = 1;
        }
        offset += record->m_size;
        recordEndPlsn = WalUtils::GetRecordPlsn<true>(insertResult.m_startPlsn, offset, walFileSize);
        for (uint32 i = 0; i < pageCount; i++) {
            PageId pageId = {firstPageId.m_fileId, firstPageId.m_blockId + i};
            Page *targetPage = nullptr;
            if (likely(walGroupPageIndex < m_numPagesNeedWal && m_pagesNeedWal[walGroupPageIndex]->GetPageId() ==
                pageId)) {
                targetPage = m_pagesNeedWal[walGroupPageIndex]->GetPage();
            } else {
                FindTargetPage(pageId, &targetPage);
            }
            if (unlikely(targetPage == nullptr)) {
                ErrLevel elevel = DSTORE_PANIC;
#ifdef UT
                elevel = DSTORE_ERROR;
#endif
                ErrLog(elevel, MODULE_WAL, ErrMsg("Page(%hu,%u) not found in RememberPage lists, can't"
                    " set lsn, record type %s", pageId.m_fileId, pageId.m_blockId,
                    g_walTypeForPrint[static_cast<uint16>(record->GetType())]));
            } else {
                /* Only if Wal id is changed, we need increase glsn */
                uint64 glsn = likely(insertResult.m_walId == targetPage->GetWalId()) ? targetPage->GetGlsn() :
                    targetPage->GetGlsn() + 1;
                targetPage->SetLsn(insertResult.m_walId, recordEndPlsn, glsn);
            }
            walGroupPageIndex++;
        }
    }
}

bool AtomicWalWriterContext::ReallocateBuf(uint32 *remBufSize, uint32 size, WalRecordAtomicGroup **logGroup)
{
    if (unlikely(size == 0 || m_memoryContext == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_WAL,
            ErrMsg("ReallocateBuf get invalid size(%u) or m_memoryContext is nullptr.", size));
        return false;
    }

    uint32 bufSizePre = m_bufSize;
    m_bufSize += ((((size - *remBufSize) - 1) / ATOMICLOG_BUF_INIT_SIZE + 1) * ATOMICLOG_BUF_INIT_SIZE);

    uint8 *temp = (uint8 *)DstoreMemoryContextAllocZero(m_memoryContext, m_bufSize);
    if (unlikely(temp == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_WAL,
            ErrMsg("ReallocateBuf alloc buffer with size(%u) failed.", m_bufSize));
        m_bufSize = bufSizePre;
        return false;
    }
    errno_t rc = memcpy_s(temp, m_bufSize, m_buf, bufSizePre);
    storage_securec_check(rc, "\0", "\0");

    /* m_bufOrigin can be assigned only when m_buf is extended for the first time. */
    rc = memset_s(m_buf, bufSizePre, 0, m_bufUsed);
    storage_securec_check(rc, "\0", "\0");

    if (bufSizePre == ATOMICLOG_BUF_INIT_SIZE) {
        m_bufOrigin = m_buf;
    } else {
        DstorePfreeExt(m_buf);
    }

    m_buf = temp;

    *logGroup = reinterpret_cast<WalRecordAtomicGroup *>(m_buf);
    *remBufSize = m_bufSize - (*logGroup)->groupLen;

    return true;
}

void AtomicWalWriterContext::PutNewWalRecord(WalRecord *record)
{
    if (unlikely(record == nullptr)) {
        storage_set_error(WAL_ERROR_INVALID_DATA);
        ErrLog(DSTORE_PANIC, MODULE_WAL,
            ErrMsg("nullptr WalRecord in atomic Wal write PutNewWalRecord process."));
        return;
    }
    uint32 recordSize = record->GetSize();
    /* Error handle if the input data invalid */
    if (unlikely(recordSize > MAX_WAL_RECORD_SIZE || recordSize < MIN_WAL_RECORD_SIZE)) {
        storage_set_error(WAL_ERROR_INVALID_DATA);
        ErrLog(DSTORE_PANIC, MODULE_WAL,
            ErrMsg("Invaild data in atomic Wal write PutNewWalRecord process."));
        return;
    }

    /* Step 1: Add new log entry to log group */
    if (unlikely(!HasAlreadyBegin())) {
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("The PutNewWalRecord interface is incorrectly invoked."));
    }
    uint16 maxCompressExpandSize = MAX_WAL_RECORD_FOR_PAGE_COMPRESSED_SIZE;
    const WalRecordCompressAndDecompressItem *walRecordItem = nullptr;
    if (record->GetType() >= WAL_UNDO_INIT_MAP_SEGMENT && record->GetType() <= WAL_TXN_ABORT) {
        walRecordItem = WalRecordUndo::GetWalRecordItem(record->GetType());
        if (walRecordItem != nullptr) {
            maxCompressExpandSize = walRecordItem->getMaxCompressedSize(record);
        }
    }

    WalRecordAtomicGroup *logGroup = reinterpret_cast<WalRecordAtomicGroup *>(m_buf);
    uint32 remBufSize = m_bufSize - logGroup->groupLen;
    if (unlikely(remBufSize < (recordSize + maxCompressExpandSize))) {
        /* If the memory space is insufficient, reallocate the space. */
        while (unlikely(!ReallocateBuf(&remBufSize, recordSize + maxCompressExpandSize, &logGroup))) {
            GaussUsleep(WAL_WAIT_MEMORY_AVAILABLE_TIME);
            ErrLog(DSTORE_ERROR, MODULE_WAL,
                ErrMsg("PutNewWalRecord fail when ReallocateBuf, retry it."));
        }
    }

    WalRecord *logRecord = reinterpret_cast<WalRecord *>(m_buf + logGroup->groupLen);
    CompressProcess(record, logRecord, remBufSize, walRecordItem);

    logGroup->groupLen += logRecord->m_size;
    m_bufUsed = logGroup->groupLen;
    if (unlikely(m_bufUsed > WAL_GROUP_MAX_SIZE)) {
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("The Wal record Group length is abnormal."));
    }
    logGroup->recordNum++;

#ifdef DSTORE_USE_ASSERT_CHECKING
    if (WalRecovery::IsSupportCompress(logRecord->GetType())) {
        const WalRecordForPageOnDisk *recordForPageOnDisk = static_cast<const WalRecordForPageOnDisk *>(logRecord);
        PageId pageidInRecord = recordForPageOnDisk->GetCompressedPageId();
        bool isFound = false;
        if (m_rememberedPageIdHtab == nullptr) {
            for (int idx = 0; idx < m_numPagesNeedWal; ++idx) {
                if (pageidInRecord == m_pagesNeedWal[idx]->GetPageId()) {
                    isFound = true;
                    break;
                }
            }
        } else {
            hash_search(m_rememberedPageIdHtab, &pageidInRecord, HASH_FIND, &isFound);
        }
        if (!isFound) {
            ErrLog(DSTORE_PANIC, MODULE_WAL,
                   ErrMsg("wal check fail, pageid %u, %u not in remember list, wal type = %s", pageidInRecord.m_fileId,
                          pageidInRecord.m_blockId, g_walTypeForPrint[static_cast<uint16>(logRecord->GetType())]));
        }
    }
#endif

    m_curLogEntry = logRecord;
    WalPerfCounter *walPerfCounter = thrd->GetCore()->threadPerfCounter->walPerfCounter;
    walPerfCounter->IncreaseWalTypeCount(logRecord->GetType(), logRecord->GetSize());
}

void AtomicWalWriterContext::CompressProcess(WalRecord *srcRecord, WalRecord *destRecord, uint32 destBufSize,
                                             const WalRecordCompressAndDecompressItem *walRecordItem)
{
    if (STORAGE_VAR_NULL(srcRecord) || STORAGE_VAR_NULL(destRecord)) {
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("Invalid WalRecord pointer."));
    }
    uint32 offset = 0;
    uint32 srcOffset = 0;
    uint32 recordSize = srcRecord->GetSize();
    uint32 leftLen = recordSize;

    if (WalRecovery::IsSupportCompress(srcRecord->GetType())) {
        uint16 compressLen = 0;
        uint16 uncompressHeaderSize = sizeof(WalRecordForPage);
        if (walRecordItem != nullptr) {
            uncompressHeaderSize = walRecordItem->headerSize;
            compressLen = walRecordItem->compress(srcRecord, STATIC_CAST_PTR_TYPE(destRecord, char *));
        } else {
            WalRecordForPage *tempRecord = static_cast<WalRecordForPage *>(srcRecord);
            compressLen = tempRecord->Compress(STATIC_CAST_PTR_TYPE(destRecord, char *));
        }
        destRecord->m_size = static_cast<uint16>((recordSize - uncompressHeaderSize) + compressLen);
        offset = compressLen;

        leftLen = static_cast<uint32>(recordSize - uncompressHeaderSize);
        srcOffset = uncompressHeaderSize;
    }

    if (leftLen > 0) {
        char *newRecord = STATIC_CAST_PTR_TYPE(srcRecord, char *) + srcOffset;
        if (unlikely(newRecord == nullptr)) {
            ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("The srcOffset(%u) is wrong.", srcOffset));
        }
        errno_t rc = memcpy_s(static_cast<void *>(STATIC_CAST_PTR_TYPE(destRecord, char *) + offset),
            destBufSize - offset, static_cast<void *>(newRecord), leftLen);
        storage_securec_check(rc, "\0", "\0");
    }
}

void AtomicWalWriterContext::Append(void *buf, uint16 size)
{
    /* Error handle if the input data invalid */
    if (buf == nullptr || size == 0) {
        storage_set_error(WAL_ERROR_INVALID_DATA);
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("invalid input in atomic Wal write Append process."));
        return;
    }

    /* Append to m_curRedoLogEntry */
    if (!HasAlreadyBegin() || m_curLogEntry == nullptr) {
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("The Append interface is incorrectly invoked."));
        return;
    }
    uint16 nowRecordSize = m_curLogEntry->GetSize();
    WalRecordAtomicGroup *logGroup = reinterpret_cast<WalRecordAtomicGroup *>(m_buf);
    if ((nowRecordSize + size) > MAX_WAL_RECORD_SIZE || (logGroup->groupLen + size) > WAL_GROUP_MAX_SIZE) {
        storage_set_error(WAL_ERROR_INVALID_DATA);
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("nullptr data in atomic Wal write Append process."));
        return;
    }

    uint32 remBufSize = m_bufSize - logGroup->groupLen;
    if (remBufSize < size) {
        intptr_t offsetLen = reinterpret_cast<uint8 *>(m_curLogEntry) - m_buf;
        /* If the memory space is insufficient, reallocate the space. */
        while (!ReallocateBuf(&remBufSize, size, &logGroup)) {
            GaussUsleep(WAL_WAIT_MEMORY_AVAILABLE_TIME);
            ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("Append fail when ReallocateBuf, retry it."));
        }
        m_curLogEntry = reinterpret_cast<WalRecord *>(m_buf + offsetLen);
    }
    m_curLogEntry->SetSize(nowRecordSize + size);

    errno_t rc = memcpy_s((m_buf + logGroup->groupLen), remBufSize, buf, size);
    storage_securec_check(rc, "\0", "\0");

    logGroup->groupLen += size;
    m_bufUsed = logGroup->groupLen;
}

WalGroupLsnInfo AtomicWalWriterContext::AtomicInsertOneWal(WalRecord *record, Xid xid)
{
    /* Step 1: BeginAtomicWal */
    this->BeginAtomicWal(xid);

    /* Step 2: PutNewWalRecord */
    this->PutNewWalRecord(record);

    /* Step 3: EndAtomicWal */
    WalGroupLsnInfo insertResult = {INVALID_WAL_ID, INVALID_PLSN, INVALID_PLSN};
    insertResult = this->EndAtomicWal();

    return insertResult;
}

void AtomicWalWriterContext::WaitTargetPlsnPersist(WalId walId, uint64 plsn)
{
    if (unlikely(m_walManager == nullptr || !m_walManager->IsInited())) {
        ErrLog(DSTORE_PANIC, MODULE_WAL,
            ErrMsg("EndAtomicWal fail, cause WalManager is nullptr or not inited."));
        return;
    }

    /* Step 1: get target WalStream */
#ifdef UT
    if (unlikely(m_walManager->GetWalStreamManager()->GetTotalWalStreamsCount() == 0)) {
        return;
    }
#endif
    WalStream *stream = m_walManager->GetWalStreamManager()->GetWalStream(walId);
    if (unlikely(stream == nullptr)) {
        storage_set_error(WAL_ERROR_STREAM_NOT_FOUND);
        ErrLog(DSTORE_PANIC, MODULE_WAL,
               ErrMsg("The corresponding Wal Stream cannot be found based on the specified WalId."));
        return;
    }

    /* Step 2: call flush interface */
    if (likely(g_storageInstance->GetGuc()->synchronousCommit)) {
        stream->WaitTargetPlsnPersist(plsn);
    }
}

void AtomicWalWriterContext::WaitTargetPlsnPersist(const WalGroupLsnInfo groupPtr)
{
    WaitTargetPlsnPersist(groupPtr.m_walId, groupPtr.m_endPlsn);
}

void AtomicWalWriterContext::RememberPageNeedWal(BufferDesc *bufDesc)
{
    if (unlikely(bufDesc == nullptr)) {
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("BufferDesc is nullptr when page wal write."));
    }
    if (g_storageInstance->GetType() == StorageInstanceType::DISTRIBUTE_COMPUTE &&
        (bufDesc->GetState(false) & Buffer::BUF_OWNED_BY_ME) == 0) {
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("BufferDesc is not owned by me."));
    }
    if (unlikely(!IsPageDirty(bufDesc))) {
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("BufferDesc is not dirty when page wal write."));
    }
    if (unlikely(!HasAlreadyBegin())) {
        ErrLog(DSTORE_PANIC,
            MODULE_WAL, ErrMsg("The RememberPageNeedWal interface is incorrectly invoked."));
    }

    if (unlikely(m_numPagesNeedWal >= MAX_PAGES_COUNT_PER_WAL_GROUP)) {
        ErrLog(DSTORE_PANIC, MODULE_WAL,
            ErrMsg("The number of registered pages that need to be Walged has exceeded the upper limit."));
    }
    if (m_numPagesNeedWal == SEQ_FIND_MAX_LEN && m_rememberedPageIdHtab == nullptr) {
        BuildRememberedPageIdHtab();
    }
    if (CheckPageHasRemembered(bufDesc)) {
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("remember same page(%hu %u) in a group, not allowed",
            bufDesc->GetPageId().m_fileId, bufDesc->GetPageId().m_blockId));
    }
    m_pagesNeedWal[m_numPagesNeedWal] = bufDesc;
    m_pagesNeedWal[m_numPagesNeedWal]->SetPageIsWritingWal();
    m_alreadySetPageIsWritingWal[m_numPagesNeedWal] = true;
    m_numPagesNeedWal++;
}

bool AtomicWalWriterContext::IsPageDirty(BufferDesc *bufDesc)
{
    return ((bufDesc->GetState(false) & Buffer::BUF_CONTENT_DIRTY) != 0);
}

void AtomicWalWriterContext::SetPagesEndWriteWal()
{
    /* use this method need ensure that method CheckPageForEndWriteWal() has been invoked before */
    for (int i = 0; i < m_numPagesNeedWal; ++i) {
        m_pagesNeedWal[i]->SetPageEndWriteWal();
    }
}

void AtomicWalWriterContext::CheckPageForEndWriteWal() const
{
    for (int i = 0; i < m_numPagesNeedWal; ++i) {
        if (unlikely(!m_alreadySetPageIsWritingWal[i])) {
            ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("RememberPageNeedWal but never PutNewWalRecord of this page."));
        }
    }
}

bool AtomicWalWriterContext::CheckPageHasRemembered(BufferDesc *bufDesc)
{
    if (m_rememberedPageIdHtab == nullptr) {
        for (int i = 0; i < m_numPagesNeedWal; ++i) {
            if (m_pagesNeedWal[i] == bufDesc) {
                return true;
            }
        }
        return false;
    } else {
        PageId pageId = bufDesc->GetPageId();
        bool isFound = false;
        RememberedPagesHtabEntry *rememberedPageEntry = static_cast<RememberedPagesHtabEntry *>(
            hash_search(m_rememberedPageIdHtab, &pageId, HASH_ENTER, &isFound));
        while (STORAGE_VAR_NULL(rememberedPageEntry)) {
            GaussUsleep(WAL_WAIT_MEMORY_AVAILABLE_TIME);
            ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("hash_search failed, retry it."));
            rememberedPageEntry = static_cast<RememberedPagesHtabEntry *>(
                hash_search(m_rememberedPageIdHtab, &pageId, HASH_ENTER, &isFound));
        }
        rememberedPageEntry->pageId = pageId;
        rememberedPageEntry->bufferDesc = bufDesc;
        return isFound;
    }
}

void AtomicWalWriterContext::BuildRememberedPageIdHtab()
{
    DstoreMemoryContext hashMemCtx =
        DstoreAllocSetContextCreate(thrd->m_memoryMgr->GetRoot(), "PageIdHashTableMemCtx", ALLOCSET_DEFAULT_SIZES);
    while (STORAGE_VAR_NULL(hashMemCtx)) {
        GaussUsleep(WAL_WAIT_MEMORY_AVAILABLE_TIME);
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("Create PageIdHashTableMemCtx failed, retry it."));
        hashMemCtx =
            DstoreAllocSetContextCreate(thrd->m_memoryMgr->GetRoot(), "PageIdHashTableMemCtx", ALLOCSET_DEFAULT_SIZES);
    }
    HASHCTL info;
    info.keysize = sizeof(PageId);
    info.entrysize = sizeof(RememberedPagesHtabEntry);
    info.hash = tag_hash;
    info.hcxt = hashMemCtx;
    m_rememberedPageIdHtab = hash_create("PageIdHashTable", MAX_PAGES_COUNT_PER_WAL_GROUP, &info,
                                         HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
    while (STORAGE_VAR_NULL(m_rememberedPageIdHtab)) {
        GaussUsleep(WAL_WAIT_MEMORY_AVAILABLE_TIME);
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("BuildRememberedPageIdHtab failed, retry it."));
        m_rememberedPageIdHtab = hash_create("PageIdHashTable", MAX_PAGES_COUNT_PER_WAL_GROUP, &info,
                                             HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
    }
    RememberedPagesHtabEntry *rememberedPageEntry;
    for (int i = 0; i < m_numPagesNeedWal; ++i) {
        PageId pageId = m_pagesNeedWal[i]->GetPageId();
        bool isFound = false;
        rememberedPageEntry = static_cast<RememberedPagesHtabEntry *>(
                hash_search(m_rememberedPageIdHtab, &pageId, HASH_ENTER, &isFound));
        while (STORAGE_VAR_NULL(rememberedPageEntry)) {
            GaussUsleep(WAL_WAIT_MEMORY_AVAILABLE_TIME);
            ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("hash_search failed, retry it."));
            rememberedPageEntry = static_cast<RememberedPagesHtabEntry *>(
                hash_search(m_rememberedPageIdHtab, &pageId, HASH_ENTER, &isFound));
        }
        StorageAssert(!isFound);
        rememberedPageEntry->pageId = pageId;
        rememberedPageEntry->bufferDesc = m_pagesNeedWal[i];
    }
}

void AtomicWalWriterContext::ResetForAbort()
{
    SetPagesEndWriteWal();
    ClearState();
}

} /* The end of namespace DSTORE */
