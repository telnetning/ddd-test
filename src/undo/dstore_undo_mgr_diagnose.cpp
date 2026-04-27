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
 * dstore_undo_mgr_diagnose.cpp
 *
 * IDENTIFICATION
 *        dstore/src/undo/dstore_undo_mgr_diagnose.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "undo/dstore_undo_mgr.h"
#include "transaction/dstore_transaction_mgr.h"
#include "diagnose/dstore_undo_mgr_diagnose.h"

namespace DSTORE {

inline RetStatus CheckBuffer(BufferDesc *bufferDesc, PageId pageId)
{
    if (unlikely(bufferDesc == INVALID_BUFFER_DESC)) {
        ErrLog(DSTORE_WARNING, MODULE_UNDO, ErrMsg("Buffer(%hu, %u) is invalid!", pageId.m_fileId, pageId.m_blockId));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus CheckUndoZoneOwner(StoragePdb *pdb, bool *is_self_owned)
{
    if (STORAGE_VAR_NULL(pdb)) {
        ErrLog(DSTORE_WARNING, MODULE_UNDO, ErrMsg("Pdb is null when check undo zone owner."));
        return DSTORE_FAIL;
    }
    if (g_storageInstance->GetType() == StorageInstanceType::SINGLE) {
        *is_self_owned = true;
    } else {
        /* Memory nodes do not allow any sql connection queries. */
        ErrLog(DSTORE_WARNING, MODULE_UNDO, ErrMsg("Invalid storage instance type."));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus GetTransactionSlotCopyForDiagnose(StoragePdb *pdb, Xid xid, TransactionSlot &outTrxSlot,
                                            CommitSeqNo recycleCsnMin, StringInfoData *string)
{
    UndoMgr *undoMgr = pdb->GetUndoMgr();
    if (unlikely(undoMgr == nullptr)) {
        ErrLog(DSTORE_WARNING, MODULE_UNDO, ErrMsg("Undo mgr is invalid!"));
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_SUCC(undoMgr->ReadTxnInfoFromCache(xid, outTrxSlot, recycleCsnMin))) {
        return DSTORE_SUCC;
    }
    PageId segmentId = undoMgr->GetUndoZoneSegmentId(xid.m_zoneId);
    if (segmentId == INVALID_PAGE_ID) {
        ErrLog(DSTORE_WARNING, MODULE_UNDO, ErrMsg("Undo segment id is invalid!"));
        return DSTORE_FAIL;
    }
    /* The first transaction slot page would be the very next page of zone's segment meta page */
    PageId startPageId = {segmentId.m_fileId, segmentId.m_blockId + 1};
    PageId trxSlotPageId = INVALID_PAGE_ID;
    trxSlotPageId.m_fileId = startPageId.m_fileId;
    trxSlotPageId.m_blockId = startPageId.m_blockId + (xid.m_logicSlotId / TRX_PAGE_SLOTS_NUM) % TRX_PAGES_PER_ZONE;
    string->append("Slot page id is (%hu, %u).\n", trxSlotPageId.m_fileId, trxSlotPageId.m_blockId);
    /* Read the page from bufferpool by exact pageID */
    BufMgrInterface *bufMgr = undoMgr->GetBufferMgr();
    if (unlikely(bufMgr == nullptr)) {
        ErrLog(DSTORE_WARNING, MODULE_UNDO, ErrMsg("Buffer mgr is invalid!"));
        return DSTORE_FAIL;
    }
    BufferDesc *trxSlotPageBufDesc = bufMgr->Read(pdb->GetPdbId(), trxSlotPageId, LW_SHARED);
    if (unlikely(trxSlotPageBufDesc == INVALID_BUFFER_DESC)) {
        ErrLog(DSTORE_WARNING, MODULE_UNDO,
               ErrMsg("Buffer(%hu, %u) is invalid!", trxSlotPageId.m_fileId, trxSlotPageId.m_blockId));
        return DSTORE_FAIL;
    }
    /* Obtain target slot page */
    TransactionSlotPage *targetSlotPage = static_cast<TransactionSlotPage *>(trxSlotPageBufDesc->GetPage());
    if (unlikely(targetSlotPage == nullptr)) {
        ErrLog(DSTORE_WARNING, MODULE_UNDO,
               ErrMsg("Slot page(%hu, %u) is invalid!", trxSlotPageId.m_fileId, trxSlotPageId.m_blockId));
        bufMgr->UnlockAndRelease(trxSlotPageBufDesc);
        return DSTORE_FAIL;
    }
    uint32 slotId = TransactionSlotPage::GetSlotId(xid.m_logicSlotId);
    TransactionSlot *targetTrxSlot = targetSlotPage->GetTransactionSlot(slotId);
    if (targetTrxSlot->status == TXN_STATUS_FROZEN || targetTrxSlot->logicSlotId != xid.m_logicSlotId) {
        /* Transaction slot location has been recycled */
        outTrxSlot.status = TXN_STATUS_FROZEN;
        outTrxSlot.csn = INVALID_CSN;
    } else {
        outTrxSlot = *targetTrxSlot;
    }
    bufMgr->UnlockAndRelease(trxSlotPageBufDesc);
    /*
     * If commit wal not persis yet, we can't use TXN_STATUS_COMMITTED to judge visibility.
     * See details in COMMIT_LOGIC_TAG.
     */
    if (outTrxSlot.status == TXN_STATUS_COMMITTED && !outTrxSlot.IsCommitWalPersist(pdb->GetPdbId())) {
        outTrxSlot.status = TXN_STATUS_PENDING_COMMIT;
    }

    return DSTORE_SUCC;
}

char *UndoMgrDiagnose::GetUndoZoneInfoByZid(PdbId pdbId, int32 zoneId)
{
    AutoMemCxtSwitch autoSwitch{thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_TRANSACTION)};
    StringInfoData string;
    if (unlikely(!string.init())) {
        storage_set_error(UNDO_ERROR_OUT_OF_MEMORY);
        return nullptr;
    }
    /* Step 1: Check input value. */
    if (!IS_VALID_ZONE_ID(zoneId)) {
        string.append("Invalid Zone ID %d, it should be [0 - %d).\n", zoneId, UNDO_ZONE_COUNT);
        return string.data;
    }
    if (unlikely((pdbId == INVALID_PDB_ID) || (pdbId > PDB_MAX_ID))) {
        string.append("Invalid Pdb ID %u, it should be [%u - %u].\n", pdbId, PDB_START_ID, PDB_MAX_ID);
        StorageAssert(false);
        return string.data;
    }
    StoragePdb *pdb = g_storageInstance->GetPdb(pdbId);
    if (unlikely(pdb == nullptr)) {
        string.append("Pdb %u is null.\n", pdbId);
        StorageAssert(false);
        return string.data;
    }
    if (!pdb->IsInit()) {
        string.append("Pdb %u is not initialized.\n", pdbId);
        StorageAssert(false);
        return string.data;
    }
    /* Step 2: Get owner node id. */
    bool is_self_owned = false;
    NodeId zoneOwnerNodeId = INVALID_NODE_ID;
    RetStatus ret = CheckUndoZoneOwner(pdb, &is_self_owned);
    if (STORAGE_FUNC_FAIL(ret)) {
        string.append("Check undo zone owner failed.\n");
        StorageAssert(false);
        return string.data;
    }
    if (zoneOwnerNodeId == INVALID_NODE_ID && !is_self_owned) {
        string.append("Zone is not owned by any node.\n");
    } else if (is_self_owned) {
        string.append("Zone owner is self node.\n");
    } else {
        string.append("Zone owner node id:%u.\n", zoneOwnerNodeId);
    }
    /* Step 3: Get start page id. */
    if ((STORAGE_VAR_NULL(pdb->GetControlFile())) || (!pdb->GetControlFile()->IsInitialized())) {
        string.append("Control is not initialized that we can not get undo map.\n");
        StorageAssert(false);
        return string.data;
    }
    PageId undoMapSegmentId = INVALID_PAGE_ID;
    ret = pdb->GetControlFile()->GetUndoZoneMapSegmentId(undoMapSegmentId);
    if (STORAGE_FUNC_FAIL(ret)) {
        string.append("Undo map segment id is invalid.\n");
        StorageAssert(false);
        return string.data;
    }
    BufMgrInterface *bufferMgr = g_storageInstance->GetBufferMgr();
    BufferDesc *segMetaPageBuf = bufferMgr->Read(pdbId, undoMapSegmentId, LW_SHARED);
    if (STORAGE_FUNC_FAIL(CheckBuffer(segMetaPageBuf, undoMapSegmentId))) {
        string.append("Get invalid undo segment page, pageID (%hu, %u), zondId (%d).\n", undoMapSegmentId.m_fileId,
                      undoMapSegmentId.m_blockId, zoneId);
        StorageAssert(false);
        return string.data;
    }
    PageId mapStartPageId = static_cast<SegmentMetaPage *>(segMetaPageBuf->GetPage())->GetLastExtent();
    /* Skip the extent meta page, store map data on the left pages. */
    mapStartPageId.m_blockId++;
    bufferMgr->UnlockAndRelease(segMetaPageBuf);
    PageId mapPageId = {mapStartPageId.m_fileId,
                        mapStartPageId.m_blockId + static_cast<uint32>(zoneId) / SEGMENT_ID_PER_PAGE};
    BufferDesc *bufDesc = bufferMgr->Read(pdbId, mapPageId, LW_SHARED);
    if (STORAGE_FUNC_FAIL(CheckBuffer(bufDesc, mapPageId))) {
        string.append("Get invalid undo map page, pageID (%hu, %u), zondId (%d).\n", mapPageId.m_fileId,
                      mapPageId.m_blockId, zoneId);
        StorageAssert(false);
        return string.data;
    }
    char *rawPage = static_cast<char *>(static_cast<void *>(bufDesc->GetPage()));
    if (STORAGE_VAR_NULL(rawPage)) {
        bufferMgr->UnlockAndRelease(bufDesc);
        string.append("Get invalid undo map page, pageID (%hu, %u), zondId (%d).\n", mapPageId.m_fileId,
                      mapPageId.m_blockId, zoneId);
        StorageAssert(false);
        return string.data;
    }
    OffsetNumber offset = (static_cast<uint32>(zoneId) % SEGMENT_ID_PER_PAGE) * sizeof(PageId) + sizeof(Page);
    if (unlikely(offset + sizeof(PageId) > BLCKSZ)) {
        bufferMgr->UnlockAndRelease(bufDesc);
        string.append("Get invalid undo map page offset, pageID (%hu, %u), offsert(%d), zondId (%d).\n",
                      mapPageId.m_fileId, mapPageId.m_blockId, offset, zoneId);
        StorageAssert(false);
        return string.data;
    }
    PageId pageId = *static_cast<PageId *>(static_cast<void *>(rawPage + offset));
    bufferMgr->UnlockAndRelease(bufDesc);
    if ((pageId.m_fileId == 0 && pageId.m_blockId == 0) || (pageId == INVALID_PAGE_ID)) {
        string.append("Undo zone has invalid start page id:(%hu, %u).\n", INVALID_VFS_FILE_ID,
                      DSTORE_INVALID_BLOCK_NUMBER);
        string.append("Undo zone alreadyInitTxnSlotPages = false.\n");
        string.append("Undo zone firstUndoPageId is invalid.\n");
        return string.data;
    }
    string.append("Undo zone has valid start page:{%hu, %u}.\n", pageId.m_fileId, pageId.m_blockId);
    /* Step 3: Get meta info. */
    BufferDesc *zoneMetaBufDesc = bufferMgr->Read(pdbId, pageId, LW_SHARED);
    if (STORAGE_FUNC_FAIL(CheckBuffer(zoneMetaBufDesc, pageId))) {
        string.append("Get invalid undo meta page:{%hu, %u}, zond id:(%d).\n", pageId.m_fileId, pageId.m_blockId,
                      zoneId);
        StorageAssert(false);
        return string.data;
    }
    UndoSegmentMetaPage *metaPage = static_cast<UndoSegmentMetaPage *>(zoneMetaBufDesc->GetPage());
    if (STORAGE_VAR_NULL(metaPage)) {
        bufferMgr->UnlockAndRelease(zoneMetaBufDesc);
        string.append("Get invalid undo meta page:{%hu, %u}, zond id:(%d).\n", pageId.m_fileId, pageId.m_blockId,
                      zoneId);
        StorageAssert(false);
        return string.data;
    }
    bool alreadyInitTxnSlotPages = metaPage->alreadyInitTxnSlotPages;
    string.append("Undo zone alreadyInitTxnSlotPages = %s.\n", (alreadyInitTxnSlotPages ? "true" : "false"));
    PageId firstUndoPageId = metaPage->firstUndoPageId;
    string.append("Undo zone firstUndoPageId = (%hu, %u).\n", firstUndoPageId.m_fileId, firstUndoPageId.m_blockId);
    bufferMgr->UnlockAndRelease(zoneMetaBufDesc);
    return string.data;
}

char *UndoMgrDiagnose::GetUndoZoneStatusByZid(PdbId pdbId, int32 zoneId)
{
    AutoMemCxtSwitch autoSwitch{thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_TRANSACTION)};
    StringInfoData string;
    if (unlikely(!string.init())) {
        storage_set_error(UNDO_ERROR_OUT_OF_MEMORY);
        return nullptr;
    }
    /* Step 1: Check input value and pdb. */
    if (!IS_VALID_ZONE_ID(zoneId)) {
        string.append("Invalid Zone ID %d, it should be [0 - %d).\n", zoneId, UNDO_ZONE_COUNT);
        return string.data;
    }
    if (unlikely((pdbId == INVALID_PDB_ID) || (pdbId > PDB_MAX_ID))) {
        string.append("Invalid Pdb ID %u, it should be [%u - %u].\n", pdbId, PDB_START_ID, PDB_MAX_ID);
        StorageAssert(false);
        return string.data;
    }
    StoragePdb *pdb = g_storageInstance->GetPdb(pdbId);
    if (unlikely(pdb == nullptr)) {
        string.append("Pdb %u is null.\n", pdbId);
        StorageAssert(false);
        return string.data;
    }
    if (!pdb->IsInit()) {
        string.append("Pdb %u is not initialized.\n", pdbId);
        StorageAssert(false);
        return string.data;
    }
    /* Step 2: Get undo zone status. */
    UndoZoneStatus undoZoneStatus;
    UndoMgr *undoMgr = pdb->GetUndoMgr();
    RetStatus ret = undoMgr->GetUndoStatusForDiagnose(zoneId, &undoZoneStatus);
    if (STORAGE_FUNC_FAIL(ret)) {
        string.append("Get invalid undo zone.\n");
        return string.data;
    }
    if (undoZoneStatus.isAsyncRollbacking) {
        string.append("Undo zone is being rolled back asynchronously.\n");
    } else {
        string.append("Undo zone is not in asynchronously rollback.\n");
    }
    string.append("Undo zone has undo page num:%u.\n", undoZoneStatus.pageNum);
    uint64 freeSlotNum = TRX_PAGES_PER_ZONE * TRX_PAGE_SLOTS_NUM -
                         (undoZoneStatus.nextFreeLogicSlotId - undoZoneStatus.recycleLogicSlotId);
    if (freeSlotNum > 0) {
        string.append("Undo zone has free slot num:%lu.\n", freeSlotNum);
    } else {
        string.append("Undo zone not has free slot.\n");
    }
    string.append("Undo zone next recycle logic slot id is:%lu.\n", undoZoneStatus.recycleLogicSlotId);
    string.append("Undo zone next free logic slot id is:%lu.\n", undoZoneStatus.nextFreeLogicSlotId);
    return string.data;
}

char *UndoMgrDiagnose::ReadUndoRecord(char *page, int startingByte, ItemPointerData &nextPtr,
                                      std::function<void(PageId &, char *, int, void *)> readPageFunc,
                                      void *readPageArg)
{
    StorageReleasePanic(readPageFunc == nullptr, MODULE_UNDO, ErrMsg("Read page function is nullptr."));
    /* step1: read undo record size */
    uint8 serializSize = 0;
    char *readPtr = page + startingByte;
    char *endPtr = page + BLCKSZ;
    serializSize = *static_cast<uint8 *>(static_cast<void *>(readPtr));
    if (serializSize < sizeof(uint8)) {
        StringInfoData str;
        if (unlikely(!str.init())) {
            storage_set_error(UNDO_ERROR_OUT_OF_MEMORY);
            return nullptr;
        }
        str.append("Invalid undo record size %d.", serializSize);
        nextPtr = INVALID_UNDO_RECORD_PTR;
        return str.data;
    }

    UndoRecord record;
    record.SetSerializeSize(serializSize);
    if (STORAGE_FUNC_FAIL(record.PrepareDiskDataForDump())) {
        StringInfoData str;
        if (unlikely(!str.init())) {
            storage_set_error(UNDO_ERROR_OUT_OF_MEMORY);
            return nullptr;
        }
        str.append("Alloc undo data failed.");
        nextPtr = INVALID_UNDO_RECORD_PTR;
        return str.data;
    }
    /* step2: read undo record head */
    readPtr += sizeof(uint8);
    uint32 remaining = serializSize - sizeof(uint8);
    char *destPtr = record.GetSerializeData() + sizeof(uint8);
    while (remaining > 0) {
        if (endPtr - readPtr >= remaining) {
            errno_t rc = memcpy_s(destPtr, static_cast<uint32>(remaining), readPtr, remaining);
            storage_securec_check(rc, "\0", "\0");
            readPtr += remaining;
            remaining = 0;
        } else {
            uint32 readSize = endPtr - readPtr;
            errno_t rc = memcpy_s(destPtr, static_cast<uint32>(remaining), readPtr, readSize);
            storage_securec_check(rc, "\0", "\0");
            destPtr += readSize;
            remaining -= readSize;
            /* read next page */
            PageId curId =
                (static_cast<UndoRecordPage *>(static_cast<Page *>(static_cast<void *>(page))))->GetNextPageId();
            readPageFunc(curId, page, BLCKSZ, readPageArg);
            readPtr = page + UNDO_RECORD_PAGE_HEADER_SIZE;
            endPtr = page + BLCKSZ;
        }
    }
    record.Deserialize();
    record.DestroyDiskDataForDump();
    /* step3: read data length and get next undo record pointer */
    errno_t rc = memcpy_s(&record.m_dataInfo.len, sizeof(int), readPtr, sizeof(int));
    storage_securec_check(rc, "\0", "\0");
    nextPtr = record.GetTxnPreUndoPtr();
    return record.Dump();
}

char *UndoMgrDiagnose::GetUndoSlotStatusByXid(PdbId pdbId, int128 transactionId)
{
    AutoMemCxtSwitch autoSwitch{thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_TRANSACTION)};
    StringInfoData string;
    if (unlikely(!string.init())) {
        storage_set_error(UNDO_ERROR_OUT_OF_MEMORY);
        return nullptr;
    }
    /* Step 1: Check input value and pdb. */
    if (transactionId > UINT64_MAX) {
        string.append("Invalid Xid.\n");
        return string.data;
    }
    Xid xid = static_cast<Xid>(transactionId);
    if (unlikely(xid == INVALID_XID)) {
        string.append("Invalid Xid.\n");
        return string.data;
    }
    int32 zoneId = static_cast<int32>(xid.m_zoneId);
    if (!IS_VALID_ZONE_ID(zoneId)) {
        string.append("Invalid Xid.\n");
        return string.data;
    }
    if (unlikely((pdbId == INVALID_PDB_ID) || (pdbId > PDB_MAX_ID))) {
        string.append("Invalid Pdb ID %u, it should be [%u - %u].\n", pdbId, PDB_START_ID, PDB_MAX_ID);
        StorageAssert(false);
        return string.data;
    }
    StoragePdb *pdb = g_storageInstance->GetPdb(pdbId);
    if (unlikely(pdb == nullptr)) {
        string.append("Pdb %u is null.\n", pdbId);
        StorageAssert(false);
        return string.data;
    }
    if (!pdb->IsInit()) {
        string.append("Pdb %u is not initialized.\n", pdbId);
        StorageAssert(false);
        return string.data;
    }
    /* Step 2: Get slot status. */
    TransactionMgr *transactionMgr = pdb->GetTransactionMgr();
    if (unlikely(transactionMgr = nullptr)) {
        string.append("Transaction mgr is invalid.\n");
        StorageAssert(false);
        return string.data;
    }
    CsnMgr *csnMgr = g_storageInstance->GetCsnMgr();
    if (unlikely(csnMgr == nullptr)) {
        string.append("Csn mgr is invalid.\n");
        StorageAssert(false);
        return string.data;
    }
    CommitSeqNo recycleCsn = csnMgr->GetRecycleCsnMin(pdbId);
    string.append("Recycle csn is %lu\n", recycleCsn);
    TransactionSlot trxSlot;
    RetStatus ret = GetTransactionSlotCopyForDiagnose(pdb, xid, trxSlot, recycleCsn, &string);
    if (STORAGE_FUNC_FAIL(ret)) {
        string.append("Undo slot is invalid.\n");
        return string.data;
    }
    /* Step 2: Check xid recycled */
    bool recycled = false;
    if (trxSlot.status == TXN_STATUS_FROZEN || trxSlot.logicSlotId != xid.m_logicSlotId) {
        recycled = true;
    } else if (trxSlot.status == TXN_STATUS_COMMITTED) {
        recycled = trxSlot.GetCsn() < recycleCsn;
    }
    string.append("Recycled is %s.\n", (recycled ? "true" : "false"));
    /* Step 3: Dump slot status */
    if (recycled) {
        trxSlot.curTailUndoPtr = INVALID_UNDO_RECORD_PTR.m_placeHolder;
        trxSlot.spaceTailUndoPtr = INVALID_UNDO_RECORD_PTR.m_placeHolder;
    }
    trxSlot.Dump(&string);
    return string.data;
}

char *UndoMgrDiagnose::GetUndoPageHeader(PdbId pdbId, int32 fileId, int64 blockNum)
{
    AutoMemCxtSwitch autoSwitch{thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_TRANSACTION)};
    StringInfoData string;
    if (unlikely(!string.init())) {
        storage_set_error(UNDO_ERROR_OUT_OF_MEMORY);
        return nullptr;
    }
    FileId undoFileId = static_cast<FileId>(fileId);
    BlockNumber undoBlockNum = static_cast<BlockNumber>(blockNum);
    /* Step 1. Check input. */
    if (unlikely(undoFileId == INVALID_VFS_FILE_ID)) {
        string.append("Invalid undo file id.\n");
        return string.data;
    }
    if (unlikely(blockNum == DSTORE_INVALID_BLOCK_NUMBER)) {
        string.append("Invalid block num.\n");
        return string.data;
    }
    if (unlikely((pdbId == INVALID_PDB_ID) || (pdbId > PDB_MAX_ID))) {
        string.append("Invalid Pdb ID %u, it should be [%u - %u].\n", pdbId, PDB_START_ID, PDB_MAX_ID);
        StorageAssert(false);
        return string.data;
    }
    TablespaceMgr *tablespaceMgr = g_storageInstance->GetTablespaceMgrByPdbId(pdbId);
    if (STORAGE_VAR_NULL(tablespaceMgr)) {
        string.append("Invalid tablespace mgr.\n");
        return string.data;
    }
    TbsDataFile *datafile = tablespaceMgr->AcquireDatafile(fileId, LW_SHARED);
    if (STORAGE_VAR_NULL(datafile)) {
        string.append("Acquire datafile failed.\n");
        return string.data;
    }
    if (datafile->GetDataFilePageItemData().tablespaceId != static_cast<TablespaceId>(TBS_ID::UNDO_TABLE_SPACE_ID)) {
        tablespaceMgr->ReleaseDatafileLock(datafile);
        string.append("Invalid undo file id.\n");
        return string.data;
    }
    tablespaceMgr->ReleaseDatafileLock(datafile);
    /* Step 2. Get page. */
    PageId undoPageId = {undoFileId, undoBlockNum};
    BufMgrInterface *bufferMgr = g_storageInstance->GetBufferMgr();
    if (unlikely(bufferMgr == nullptr)) {
        string.append("Invalid buffer mgr.\n");
        StorageAssert(false);
        return string.data;
    }
    BufferDesc *pageBuf = bufferMgr->Read(pdbId, undoPageId, LW_SHARED);
    if (STORAGE_FUNC_FAIL(CheckBuffer(pageBuf, undoPageId))) {
        string.append("Invalid undo page id:(%hu, %u).\n", undoFileId, undoBlockNum);
        return string.data;
    }
    Page *undoPage = static_cast<Page *>(pageBuf->GetPage());
    if (unlikely(undoPage == nullptr)) {
        bufferMgr->UnlockAndRelease(pageBuf);
        string.append("Get invalid undo page, page id:(%hu, %u).\n", undoFileId, undoBlockNum);
        StorageAssert(false);
        return string.data;
    }
    string.append("glsn = %lu \n", undoPage->m_header.m_glsn);
    string.append("plsn = %lu \n", undoPage->m_header.m_plsn);
    string.append("walId = %lu \n", undoPage->m_header.m_walId);
    string.append("type = %hu \n", undoPage->m_header.m_type);
    string.append("myself = (%hu, %u)\n", undoPage->m_header.m_myself.m_fileId, undoPage->m_header.m_myself.m_blockId);
    bufferMgr->UnlockAndRelease(pageBuf);
    return string.data;
}

}  // namespace DSTORE