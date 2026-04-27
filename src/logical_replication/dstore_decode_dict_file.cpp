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
 * dstore_decode_dict_file.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "framework/dstore_instance.h"
#include "framework/dstore_pdb.h"
#include "common/algorithm/dstore_checksum_impl.h"
#include "logical_replication/dstore_decode_dict.h"
#include "logical_replication/dstore_decode_dict_file.h"

#ifdef UT
#ifndef ENABLE_LOGICAL_REPL
/* LOGICAL REPL is not supported for now. */
#define ENABLE_LOGICAL_REPL
#endif
#endif

namespace DSTORE {

DecodeDictFile::DecodeDictFile(PdbId pdbId)
    : m_pdbId(pdbId),
      m_fd1(nullptr),
      m_fd2(nullptr),
      m_file1Path(nullptr),
      m_file2Path(nullptr),
      m_metaPage(nullptr),
      m_pageBuf{nullptr},
      m_state{nullptr},
      m_pageCount(0),
      m_vfs(nullptr)
{}

DecodeDictFile::~DecodeDictFile()
{
    m_fd1 = nullptr;
    m_fd2 = nullptr;
    m_file1Path = nullptr;
    m_file2Path = nullptr;
    m_metaPage = nullptr;
    m_state = nullptr;
    m_pageBuf = nullptr;
    m_vfs = nullptr;
}

#ifdef ENABLE_LOGICAL_REPL
RetStatus DecodeDictFile::Init()
{
    StoragePdb *pdb = g_storageInstance->GetPdb(m_pdbId);
    StorageAssert(pdb != nullptr);
    m_vfs = pdb->GetVFS();
    if (STORAGE_FUNC_FAIL(InitDecodeFilePath())) {
        return DSTORE_FAIL;
    }

    if (STORAGE_FUNC_FAIL(m_vfs->OpenFile(m_file1Path, FILE_READ_AND_WRITE_FLAG, &m_fd1)) ||
        STORAGE_FUNC_FAIL(m_vfs->OpenFile(m_file2Path, FILE_READ_AND_WRITE_FLAG, &m_fd2))) {
        ErrLog(DSTORE_ERROR, MODULE_LOGICAL_REPLICATION, ErrMsg("Open deocde dict failed."));
        return DSTORE_FAIL;
    }

    if (STORAGE_FUNC_SUCC(CheckAndLoadMetaPage(m_fd1)) || STORAGE_FUNC_SUCC(CheckAndLoadMetaPage(m_fd2))) {
        return DSTORE_SUCC;
    }

    ErrLog(DSTORE_PANIC, MODULE_LOGICAL_REPLICATION, ErrMsg("decode dict meta page wrong!"));
    return DSTORE_FAIL;
}

void DecodeDictFile::Destroy()
{
    CheckDirtyPageBufEmpty();
    DstorePfreeExt(m_file1Path);
    DstorePfreeExt(m_file2Path);
    DstorePfreeExt(m_metaPage);
    DstorePfreeExt(m_state);
    DstorePfreeExt(m_pageBuf);
    if (m_fd1 != nullptr)
        m_vfs->CloseFile(m_fd1);
    if (m_fd2 != nullptr)
        m_vfs->CloseFile(m_fd2);
}

RetStatus DecodeDictFile::Reset()
{
    CheckDirtyPageBufEmpty();
    CleanAllDirtyTagAndValidBuf();
    CleanPageBuf(DECODE_DICT_META_BLOCK);
    if (STORAGE_FUNC_SUCC(CheckAndLoadMetaPage(m_fd1)) || STORAGE_FUNC_SUCC(CheckAndLoadMetaPage(m_fd2))) {
        return DSTORE_SUCC;
    }
    ErrLog(DSTORE_PANIC, MODULE_LOGICAL_REPLICATION, ErrMsg("decode dict meta page wrong!"));
    return DSTORE_FAIL;
}

BlockNumber DecodeDictFile::GetFirstPage() const
{
    StorageAssert(m_metaPage != nullptr);
    return m_metaPage->firstDecodeInfoPage;
}

BlockNumber DecodeDictFile::GetLastPage() const
{
    StorageAssert(m_metaPage != nullptr);
    return m_metaPage->lastDecodeInfoPage;
}

void DecodeDictFile::LoadToHTAB(HTAB *tableInfo)
{
    DecodeDictPageIterator iterator{this};
    while (iterator.NextItem()) {
        DecodeTableInfoDiskData *item = iterator.GetItem();
        StorageAssert(item != nullptr);
        DecodeTableInfo *decodeInfo = DecodeTableInfo::ConvertFromItem(item);
        StorageAssert(decodeInfo != nullptr);
        DecodeTableInfoPos *newInfo = static_cast<DecodeTableInfoPos *>(DstorePalloc(sizeof(DecodeTableInfoPos)));
        newInfo->Init(decodeInfo, iterator.GetCurrentBlock());
        bool found;
        DecodeTableInfoEntry *ent =  static_cast<DecodeTableInfoEntry *>(
            hash_search(tableInfo, static_cast<void *>(&decodeInfo->tableOid), HASH_ENTER, &found));
        StorageAssert(ent != nullptr);
        if (!found) {
            ent->tableOid = decodeInfo->tableOid;
            ent->versionNum = 1;
            DListInit(&ent->head);
            DListPushHead(&ent->head, &newInfo->posNode);
        } else {
            StorageAssert(ent != nullptr);
            bool add = false;
            dlist_mutable_iter iter;
            dlist_foreach_modify(iter, &ent->head) {
                DecodeTableInfoPos *cur = dlist_container(DecodeTableInfoPos, posNode, iter.cur);
                if (cur->tableInfo->csn > newInfo->tableInfo->csn) {
                    DListInsertBefore(&cur->posNode, &newInfo->posNode);
                    add = true;
                    break;
                }
            }
            if (!add) {
                DListPushTail(&ent->head, &newInfo->posNode);
            }
            ent->versionNum += 1;
        }
    }
    CleanAllDirtyTagAndValidBuf();
}

RetStatus DecodeDictFile::AddDecodeTableInfoItem(const DecodeTableInfoDiskData *newItem, BlockNumber &outAddBlock)
{
    /* step1: Find an available page from the current lastPage->(header->usedPageCount - 1) * and write Item. */
    if (InsertIntoAvailablePage(newItem, outAddBlock) == DSTORE_SUCC) {
        return PostWriteFile();
    }

    /* step2: If insert item fail, then we extend the file and retry. */
    if (ExtendDecodeDict() == DSTORE_SUCC) {
        if (InsertIntoAvailablePage(newItem, outAddBlock) == DSTORE_SUCC) {
            return PostWriteFile();
        }
    }
    return DSTORE_FAIL;
}

RetStatus DecodeDictFile::InsertIntoAvailablePage(const DecodeTableInfoDiskData *newItem, BlockNumber &outAddBlock)
{
    BlockNumber curIndex = GetLastPage();
    while (curIndex != DSTORE_INVALID_BLOCK_NUMBER && curIndex < m_metaPage->usedPageCount) {
        DecodeDictPage *dictPage = GetPage(curIndex);
        StorageAssert(dictPage != nullptr);
        if ((dictPage->GetAvailableSize() >= newItem->size)) {
            dictPage->AddItem(newItem);
            MarkPageDirty(curIndex);
            outAddBlock = curIndex;
            SetLastPageBlockNumber(curIndex);
            ErrLog(DSTORE_DEBUG1, MODULE_LOGICAL_REPLICATION,
                ErrMsg("insert tableInfo: %d on block: %u", newItem->tableOid, curIndex));
            return DSTORE_SUCC;
        }
        if (curIndex != DSTORE_INVALID_BLOCK_NUMBER) {
            SetLastPageBlockNumber(curIndex);
        }
        curIndex = dictPage->GetNextPage();
    }
    ErrLog(DSTORE_DEBUG1, MODULE_LOGICAL_REPLICATION,
        ErrMsg("all used pages have no place to insert, ready to get new page"));
    return DSTORE_FAIL;
}

RetStatus DecodeDictFile::RemoveDecodeTableInfoItem(const Oid oldTableOid, const CommitSeqNo oldTableCsn,
    const BlockNumber block)
{
    DecodeDictPageIterator iterator{this, block};
    while (iterator.NextItem()) {
        DecodeTableInfoDiskData *item = iterator.GetItem();
        StorageAssert(item != nullptr);
        if (item->tableOid == oldTableOid && item->csn == oldTableCsn) {
            if (unlikely(block != iterator.GetCurrentBlock())) {
                ErrLog(DSTORE_PANIC, MODULE_LOGICAL_REPLICATION, ErrMsg("memory and disk mismatch!"));
                return DSTORE_FAIL;
            }
            DecodeDictPage *page = GetPage(block);
            StorageAssert(page != nullptr);
            page->RemoveItem(iterator.GetCurrentOffset(), item->size);
            ErrLog(DSTORE_DEBUG1, MODULE_LOGICAL_REPLICATION,
                ErrMsg("remove tableInfo Oid: %d on block: %u", oldTableOid, block));
            /* mark page(blockNumber) as dirty */
            MarkPageDirty(block);
            SetLastPageBlockNumber(block);
            return PostWriteFile();
        }
    }
    ErrLog(DSTORE_PANIC, MODULE_LOGICAL_REPLICATION, ErrMsg("item not fount!"));
    return DSTORE_FAIL;
}

RetStatus DecodeDictFile::UpdateDecodeTableInfoItem(const BlockNumber oldBlock, const Oid oldTableOid,
    const CommitSeqNo oldTableCsn, const DecodeTableInfoDiskData *newItem, BlockNumber &newBlock)
{
    if (STORAGE_FUNC_FAIL(RemoveDecodeTableInfoItem(oldTableOid, oldTableCsn, oldBlock)) ||
        STORAGE_FUNC_FAIL(AddDecodeTableInfoItem(newItem, newBlock))) {
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus DecodeDictFile::InitDecodeFilePath()
{
    m_file1Path = static_cast<char *>(DstorePalloc(MAXPGPATH));
    m_file2Path = static_cast<char *>(DstorePalloc(MAXPGPATH));

    errno_t rc = 0;
    char *dataDir = StorageInstance::GetPdbPath(g_storageInstance->GetGuc()->dataDir, m_pdbId);
    rc = sprintf_s(m_file1Path, MAXPGPATH, "%s", DECODEDICT_FILE_1_NAME);
    storage_securec_check_ss(rc);
    rc = sprintf_s(m_file2Path, MAXPGPATH, "%s", DECODEDICT_FILE_2_NAME);
    storage_securec_check_ss(rc);
    DstorePfreeExt(dataDir);
    return DSTORE_SUCC;
}

RetStatus DecodeDictFile::CheckAndLoadMetaPage(FileDescriptor *fd)
{
    char *buffer = static_cast<char *>(DstorePalloc(BLCKSZ));
    if (buffer == nullptr) {
        return DSTORE_FAIL;
    }
    RetStatus rc = m_vfs->ReadPageSync(fd, DECODE_DICT_META_BLOCK, buffer);
    if (STORAGE_FUNC_FAIL(rc)) {
        ErrLog(DSTORE_ERROR, MODULE_LOGICAL_REPLICATION, ErrMsg("Read decode dict meta page failed."));
        DstorePfree(buffer);
        return DSTORE_FAIL;
    }
    /* check checksum in DecodeDict Meta page */
    m_metaPage = STATIC_CAST_PTR_TYPE(buffer, DecodeDictMetaPage*);
    if ((!CheckPageCrcMatch(&m_metaPage->checksum, static_cast<void *>(buffer)))) {
        DstorePfree(buffer);
        return DSTORE_FAIL;
    }
    AllocPageStateAndPageBufTag(m_metaPage->totalPageCount);
    MarkPageValid(DECODE_DICT_META_BLOCK, buffer);
    return DSTORE_SUCC;
}

void DecodeDictFile::AllocPageStateAndPageBufTag(uint32 blockCount)
{
    StorageAssert(m_metaPage != nullptr);
    if (blockCount <= m_pageCount) { /* no need to fresh */
        StorageAssert(m_state != nullptr && m_pageBuf != nullptr);
        return;
    }
    uint32 *tmpState = static_cast<uint32 *>(DstorePalloc0(blockCount * sizeof(uint32)));
    char **tmpDirtyPageBuf = static_cast<char **>(DstorePalloc0(blockCount * sizeof(char *)));
    StorageReleasePanic(tmpState == nullptr || tmpDirtyPageBuf == nullptr, MODULE_LOGICAL_REPLICATION,
                        ErrMsg("TempPageState alloc memory is not enough!"));
    if (m_state != nullptr) {
        errno_t rc = memcpy_s(tmpState, blockCount * sizeof(uint32), m_state, m_pageCount * sizeof(uint32));
        storage_securec_check(rc, "\0", "\0");
    }
    if (m_pageBuf != nullptr) {
        errno_t rc = memcpy_s(tmpDirtyPageBuf, blockCount * sizeof(char *), m_pageBuf, m_pageCount * sizeof(char *));
        storage_securec_check(rc, "\0", "\0");
    }
    for (uint32 i = m_pageCount; i < blockCount; i++) {
        tmpState[i] &= (~(DECODEDICT_BUF_DIRTY | DECODEDICT_BUF_VALID | DECODEDICT_BUF_NEED_CRC));
        tmpDirtyPageBuf[i] = nullptr;
    }
    DstorePfreeExt(m_state);
    DstorePfreeExt(m_pageBuf);
    m_state = tmpState;
    m_pageBuf = tmpDirtyPageBuf;
    m_pageCount = blockCount;
}

void DecodeDictFile::MarkPageDirty(BlockNumber blockNumber)
{
    StorageAssert(blockNumber < m_pageCount);
    StorageAssert(IsPageValid(blockNumber));
    StorageAssert(m_pageBuf[blockNumber] != nullptr);
    StorageReleasePanic(m_pageBuf[blockNumber] == nullptr, MODULE_LOGICAL_REPLICATION, ErrMsg("m_pageBuf is nullptr!"));
    StorageReleasePanic(m_state == nullptr, MODULE_LOGICAL_REPLICATION, ErrMsg("m_state is nullptr!"));
    m_state[blockNumber] |= DECODEDICT_BUF_DIRTY;
}

bool DecodeDictFile::IsPageDirty(BlockNumber blockNumber) const
{
    return static_cast<bool>(m_state[blockNumber] & DECODEDICT_BUF_DIRTY);
}

void DecodeDictFile::MarkPageValid(BlockNumber blockNumber, char *blockBuf)
{
    StorageAssert(blockNumber < m_pageCount);
    StorageAssert(m_pageBuf[blockNumber] == nullptr);
    StorageReleasePanic(m_state == nullptr, MODULE_LOGICAL_REPLICATION, ErrMsg("m_state is nullptr!"));
    m_state[blockNumber] |= DECODEDICT_BUF_VALID;
    m_pageBuf[blockNumber] = blockBuf;
}

bool DecodeDictFile::IsPageValid(BlockNumber blockNumber) const
{
    return static_cast<bool>(m_state[blockNumber] & DECODEDICT_BUF_VALID);
}

bool DecodeDictFile::IsPageNeedCrcCheck(BlockNumber blockNumber) const
{
    return static_cast<bool>(m_state[blockNumber] & DECODEDICT_BUF_NEED_CRC);
}

void DecodeDictFile::MarkPageNeedCrcCheck(BlockNumber blockNumber)
{
    m_state[blockNumber] |= DECODEDICT_BUF_NEED_CRC;
}

void DecodeDictFile::CleanAllDirtyTagAndValidBuf()
{
    for (uint32 i = DECODE_DICT_META_BLOCK + 1; i < m_pageCount; i++) {
        if (IsPageValid(i)) {
            StorageAssert(m_pageBuf[i] != nullptr);
            CleanPageBuf(i);
        }
        m_state[i] &= (~DECODEDICT_BUF_DIRTY);
    }

    /* meta block always keep in memory. */
    StorageAssert(IsPageValid(DECODE_DICT_META_BLOCK));
    m_state[DECODE_DICT_META_BLOCK] &= (~DECODEDICT_BUF_DIRTY);
}

void DecodeDictFile::CleanPageBuf(BlockNumber block)
{
    m_state[block] &= (~DECODEDICT_BUF_VALID);
    DstorePfreeExt(m_pageBuf[block]);
}

void DecodeDictFile::CheckDirtyPageBufEmpty() const
{
    for (uint32 i = 0; i < m_pageCount; i++) {
        if (IsPageDirty(i)) {
            StorageAssert(0);
        }
    }
}

DecodeDictPage* DecodeDictFile::GetPage(BlockNumber blockNumber)
{
    if (blockNumber < m_pageCount && IsPageValid(blockNumber)) {
        StorageAssert(m_pageBuf[blockNumber] != nullptr);
        if (m_pageBuf[blockNumber] == nullptr) {
            return nullptr;
        }
        return static_cast<DecodeDictPage *>(static_cast<void *>(m_pageBuf[blockNumber])); /* use newest page */
    } else {
        return ReadFromFile(blockNumber);
    }
}

DecodeDictPage* DecodeDictFile::ReadFromFile(BlockNumber blockNumber)
{
    /* read from file */
    char *pageBuffer = static_cast<char *>(DstorePalloc0(BLCKSZ));
    if (pageBuffer == nullptr) {
        return nullptr;
    }
    if (STORAGE_FUNC_FAIL(m_vfs->ReadPageSync(m_fd1, blockNumber, pageBuffer))) {
        ErrLog(DSTORE_ERROR, MODULE_LOGICAL_REPLICATION,
            ErrMsg("Read decodedict failed, blockNumber(%u).", blockNumber));
        DstorePfree(pageBuffer);
        /* need recovery decode dict file here */
        return nullptr;
    }
    DecodeDictPage *dictPage = static_cast<DecodeDictPage *>(static_cast<void *>(pageBuffer));
    uint32_t checksum = dictPage->dictPageHeader.checksum;
    if (IsPageNeedCrcCheck(blockNumber) && !CheckPageCrcMatch(&checksum, pageBuffer)) {
        DstorePfree(pageBuffer);
        return nullptr;
    }
    MarkPageValid(blockNumber, static_cast<char *>(static_cast<void *>(dictPage)));
    return dictPage;
}

RetStatus DecodeDictFile::PostWriteFile()
{
    if (STORAGE_FUNC_FAIL(WriteAllDirtyPage(m_fd1)) || STORAGE_FUNC_FAIL(FlushFileBuffer(m_fd1))) {
        return DSTORE_FAIL;
    }

    /* After DecodeDict1 is successfully written, continue to write DecodeDict2. */
    if (STORAGE_FUNC_FAIL(WriteAllDirtyPage(m_fd2)) || STORAGE_FUNC_FAIL(FlushFileBuffer(m_fd2))) {
        return DSTORE_FAIL;
    }
    CleanAllDirtyTagAndValidBuf();

    return DSTORE_SUCC;
}

RetStatus DecodeDictFile::WriteAllDirtyPage(FileDescriptor *fd)
{
    for (uint32 i = 0; i < m_pageCount; i++) {
        /* only if page[i] is dirty, we write the page. */
        if (IsPageDirty(i) && STORAGE_FUNC_FAIL(WritePage(fd, i))) {
            return DSTORE_FAIL;
        }
    }
    return DSTORE_SUCC;
}

RetStatus DecodeDictFile::FlushFileBuffer(FileDescriptor *fd)
{
    RetStatus rc = m_vfs->Fsync(fd);
    if (STORAGE_FUNC_FAIL(rc)) {
        ErrLog(DSTORE_ERROR, MODULE_LOGICAL_REPLICATION,
               ErrMsg("Fsync decode dict File failed"));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

bool DecodeDictFile::CheckPageCrcMatch(uint32 *checksum, const void *page)
{
    uint32 curChecksum = *checksum;
    *checksum = 0;
    uint32 newChecksum = CompChecksum(page, BLCKSZ, CHECKSUM_CRC);
    *checksum = curChecksum;
    return curChecksum == newChecksum;
}

void DecodeDictFile::UpdatePageCrc(uint32 *checksum, const void *page) const
{
    *checksum = 0;
    *checksum = CompChecksum(page, BLCKSZ, CHECKSUM_CRC);
}

RetStatus DecodeDictFile::WritePage(FileDescriptor *fd, BlockNumber blockNumber)
{
    StorageAssert(blockNumber < m_pageCount);
    StorageAssert(IsPageDirty(blockNumber));
    char *buff = m_pageBuf[blockNumber];
    if (buff == nullptr) {
        return DSTORE_FAIL;
    }
    if (blockNumber == DECODE_DICT_META_BLOCK) {
        UpdatePageCrc(&m_metaPage->checksum, static_cast<const void*>(buff));
    } else {
        DecodeDictPage *page = static_cast<DecodeDictPage *>(static_cast<void *>(buff));
        uint32_t checksum = page->dictPageHeader.checksum;
        UpdatePageCrc(&checksum, static_cast<const void*>(buff));
    }
    RetStatus rc = m_vfs->WritePageSync(fd, blockNumber, buff);
    if (STORAGE_FUNC_FAIL(rc)) {
        ErrLog(DSTORE_PANIC, MODULE_LOGICAL_REPLICATION, ErrMsg("Write decode dict failed."));
        return DSTORE_FAIL;
    }
    MarkPageNeedCrcCheck(blockNumber);
    return DSTORE_SUCC;
}

RetStatus DecodeDictFile::ExtendDecodeDict()
{
    BlockNumber nextUsedPageBlockNumber = m_metaPage->usedPageCount;
    /* step1. extend file and status buffer in memory */
    if (nextUsedPageBlockNumber >= m_metaPage->totalPageCount) {
        /* extend decode dict file */
        if (STORAGE_FUNC_FAIL(ExtendFile())) {
            return DSTORE_FAIL;
        }
        /* we extend status Buffer */
        ExtendStatusBuf();
    }

    /* step2. link new page it to the end */
    BlockNumber oldLastBlockNumber = GetLastPage();
    StorageAssert(oldLastBlockNumber == m_metaPage->usedPageCount - 1 ||
                  oldLastBlockNumber == DSTORE_INVALID_BLOCK_NUMBER);

    if (oldLastBlockNumber == DSTORE_INVALID_BLOCK_NUMBER) {     /* first time */
        SetFirstPageBlockNumber(nextUsedPageBlockNumber);
    } else {
        DecodeDictPage *oldPage = GetPage(oldLastBlockNumber);
        if (unlikely(oldPage == nullptr)) {
            return DSTORE_FAIL;
        }
        oldPage->SetNextPage(nextUsedPageBlockNumber);
        MarkPageDirty(oldLastBlockNumber);
    }

    /* step3. increse used page count and set newest last page */
    m_metaPage->usedPageCount = nextUsedPageBlockNumber + 1;
    SetLastPageBlockNumber(nextUsedPageBlockNumber);

    /* step4. init the new page, must be read from file */
    DecodeDictPage *nextUsedPage = GetPage(nextUsedPageBlockNumber);
    if (unlikely(nextUsedPage == nullptr)) {
        return DSTORE_FAIL;
    }
    nextUsedPage->Init();
    MarkPageDirty(nextUsedPageBlockNumber);
    return DSTORE_SUCC;
}

void DecodeDictFile::SetLastPageBlockNumber(BlockNumber lastBlock)
{
    StorageAssert(m_metaPage != nullptr);
    m_metaPage->lastDecodeInfoPage = lastBlock;
    MarkPageDirty(DECODE_DICT_META_BLOCK);
}

void DecodeDictFile::SetFirstPageBlockNumber(BlockNumber firstBlock)
{
    StorageAssert(m_metaPage != nullptr);
    m_metaPage->firstDecodeInfoPage = firstBlock;
    MarkPageDirty(DECODE_DICT_META_BLOCK);
}

RetStatus DecodeDictFile::ExtendFile()
{
    uint32 newPageCount = m_metaPage->totalPageCount + DECODE_DICT_EXTEND_PAGE_COUNT;
    RetStatus rc = m_vfs->Extend(m_fd1, GetOffsetByBlockNo(newPageCount));
    if (STORAGE_FUNC_FAIL(rc)) {
        ErrLog(DSTORE_ERROR, MODULE_LOGICAL_REPLICATION, ErrMsg("Extend decode dict file failed."));
        StorageAssert(0);
        return DSTORE_FAIL;
    }
    rc = m_vfs->Extend(m_fd2, GetOffsetByBlockNo(newPageCount));
    if (STORAGE_FUNC_FAIL(rc)) {
        ErrLog(DSTORE_ERROR, MODULE_LOGICAL_REPLICATION, ErrMsg("Extend decode dict file failed."));
        StorageAssert(0);
        return DSTORE_FAIL;
    }
    /* update MetaPage */
    m_metaPage->totalPageCount = newPageCount;
    MarkPageDirty(DECODE_DICT_META_BLOCK);
    return DSTORE_SUCC;
}

void DecodeDictFile::ExtendStatusBuf()
{
    /* make sure the decode dict file has been extend */
    StorageAssert(m_metaPage->totalPageCount > m_pageCount);
    AllocPageStateAndPageBufTag(m_metaPage->totalPageCount);
    return;
}
#endif

RetStatus DecodeDictFile::CreateFile(PdbId pdbId, const char * decodedict_file1_name,
                                     const char * decodedict_file2_name, const char *dataDir)
{
    /*
     * NOTE: We can not create decode dict file concurrentlly so it has Object lock to make sure that
     * only one node in cluster creat this file, others just load.
     */
    /* double write */
    FileDescriptor* fd1;
    FileDescriptor* fd2;
    if (STORAGE_FUNC_FAIL(CreateOneFile(pdbId, decodedict_file1_name, dataDir, &fd1)) ||
        STORAGE_FUNC_FAIL(CreateOneFile(pdbId, decodedict_file2_name, dataDir, &fd2))) {
        return DSTORE_FAIL;
    }

    if (STORAGE_FUNC_FAIL(InitMetaPage(pdbId, fd1, fd2))) {
        return DSTORE_FAIL;
    }
    StoragePdb *storagePdb = g_storageInstance->GetPdb(pdbId);
    StorageAssert(storagePdb != nullptr);
    VFSAdapter *vfs = storagePdb->GetVFS();
    vfs->CloseFile(fd1);
    vfs->CloseFile(fd2);
    return DSTORE_SUCC;
}

RetStatus DecodeDictFile::CreateOneFile(PdbId pdbId, const char *const fileName,
                                        const char *dataDir, FileDescriptor** fd)
{
    StoragePdb *storagePdb = g_storageInstance->GetPdb(pdbId);
    StorageAssert(storagePdb != nullptr);
    if (storagePdb == nullptr) {
        return DSTORE_FAIL;
    }
    VFSAdapter *vfs = storagePdb->GetVFS();
    TenantConfig *tenantConfig = g_storageInstance->GetGuc()->tenantConfig;
    StorageAssert(tenantConfig);
    FileParameter filePara;
    filePara.streamId = VFS_DEFAULT_FILE_STREAM_ID;
    filePara.flag = IN_PLACE_WRITE_FILE;
    filePara.fileSubType = DATA_FILE_TYPE;
    filePara.rangeSize = DECODE_DICT_FILE_RANGE_SIZE;
    filePara.maxSize = (uint64) DSTORE_MAX_BLOCK_NUMBER * BLCKSZ;
    filePara.recycleTtl = 0;
    filePara.mode = FILE_READ_AND_WRITE_MODE;
    filePara.isReplayWrite = false;
    errno_t rc = strcpy_s(filePara.storeSpaceName, STORESPACE_NAME_MAX_LEN,
                          tenantConfig->storeSpaces[0].storeSpaceName);
    storage_securec_check(rc, "\0", "\0");
    /* construct absolute path of decodedict file */
    char *decodeFilePath = static_cast<char *>(DstorePalloc(MAXPGPATH));
    if (decodeFilePath == nullptr) {
        return DSTORE_FAIL;
    }
    rc = sprintf_s(decodeFilePath, MAXPGPATH, "%s/%s", dataDir, fileName);
    storage_securec_check_ss(rc);

    /* make sure it has never created decodedict file */
    StorageAssert(!vfs->FileExists(decodeFilePath));

    RetStatus ret = vfs->CreateFile(fileName, filePara, fd);
    if (ret != DSTORE_SUCC) {
        ErrLog(DSTORE_PANIC, MODULE_LOGICAL_REPLICATION, ErrMsg("Create vfs file failed."));
        DstorePfree(decodeFilePath);
        return DSTORE_FAIL;
    }
    ret = vfs->Extend(*fd, GetOffsetByBlockNo(DECODE_DICT_EXTEND_PAGE_COUNT));
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_LOGICAL_REPLICATION,
            ErrMsg("extend decodedict failed, filePath(%s).", decodeFilePath));
        DstorePfree(decodeFilePath);
        return DSTORE_FAIL;
    }
    DstorePfree(decodeFilePath);
    return DSTORE_SUCC;
}

RetStatus DecodeDictFile::InitMetaPage(PdbId pdbId, FileDescriptor *fd1, FileDescriptor *fd2)
{
    StoragePdb *storagePdb = g_storageInstance->GetPdb(pdbId);
    StorageAssert(storagePdb != nullptr);
    VFSAdapter *vfs = storagePdb->GetVFS();
    /* initialize decode dict meta page */
    char *pageBuffer = static_cast<char *>(DstorePalloc0(BLCKSZ));
    if (STORAGE_VAR_NULL(pageBuffer)) {
        return DSTORE_FAIL;
    }
    DecodeDictMetaPage *metaPage = STATIC_CAST_PTR_TYPE(pageBuffer, DecodeDictMetaPage*);
    metaPage->Init();
    if (STORAGE_FUNC_FAIL(WritePage(pdbId, fd1, &metaPage->checksum, DECODE_DICT_META_BLOCK, pageBuffer)) ||
        STORAGE_FUNC_FAIL(WritePage(pdbId, fd2, &metaPage->checksum, DECODE_DICT_META_BLOCK, pageBuffer))) {
        DstorePfree(pageBuffer);
        return DSTORE_FAIL;
    }

    if ((vfs->Fsync(fd1) != DSTORE_SUCC) || (vfs->Fsync(fd2) != DSTORE_SUCC)) {
        ErrLog(DSTORE_ERROR, MODULE_LOGICAL_REPLICATION, ErrMsg("fsync decode dict file fail"));
        DstorePfree(pageBuffer);
        return DSTORE_FAIL;
    }

    DstorePfree(pageBuffer);
    return DSTORE_SUCC;
}


RetStatus DecodeDictFile::WritePage(PdbId pdbId, FileDescriptor *fd, uint32 *checksum,
                                    BlockNumber block, const char *page)
{
    StoragePdb *storagePdb = g_storageInstance->GetPdb(pdbId);
    StorageAssert(storagePdb != nullptr);
    VFSAdapter *vfs = storagePdb->GetVFS();
    *checksum = 0;
    *checksum = CompChecksum(static_cast<const void *>(page), BLCKSZ, CHECKSUM_CRC);
    RetStatus rc = vfs->WritePageSync(fd, block, page);
    if (STORAGE_FUNC_FAIL(rc)) {
        ErrLog(DSTORE_ERROR, MODULE_LOGICAL_REPLICATION, ErrMsg("Write failed. block(%u).", block));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}
}