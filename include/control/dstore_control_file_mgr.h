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
 * dstore_control_file_mgr.h
 *  control file mgr for dstore
 *
 *
 * IDENTIFICATION
 *        dstore/include/control/dstore_control_file_mgr.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_CONTROL_FILE_MGR_H
#define DSTORE_CONTROL_FILE_MGR_H

#include "errorcode/dstore_control_error_code.h"
#include "control/dstore_control_disk_file.h"
#include "control/dstore_control_file_page.h"
#include "control/dstore_control_struct.h"

namespace DSTORE {

/* dump tool need this method */
#pragma GCC visibility push(default)
MetaPageCheckResult CheckMetaPage(void* firstMetaPage, void* secondMetaPage); /* Only use in dump */
#pragma GCC visibility pop
bool CheckPageCrc(uint32 *checksum, const uint8 *page);

struct PageHandle {
    ControlDiskFile *file;
    uint64 metaPageTerm;
    uint16 numDirtyBlocks;
    uint16 maxSize;
    BlockNumber *dirtyBlocks;
    ControlGroupType groupType;
    MetaPageCheckResult checkResult;
    void Destroy()
    {
        checkResult = NO_VALID_META_PAGE;
        file = nullptr;
        metaPageTerm = 0;
        numDirtyBlocks = 0;
        maxSize = 0;
        DstorePfreeExt(dirtyBlocks);
        dirtyBlocks = nullptr;
    }
};

class ControlFileMgr : public BaseObject {
public:
    explicit ControlFileMgr(PdbId pdbId, DeployType deployType, DstoreMemoryContext context);

    ~ControlFileMgr();

    DISALLOW_COPY_AND_MOVE(ControlFileMgr);

    RetStatus Init(VFSAdapter *vfs, char *storeSpaceName, const char *dataDir);

    RetStatus CreateControlFiles();

    RetStatus OpenControlFiles();
    RetStatus CloseControlFiles();
    RetStatus LoadControlFile();

    inline void SetEnableCachePage(bool isEnable)
    {
        m_enableCachePage.store(isEnable, std::memory_order_release);
    }
    inline uint32 GetBlockCount()
    {
        return static_cast<uint32>(ControlFilePageMap::CONTROLFILE_PAGEMAP_MAX);
    }
    inline uint64 GetFileSize()
    {
        return static_cast<uint64>(ControlFilePageMap::CONTROLFILE_PAGEMAP_MAX) * BLCKSZ;
    }
    inline PdbId GetPdbId()
    {
        return m_pdbId;
    }

    RetStatus GetValidMetaPage(PageHandle *pageHandle, BlockNumber metaBlockNum);
    char *ReadOnePage(PageHandle *pageHandle, BlockNumber blockNumber);
    RetStatus MarkPageDirty(PageHandle *pageHandle, BlockNumber blockNumber);
    RetStatus PostPageHandle(PageHandle *pageHandle, BlockNumber metaPageBlockNum);
    void CleanCache(PageHandle *pageHandle, BlockNumber metaPageBlockNum);

    bool CheckPageConsistency(BlockNumber block);
    RetStatus CheckCrcAndRecoveryForGroup(BlockNumber metaBlock, ControlGroupType groupType,
        PageHandle *pageHandle = nullptr);
    RetStatus CheckCrcAndRecoveryForFileMeta();
    RetStatus CheckPageCrcMatch(ControlDiskFile *file, BlockNumber block, bool &isMatch);

    void CleanPageStateValid(uint32 blockNumber);
    char *GetPage(BlockNumber blockNumber, ControlDiskFile *file);
    RetStatus WritePage(ControlDiskFile *file, BlockNumber blockNumber, char* inbuffer);
    RetStatus FlushPageBuffer(ControlDiskFile *file);

    void CleanPageValid(uint32 blockNumber);

    inline ControlDiskFile* GetFile1()
    {
        return m_file1;
    }

    inline ControlDiskFile* GetFile2()
    {
        return m_file2;
    }

    RetStatus WriteAndSyncFiles(); /* Only usable when create control file */

#ifdef UT
    /* the following interface is used for unit tests */
    uint32 UtGetPageCount();
    ControlPage *UtGetPage(BlockNumber blockNumber);
    uint32 UtGetPageState(uint32 blockNumber) const;
    void UtInvalidateAll();
    char *UtGetFile1Path()
    {
        return m_file1->UTGetFilePath();
    }
#endif

#ifndef UT
protected:
#else
    RetStatus UTWriteFileForFaultInjection(BlockNumber block, bool isFile1Fault, bool isUpdateCheckSum);
#endif
    RetStatus CreateOneControlFile(ControlDiskFile *file);
    RetStatus AllocPageBuffer(uint32 blockCount);
    void FreePageBuffer(uint32 pageCount, char **pageBuffer) const noexcept;

    void MarkPageStateValid(uint32 blockNumber);
    void MarkPageStateDirty(uint32 blockNumber);
    void CleanPageStateDirty(uint32 blockNumber);
    bool IsPageStateDirty(uint32 blockNumber) const;
    bool IsPageStateValid(uint32 blockNumber) const;

    ControlFileMetaPage *GetMetaPage(ControlDiskFile *file);
    RetStatus WritePage(ControlDiskFile *file, BlockNumber blockNumber);
    void UpdateControlPageCrc(uint32 *checksum, const uint8 *page);
    RetStatus WriteAllDirtyPage(PageHandle *pageHandle, ControlDiskFile *file);
    void CleanAllPageStateDirty(PageHandle *pageHandle);
    RetStatus CheckMetaPage(bool *isValid, bool *isFileWriting, uint64 *term, BlockNumber metaBlockNum,
        ControlDiskFile *file);
    RetStatus SinglePageRecovery(ControlDiskFile *targetFile, char *sourceBuffer, BlockNumber block);
    RetStatus GroupPagesRecovery(ControlDiskFile *targetFile, ControlDiskFile *sourceFile, char *sourceMetaBuffer,
        BlockNumber metaBlock);
    RetStatus CheckCrcAndRecoveryWithMetaPage(BlockNumber metaBlock, PageHandle *pageHandle = nullptr);
    RetStatus RecoveryWithMetaPage(ControlDiskFile *targetFile, ControlDiskFile *sourceFile, char *sourceMetaBuffer,
        BlockNumber metaBlock);
    bool IsSameDataPages(ControlBasePage *page1, ControlBasePage *page2);

    ControlDiskFile *m_file1;
    ControlDiskFile *m_file2;
    PdbId m_pdbId;
    char **m_pageBuffer;
    uint32 *m_state;
    BlockNumber m_pageCount;
    DeployType m_deployType; /* Decide whether to invalidate the cache */
    DstoreMemoryContext m_memoryContext;
    std::atomic<bool> m_enableCachePage;
};

}  // namespace DSTORE

#endif  // DSTORE_CONTROL_FILE_MGR_H