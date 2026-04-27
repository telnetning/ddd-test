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
 * dstore_tablespace_internal.h
 * define class TbsDataFileBitmapMgr and TbsDataFile
 *
 *
 * IDENTIFICATION
 *        storage/include/page/dstore_tablespace_internal.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_DSTORE_TABLESPACE_INTERNAL_H
#define DSTORE_DSTORE_TABLESPACE_INTERNAL_H

#include <fcntl.h>
#include "page/dstore_itemptr.h"
#include "page/dstore_bitmap_page.h"
#include "buffer/dstore_buf_mgr.h"
#include "page/dstore_bitmap_meta_page.h"
#include "control/dstore_control_file.h"
#include "dstore_tbs_temp_bitmappage_hashtable.h"
#include "page/dstore_tbs_file_meta_page.h"
#include "page/dstore_tbs_space_meta_page.h"
#include "framework/dstore_vfs_adapter.h"

namespace DSTORE {

#define TBS_CURRENT_XID (Xid(0))

constexpr uint32_t MAX_BITMAP_PAGE_COUNT = 8448;
constexpr uint64 SEGMENT_HEAD_MAGIC = 0x44414548544e454d;
constexpr uint32_t INIT_FILE_PAGE_COUNT = 1024 * 8;  /* page count */
/* When the size of a file is less than 1 GB, the file is expanded FILE_EXTEND_SMALL_STEP each time. */
constexpr uint32_t FILE_EXTEND_SMALL_STEP = 16 * 1024;    /* 128M (16 * 1024 * 8K) */
constexpr uint16 MAXTABLESPACENAME = 256;

#ifdef UT
constexpr uint32_t TEMPLATE_FILE_EXTEND_SMALL_STEP = FILE_EXTEND_SMALL_STEP;
#else
constexpr uint32_t TEMPLATE_FILE_EXTEND_SMALL_STEP = 1024;    /* 8M */
#endif

/* When the size of a file is more than 1 GB, the file is expanded FILE_EXTEND_SMALL_STEP each time. */
constexpr uint32_t FILE_EXTEND_BIG_STEP = FILE_EXTEND_SMALL_STEP * 8;

enum MetaBlockId : uint32 {
    TBS_FILE_META_PAGE = 0,
    TBS_SPACE_META_PAGE = 1,
    TBS_BITMAP_META_PAGE = 2,
};

enum class TbsStatus {
    TBS_INVALID,
    TBS_OPENED
};

struct FreeBitsSearchPos {
    PageId m_bitmapPageId;   // TbsBitmapPage's Offset in the TbsDataFile
    uint32 m_freeBitsSearchPos;
};

class TbsDataFileBitmapMgr : public BaseObject {
public:
    explicit TbsDataFileBitmapMgr();

    ~TbsDataFileBitmapMgr();

    RetStatus InitFreeBitsSearchPos();

    FreeBitsSearchPos *GetFreeBitsSearchPos(uint32 pageNo);

    PageId GetNewGroupStart(TbsBitmapMetaPage *bitmapMetaPage);

    RetStatus ForwardFreeBitsSearchPos(uint32 bitNo, uint32 bitmapPageNo);

    void BackwardFreeBitsSearchPos(uint32 bitNo, uint32 bitmapPageNo);

    uint32 FindExtentStartPos(uint32 bitmapPageNo);

private:
    uint32 m_pageCount;
    FreeBitsSearchPos *m_startPosMem; /* One-piece memory used by m_startPos */
    FreeBitsSearchPos *m_startPos[MAX_BITMAP_PAGE_COUNT];
};

class TbsDataFile : public BaseObject {
public:
    /* Each extentSize corresponds to a file. */
    TbsDataFile(PdbId pdbId, VFSAdapter *vfs, FileId fileId, uint64 maxBlockCount, ExtentSize extentSize, bool isTemp);

    virtual ~TbsDataFile();

    RetStatus Create(BlockNumber initBlockCount, const char* storeSpaceName);
    virtual RetStatus Init();
    virtual void Destroy();

    FileId GetFileId() const;

    FileParameter GetFileParameter() const;

    const char *GetFileName() const;
    const char *GetStoreSpaceName() const;
    void SetStoreSpaceName(const char *storeSpaceName);
    PdbId GetPdbId() const;
    uint64 GetMaxBlockCount() const;
    void SetMaxBlockCount(uint64 maxBlockCount);

    uint64 GetTotalBlockCount();

    RetStatus AllocOid(bool isInitDb, uint32 *nextOid, uint32 prefetchCount);

    RetStatus ProcessFileSizeExceedLimit(int64 targetBlockCount);
    RetStatus AllocExtent(PageId *newExtentPageId, bool *isReUseFlag);

    RetStatus DoFreeExtent(TbsBitmapMetaPage *bitmapMetaPage, const PageId curExtentPageId,
                           uint16 bitmapGroupNo, uint16 bitmapNo, uint16 bitNo);
    RetStatus FreeExtent(const PageId &curExtentPageId);

    RetStatus RemoveFile(const char *fullFileName);
    TbsBitmapMetaPage *ReadBitmapMetaPage();
    ExtentSize GetExtentSize();
    void SetExtentSize(ExtentSize extentSize);

    BufMgrInterface *GetBufferMgr();
    FreeBitsSearchPos *GetFreeBitsSearchPos(uint32 bitmapPageNo);

    virtual RetStatus GetBitmapMetaPage(LWLockMode lock, BufferDesc **pageDesc, TbsBitmapMetaPage **page);

    virtual RetStatus GetBitmapPage(const PageId &pageId, LWLockMode lock, BufferDesc **pageDesc, TbsBitmapPage **page);

    virtual RetStatus GetFileMetaPage(LWLockMode lock, BufferDesc **pageDesc, TbsFileMetaPage **page);

    virtual RetStatus GetSpaceMetaPage(LWLockMode lock, BufferDesc **pageDesc, TbsSpaceMetaPage **page);

    ControlDataFilePageItemData GetDataFilePageItemData();
    void SetDataFilePageItemData(ControlDataFilePageItemData &datafileItem);

    virtual void UnlockAndReleaseFileMetaPage(BufferDesc *pageDesc);

    virtual void UnlockAndReleaseMetaPage(BufferDesc *pageDesc);
    
    virtual void UnlockAndReleasePage(BufferDesc *pageDesc,
        BufferPoolUnlockContentFlag flag = BufferPoolUnlockContentFlag());

    RetStatus InitBitmap();

    RetStatus InitBitmapMgr();

    RetStatus InitTbsFileMeta();

    RetStatus InitTbsSpaceMeta();

#ifdef UT
    bool LocateBitsPosByPageId(TbsBitmapMetaPage *bitmapMetaPage, const PageId &extentMetaPageId,
                               uint16 *mapGroupNo, uint8 *bitmapPageNo, uint16 *bitNo) const;
#endif
    
private:
    char m_fileName[MAX_FILE_NAME_LEN];  /* Persistence to ControlFile */
    char m_storeSpaceName[MAXTABLESPACENAME];
    FileId m_fileId;   /* Persistence to ControlFile */
    FileParameter m_filePara; /* para when create file. Persistence to ControlFile */
    uint64 m_maxBlockCount;  /* the max block count of the file. Persistence to ControlFile */
    bool m_isTempFile;       /* temporary files in temporary tablespace */
    TbsDataFileBitmapMgr *m_bitmapMgr;  /* One file corresponds to one m_bitmapMgr. */
    ExtentSize m_extentSize;
    PdbId m_pdbId;
    ControlDataFilePageItemData m_ctrlDatafileItem;
    pthread_rwlock_t m_mutex;
    std::atomic<FileDescriptor *> m_fd;
    VFSAdapter *m_vfs;

    void InitFilePara(const char* storeSpaceName);

    PageId AllocExtentFromBitmapPage(const PageId &bitmapPageId, int64 *targetBlockCount,
            uint32 bitmapGroup, uint32 freePage, TbsBitmapMetaPage *bitmapMeta);

    RetStatus InitBitmapPages(PageId groupFirstPage);
#ifndef UT
    bool LocateBitsPosByPageId(TbsBitmapMetaPage *bitmapMetaPage, const PageId &extentMetaPageId,
                               uint16 *mapGroupNo, uint8 *bitmapPageNo, uint16 *bitNo) const;
#endif
    void DoAddBitmapGroup(BufferDesc *bitmapMetaDesc, TbsBitmapMetaPage *mapMeta, const PageId &startMapNo,
        BlockNumber realBlockCount);

    RetStatus AddBitmapGroup(uint16 oldGroupCount);

    RetStatus ExtendDataFile(uint64 size, TbsBitmapMetaPage *bitmapMetaPage, uint64 *realBlockCount);

    void UpdateHwmIfNeed(const PageId &allocatedPageId, uint32 beforeAllocHwm, bool *isReUseFlag);

    RetStatus AllocExtentFromExistGroups(int64 *targetBlockCount, PageId *newExtentPageId, uint16 *oldGroupCount,
                                         bool *isReUseFlag);

    PageId AllocExtentAsBit(uint32 bitmapPageNo, TbsBitmapMetaPage *bitmapMeta, BufferDesc *bitmapBufferDesc,
        TbsBitmapPage *bitmapPage, int64 *targetBlockCount);

    void WriteWalForSetBit(BufferDesc *bitmapBuffer, TbsBitmapPage *bitmapPage,
                            const PageId &bitmapPageFound, uint16 bitNo);
    /**
     * @param groupIndex which bitmapGroup to modify;
     * @param firstFreePageNo the number of first bitmap Page which has free bit in group;
     * @param afterAlloc true if after AllocExtent to modify firstFreePageNo, false if after FreeExtent;
     * @param needBeginAtomicWal true if need to call BeginAtomicWal;
     */
    RetStatus UpdateFirstFreePageInGroup(uint16 groupIndex, uint8 firstFreePageNo, bool afterAlloc,
        bool needBeginAtomicWal);
};

/*
 * The difference between a TempTbsDataFile and a TbsDataFile is that bitmapMeta page and bitmap pages
 * are stored in the memory but not in buffer pool.
 * No need to write wal when modifying these two kinds of pages.
 * But, LWlock is required when access them.
*/
class TempTbsDataFile final: public TbsDataFile {
public:
    TempTbsDataFile(PdbId pdbId, VFSAdapter *vfs, FileId fileId, uint64 maxBlockCount,
                    TbsTempBitmapPageHashTable *bitmapPagetable);

    ~TempTbsDataFile() override;
    void Destroy() override;

    RetStatus InitBitmapPageTable();
private:
    TbsTempBitmapPageHashTable *m_bitmapPagetable;  /* manage bitmapMeta page and bitmap pages in memory */
    RetStatus GetBitmapMetaPage(LWLockMode lock, BufferDesc **pageDesc, TbsBitmapMetaPage **page) override;

    RetStatus GetBitmapPage(const PageId &pageId, LWLockMode lock,
        BufferDesc **pageDesc, TbsBitmapPage **page) override;

    RetStatus GetFileMetaPage(LWLockMode lock, BufferDesc **pageDesc, TbsFileMetaPage **page) override;

    RetStatus GetSpaceMetaPage(LWLockMode lock, BufferDesc **pageDesc, TbsSpaceMetaPage **page) override;

    void UnlockAndReleaseFileMetaPage(BufferDesc *pageDesc) override;

    void UnlockAndReleaseMetaPage(BufferDesc *pageDesc) override;

    void UnlockAndReleasePage(BufferDesc *pageDesc,
        BufferPoolUnlockContentFlag flag = BufferPoolUnlockContentFlag()) override;
};
}  // namespace DSTORE

#endif  // DSTORE_STORAGE_TABLESPACE_INTERNAL_H
