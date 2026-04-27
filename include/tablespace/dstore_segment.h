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
 * dstore_segment.cpp
 *
 * IDENTIFICATION
 *        include/tablespace/dstore_segment.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_DSTORE_SEGMENT_H
#define DSTORE_DSTORE_SEGMENT_H

#include "common/memory/dstore_mctx.h"
#include "tablespace/dstore_tablespace.h"
#include "page/dstore_segment_meta_page.h"
#include "errorcode/dstore_tablespace_error_code.h"

namespace DSTORE {

#define INVALID_SEGMENT (nullptr)

const int EXTENT_SIZE_COUNT = 4; /* 4 level extent size provided */
const uint64 MAX_EXTENT_COUNT = 0xFFFFFFFFFFFFFFFFU;

/**
 * extent index range [0, 16)    : 64K
 * extent index range [16, 144)  : 1M
 * extent index range [144, 272) : 8M
 * extent index range [272, ...) : 64M
 */
const uint64 EXT_NUM_LINE[EXTENT_SIZE_COUNT] = {0, 16, 144, 272};
const ExtentSize EXT_SIZE_LIST[EXTENT_SIZE_COUNT] = {EXT_SIZE_8, EXT_SIZE_128, EXT_SIZE_1024, EXT_SIZE_8192};

class SegmentInterface : public BaseObject {
public:
    static SegmentInterface *AllocUndoSegment(PdbId pdbId, TablespaceId tablespaceId, SegmentType type,
                                          BufMgrInterface *bufMgr);
    static RetStatus AllocExtent(PdbId pdbId, TablespaceId tablespaceId, ExtentSize extentSize, PageId *newExtentPageId,
                                 bool *isReUseFlag);
    virtual RetStatus DropSegment() = 0;
    virtual RetStatus Extend(ExtentSize extSize, PageId *extMetaPageId) = 0;
    virtual PageId GetSegmentMetaPageId() = 0;
    virtual ~SegmentInterface() = default;
};

class Segment : public SegmentInterface {
public:
    /**
     * Construct a instance to read a existing segment, must call Init() to use Segment functions.
     * @param segmentId First PageId of Segment
     * @param type Segment type
     * @param tablespace TableSpace instance of the Segment
     * @param bufMgr Buffer manager instance
     */
    Segment(PdbId pdbId, const PageId &segmentId, SegmentType type, TablespaceId tablespaceId, BufMgrInterface *bufMgr);
    ~Segment() override;

    /**
     * Call after Segment constructor to get segment head buffer (do not release)
     * @return DSTORE_SUCC if Segment is ready, or DSTORE_FAIL if something wrong
     */
    RetStatus Init();
    RetStatus Extend(ExtentSize extSize, PageId *extMetaPageId) override;
    PageId GetSegmentMetaPageId() override;
    RetStatus DropSegment() override;
    inline bool IsTempSegment() const
    {
        return (m_type == SegmentType::HEAP_TEMP_SEGMENT_TYPE || m_type == SegmentType::INDEX_TEMP_SEGMENT_TYPE);
    }
    inline TablespaceId GetTablespaceId() const
    {
        return m_tablespaceId;
    }

    inline SegmentType GetSegmentType()
    {
        return m_type;
    }
    PageId GetLastExtentPageId()
    {
        BufferDesc *segMetaPageBuf = ReadSegMetaPageBuf(LW_SHARED);
        PageId lastExtentStartPageId = GetSegMetaPage(segMetaPageBuf)->GetLastExtent();
        UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
        return lastExtentStartPageId;
    }
    inline PdbId GetPdbId()
    {
        return m_pdbId;
    }
protected:
    PageId m_segmentId; /* First page id of segment is Segment ID */
    BufMgrInterface *m_bufMgr;
    SegmentType m_type;
    bool m_isInitialized = false; /* Must call Init() after construct Segment object */
    bool m_isDrop = false; /* Make sure Segment could not use after Drop() called */
    TablespaceId m_tablespaceId;
    PdbId m_pdbId;

    virtual RetStatus ExtendSegmentInternal(ExtentSize extSize, PageId *extMetaPageId);
    RetStatus InitExtMetaPage(const PageId &extMeta, ExtentSize extSize) const;
    RetStatus LinkNextExtInPrevExt(const PageId &prevExtMetaPageId, const PageId &nextExtMetaPageId) const;
    RetStatus SegMetaLinkExtent(const PageId &newExtMetaPageId, ExtentSize extSize, bool isSecondExtent);

    RetStatus SegMetaUnlinkExtent(const PageId &unlinkExtMetaPageId, ExtentSize unlinkExtSize,
        const PageId &newNextExtMetaPageId);
    RetStatus DropSegmentInternal();
    RetStatus InvalidateBufferInExtent(const PageId &startPageId, ExtentSize extentSize);

    inline bool IsInitialized() const
    {
        return m_isInitialized;
    }
    inline SegmentMetaPage *GetSegMetaPage(BufferDesc *segMetaPageBuf)
    {
        return static_cast<SegmentMetaPage *>(segMetaPageBuf->GetPage());
    }

    BufferDesc *ReadSegMetaPageBuf(LWLockMode mode)
    {
        BufferDesc *segMetaPageBuf = m_bufMgr->Read(m_pdbId, m_segmentId, mode);
        STORAGE_CHECK_BUFFER_PANIC(segMetaPageBuf, MODULE_SEGMENT, m_segmentId);
        return segMetaPageBuf;
    }

    void UnlockAndReleaseSegMetaPageBuf(BufferDesc *&segMetaPageBuf)
    {
        m_bufMgr->UnlockAndRelease(segMetaPageBuf);
        segMetaPageBuf = nullptr;
    }
};

inline bool SegmentIsValid(SegmentInterface *segment)
{
    return (segment != INVALID_SEGMENT);
}

class ExtentsScanner : public BaseObject {
public:
    ExtentsScanner(PdbId pdbId, const PageId &segmentId, BufMgrInterface *bufMgr)
        : m_pdbId(pdbId),
          m_segmentId(segmentId),
          m_extMetaPageId(INVALID_PAGE_ID),
          m_extSize(INVALID_EXT_SIZE),
          m_nextExtMeta(INVALID_PAGE_ID),
          m_bufMgr(bufMgr)
    {}
    ~ExtentsScanner() = default;

    bool Next();
    bool CheckSegmentMeta();
    PageId GetExtMetaPageId()
    {
        return m_extMetaPageId;
    }
    ExtentSize GetExtSize()
    {
        return m_extSize;
    }

private:
    PdbId m_pdbId;
    PageId m_segmentId;

    PageId m_extMetaPageId;
    ExtentSize m_extSize;

    PageId m_nextExtMeta;
    BufMgrInterface *m_bufMgr;
};

} /* namespace DSTORE */

#endif  // DSTORE_STORAGE_SEGMENT_H
