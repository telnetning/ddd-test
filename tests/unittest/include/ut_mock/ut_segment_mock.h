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
 */
#ifndef UT_SEGMENT_MOCK_H
#define UT_SEGMENT_MOCK_H

#include "common/dstore_datatype.h"
#include "tablespace/dstore_tablespace.h"
#include "tablespace/dstore_heap_normal_segment.h"
#include "tablespace/dstore_index_normal_segment.h"
#include "tablespace/dstore_segment.h"
#include "gmock/gmock.h"
#include "ut_utilities/ut_dstore_framework.h"

#include <vector>
#include <map>
#include <mutex>

namespace DSTORE {

static const BlockNumber MAX_BLOCK_NUM_MOCK = 0xFEEEU;
#define BLKSIZE_ROUND_DOWN(x) (((uint32)(x)) & (~(BLCKSZ-1)))
#define BLKSIZE_ROUND_UP(x) ( (((uint32)(x)) + BLCKSZ-1)  & (~(BLCKSZ-1)) )

class MockVfsFile {
public:
    PageId GetNewPage(bool isBgExtension = false);
    RetStatus Extend(int32 size, PageId *first_page = nullptr);
    inline FileId get_file_id() const
    {
        return m_fileId;
    }
private:
    const FileId m_fileId = 1;
    BlockNumber m_curBlkNum = 1;
    BlockNumber m_upperBlkNum = 1024;
    BlockNumber m_undo_upper_blk_num = 0;
    std::mutex m_mutex;
};

class MockSegment : public SegmentInterface {
public:
    RetStatus DropSegment() override;
    RetStatus Extend(ExtentSize extSize, PageId *extMetaPageId) override;
    PageId GetSegmentMetaPageId() override
    {
        if (m_isDrop) {
            return INVALID_PAGE_ID;
        }
        return m_segmentId;
    }
    inline PdbId GetPdbId()
    {
        return m_pdbId;
    }
protected:
    static SegmentInterface *AllocMockSegment(PdbId pdbId, TablespaceId tablespaceId, SegmentType type,
                                              BufMgrInterface *bufMgr)
    {
        PageId segmentId{};
        bool isReusedFlag = false;
        if (STORAGE_FUNC_FAIL(AllocExtent(pdbId, tablespaceId, FIRST_EXT_SIZE, &segmentId, &isReusedFlag))) {
            return INVALID_SEGMENT;
        }
        BufferDesc *bufDesc = bufMgr->Read(pdbId, segmentId, LW_EXCLUSIVE);
        auto *headPage = (SegmentMetaPage*)bufDesc->GetPage();

        bufMgr->UnlockAndRelease(bufDesc);
        SegmentInterface *segment = DstoreNew(g_dstoreCurrentMemoryContext) MockSegment(pdbId, segmentId, type, tablespaceId, bufMgr);
        return segment;
    }

private:
    PageId m_segmentId;
    PageId m_lastExtPageId;
    bool m_isDrop{false};
    SegmentType m_type;
    BufMgrInterface* m_bufMgr;
    BufferDesc *m_metaPageBuf;
    TablespaceId m_tablespaceId;
    PdbId m_pdbId;

    friend class UtMockModule;
    friend class MockDataSegment;
    friend class MockIndexSegment;

    MockSegment(PdbId pdbId, const PageId &segmentId, SegmentType type, TablespaceId tablespaceId, BufMgrInterface *bufMgr)
            : m_pdbId(pdbId), m_segmentId(segmentId), m_lastExtPageId(segmentId), m_type(type), m_bufMgr(bufMgr)
    {
        m_tablespaceId = tablespaceId;
    }

    ~MockSegment()
    {
        if (m_metaPageBuf) {
            m_bufMgr->Release(m_metaPageBuf);
        }
    }
};

class MockDataSegment : public DataSegment {
public:
    RetStatus DropSegment() override;
    PageId GetPageFromFsm(uint32 size, uint16 retryTime);
    PageId GetPageFromFsm(uint32 spaceNeeded, uint16 retryTime, uint32 *spaceInFsm);
    PageId GetNewPage();
    PageId GetSegmentMetaPageId();
    RetStatus UpdateFsm(const PageId &dataPageId, uint32 size);
    RetStatus UpdateFsm(const FsmIndex &fsmIndex, uint32 remainSpace);
    RetStatus UpdateFsmAndSearch(const FsmIndex &fsmIndex, uint32 remainSpace, uint32 spaceNeeded,
                                 uint16 retryTimes, PageId *pageId, uint32* spaceInfsm);
    PageId GetFirstDataPage();
    RetStatus InitSegment() override;

    PageId GetPage()
    {
        return INVALID_PAGE_ID;
    }

    PageId GetFsmMetaPageId(uint32 fsmId)
    {
        return INVALID_PAGE_ID;
    }

    uint64 GetDataBlockCount()
    {
        return 0;
    }
    ~MockDataSegment()
    {
        delete m_segment;
    }
    PageId GetNewPageFromUnassignedPages()
    {
        return INVALID_PAGE_ID;
    }

protected:
    static DataSegment *AllocMockDataSegment(PdbId pdbId, SegmentType type,
        TablespaceId tablespaceId, BufMgrInterface *bufMgr)
    {
        SegmentInterface *segment = MockSegment::AllocMockSegment(pdbId, tablespaceId, type, bufMgr);
        if (!SegmentIsValid(segment)) {
            return INVALID_SEGMENT;
        }
        return DstoreNew(g_dstoreCurrentMemoryContext) MockDataSegment(pdbId, segment, type, bufMgr, tablespaceId);
    }

private:
    struct Cmp {
        /* the functor should be strict weak ordering */
        bool operator ()(PageId const &a, PageId const &b) const
        {
            if (a.m_fileId < b.m_fileId) {
                return true;
            } else if (b.m_fileId < a.m_fileId) {
                return false;
            } else if (a.m_blockId < b.m_blockId) {
                return true;
            } else if (b.m_blockId < a.m_blockId) {
                return false;
            } else {
                return false;
            }
        }
    };
    std::map<PageId, uint32, Cmp> m_fsm;
    std::mutex m_fsmMutex;
    bool m_isDrop{false};
    PageId m_segmentHead{INVALID_PAGE_ID};
    PageId m_curPage{INVALID_PAGE_ID};
    PageId m_endPage{INVALID_PAGE_ID};
    std::vector<PageId> m_pageList;
    uint64 m_curScanIndex{0};
    uint64 m_pageCount{0};
    PageId m_firstDataPage = INVALID_PAGE_ID;
    BufMgrInterface* m_mockBufMgr;
    SegmentType m_segType;
    TablespaceId tablespaceId;
    PageType m_pageType;
    SegmentInterface *m_segment;
    friend class UtMockModule;

    MockDataSegment(PdbId pdbId, SegmentInterface *segment, SegmentType type, BufMgrInterface *bufMgr, TablespaceId tablespaceId)
            : DataSegment(pdbId, segment->GetSegmentMetaPageId(), type, tablespaceId, bufMgr, g_dstoreCurrentMemoryContext)
    {
        m_segment = segment;
        m_segType = type;
        m_mockBufMgr = bufMgr;
        m_tablespaceId = tablespaceId;
        m_curPage = segment->GetSegmentMetaPageId();
        m_endPage = CalculateExtEndPage(segment->GetSegmentMetaPageId(), FIRST_EXT_SIZE);
        if (m_segType == SegmentType::HEAP_SEGMENT_TYPE) {
            m_pageType = PageType::HEAP_PAGE_TYPE;
        } else {
            m_pageType = PageType::INDEX_PAGE_TYPE;
        }
    }

    static PageId CalculateExtEndPage(const PageId &firstPage, ExtentSize extSize)
    {
        return {firstPage.m_fileId, firstPage.m_blockId + extSize - 1};
    }
};

class MockIndexSegment : public IndexNormalSegment {
public:
    MOCK_METHOD(PageId, GetNewPageFromUnassignedPages, (), (override));

protected:
    static MockIndexSegment *AllocMockIndexSegment(PdbId pdbId, SegmentType type,
        TablespaceId tablespaceId, BufMgrInterface *bufMgr)
    {
        SegmentInterface *segment = MockSegment::AllocMockSegment(pdbId, tablespaceId, type, bufMgr);
        if (!SegmentIsValid(segment)) {
            return INVALID_SEGMENT;
        }
        return DstoreNew(g_dstoreCurrentMemoryContext) MockIndexSegment(pdbId, segment, type, bufMgr, tablespaceId);
    }

private:
    MockIndexSegment(PdbId pdbId, SegmentInterface *segment, SegmentType type, BufMgrInterface *bufMgr, TablespaceId tablespaceId)
            : IndexNormalSegment(pdbId, segment->GetSegmentMetaPageId(), tablespaceId, bufMgr, g_dstoreCurrentMemoryContext)
    {}
};

};

#endif
