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

#ifndef DSTORE_UT_SEGMENT_H
#define DSTORE_UT_SEGMENT_H

#include "ut_utilities/ut_dstore_framework.h"
#include "tablespace/dstore_segment.h"
#include "ut_mock/ut_mock.h"
#include "ut_buffer/ut_buffer_fake_instance.h"

using namespace DSTORE;
const uint32 TEST_SEGMENT_TYPE_COUNT = 3;
const SegmentType segmentType[TEST_SEGMENT_TYPE_COUNT] = {SegmentType::HEAP_SEGMENT_TYPE,
    SegmentType::INDEX_SEGMENT_TYPE, SegmentType::UNDO_SEGMENT_TYPE};

class SegmentTest : public DSTORETEST {
public:
    static DataSegment* UTAllocSegment(PdbId pdbId, TablespaceId tablespaceId, BufMgrInterface *bufMgr,
        DSTORE::SegmentType segType, Oid tableOid = DSTORE_INVALID_OID);
protected:
    void SetUp() override
    {
        DSTORETEST::SetUp();
        UtInstance *instance = DstoreNew(m_ut_memory_context)UtInstance();
        instance->Install(&DSTORETEST::m_guc, m_ut_memory_context);
        instance->Startup(&DSTORETEST::m_guc);

        m_instance = g_storageInstance;
        m_testBufMgr = g_storageInstance->GetBufferMgr();
        m_testTbs = UtMockModule::UtGetTableSpace(2);
    }
    void TearDown() override
    {
        UtMockModule::UtDropTableSpace(m_testTbs);
        UtInstance *instance = (UtInstance *)g_storageInstance;
        instance->Shutdown();
        delete instance;
        DSTORETEST::TearDown();
    }
    void AllocNewExtentTest(SegmentInterface *segment, ExtentSize extSize, PageType type)
    {
        BufferDesc *buffer = m_testBufMgr->
            Read(g_defaultPdbId, segment->GetSegmentMetaPageId(), LW_SHARED);
        auto *headPage = (SegmentMetaPage*)buffer->GetPage();
        PageId curExt = headPage->GetLastExtent();
        uint64 curExtCount = headPage->GetExtentCount();
        uint64 curTotalBlock = headPage->GetTotalBlockCount();
        m_testBufMgr->UnlockAndRelease(buffer);
        PageId newExt;
        ASSERT_EQ(segment->Extend(extSize, &newExt), 0);
        ASSERT_NE(newExt, INVALID_PAGE_ID);

        /* Check segment head info */
        buffer = m_testBufMgr->Read(g_defaultPdbId, segment->GetSegmentMetaPageId(), LW_SHARED);
        headPage = (SegmentMetaPage*)buffer->GetPage();
        ASSERT_EQ(headPage->GetLastExtent(), newExt);
        ASSERT_EQ(headPage->GetExtentCount(), curExtCount + 1);
        ASSERT_EQ(headPage->GetTotalBlockCount(), curTotalBlock + extSize);
        if (curExt == segment->GetSegmentMetaPageId()) {
            ASSERT_EQ(headPage->extentMeta.nextExtMetaPageId, newExt);
        }
        m_testBufMgr->UnlockAndRelease(buffer);

        /* Check old last extent info */
        if (curExt != segment->GetSegmentMetaPageId()) {
            buffer = m_testBufMgr->Read(g_defaultPdbId, curExt, LW_SHARED);
            auto *extMetaPage = (SegExtentMetaPage *)buffer->GetPage();
            ASSERT_EQ(extMetaPage->extentMeta.nextExtMetaPageId, newExt);
            m_testBufMgr->UnlockAndRelease(buffer);
        }

        /* Check new extent info */
        buffer = m_testBufMgr->Read(g_defaultPdbId, newExt, LW_SHARED);
        auto *extMetaPage = (SegExtentMetaPage *)buffer->GetPage();
        ASSERT_EQ(extMetaPage->extentMeta.extSize, extSize);
        ASSERT_EQ(extMetaPage->extentMeta.nextExtMetaPageId, INVALID_PAGE_ID);
        m_testBufMgr->UnlockAndRelease(buffer);
    }
    StorageInstance *m_instance;
    TableSpaceInterface *m_testTbs;
    BufMgrInterface *m_testBufMgr;
};

#endif //DSTORE_UT_SEGMENT_H
