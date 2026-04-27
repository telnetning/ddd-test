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

#ifndef DSTORE_UT_DATA_SEGMENT_H
#define DSTORE_UT_DATA_SEGMENT_H

#include "ut_utilities/ut_dstore_framework.h"
#include "tablespace/dstore_data_segment.h"
#include "heap/dstore_heap_struct.h"
#include "heap/dstore_heap_handler.h"
#include "page/dstore_heap_page.h"
#include "page/dstore_index_page.h"
#include "ut_mock/ut_mock.h"
#include "ut_buffer/ut_buffer_fake_instance.h"
#include "ut_tablespace/ut_segment.h"

using namespace DSTORE;


struct FsmParam {
    HeapNormalSegment *segment;
    PageId pageId;
    uint32 remainSpace;
};

class DataSegmentTest : virtual public DSTORETEST {
protected:
    void SetUp() override
    {
        DSTORETEST::SetUp();
        UtInstance *instance = DstoreNew(m_ut_memory_context)UtInstance();
        instance->Install(&DSTORETEST::m_guc, m_ut_memory_context);
        instance->Startup(&DSTORETEST::m_guc);
        m_instance = g_storageInstance;
        m_testBufMgr = m_instance->GetBufferMgr();

        TablespaceMgr *tablespaceMgr = g_storageInstance->GetPdb(g_defaultPdbId)->GetTablespaceMgr();
        m_testTbs = (TableSpaceInterface *)tablespaceMgr->OpenTablespace(2, DSTORE::DSTORE_ACCESS_EXCLUSIVE_LOCK);
        FileId fileId;
        for (uint16 i = 0; i < EXTENT_TYPE_COUNT; i++) {
            (void)((TableSpace *)m_testTbs)->AllocAndAddDataFile(g_defaultPdbId, &fileId, EXTENT_SIZE_ARRAY[i], false);
        }
        tablespaceMgr->CloseTablespace((TableSpace *)m_testTbs, DSTORE::DSTORE_ACCESS_EXCLUSIVE_LOCK);
        m_testTbs = (TableSpace *)UtMockModule::UtGetTableSpace(2);
    }
    void TearDown() override
    {
        UtMockModule::UtDropTableSpace(m_testTbs);
        UtInstance *instance = (UtInstance *)g_storageInstance;
        instance->Shutdown();
        delete instance;
        DSTORETEST::TearDown();
    }
    HeapNormalSegment *UtAllocNewHeapSegment()
    {
        return (HeapNormalSegment*)SegmentTest::UTAllocSegment(g_defaultPdbId, m_testTbs->GetTablespaceId(), m_testBufMgr, SegmentType::HEAP_SEGMENT_TYPE);
    }
    IndexNormalSegment *UtAllocNewIndexSegment()
    {
        AutoMemCxtSwitch autoSwitch{m_ut_memory_context};
        return (IndexNormalSegment*)SegmentTest::UTAllocSegment(g_defaultPdbId, m_testTbs->GetTablespaceId(), m_testBufMgr, SegmentType::INDEX_SEGMENT_TYPE);
    }
    template<typename T>
    void ReadBufferAndPage(const PageId& pageId, LWLockMode mode, BufferDesc* &buf, T* &page)
    {
        buf = m_testBufMgr->Read(g_defaultPdbId, pageId, mode);
        page = (T*)buf->GetPage();
    }
    void ReadPageForFsmIndex(const PageId& pageId, LWLockMode mode, FsmIndex *fsmIndex)
    {
        /* Read fsm index in pageId */
        BufferDesc *pageBuf =m_testBufMgr->Read(g_defaultPdbId, pageId, mode);
        auto *dataPage = (HeapPage*)pageBuf->GetPage();
        *fsmIndex = dataPage->GetFsmIndex();
        m_testBufMgr->UnlockAndRelease(pageBuf);
    }

    static void UpdatePagesFreeSpace(HeapNormalSegment *segment, uint64 pageCount, uint32 space)
    {
        for (uint64 i = 0; i < pageCount; ++i) {
            PageId tempPageId = segment->GetPageFromFsm(1, 0);
            ASSERT_NE(tempPageId, INVALID_PAGE_ID);
            ASSERT_EQ(segment->UpdateFsm(tempPageId, space), 0);
        }
    }
    void SimulateFsmLevelFull(uint16 level, DataSegment *segment)
    {
        BufferDesc *fsmBuf, *headBuf, *fsmMetaBuf;
        FsmPage *fsmPage;
        HeapSegmentMetaPage *headPage;
        FreeSpaceMapMetaPage *fsmMetaPage;
        ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_EXCLUSIVE, headBuf, headPage);
        level = DstoreMin(level, HEAP_MAX_MAP_LEVEL - 1);
        PageId fsmMetaPageId = headPage->fsmInfos[0].fsmMetaPageId;
        ASSERT_NE(fsmMetaPageId, INVALID_PAGE_ID);
        ReadBufferAndPage(fsmMetaPageId, LW_EXCLUSIVE, fsmMetaBuf, fsmMetaPage);
        PageId fsmPageId = fsmMetaPage->currMap[0];
        ReadBufferAndPage(fsmPageId, LW_EXCLUSIVE, fsmBuf, fsmPage);
        fsmPage->fsmPageHeader.hwm = FSM_MAX_HWM;
        m_testBufMgr->UnlockAndRelease(fsmBuf);
        for (uint16 i = 1; i <= level; ++i) {
            fsmMetaPage->mapCount[i] = 1;
            fsmMetaPage->currMap[i] = fsmPageId;
        }
        fsmMetaPage->numFsmLevels = level;
        m_testBufMgr->UnlockAndRelease(fsmMetaBuf);
        m_testBufMgr->UnlockAndRelease(headBuf);
    }

    static void * UpdateFsmInThread(void *args) {
        printf("entering thread %lu\n", pthread_self());
        ((UtInstance *)g_storageInstance)->ThreadSetupAndRegister();
        FsmParam* param = (FsmParam*)args;
        HeapNormalSegment *segment = param->segment;
        PageId targetPageId = param->pageId;
        uint32 remainSpace = param->remainSpace;
        for (int i = 0; i < 1; i++) {
            (void) segment->UpdateFsm(targetPageId, remainSpace);
        }
        ((UtInstance *)g_storageInstance)->ThreadUnregisterAndExit();
    }

    void UtCheckInitInfo(DataSegment *segment, SegmentType type);
    StorageInstance *m_instance;
    TableSpaceInterface *m_testTbs;
    BufMgrInterface *m_testBufMgr;
    InitDataPageCallback m_initHeapPageCallback = HeapPage::InitHeapPage;
    InitDataPageCallback m_initIndexPageCallback = BtrPage::InitBtrPage;
    const uint16 m_retryUpgradeThreshold = 100; /* Upgrade threshold for GetPageFromFsm retry time */
};

#endif //DSTORE_UT_DATA_SEGMENT_H
