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

#include "ut_utilities/ut_dstore_framework.h"
#include "page/dstore_bitmap_page.h"

using namespace DSTORE;

class BitmapTest : public DSTORETEST {
protected:
    void SetUp() override
    {
        DSTORETEST::SetUp();
        UtInstance *instance = DstoreNew(m_ut_memory_context) UtInstance();
        instance->Install(&DSTORETEST::m_guc, m_ut_memory_context);
        instance->Startup(&DSTORETEST::m_guc);
    }

    void TearDown() override
    {
        UtInstance *instance = (UtInstance *)g_storageInstance;
        instance->Shutdown();
        delete instance;
        DSTORETEST::TearDown();
    }
};

TEST_F(BitmapTest, SetAndUnsetByBitTest_level0) {
    char block[BLCKSZ];
    TbsBitmapPage *bitmapPage = (TbsBitmapPage *)block;
    PageId firstPage = {1, 11};
    bitmapPage->InitBitmapPage({0, 0}, firstPage);
    ASSERT_EQ(bitmapPage->allocatedExtentCount, 0);  // DF_BITMAP_BIT_CNT = 63 * 128 * 8
    for (int i = 0; i < DF_BITMAP_BIT_CNT; i++) {
        bitmapPage->SetByBit(i);
    }
    ASSERT_EQ(bitmapPage->allocatedExtentCount, DF_BITMAP_BIT_CNT);
    for (int i = 0; i < DF_BITMAP_BIT_CNT; i++) {
        bitmapPage->UnsetByBit(i);
    }
}

TEST_F(BitmapTest, TestBitTest_level0) {
    char block[BLCKSZ];
    TbsBitmapPage *bitmapPage = (TbsBitmapPage *)block;
    PageId firstPage = {1, 11};
    bitmapPage->InitBitmapPage({0, 0}, firstPage);
    ASSERT_EQ(bitmapPage->allocatedExtentCount, 0);  // DF_BITMAP_BIT_CNT = 63 * 128 * 8
    for (int i = 0; i < DF_BITMAP_BIT_CNT / 2; i++) {
        bitmapPage->SetByBit(i);
    }
    ASSERT_EQ(bitmapPage->allocatedExtentCount, DF_BITMAP_BIT_CNT / 2);
    for (int i = 0; i < DF_BITMAP_BIT_CNT / 2; i++) {
        ASSERT_FALSE(bitmapPage->TestBitZero(i));
    }
    for (int i = DF_BITMAP_BIT_CNT / 2; i < DF_BITMAP_BIT_CNT; i++) {
        ASSERT_TRUE(bitmapPage->TestBitZero(i));
    }

    // 从0开始找第1个可用的bit
    uint32 result = bitmapPage->FindFirstFreeBitByBit(0);
    ASSERT_EQ(result, DF_BITMAP_BIT_CNT / 2);
}

void ExecuteBitmapLocateBitsTestForBitmapGroup(TbsDataFile *datafile, ExtentSize extentSize)
{
    /* 1.prepare a bitmapMetaPage */
    char block[BLCKSZ];
    TbsBitmapMetaPage *bitmapMetaPage = (TbsBitmapMetaPage*)block;
    BlockNumber blockCount = 0xFFFFFFFFU;
    uint32 extentCount = 0;
    uint16 bitmapPageCount = 0;
    uint8 firstExtent = TBS_BITMAP_META_PAGE + BITMAP_PAGES_PER_GROUP + 1;
    bitmapMetaPage->InitBitmapMetaPage({0, 0}, blockCount, extentSize);
    bitmapMetaPage->groupCount = 1;
    bitmapMetaPage->bitmapGroups[0].firstBitmapPageId = {0, TBS_BITMAP_META_PAGE + BITMAP_PAGES_PER_GROUP};
    bitmapMetaPage->bitmapGroups[0].firstFreePageNo = 0;
    bitmapMetaPage->totalBlockCount = blockCount;
    for (uint64 i = firstExtent; i < blockCount; i += extentSize) {
        if (extentCount >= DF_BITMAP_BIT_CNT) {
            bitmapPageCount += 1;
            extentCount = 1;
            if (bitmapPageCount >= BITMAP_PAGES_PER_GROUP) {
                bitmapMetaPage->bitmapGroups[bitmapMetaPage->groupCount].firstBitmapPageId = {0, i};
                bitmapMetaPage->bitmapGroups[bitmapMetaPage->groupCount].firstFreePageNo = 0;
                bitmapMetaPage->groupCount += 1;
                i += BITMAP_PAGES_PER_GROUP;
                if (bitmapMetaPage->groupCount >= MAX_BITMAP_GROUP_CNT) {
                    break;
                }
            }
        } else {
            extentCount ++;
        }
    }

    /* 2.test TbsDataFile::LocateBitsPosByPageId returns with correct bitNo */
    PageId extentMetaPageId;
    uint16 mapGroupNo;
    uint16 mapGroupNoExpect = 0;
    uint8 bitmapPageNo;
    uint8 bitmapPageNoExpect = 0;
    uint16 bitNo;
    uint16 bitNoExpect = 0;
    extentCount = 0;
    for (uint64 i = firstExtent; i < blockCount; i += extentSize) {
        extentMetaPageId.m_fileId = 0;
        extentMetaPageId.m_blockId = i;
        ASSERT_TRUE(datafile->LocateBitsPosByPageId(bitmapMetaPage, extentMetaPageId, &mapGroupNo, &bitmapPageNo, &bitNo));
        ASSERT_EQ(mapGroupNo, mapGroupNoExpect);
        ASSERT_EQ(bitmapPageNo, bitmapPageNoExpect);
        ASSERT_EQ(bitNo, bitNoExpect);
        extentCount += 1;
        bitNoExpect += 1;
        if (extentCount >= DF_BITMAP_BIT_CNT) {
            bitmapPageNoExpect += 1;
            extentCount = 0;
            if (bitmapPageNoExpect >= BITMAP_PAGES_PER_GROUP) {
                bitmapPageNoExpect = 0;
                mapGroupNoExpect += 1;
                i += BITMAP_PAGES_PER_GROUP;
                if (mapGroupNoExpect >= MAX_BITMAP_GROUP_CNT) {
                    break;
                }
            }
            bitNoExpect = 0;
        }
    }

    /* 3.test TbsDataFile::LocateBitsPosByPageId with invalid input */
    extentMetaPageId.m_fileId = 0;
    extentMetaPageId.m_blockId = firstExtent + 1;
    ASSERT_FALSE(datafile->LocateBitsPosByPageId(bitmapMetaPage, extentMetaPageId, &mapGroupNo, &bitmapPageNo, &bitNo));
    extentMetaPageId.m_fileId = 0;
    extentMetaPageId.m_blockId = firstExtent - 1;
    ASSERT_FALSE(datafile->LocateBitsPosByPageId(bitmapMetaPage, extentMetaPageId, &mapGroupNo, &bitmapPageNo, &bitNo));
    extentMetaPageId.m_fileId = 0;
    extentMetaPageId.m_blockId = blockCount;
    ASSERT_FALSE(datafile->LocateBitsPosByPageId(bitmapMetaPage, extentMetaPageId, &mapGroupNo, &bitmapPageNo, &bitNo));
}

TEST_F(BitmapTest, TestBitmapPage_level0) {
    /* prepare a valid TbsDataFile instance */
    TbsDataFile *datafile =
        DstoreNew(g_dstoreCurrentMemoryContext) TbsDataFile(g_defaultPdbId, nullptr, 0, 0, EXT_SIZE_8192, false);
    ASSERT_NE(datafile, nullptr);

    ExecuteBitmapLocateBitsTestForBitmapGroup(datafile, EXT_SIZE_8);
    ExecuteBitmapLocateBitsTestForBitmapGroup(datafile, EXT_SIZE_128);
    ExecuteBitmapLocateBitsTestForBitmapGroup(datafile, EXT_SIZE_1024);
    ExecuteBitmapLocateBitsTestForBitmapGroup(datafile, EXT_SIZE_8192);

    delete datafile;
}

