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
 * ut_decode_plugin.cpp
 *
 * Description:
 *
 * ---------------------------------------------------------------------------------------
 *
 */

#include "ut_logical_replication/ut_decode_dict_file.h"
#include "common/memory/dstore_mctx.h"
#include "framework/dstore_pdb.h"

using namespace DSTORE;

void DECODEDICTFILETEST::SetUp()
{
    LOGICALREPBASETEST::SetUp();
    m_decodeDictFile = DstoreNew(m_ut_memory_context) DecodeDictFile(g_defaultPdbId);
    RetStatus rt = m_decodeDictFile->Init();
    ASSERT_EQ(rt, DSTORE_SUCC);
}

void DECODEDICTFILETEST::TearDown()
{
    m_decodeDictFile->Destroy();
    delete m_decodeDictFile;
    m_decodeDictFile = nullptr;
    LOGICALREPBASETEST::TearDown();
}

void DECODEDICTFILETEST::CheckDiskDecodeTableInfoEqual(DecodeTableInfoDiskData *a, DecodeTableInfoDiskData *b)
{
    DecodeTableInfo *info1 = DecodeTableInfo::ConvertFromItem(a);
    DecodeTableInfo *info2 = DecodeTableInfo::ConvertFromItem(b);
    CheckDecodeTableInfoEqual(info1, info2);
    DstorePfree(info1);
    DstorePfree(info2);
}

DecodeTableInfoDiskData* DECODEDICTFILETEST::GenerateRandomDiskDecodeTableInfo(Oid tableOid, CommitSeqNo tableCsn)
{
    DecodeTableInfo *tableInfo = GenerateRandomDecodeTableInfo(tableOid, tableCsn);
    DecodeTableInfoDiskData *item = DecodeTableInfo::ConvertToItem(tableInfo);
    DstorePfree(tableInfo);
    return item;
}

TEST_F(DECODEDICTFILETEST, BasicTransferTest)
{
    DecodeTableInfo *origTableInfo = GenerateRandomDecodeTableInfo(1, 1);
    DecodeTableInfoDiskData *item = DecodeTableInfo::ConvertToItem(origTableInfo);
    DecodeTableInfo *newTableInfo = DecodeTableInfo::ConvertFromItem(item);
    CheckDecodeTableInfoEqual(origTableInfo, newTableInfo);
    DstorePfree(origTableInfo);
    DstorePfree(newTableInfo);
    DstorePfree(item);
}

TEST_F(DECODEDICTFILETEST, DISABLED_AddDecodeTableItemTest)
{
    RetStatus rt;
    std::vector<DecodeTableInfoDiskData*> addItems;
    std::vector<BlockNumber> addOn;
    for (int i = 1; i <= 1000; i++) {
        BlockNumber outAddBlock;
        DecodeTableInfoDiskData* item = GenerateRandomDiskDecodeTableInfo(i, i);
        rt = m_decodeDictFile->AddDecodeTableInfoItem(item, outAddBlock);
        ASSERT_EQ(rt, DSTORE_SUCC);
        addItems.push_back(item);
        addOn.push_back(outAddBlock);
    }

    /* check total add nums */
    int addNums = 0;
    DecodeDictFile::DecodeDictPageIterator iter(m_decodeDictFile);
    while (iter.NextItem()) {
        addNums++;
    }
    ASSERT_EQ(addNums, 1000);

    /* check each one */
    for (int i = 0; i < 1000; i++) {
        bool found = false;
        DecodeDictFile::DecodeDictPageIterator iterBlock(m_decodeDictFile, addOn[i]);
        while (iterBlock.NextItem()) {
            DecodeTableInfoDiskData *storedItem = iterBlock.GetItem();
            if (storedItem->tableOid == addItems[i]->tableOid && storedItem->csn == addItems[i]->csn) {
                ASSERT_EQ(iterBlock.GetCurrentBlock(), addOn[i]);
                CheckDiskDecodeTableInfoEqual(addItems[i], storedItem);
                found = true;
                break;
            }
        }
        ASSERT_EQ(found, true);
    }

    /* release */
    for (int i = 0; i < 1000; i++) {
        DstorePfree(addItems[i]);
    }
}

TEST_F(DECODEDICTFILETEST, DISABLED_RemoveDecodeTableItemTest)
{
    RetStatus rt;
    std::vector<DecodeTableInfoDiskData*> addItems;
    std::vector<BlockNumber> addOn;
    for (int i = 1; i <= 1000; i++) {
        BlockNumber outAddBlock;
        DecodeTableInfoDiskData* item = GenerateRandomDiskDecodeTableInfo(i, i);
        rt = m_decodeDictFile->AddDecodeTableInfoItem(item, outAddBlock);
        ASSERT_EQ(rt, DSTORE_SUCC);
        addItems.push_back(item);
        addOn.push_back(outAddBlock);
    }

    for (int i = 0; i < 1000; i++) {
        RetStatus rt = m_decodeDictFile->RemoveDecodeTableInfoItem(addItems[i]->tableOid, addItems[i]->csn, addOn[i]);
        ASSERT_EQ(rt, DSTORE_SUCC);
    }

    int curNums = 0;
    DecodeDictFile::DecodeDictPageIterator iter(m_decodeDictFile);
    while (iter.NextItem()) {
        curNums++;
    }
    ASSERT_EQ(curNums, 0);

    /* release */
    for (int i = 0; i < 1000; i++) {
        DstorePfree(addItems[i]);
    }
}

TEST_F(DECODEDICTFILETEST, DISABLED_UpdateDecodeTableItemTest)
{
    /* add */
    RetStatus rt;
    int curNums = 0;
    std::vector<DecodeTableInfoDiskData*> addItems;
    std::vector<BlockNumber> addOn;
    for (int i = 1; i <= 1000; i++) {
        BlockNumber outAddBlock;
        DecodeTableInfoDiskData* item = GenerateRandomDiskDecodeTableInfo(i, i);
        rt = m_decodeDictFile->AddDecodeTableInfoItem(item, outAddBlock);
        ASSERT_EQ(rt, DSTORE_SUCC);
        addItems.push_back(item);
        addOn.push_back(outAddBlock);
    }
    curNums = 0;
    DecodeDictFile::DecodeDictPageIterator iter1(m_decodeDictFile);
    while (iter1.NextItem()) {
        curNums++;
    }
    ASSERT_EQ(curNums, 1000);

    /* update */
    std::vector<DecodeTableInfoDiskData*> newItems;
    std::vector<BlockNumber> newAddOn;
    for (int i = 0; i < 1000; i++) {
        DecodeTableInfoDiskData* newItem = GenerateRandomDiskDecodeTableInfo(addItems[i]->tableOid, addItems[i]->csn);
        BlockNumber newBlock;
        RetStatus rt = m_decodeDictFile->UpdateDecodeTableInfoItem(addOn[i], addItems[i]->tableOid, addItems[i]->csn,
            newItem, newBlock);
        ASSERT_EQ(rt, DSTORE_SUCC);
        newItems.push_back(newItem);
        newAddOn.push_back(newBlock);
    }
    curNums = 0;
    DecodeDictFile::DecodeDictPageIterator iter2(m_decodeDictFile);
    while (iter2.NextItem()) {
        curNums++;
    }
    ASSERT_EQ(curNums, 1000);

    /* check each updated one */
    for (int i = 0; i < 1000; i++) {
        bool found = false;
        DecodeDictFile::DecodeDictPageIterator iterBlock(m_decodeDictFile, newAddOn[i]);
        while (iterBlock.NextItem()) {
            DecodeTableInfoDiskData *storedItem = iterBlock.GetItem();
            if (storedItem->tableOid == newItems[i]->tableOid && storedItem->csn == newItems[i]->csn) {
                ASSERT_EQ(iterBlock.GetCurrentBlock(), newAddOn[i]);
                CheckDiskDecodeTableInfoEqual(newItems[i], storedItem);
                found = true;
                break;
            }
        }
        ASSERT_EQ(found, true);
    }

    /* release */
    for (int i = 0; i < 1000; i++) {
        DstorePfree(addItems[i]);
        DstorePfree(newItems[i]);
    }
}