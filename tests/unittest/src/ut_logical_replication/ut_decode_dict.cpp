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
#include "ut_logical_replication/ut_decode_dict.h"
#include "common/memory/dstore_mctx.h"
#include "framework/dstore_pdb.h"

using namespace DSTORE;

static int typeCount = sizeof(TYPE_CACHE_TABLE) / sizeof(TypeCache);

void DECODEDICTTEST::SetUp()
{
    LOGICALREPBASETEST::SetUp();
    m_testMctx = DstoreAllocSetContextCreate(m_ut_memory_context, "DecodeDict",
        ALLOCSET_DEFAULT_SIZES, MemoryContextType::SHARED_CONTEXT);
    m_decodeDict = DstoreNew(m_ut_memory_context) DecodeDict(g_defaultPdbId, m_testMctx);
    RetStatus rt = m_decodeDict->Init();
    ASSERT_EQ(rt, DSTORE_SUCC);
}

void DECODEDICTTEST::TearDown()
{
    m_decodeDict->Destroy();
    delete m_decodeDict;
    m_decodeDict = nullptr;
    LOGICALREPBASETEST::TearDown();
}

CatalogInfo *DECODEDICTTEST::GenerateRandomCatalogeInfo(Oid tableOid, CommitSeqNo tableCsn)
{
    CatalogInfo *rawInfo = static_cast<CatalogInfo *>(DstorePalloc(sizeof(CatalogInfo)));
    rawInfo->csn = tableCsn;
    rawInfo->tableOid = tableOid;
    rawInfo->nspOid = (int32)rand() % 10;
    int natts = (int32)rand() % 50;
    rawInfo->natts = natts;
    rawInfo->sysAttr = static_cast<Form_pg_attribute *>(DstorePalloc(natts * sizeof(DSTORE::Form_pg_attribute)));
    rawInfo->sysRel = static_cast<Form_pg_class>(DstorePalloc(sizeof(FormData_pg_class)));
    rawInfo->nspName = static_cast<char *>(DstorePalloc0(NAME_DATA_LEN));
    rawInfo->nspName[0] = 'p';
    rawInfo->nspName[1] = 'u';
    rawInfo->nspName[2] = 'b';
    rawInfo->nspName[3] = 'l';
    rawInfo->nspName[4] = 'i';
    rawInfo->nspName[5] = 'c';
    rawInfo->nspName[6] = '\0';
    for (int i = 0; i < natts; i++) {
        rawInfo->sysAttr[i] = static_cast<DSTORE::Form_pg_attribute>(DstorePalloc(MAXALIGN(ATTRIBUTE_FIXED_PART_SIZE)));
        GenerateRandomAttribute(tableOid, rawInfo->sysAttr[i], i + 1);
    }
    int32 len = (int32)rand() % (NAME_DATA_LEN - 2) + 1;
    for (int i = 0; i < len; i++) {
        rawInfo->sysRel->relname.data[i] = (char) (rand() % 26 + 'A');
    }
    return rawInfo;
}

DecodeTableInfo *DECODEDICTTEST::GenerateRandomDecodeTableInfo(Oid tableOid, CommitSeqNo tableCsn)
{
    int natts = (int32)rand() % 50;
    DecodeTableInfo *tableInfo = DecodeTableInfo::Create(natts);
    StorageAssert(tableInfo != nullptr);
    int32 len = (int32)rand() % (NAME_DATA_LEN - 2) + 1;
    DstoreName tableName = (DstoreName)DstorePalloc0(NAME_DATA_LEN);
    for (int i = 0; i < len; i++) {
        tableName->data[i] = (char) (rand() % 26 + 'A');
    }
    tableName->data[len] = '\0';
    char nspName[NAME_DATA_LEN] = "public\0";
    tableInfo->Init(tableOid, tableCsn, DecodeTableInfoStatus::COLLECTED);
    tableInfo->InitDesc(natts, true);
    tableInfo->SetRelName(tableName->data);
    tableInfo->SetNsp((int32)rand() % 10, nspName);
    errno_t rc;
    for (int i = 0; i < natts; i++) {
        GenerateRandomAttribute(tableOid, tableInfo->fakeDescData.attrs[i], i + 1);
    }
    return tableInfo;
}

void DECODEDICTTEST::FreeCatalogeInfo(CatalogInfo *rawInfo)
{
    DstorePfree(rawInfo->nspName);
    for (int i = 0; i < rawInfo->natts; i++) {
        DstorePfree(rawInfo->sysAttr[i]);
    }
    DstorePfree(rawInfo->sysAttr);
    DstorePfree(rawInfo->sysRel);
    DstorePfree(rawInfo);
}

void DECODEDICTTEST::GenerateRandomAttribute(Oid tableOid, Form_pg_attribute &attr, int attnum)
{
    int attrIdx = (int32)rand() % typeCount;
    attr->attrelid = tableOid;
    errno_t rc = memcpy_s(attr->attname.data, NAME_DATA_LEN, TYPE_CACHE_TABLE[attrIdx].name, NAME_DATA_LEN);
    storage_securec_check(rc, "\0", "\0");
    attr->atttypid = TYPE_CACHE_TABLE[attrIdx].type;
    attr->attnum = attnum;
    attr->attnotnull = true;
    attr->attlen = TYPE_CACHE_TABLE[attrIdx].attlen;
    attr->attbyval = TYPE_CACHE_TABLE[attrIdx].attbyval;
    attr->attalign = TYPE_CACHE_TABLE[attrIdx].attalign;
    attr->attcacheoff = -1;
    attr->atthasdef = false;
    attr->atttypmod = -1;
    attr->attstorage = 'p';
}

void DECODEDICTTEST::CheckDecodeTableInfoEqual(DecodeTableInfo *a, DecodeTableInfo *b)
{
    ASSERT_EQ(a->tableOid, b->tableOid);
    ASSERT_EQ(a->csn, b->csn);
    ASSERT_EQ(a->nspId, b->nspId);
    ASSERT_EQ(a->status, b->status);
    ASSERT_EQ(memcmp(a->relName.data, b->relName.data, NAME_DATA_LEN), 0);
    ASSERT_EQ(memcmp(a->nspName.data, b->nspName.data, NAME_DATA_LEN), 0);
    ASSERT_EQ(a->fakeDescData.natts, b->fakeDescData.natts);
    ASSERT_EQ(a->fakeDescData.tdisredistable, b->fakeDescData.tdisredistable);
    ASSERT_EQ(a->fakeDescData.tdtypeid, b->fakeDescData.tdtypeid);
    ASSERT_EQ(a->fakeDescData.tdtypmod, b->fakeDescData.tdtypmod);
    ASSERT_EQ(a->fakeDescData.tdhasoid, b->fakeDescData.tdhasoid);
    ASSERT_EQ(a->fakeDescData.tdrefcount, b->fakeDescData.tdrefcount);
    ASSERT_EQ(a->fakeDescData.tdhasuids, b->fakeDescData.tdhasuids);
    for (int i = 0; i < a->fakeDescData.natts; i++) {
        ASSERT_EQ(a->fakeDescData.attrs[i]->attrelid, b->fakeDescData.attrs[i]->attrelid);
        ASSERT_EQ(memcmp(a->fakeDescData.attrs[i]->attname.data, b->fakeDescData.attrs[i]->attname.data, NAME_DATA_LEN), 0);
        ASSERT_EQ(a->fakeDescData.attrs[i]->atttypid, b->fakeDescData.attrs[i]->atttypid);
    }
}

TEST_F(DECODEDICTTEST, DISABLED_DecodeDictSyncCatalogTest)
{
    std::vector<CatalogInfo *> catalog;
    std::vector<DecodeTableInfo *> tableInfos;
    RetStatus rt;
    for (int i = 1; i <= 1000; i++) {
        CatalogInfo *rawInfo = GenerateRandomCatalogeInfo(i, i);
        catalog.push_back(rawInfo);
        DecodeTableInfo *tableInfo = rawInfo->ConvertToDecodeTableInfo();
        tableInfos.push_back(tableInfo);
        rt = m_decodeDict->SynchronizeCatalog(rawInfo);
        ASSERT_EQ(rt, DSTORE_SUCC);
        StringInfoData buf;
        buf.init();
        m_decodeDict->GetALLDecodeTableInfoVersion(rawInfo->tableOid, &buf);
        // std::cout << buf.data << std::endl;
        DstorePfree(buf.data);
    }

    for (int i = 1; i <= 1000; i++) {
        DecodeTableInfo* res = m_decodeDict->GetVisibleDecodeTableInfo(i, i + 1);
        ASSERT_EQ(res->tableOid, i);
        ASSERT_EQ(res->csn, i);
        CheckDecodeTableInfoEqual(tableInfos[i - 1], res);
    }

    for (int i = 0; i < 1000; i++) {
        DstorePfree(tableInfos[i]);
        FreeCatalogeInfo(catalog[i]);
    }
}

TEST_F(DECODEDICTTEST, DISABLED_DecodeDictMixTest)
{
    RetStatus rt;
    for (int i = 1; i <= 1000; i++) {
        rt = m_decodeDict->CollectDecodeDictChange(i, i);
        ASSERT_EQ(rt, DSTORE_SUCC);
        rt = m_decodeDict->CollectDecodeDictChange(i, i + 1);
        ASSERT_EQ(rt, DSTORE_SUCC);
    }

    for (int i = 1; i <= 1000; i++) {
        DecodeTableInfo* tableInfo1 = GenerateRandomDecodeTableInfo(i, i);
        rt = m_decodeDict->UpdateDecodeDictChangeFromWal(tableInfo1);
        ASSERT_EQ(rt, DSTORE_SUCC);
        DecodeTableInfo* tableInfo2 = GenerateRandomDecodeTableInfo(i, i + 1);
        rt = m_decodeDict->UpdateDecodeDictChangeFromWal(tableInfo2);
        ASSERT_EQ(rt, DSTORE_SUCC);
    }

    for (int i = 1; i <= 1000; i++) {
        DecodeTableInfo* res = m_decodeDict->GetVisibleDecodeTableInfo(i, i + 1);
        ASSERT_EQ(res->tableOid, i);
        ASSERT_EQ(res->csn, i);
    }
}
