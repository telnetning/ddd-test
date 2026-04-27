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
#include <gtest/gtest.h>
#include <algorithm>

#include <fstream>
#include "index/dstore_btree_build.h"
#include "ut_btree/ut_btree.h"
#include "heap/dstore_heap_insert.h"

/* sort tuples with single index */
TEST_F(UTBtree, TupleSortBoolIndexTest_level0)
{
    m_utTableHandler->FillTableWithRandomData(200);
    int indexCols[] = {0};
    int numKeyAttrs = sizeof(indexCols) / sizeof(int);
    Transaction *transaction = thrd->GetActiveTransaction();
    transaction->Start();
    transaction->GetSnapshot();
    m_utTableHandler->CreateBtreeContext(indexCols, numKeyAttrs, false);
    transaction->Commit();
    TestTupleSort();
}

TEST_F(UTBtree, TupleSortInt2IndexTest_level0)
{
    m_utTableHandler->FillTableWithRandomData(200);
    int indexCols[] = {1};
    int numKeyAttrs = sizeof(indexCols) / sizeof(int);
    Transaction *transaction = thrd->GetActiveTransaction();
    transaction->Start();
    transaction->GetSnapshot();
    m_utTableHandler->CreateBtreeContext(indexCols, numKeyAttrs, false);
    transaction->Commit();
    TestTupleSort();
}

/* sort tuples with multi index */
TEST_F(UTBtree, TupleSortMultiIndexTest_level0)
{
    m_utTableHandler->FillTableWithRandomData(200);
    /* int16 and int32 index */
    int indexCols[] = {1, 2};
    int numKeyAttrs = sizeof(indexCols) / sizeof(int);
    Transaction *transaction = thrd->GetActiveTransaction();
    transaction->Start();
    transaction->GetSnapshot();
    m_utTableHandler->CreateBtreeContext(indexCols, numKeyAttrs, false);
    transaction->Commit();
    TestTupleSort();
}

/*
 * Test if all tuples are ordered after sort function.
 */
void UTBtree::TestTupleSort()
{
    TupleDesc tupleDesc = m_utTableHandler->GetIndexTupleDesc();
    EXPECT_NE(tupleDesc, nullptr);

    Transaction *transaction = thrd->GetActiveTransaction();
    transaction->Start();
    /* step 1: prepare sortState */
    char tmpFileNameBase[MAXPGPATH] = {0};
    int rc = sprintf_s(tmpFileNameBase, MAXPGPATH, "%s_%d_%d_", GetTestCaseName(), 1, 1);
    storage_securec_check_ss(rc);
    TuplesortMgr *tuplesortMgr = DstoreNew(g_dstoreCurrentMemoryContext) TuplesortMgr(tmpFileNameBase, g_defaultPdbId);
    tuplesortMgr->PrepareTupleSortInfo(m_utTableHandler->GetIndexBuildInfo()->baseInfo, 64,
                                       m_utTableHandler->GetIndexScanKey());
    uint16 keyNum = m_utTableHandler->GetIndexInfo()->indexKeyAttrsNum;
    Datum *values = (Datum*) DstorePalloc(keyNum * sizeof (Datum));
    bool *isnulls = (bool*) DstorePalloc(keyNum * sizeof (bool));
    transaction->SetSnapshotCsn();
    HeapTuple *heapTuple;
    {
        HeapScanHandler heap_handler(g_storageInstance, thrd, m_utTableHandler->GetTableRel());
        heap_handler.Begin(transaction->GetSnapshot());
        /* step 2: read table data and put into sortstate */
        while ((heapTuple = heap_handler.SeqScan()) != nullptr) {
            IndexTuple *indexTuple = IndexTuple::FormTuple(heapTuple, m_utTableHandler->GetIndexBuildInfo(),
                                                           values, isnulls);
            tuplesortMgr->PutIndexTuple(indexTuple);
        }
        heap_handler.End();
    }

    DstorePfree(isnulls);
    DstorePfree(values);

    /* step 3: do the real sort */
    tuplesortMgr->PerformSortTuple();
    bool uniqueCheckSucc = tuplesortMgr->UniqueCheckSucc();
    StorageAssert(uniqueCheckSucc);

    /* step 4: check tuples sorted or not */
    IndexTuple *itup = nullptr;
    IndexTuple *oldITup = nullptr;
    bool firstFlag = true;
    while (true) {
        tuplesortMgr->GetNextIndexTuple(&itup);
        if (itup != nullptr) {
            if (firstFlag) {
                firstFlag = false;
            } else {
                int ret = CompareIndexTuple(itup, oldITup, tupleDesc);
                EXPECT_GE(ret, 0);
            }
            oldITup = itup;
        } else {
            break;
        }
    }

    /* step 5: end sort immediately */
    tuplesortMgr->Destroy();
    delete tuplesortMgr;

    transaction->Commit();
}

TEST_F(UTBtree, TupleSortOnFinalMerge_TIER2_level2)
{
    g_storageInstance->GetGuc()->maintenanceWorkMem /= 4;
    int rowNum = 200000;
    m_utTableHandler->FillTableWithRandomData(rowNum);
    int indexCols[] = {1, 2, 3, 4};
    bool isUnique = false;
    int numKeyAttrs = sizeof(indexCols) / sizeof(int);
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();
    m_utTableHandler->CreateBtreeContext(indexCols, numKeyAttrs, isUnique);
    BtreeBuild btreeBuild(m_utTableHandler->GetIndexRel(), m_utTableHandler->GetIndexBuildInfo(),
                          m_utTableHandler->GetIndexScanKey());
    EXPECT_TRUE(btreeBuild.BuildIndex() == DSTORE_SUCC);
    txn->Commit();
    g_storageInstance->GetGuc()->maintenanceWorkMem *= 4;
}

TEST_F(UTBtree, DISABLED_TupleSortOnTape)
{
    int rowNum = 4000000;
    m_utTableHandler->FillTableWithRandomData(rowNum);
    int indexCols[] = {1, 2, 3, 4};
    bool isUnique = false;
    int numKeyAttrs = sizeof(indexCols) / sizeof(int);
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();
    m_utTableHandler->CreateBtreeContext(indexCols, numKeyAttrs, isUnique);
    BtreeBuild btreeBuild(m_utTableHandler->GetIndexRel(), m_utTableHandler->GetIndexBuildInfo(),
                          m_utTableHandler->GetIndexScanKey());
    EXPECT_TRUE(btreeBuild.BuildIndex() == DSTORE_SUCC);
    txn->Commit();
}

TEST_F(UTBtree, DISABLED_TupleSortInsert)
{
    int indexCols[] = {1, 2, 3, 4};
    bool isUnique = false;
    int numAttrs = sizeof(indexCols) / sizeof(int);
    m_utTableHandler->CreateIndex(indexCols, numAttrs, isUnique);
    int rowNum = 4000000;
    for (int i = 0; i < rowNum; ++i) {
        m_utTableHandler->InsertRandomIndexTuple();
    }
}

TEST_F(UTBtree, DatumSortTest_level0)
{
    /* Step 1. Prepare index context */
    /* test all supported types except Blob and Clob. */
    int numAttrs = TYPE_CACHE_NUM - 2;
    int indexCols[numAttrs];
    for (int i = 0; i < numAttrs; i++) {
        indexCols[i] = i;
    }
    Transaction *transaction = thrd->GetActiveTransaction();
    transaction->Start();
    transaction->GetSnapshot();
    m_utTableHandler->CreateBtreeContext(indexCols, numAttrs, false);
    TupleDesc tupleDesc = m_utTableHandler->GetHeapTupDesc();
    transaction->Commit();
    char tmpFileNameBase[MAXPGPATH] = {0};
    int rc = sprintf_s(tmpFileNameBase, MAXPGPATH, "%s_%d_%d_", GetTestCaseName(), 1, 1);
    storage_securec_check_ss(rc);
    TuplesortMgr sortMgr(tmpFileNameBase, g_defaultPdbId);
    for (int i = 0; i < numAttrs; i++) {
        /* Step 2. Prepare scankey for sorting */
        TypeCache tc = TYPE_CACHE_TABLE[i];
        ScanKeyData skey;
        g_storageInstance->GetCacheHashMgr()->GenerateScanKey(tc.type, 0, MAINTAIN_ORDER, &skey, i + 1);
        sortMgr.PrepareDatumSortInfo(m_utTableHandler->GetIndexInfo(), g_storageInstance->GetGuc()->maintenanceWorkMem,
                                     &skey);

        /* Step 3. Generate random data and perform sort */
        int numValues = 20;
        Datum value;
        bool isNull;
        for (int j = 0; j < numValues; j++) {
            value = UTTableHandler::GenerateRandomDataByType(tupleDesc->attrs[i]);
            isNull = tupleDesc->attrs[i]->attlen == -1 && value == static_cast<Datum>(0);
            sortMgr.PutDatum(value, isNull);
        }
        sortMgr.PerformSortDatum();

        /* Step 4. Check order */
        Datum prevValue;
        sortMgr.GetNextDatum(&prevValue);
        for (int j = 1; j < numValues; j++) {
            sortMgr.GetNextDatum(&value);
            EXPECT_LE(DatumGetInt32(FunctionCall2Coll(&skey.skFunc, DSTORE_INVALID_OID, prevValue, value)), 0);
            prevValue = value;
        }
        sortMgr.Clear();
    }
    sortMgr.Destroy();
}

static int GetTmpFileCnt(const char *basePath, const char *fileNamePrefix, bool checkDir = false)
{
    DIR *dir;
    struct dirent *entry;
    int ret = 0;
    EXPECT_TRUE((dir = opendir(basePath)) != NULL);
    while ((entry = readdir(dir)) != NULL) {
        if (std::string(entry->d_name).find(fileNamePrefix) == 0) {
            if (checkDir) {
                ret = entry->d_type == DT_DIR ? ret + 1 : ret;
            } else {
                ret = entry->d_type == DT_REG ? ret + 1 : ret;
            }
        }
    }
    closedir(dir);
    return ret;
}

static bool IsDirectoryExists(const char *path)
{
    struct stat info;
    
    if (stat(path, &info) != 0) {
        return false;
    }

    if (S_ISDIR(info.st_mode)) {
        return true;
    }

    return false;
}

static void CreateFile(const char *filePath)
{
    std::ofstream file(filePath);

    EXPECT_TRUE(file.is_open());

    file.close();
}

static void CreateDir(const char *dirPath)
{
    EXPECT_TRUE(mkdir(dirPath, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH) == 0);
}

/**
 * @tc.desc  : Test when directory is not null, all index sort tmp files should be deleted.
 */
TEST_F(UTBtree, DeleteAllIdxSortTmpFile_ShouldDeleteAllIdxSortTmpFile_WhenDirIsNotNull_level0)
{
    char basePath[MAXPGPATH] = {0};
    (void)sprintf_s(basePath, MAXPGPATH, "%s/%s", g_storageInstance->GetGuc()->dataDir, PDB_BASE_PATH);

    EXPECT_TRUE(IsDirectoryExists(basePath));
    EXPECT_EQ(GetTmpFileCnt(basePath, INDEX_BASE_PREFIX), 0);

    /* Make that the directory is not null and contains index sort tmp files. */
    int createTmpFileNum = 10;
    char filePath[MAXPGPATH] = {0};
    for(int i = 0; i < createTmpFileNum; i++) {
        (void)sprintf_s(filePath, MAXPGPATH, "%s%s_%d", basePath, INDEX_BASE_PREFIX, i);
        CreateFile(filePath);
    }
    const char *fakeIndexBasePrefix = "_idxtmp_";
    for(int i = 0; i < createTmpFileNum; i++) {
        (void)sprintf_s(filePath, MAXPGPATH, "%s%s_%d", basePath, fakeIndexBasePrefix, i);
        CreateFile(filePath);
    }
    EXPECT_EQ(GetTmpFileCnt(basePath, INDEX_BASE_PREFIX), createTmpFileNum);
    EXPECT_EQ(GetTmpFileCnt(basePath, fakeIndexBasePrefix), createTmpFileNum);

    TuplesortMgr::DeleteAllIdxSortTmpFile();

    /* Check if all index sort tmp files are deleted. */
    EXPECT_EQ(GetTmpFileCnt(basePath, INDEX_BASE_PREFIX), 0);
    /* Check not index tmp file are not deleted. */
    EXPECT_EQ(GetTmpFileCnt(basePath, fakeIndexBasePrefix), createTmpFileNum);
}

/**
 * @tc.desc  : Test when contains dir, no dir should be deleted.
 */
TEST_F(UTBtree, DeleteAllIdxSortTmpFile_ShouldNotDeleteAnyDir_WhenContainDirs_level0)
{
    char basePath[MAXPGPATH] = {0};
    (void)sprintf_s(basePath, MAXPGPATH, "%s/%s", g_storageInstance->GetGuc()->dataDir, PDB_BASE_PATH);

    EXPECT_TRUE(IsDirectoryExists(basePath));
    EXPECT_EQ(GetTmpFileCnt(basePath, INDEX_BASE_PREFIX), 0);

    /* Create inde sort files and same prefix dirs */
    int createTmpFileNum = 10;
    char filePath[MAXPGPATH] = {0};
    for(int i = 0; i < createTmpFileNum; i++) {
        (void)sprintf_s(filePath, MAXPGPATH, "%s%s_%d", basePath, INDEX_BASE_PREFIX, i);
        CreateFile(filePath);
        (void)sprintf_s(filePath, MAXPGPATH, "%s%s_%d_dir", basePath, INDEX_BASE_PREFIX, i);
        CreateDir(filePath);
    }

    EXPECT_EQ(GetTmpFileCnt(basePath, INDEX_BASE_PREFIX), createTmpFileNum);
    EXPECT_EQ(GetTmpFileCnt(basePath, INDEX_BASE_PREFIX, true), createTmpFileNum);

    TuplesortMgr::DeleteAllIdxSortTmpFile();

    /* Check if all index sort tmp files are deleted. */
    EXPECT_EQ(GetTmpFileCnt(basePath, INDEX_BASE_PREFIX), 0);
    /* Check all dirs are not deleted. */
    EXPECT_EQ(GetTmpFileCnt(basePath, INDEX_BASE_PREFIX, true), createTmpFileNum);
}
