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

#include "fault_injection/fault_injection.h"
#include "ut_utilities/ut_dstore_framework.h"
#include "control/dstore_control_file.h"
#include "ut_buffer/ut_buffer_fake_instance.h"
#include "ut_tablehandler/ut_table_handler.h"
#include "systable/sys_database.h"
#include "pdb/dstore_pdb_interface.h"
#include "common/datatype/dstore_uuid_utils.h"

using namespace DSTORE;
UtInstance *instance;

class PdbTest : public DSTORETEST {
protected:
    char m_configFilePath[MAXPGPATH];
    RelMapNode *nodes;
    int count;
    static int result[5000];
    void SetUp() override {
        SetDefaultPdbId(PDB_TEMPLATE1_ID);
        IoFencingVFSCollection::GetSingleton()->ClearIoFencingCallback();
        DSTORETEST::SetUp();
        /* Get tenant config file. */
        InitTenantConfig(&DSTORETEST::m_guc);

        instance = DstoreNew(m_ut_memory_context)UtInstance();
        instance->Install(&DSTORETEST::m_guc, m_ut_memory_context);
        instance->Startup(&DSTORETEST::m_guc);

        PreAllocPdbId();
        
        ASSERT_EQ(instance->InitPdbByName(PDB_ROOT_NAME, false), DSTORE_SUCC);
        DSTORE::SetDefaultPdbId(PDB_ROOT_ID);
        nodes = (RelMapNode *)malloc(sizeof(RelMapNode) * 512);
    }

    void TearDown() override {
        UtInstance *instance = (UtInstance *)g_storageInstance;
        free(nodes);
        nodes = nullptr;
        instance->Shutdown();
        delete instance;
        DSTORETEST::TearDown();
    }

    RetStatus InsertTupleTest(PdbId pdbId, const char *tableName, int rowNum) {
        AutoMemCxtSwitch autoSwitch(m_ut_memory_context);
        /* Fill table with specific data */
        UTTableHandler *m_utTableHandler = UTTableHandler::CreateTableHandler(pdbId, m_ut_memory_context);

        DefaultRowDef *tableDef = (DefaultRowDef*)DstorePalloc(rowNum * sizeof(DefaultRowDef));
        for (int i = 0; i < rowNum; i++) {
            int16 r1 = (int16)i;
            int32 r2 = (int32)i;
            DefaultRowDef rowDef = m_utTableHandler->GetDefaultRowDef();
            rowDef.column_int16 = r1;
            rowDef.column_int32 = r2;
            tableDef[i] = rowDef;
        }
        m_utTableHandler->FillTableWithSpecificData(tableDef, rowNum);
        DstorePfree(tableDef);

        UTTableHandler::Destroy(m_utTableHandler);

        return DSTORE::DSTORE_SUCC;
    }

    void PreAllocPdbId() {
        ControlFile *controlFile = instance->GetPdb(PDB_TEMPLATE1_ID)->GetControlFile();
        PdbId pdbId = INVALID_PDB_ID;
        ControlPdbInfoPageItemData pdbInfo;

        /* Step1: Alloc template1 pdb id. */
        RetStatus ret = controlFile->AllocPdbId("template1", pdbId, true);
        ASSERT_EQ(ControlPdbInfoPageItemData::Init(&pdbInfo, PDB_TEMPLATE1_ID, PDB_DEFAULT_UUID_STR, PdbStatus::PDB_STATUS_OPENED_READ_WRITE,
                                                   "template1", m_guc.tenantConfig->storageConfig.template1VfsName),
                  DSTORE_SUCC);
        ASSERT_EQ(controlFile->UpdatePdbItemData(PDB_TEMPLATE1_ID, &pdbInfo, sizeof(ControlPdbInfoPageItemData)),
                  DSTORE_SUCC);
        /* Step2: Alloc template0 pdb id. */
        ret = controlFile->AllocPdbId("template0", pdbId, true);
        ASSERT_EQ(ControlPdbInfoPageItemData::Init(&pdbInfo, PDB_TEMPLATE0_ID, PDB_DEFAULT_UUID_STR, PdbStatus::PDB_STATUS_OPENED_READ_WRITE,
                                                   "template0", m_guc.tenantConfig->storageConfig.template1VfsName),
                  DSTORE_SUCC);
        ASSERT_EQ(controlFile->UpdatePdbItemData(PDB_TEMPLATE0_ID, &pdbInfo, sizeof(ControlPdbInfoPageItemData)),
                  DSTORE_SUCC);
        /* Step3: Alloc template1 pdb id. */
        ret = controlFile->AllocPdbId(PDB_ROOT_NAME, pdbId, true);
        ASSERT_EQ(ControlPdbInfoPageItemData::Init(&pdbInfo, PDB_ROOT_ID, PDB_DEFAULT_UUID_STR, PdbStatus::PDB_STATUS_OPENED_READ_WRITE, PDB_ROOT_NAME,
                                                   m_guc.tenantConfig->storageConfig.template1VfsName),
                  DSTORE_SUCC);
        ASSERT_EQ(controlFile->UpdatePdbItemData(PDB_ROOT_ID, &pdbInfo, sizeof(ControlPdbInfoPageItemData)), DSTORE_SUCC);
    }
};

/* Must execute make install before you run CreatePdbTest. */
TEST_F(PdbTest, CreatePdbTest_level1) {
    const char tableName[20] = "PDBTestTable_1";
    int rowNum = 350;
    PdbInfo pdbInfo[TENANT_PDB_COUNT];
    StorageSession *sc;

    int rc;
    for (uint32 i = 0; i < TENANT_PDB_COUNT; i++) {
        rc = memset_s(pdbInfo[i].pdbName, MAX_FILE_NAME_LEN, 0, MAX_FILE_NAME_LEN);
        storage_securec_check(rc, "\0", "\0");
        char pdbName[PDB_NAME_LEN];
        sprintf(pdbName, "pdb%d", i + 1);
        rc = memcpy_s(pdbInfo[i].pdbName, MAX_FILE_NAME_LEN, pdbName, strlen(pdbName));
        storage_securec_check(rc, "\0", "\0");
        pdbInfo[i].pdbRoleMode = PdbRoleMode::PDB_PRIMARY;
        pdbInfo[i].templateId = PDB_TEMPLATE1_ID;
        rc = memcpy_s(pdbInfo[i].pdbUuid, FORMATTED_UUID_ARR_LEN, PDB_DEFAULT_UUID, FORMATTED_UUID_ARR_LEN);
        storage_securec_check(rc, "\0", "\0");
        instance->AllocPdb(&pdbInfo[i]);
        ASSERT_EQ(instance->CreatePDB(&pdbInfo[i]), DSTORE_SUCC);
        ASSERT_EQ(instance->AllocPdb(&pdbInfo[i]), DSTORE_FAIL);
        instance->SetPdbStatusOpened(pdbInfo[i].pdbId);
        ASSERT_EQ(instance->OpenPDB(&pdbInfo[i], false), DSTORE_SUCC);
        StoragePdbInterface::SwitchContextToTargetPdb(pdbInfo[i].pdbId);
        StoragePdbInterface::SwitchContextToTargetPdb(PDB_TEMPLATE1_ID);
        char pdbPath[PDB_NAME_LEN];
        StoragePdbInterface::GetPdbPath(pdbInfo[i].pdbId, pdbPath);
        StoragePdbInterface::FlushAllDirtyPages(pdbInfo[i].pdbId);
        int64 pdbSize = 0;
        instance->GetPdbSize(pdbInfo[i].pdbId, pdbSize);
        instance->GetPdb(PDB_ROOT_ID)->GetControlFile()
                                    ->GetPdbInfo(pdbInfo[i].pdbName, PdbStatus::PDB_STATUS_OPENED_READ_WRITE);
        instance->GetPdb(PDB_ROOT_ID)->GetControlFile()
                                    ->GetPdbInfo(pdbInfo[i].pdbId, PdbStatus::PDB_STATUS_OPENED_READ_WRITE);
        PdbId pdbTmpId = INVALID_PDB_ID;
        ASSERT_EQ(instance->GetPdb(PDB_ROOT_ID)->GetControlFile()
                  ->GetPdbIdByName(pdbInfo[i].pdbName, &pdbTmpId, PdbStatus::PDB_STATUS_OPENED_READ_WRITE), DSTORE_SUCC);
        pdbInfo[i].grpRespoolOid++;
        PdbBriefInfo pdbidInfo = {0, "", "", PdbStatus::PDB_STATUS_CLOSED, (Oid)0};
        ASSERT_EQ(instance->GetPdbBriefInfoByName(pdbName, &pdbidInfo), DSTORE_SUCC);
        instance->UpdatePdbInfoById(pdbidInfo.pdbId, &pdbInfo[i]);
        PdbInfo newPdbInfo = {0};
        ASSERT_EQ(instance->GetPdbInfoById(pdbidInfo.pdbId, &newPdbInfo), DSTORE_SUCC);
        ASSERT_EQ(pdbInfo[i].grpRespoolOid, newPdbInfo.grpRespoolOid);

        sc = thrd->GetSession();
        thrd->DetachSessionFromThread();
        instance->UnregisterThread(false);
        CleanUpSession(sc);
        instance->CreateThreadAndRegister(i + DSTORE::FIRST_USER_PDB_ID, false, "newPdbWorker");
        sc = CreateStorageSession(1ULL);
        thrd->AttachSessionToThread(sc);
        (void)thrd->InitTransactionRuntime(pdbidInfo.pdbId, nullptr, nullptr, true);
        ASSERT_EQ(InsertTupleTest(i + DSTORE::FIRST_USER_PDB_ID, tableName, rowNum), DSTORE_SUCC);
    }
    uint32_t pdbIdInfoCount = TENANT_PDB_COUNT;
    DSTORE::PdbBriefInfo *pdbIdInfos = (DSTORE::PdbBriefInfo *)DstorePalloc(sizeof(DSTORE::PdbBriefInfo) * TENANT_PDB_COUNT);
    ASSERT_EQ(instance->GetPdbBriefInfoAll(pdbIdInfos, &pdbIdInfoCount), DSTORE_SUCC);
    ASSERT_EQ(pdbIdInfoCount, TENANT_PDB_COUNT);
    DstorePfree(pdbIdInfos);

    uint32_t pdbIdInfoCountControl = TENANT_PDB_COUNT;
    DSTORE::PdbBriefInfo *pdbIdInfosControl = (DSTORE::PdbBriefInfo *)DstorePalloc(sizeof(DSTORE::PdbBriefInfo) * TENANT_PDB_COUNT);
    ASSERT_EQ(instance->GetPdb(PDB_ROOT_ID)->GetControlFile()->GetVisiablePdbInfoAll(pdbIdInfosControl, &pdbIdInfoCountControl), DSTORE_SUCC);
    DstorePfree(pdbIdInfosControl);

    sc = thrd->GetSession();
    thrd->DetachSessionFromThread();
    CleanUpSession(sc);
    instance->UnregisterThread(false);
    /* Currently, StorageInstance startups with templatePdb */
    instance->CreateThreadAndRegister(PDB_TEMPLATE1_ID, false, "rootPdbWorker");
    sc = CreateStorageSession(1ULL);
    thrd->AttachSessionToThread(sc);
    (void)thrd->InitTransactionRuntime(PDB_TEMPLATE1_ID, nullptr, nullptr, true);

    PdbInfo pdbInfo17;
    rc = memset_s(pdbInfo17.pdbName, MAX_FILE_NAME_LEN, 0, MAX_FILE_NAME_LEN);
    storage_securec_check(rc, "\0", "\0");
    const char *pdbName17 = "pdb17";
    rc = memcpy_s(pdbInfo17.pdbName, MAX_FILE_NAME_LEN, pdbName17, strlen(pdbName17));
    storage_securec_check(rc, "\0", "\0");
    pdbInfo17.pdbRoleMode = PdbRoleMode::PDB_PRIMARY;
    pdbInfo17.templateId = PDB_TEMPLATE1_ID;
    pdbInfo17.pdbStatus = PdbStatus::PDB_STATUS_OPENED_READ_WRITE;
    rc = memcpy_s(pdbInfo17.pdbUuid, FORMATTED_UUID_ARR_LEN, PDB_DEFAULT_UUID, FORMATTED_UUID_ARR_LEN);
    storage_securec_check(rc, "\0", "\0");
    ASSERT_EQ(instance->AllocPdb(&pdbInfo17), DSTORE_FAIL);

    for (uint32 i = 0; i < TENANT_PDB_COUNT; i++) {
        ASSERT_EQ(instance->ClosePDB(pdbInfo[i].pdbName), DSTORE_SUCC);
        instance->UpdatePdbStatusById(pdbInfo[i].pdbId, PdbStatus::PDB_STATUS_CLOSED);
        ASSERT_EQ(instance->DropPDB(pdbInfo[i].pdbName), DSTORE_SUCC);
    }
}

// Scenario1: Test when nodes is nullptr and count is nullptr then GetBuiltinRelMap returns DSTORE_FAIL.
TEST_F(PdbTest, GetBuiltinRelMap_NullPtr_Test_level1)
{
    DSTORE::StoragePdb *storagePdb = g_storageInstance->GetPdb(PDB_ROOT_ID);
    ASSERT_DEATH(storagePdb->GetBuiltinRelMap(RelMapType::RELMAP_SHARED, nullptr, nullptr), "");
}

// Scenario2: Test when nodes is not nullptr and count is nullptr then GetBuiltinRelMap returns DSTORE_FAIL.
TEST_F(PdbTest, GetBuiltinRelMap_CountNull_Test_level0)
{
    DSTORE::StoragePdb *storagePdb = g_storageInstance->GetPdb(PDB_ROOT_ID);
    ASSERT_DEATH(storagePdb->GetBuiltinRelMap(RelMapType::RELMAP_SHARED, nodes, nullptr), "");
}

// Scenario3: Test when nodes is nullptr and count is not nullptr then GetBuiltinRelMap returns DSTORE_FAIL.
TEST_F(PdbTest, GetBuiltinRelMap_NodesNull_Test_level1)
{
    DSTORE::StoragePdb *storagePdb = g_storageInstance->GetPdb(PDB_ROOT_ID);
    ASSERT_DEATH(storagePdb->GetBuiltinRelMap(RelMapType::RELMAP_SHARED, nullptr, &count), "");
}

// Scenario4: Test when nodes and count are not nullptr and m_builtinRelMap.count is 0 then GetBuiltinRelMap returns
// DSTORE_SUCC.
TEST_F(PdbTest, GetBuiltinRelMap_CountZero_Test_level0)
{
    DSTORE::StoragePdb *storagePdb = g_storageInstance->GetPdb(PDB_ROOT_ID);
    EXPECT_EQ(storagePdb->GetBuiltinRelMap(RelMapType::RELMAP_SHARED, nodes, &count), DSTORE_SUCC);
}

// Scenario5: Test when nodes and count are not nullptr and m_builtinRelMap.count is greater than 0 then
// GetBuiltinRelMap returns DSTORE_SUCC.
TEST_F(PdbTest, GetBuiltinRelMap_CountGreaterThanZero_Test_level0)
{
    DSTORE::StoragePdb *storagePdb = g_storageInstance->GetPdb(PDB_ROOT_ID);
    nodes[0].relid = 1;
    (void)thrd->InitTransactionRuntime(PDB_ROOT_ID, nullptr, nullptr);
    EXPECT_EQ(storagePdb->WriteBuiltinRelMap(RelMapType::RELMAP_SHARED, nodes, 1), DSTORE_SUCC);
    thrd->GetActiveTransaction()->Commit();
    nodes[0].relid = 0;
    EXPECT_EQ(storagePdb->GetBuiltinRelMap(RelMapType::RELMAP_SHARED, nodes, &count), DSTORE_SUCC);
    EXPECT_EQ(count, 1);
    EXPECT_EQ(nodes[0].relid, 1);
}

// Scenario1: Test when count > RELMAP_NODE_MAX then return DSTORE_FAIL.
TEST_F(PdbTest, WriteBuiltinRelMap_01_level0)
{
    count = RELMAP_NODE_MAX + 1;
    DSTORE::StoragePdb *storagePdb = g_storageInstance->GetPdb(PDB_ROOT_ID);
    (void)thrd->InitTransactionRuntime(PDB_ROOT_ID, nullptr, nullptr);
    RetStatus ret = storagePdb->WriteBuiltinRelMap(RelMapType::RELMAP_SHARED, nodes, count);
    EXPECT_EQ(ret, DSTORE_FAIL);
    thrd->GetActiveTransaction()->Commit();
}

// Scenario2: Test when count <= RELMAP_NODE_MAX then return not DSTORE_FAIL.
TEST_F(PdbTest, WriteBuiltinRelMap_02_level0)
{
    count = RELMAP_NODE_MAX;
    DSTORE::StoragePdb *storagePdb = g_storageInstance->GetPdb(PDB_ROOT_ID);
    (void)thrd->InitTransactionRuntime(PDB_ROOT_ID, nullptr, nullptr);
    RetStatus ret = storagePdb->WriteBuiltinRelMap(RelMapType::RELMAP_SHARED, nodes, count);
    EXPECT_EQ(ret, DSTORE_SUCC);
    thrd->GetActiveTransaction()->Commit();
    (void)thrd->InitTransactionRuntime(PDB_ROOT_ID, nullptr, nullptr);
    ret = storagePdb->WriteBuiltinRelMap(RelMapType::RELMAP_LOCAL, nodes, count);
    EXPECT_EQ(ret, DSTORE_SUCC);
    thrd->GetActiveTransaction()->Commit();
}

// Scenario3: Test when nodes is nullptr then WriteBuiltinRelMap returns DSTORE_FAIL.
TEST_F(PdbTest, WriteBuiltinRelMapTest05_level0)
{
    DSTORE::StoragePdb *storagePdb = g_storageInstance->GetPdb(PDB_ROOT_ID);
    (void)thrd->InitTransactionRuntime(PDB_ROOT_ID, nullptr, nullptr);
    RetStatus ret = storagePdb->WriteBuiltinRelMap(RelMapType::RELMAP_SHARED, nullptr, count);
    EXPECT_EQ(ret, DSTORE_FAIL);
    thrd->GetActiveTransaction()->Commit();
}

// Scenario4: Test when count is less than 0 then WriteBuiltinRelMap returns DSTORE_FAIL.
TEST_F(PdbTest, WriteBuiltinRelMapTest06_level0)
{
    DSTORE::StoragePdb *storagePdb = g_storageInstance->GetPdb(PDB_ROOT_ID);
    (void)thrd->InitTransactionRuntime(PDB_ROOT_ID, nullptr, nullptr);
    RetStatus ret = storagePdb->WriteBuiltinRelMap(RelMapType::RELMAP_SHARED, nodes, -1);
    EXPECT_EQ(ret, DSTORE_FAIL);
    thrd->GetActiveTransaction()->Commit();
}

// Scenario1: Test when nodes is nullptr and count is nullptr then GetBuiltinRelMap returns DSTORE_FAIL.
TEST_F(PdbTest, GetInterfaceBuiltinRelMap_Test_level0)
{
    (void)thrd->InitTransactionRuntime(PDB_ROOT_ID, nullptr, nullptr);
    StoragePdbInterface::WriteBuiltinRelMap(RelMapType::RELMAP_SHARED, PDB_ROOT_ID, nodes, 1);
    thrd->GetActiveTransaction()->Commit();
    StoragePdbInterface::GetBuiltinRelMap(RelMapType::RELMAP_SHARED, PDB_ROOT_ID, nodes, &count);
    StoragePdbInterface::waitAllRollbackTaskFinished(PDB_ROOT_ID);
}
