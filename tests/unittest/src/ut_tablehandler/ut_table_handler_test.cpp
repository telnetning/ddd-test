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
#include "ut_tablehandler/ut_table_handler_test.h"
#include "ut_mock/ut_instance_mock.h"


void UTTableHandlerTest::SetUp()
{
    DSTORETEST::SetUp();

    MockStorageInstance *instance = DstoreNew(m_ut_memory_context) MockStorageInstance();
    instance->Install(&DSTORETEST::m_guc, m_ut_memory_context);
    instance->Startup(&DSTORETEST::m_guc);

    m_tableHandler = UTTableHandler::CreateTableHandler(g_defaultPdbId, m_ut_memory_context);
}

void UTTableHandlerTest::TearDown()
{
    UTTableHandler::Destroy(m_tableHandler);
    m_tableHandler = nullptr;

    MockStorageInstance *instance = (MockStorageInstance *)g_storageInstance;
    instance->Shutdown();
    delete instance;

    DSTORETEST::TearDown();
}

TEST_F(UTTableHandlerTest, TypeCacheTable)
{
    int32 ret;
    ScanKey scanKey1 = (ScanKey)DstorePalloc0(sizeof(ScanKeyData));
    ScanKey scanKey2 = (ScanKey)DstorePalloc0(sizeof(ScanKeyData));

    /* BtreeBoolCmp */
    m_tableHandler->GetScanFuncByFnOid(scanKey1, 1693);
    ret = DatumGetInt32(FunctionCall2Coll(&scanKey1->skFunc, scanKey1->skCollation, BoolGetDatum(true),
                                          BoolGetDatum(false)));
    EXPECT_EQ(ret, 1);

    /* BtreeInt2Cmp */
    m_tableHandler->GetScanFuncByType(scanKey1, INT2OID);
    ret = DatumGetInt32(FunctionCall2Coll(&scanKey1->skFunc, scanKey1->skCollation, Int16GetDatum((int16)0),
                                          BoolGetDatum((int16)255)));
    EXPECT_EQ(ret, -1);

    m_tableHandler->GetScanFuncByFnOid(scanKey2, 350);
    EXPECT_EQ(memcmp(scanKey1, scanKey2, sizeof(ScanKeyData)), 0);

    ret = DatumGetInt32(FunctionCall2Coll(&scanKey2->skFunc, scanKey2->skCollation, Int16GetDatum((int16)254),
                                          BoolGetDatum((int16)1)));
    EXPECT_EQ(ret, 1);

    /* BtreeInt48Cmp */
    m_tableHandler->GetScanFuncByType(scanKey1, INT4OID, INT8OID);
    ret = DatumGetInt32(FunctionCall2Coll(&scanKey1->skFunc, scanKey1->skCollation, Int16GetDatum((int32)0),
                                          BoolGetDatum((int64)1024)));
    EXPECT_EQ(ret, -1);

    m_tableHandler->GetScanFuncByFnOid(scanKey2, 2188);
    EXPECT_EQ(memcmp(scanKey1, scanKey2, sizeof(ScanKeyData)), 0);

    ret = DatumGetInt32(FunctionCall2Coll(&scanKey2->skFunc, scanKey2->skCollation, Int32GetDatum((int32)255),
                                          Int64GetDatum((int64)255)));
    EXPECT_EQ(ret, 0);

    /* BtreeNameCmp */
    DstoreNameData name = {"testName"};
    m_tableHandler->GetScanFuncByFnOid(scanKey1, 359);
    ret = DatumGetInt32(FunctionCall2Coll(&scanKey1->skFunc, scanKey1->skCollation, PointerGetDatum(&name),
                                          PointerGetDatum(&name)));
    EXPECT_EQ(ret, 0);

    DstorePfree(scanKey1);
    DstorePfree(scanKey2);
}


