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
#include "ut_tuple/ut_tuple.h"


void TupleTest::SetUp()
{
    DSTORETEST::SetUp();

    MockStorageInstance *instance = DstoreNew(m_ut_memory_context) MockStorageInstance();
    instance->Install(&DSTORETEST::m_guc, m_ut_memory_context);
    instance->Startup(&DSTORETEST::m_guc);

    m_tableHandler = UTTableHandler::CreateTableHandler(g_defaultPdbId, m_ut_memory_context);
}

void TupleTest::TearDown()
{
    /* m_tableHandler->Destroy() will delete m_table */
    UTTableHandler::Destroy(m_tableHandler);
    m_tableHandler = nullptr;

    MockStorageInstance *instance = (MockStorageInstance *)g_storageInstance;
    instance->Shutdown();
    delete instance;

    DSTORETEST::TearDown();
}


TEST_F(TupleTest, FormAndDeformHeapTupleTest)
{
    /* Step 1. generate random values */
    TupleDesc heapTupleDesc = m_tableHandler->GetHeapTupDesc();
    int nAttrs = heapTupleDesc->natts;

    Datum *valuesWriten = (Datum *)DstorePalloc(nAttrs * sizeof(Datum));
    bool *isnullWriten = (bool *)DstorePalloc(nAttrs * sizeof(bool));

    m_tableHandler->GenerateRandomData(valuesWriten, isnullWriten);

    /* Step 2. form tuple using valuesWriten */
    HeapTuple *heapTuple = HeapTuple::FormTuple(heapTupleDesc, valuesWriten, isnullWriten);

    Datum *valuesRead = (Datum *)DstorePalloc(nAttrs * sizeof(Datum));
    bool *isnullRead = (bool *)DstorePalloc(nAttrs * sizeof(bool));

    /* Step 3. deform heap tuple & get valuesRead from heap tuple */
    heapTuple->DeformTuple(heapTupleDesc, valuesRead, isnullRead);

    /* Step 4. compare values */
    for (int i = 0; i < nAttrs; i++) {
        if (heapTupleDesc->attrs[i]->attbyval)
        EXPECT_EQ(m_tableHandler->CompareDatum(valuesRead[i], valuesWriten[i], heapTupleDesc->attrs[i]->atttypid), 0);
        EXPECT_EQ(isnullRead[i], isnullWriten[i]);
    }

    /* Step 5. free all memory we used */
    DstorePfree(valuesWriten);
    DstorePfree(isnullWriten);
    DstorePfree(valuesRead);
    DstorePfree(isnullRead);
    DstorePfreeExt(heapTuple);
}

TEST_F(TupleTest, ModifyHeapTupleTest)
{
    /* Step 1. generate random values */
    TupleDesc heapTupleDesc = m_tableHandler->GetHeapTupDesc();
    int nAttrs = heapTupleDesc->natts;

    Datum *values = (Datum *)DstorePalloc(nAttrs * sizeof(Datum));
    bool *isnull = (bool *)DstorePalloc(nAttrs * sizeof(bool));
    m_tableHandler->GenerateRandomData(values, isnull);

    /* Step 2. form tuple using valuesWriten */
    HeapTuple *heapTuple = HeapTuple::FormTuple(heapTupleDesc, values, isnull);

    /* Step 3. generate new values */
    Datum *replValues = (Datum *)DstorePalloc(nAttrs * sizeof(Datum));
    bool *replIsnull = (bool *)DstorePalloc(nAttrs * sizeof(bool));
    m_tableHandler->GenerateRandomData(replValues, replIsnull);

    /* Step 3. modify the tuple generated in step 2 */
    bool *doReplace = (bool *)DstorePalloc0(nAttrs * sizeof(bool));
    for(int i = 0; i < nAttrs; i++)
        doReplace[i] = true;
    TupleAttrContext context = {heapTupleDesc, replValues, replIsnull, 0, false};
    HeapTuple *newTuple = TupleInterface::ModifyTuple(heapTuple, context, doReplace, nullptr);

    /* Step 4. deform modified tuple into values and isnull array again */
    newTuple->DeformTuple(heapTupleDesc, values, isnull);

    /* Step 5. compare values */
    for (int i = 0; i < nAttrs; i++) {
        if (heapTupleDesc->attrs[i]->attbyval)
        EXPECT_EQ(m_tableHandler->CompareDatum(values[i], replValues[i], heapTupleDesc->attrs[i]->atttypid), 0);
        EXPECT_EQ(isnull[i], replIsnull[i]);
    }

    /* Step 6. free all memory we used */
    DstorePfree(values);
    DstorePfree(isnull);
    DstorePfree(replValues);
    DstorePfree(replIsnull);
    DstorePfreeExt(heapTuple);
    DstorePfreeExt(newTuple);
}

TEST_F(TupleTest, SetAndGetOidTest)
{
    /* Step 1. generate random values */
    TupleDesc heapTupleDesc = m_tableHandler->GetHeapTupDesc();
    int nAttrs = heapTupleDesc->natts;

    /* Step 2. declare that we have oid for this tuple */
    heapTupleDesc->tdhasoid = true;

    /* Step 3. Generate data and form tuple */
    Datum *valuesWriten = (Datum *)DstorePalloc(nAttrs * sizeof(Datum));
    bool *isnullWriten = (bool *)DstorePalloc(nAttrs * sizeof(bool));

    m_tableHandler->GenerateRandomData(valuesWriten, isnullWriten);

    HeapTuple *heapTuple = HeapTuple::FormTuple(heapTupleDesc, valuesWriten, isnullWriten);

    /* Step 4. Check the initial oid is DSTORE_INVALID_OID */
    Oid oid = heapTuple->GetOid();
    EXPECT_EQ(oid, DSTORE_INVALID_OID);

    /* Step 5. Set the oid of heapTuple */
    oid = BOOLOID; /* use bool type oid here for testing */
    heapTuple->SetOid(BOOLOID);
    EXPECT_EQ(heapTuple->GetOid(), oid);

    /* Step 6. free all memory we used */
    DstorePfree(valuesWriten);
    DstorePfree(isnullWriten);
    DstorePfreeExt(heapTuple);
}

TEST_F(TupleTest, CopyTupleTest)
{
    /* Step 1. generate random values */
    TupleDesc heapTupleDesc = m_tableHandler->GetHeapTupDesc();
    int nAttrs = heapTupleDesc->natts;

    Datum *valuesWriten = (Datum *)DstorePalloc(nAttrs * sizeof(Datum));
    bool *isnullWriten = (bool *)DstorePalloc(nAttrs * sizeof(bool));

    m_tableHandler->GenerateRandomData(valuesWriten, isnullWriten);

    /* Step 2. form tuple using valuesWriten */
    HeapTuple *heapTuple = HeapTuple::FormTuple(heapTupleDesc, valuesWriten, isnullWriten);

    Datum *valuesRead = (Datum *)DstorePalloc(nAttrs * sizeof(Datum));
    bool *isnullRead = (bool *)DstorePalloc(nAttrs * sizeof(bool));

    /* Step 3. Copy tuple and deform copied tuple */
    HeapTuple *newTuple = heapTuple->Copy();
    newTuple->DeformTuple(heapTupleDesc, valuesRead, isnullRead);

    /* Step 4. compare HeapTupleHeader fields */
    EXPECT_EQ(memcmp((void *)newTuple, (void *)heapTuple, offsetof(HeapTuple, m_diskTuple)), 0);

    /* Step 5. compare values */
    for (int i = 0; i < nAttrs; i++) {
        if (heapTupleDesc->attrs[i]->attbyval)
        EXPECT_EQ(m_tableHandler->CompareDatum(valuesRead[i], valuesWriten[i], heapTupleDesc->attrs[i]->atttypid), 0);
        EXPECT_EQ(isnullRead[i], isnullWriten[i]);
    }

    /* Step 6. free all memory we used */
    DstorePfree(valuesWriten);
    DstorePfree(isnullWriten);
    DstorePfree(valuesRead);
    DstorePfree(isnullRead);
    DstorePfreeExt(heapTuple);
    DstorePfreeExt(newTuple);
}
