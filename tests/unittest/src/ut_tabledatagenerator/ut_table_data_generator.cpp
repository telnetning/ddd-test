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
#include "ut_tabledatagenerator/ut_table_data_generator.h"
#include "catalog/dstore_fake_type.h"
#include "catalog/dstore_function_struct.h"
#include "common/datatype/dstore_array_utils.h"
TEST_F(UTTableDataGenerator, TableCreateTest)
{
    TableDataGenerator test;
    const uint32 colNum = 11;
    Oid testColTypes[colNum] = {BOOLOID, INT2OID, INT4OID,INT8OID,
                                CHAROID, OIDOID, TIMESTAMPOID, TIMESTAMPTZOID,
                                FLOAT4OID, FLOAT8OID, VARCHAROID};
    test.Create(testColTypes, colNum);
    ColDef *ret = test.GetColDefs();
    ASSERT_NE(ret, nullptr);
    for (int i = 0; i < colNum; i++) {
        EXPECT_EQ(ret[i].type, testColTypes[i]);
    }
}

TEST_F(UTTableDataGenerator, TableGenerateTest)
{
    TableDataGenerator test;
    const int colNum = 15;
    const int rowNum = 1;
    int varcharByteNum = 10;
    int int8Index = 3;
    int varcharIndex = 11;
    int oidVectorIndex = 12;
    int oidVectorLen = 20;
    int int2VectorIndex = 13;
    int int2VectorLen = 10;
    int anyArrayIndex = 14;
    int anyArrayLen = 15;
    int64 minValue = 2;
    int64 maxValue = 100; 
    Oid testColTypes[colNum] = {BOOLOID, INT2OID, INT4OID,INT8OID, INT8OID,
                                CHAROID, OIDOID, TIMESTAMPOID, TIMESTAMPTZOID,
                                FLOAT4OID, FLOAT8OID, VARCHAROID, OIDVECTOROID,
                                INT2VECTOROID, ANYARRAYOID};
    test.Create(testColTypes, colNum);
    test.Describe(varcharIndex, SetLen, varcharByteNum);
    test.Describe(int8Index, SetMinVal, minValue);
    test.Describe(int8Index, SetMaxVal, maxValue);
    test.Describe(oidVectorIndex, SetLen, oidVectorLen);
    test.Describe(int2VectorIndex, SetLen, int2VectorLen);
    test.Describe(anyArrayIndex, SetLen, anyArrayLen);
    test.GenerateData(rowNum);
    TestTuple* testtuples = test.GetTestTuples();
    for (int i = 0; i < rowNum; i++) {
        ASSERT_NE(testtuples[i].values, nullptr);
    }
 
    for (int i = 0; i < rowNum; i++) {
        text *tmp = (text *)(testtuples[i].values[varcharIndex]);
        ASSERT_NE(tmp, nullptr);
        for (int j = 0; j < varcharByteNum; j++) {
            EXPECT_GE(tmp->vl_dat[i], 'A');
            EXPECT_LE(tmp->vl_dat[i], 'Z');
        }
        OidVector *tmpOidVector = (OidVector *)(testtuples[i].values[oidVectorIndex]);
        ASSERT_NE(tmpOidVector, nullptr);
        for (int j = 0; j < oidVectorLen; j++) {
            EXPECT_GE(tmpOidVector->values[j], 0);
            EXPECT_LE(tmpOidVector->values[j], UINT_MAX);
        }
        int2vector *tmpInt2Vector = (int2vector *)(testtuples[i].values[int2VectorIndex]);
        ASSERT_NE(tmpInt2Vector, nullptr);
        for (int j = 0; j < int2VectorLen; j++) {
            EXPECT_GE(tmpInt2Vector->values[j], SHRT_MIN);
            EXPECT_LE(tmpInt2Vector->values[j], SHRT_MAX);
        }
        ArrayType *tmpArrayTypeP = (ArrayType *)(testtuples[i].values[anyArrayIndex]);
        ASSERT_NE(tmpArrayTypeP, nullptr);
        for (int j = 0; j < anyArrayLen; j++) {
            EXPECT_GE(ARR_DATA_PTR(tmpArrayTypeP)[j], INT_MIN);
            EXPECT_LE(ARR_DATA_PTR(tmpArrayTypeP)[j], INT_MAX);
        }

    }
    for (int i = 0; i < rowNum; i++) {
        int64 tmp = (int64)(testtuples[i].values[int8Index]);
        EXPECT_GE(tmp, minValue);
        EXPECT_LE(tmp, maxValue);
    }
}
 
TEST_F(UTTableDataGenerator, TableDescribeTest)
{
    TableDataGenerator test;
    const int colNum = 3;
    const int rowNum = 3;
    Oid colTypes[colNum] = {CHAROID, INT4OID, BOOLOID};
    test.Create(colTypes, colNum);
    test.Describe(1, SetMinVal, 10);
    ColDef *ret = test.GetColDefs();
    EXPECT_EQ(ret[1].minValue, 10);
}

TEST_F(UTTableDataGenerator, DISABLED_TableInsertRowTest)
{
    TableDataGenerator test;
    const uint32 colNum = 12;
    const uint32 rowNum = 1;
    int varcharByteNum = 10;
    Oid testColTypes[colNum] = {BOOLOID, INT2OID, INT4OID,INT8OID, INT8OID,
                                CHAROID, OIDOID, TIMESTAMPOID, TIMESTAMPTZOID,
                                FLOAT4OID, FLOAT8OID, VARCHAROID};
    test.Create(testColTypes, colNum);
    test.GenerateData(rowNum);
    const uint32 insertRowNum = 2;
    char *a = (char *)"abc";
    char *b = (char *)"def";
    Datum insertValues[insertRowNum][colNum] = {
        {false, 1, 2, 3, 4, 'a', 123, 456, 789, Float32GetDatum(12.3), Float64GetDatum(14.5), CStringGetDatum(a)},
        {true, 2, 3, 4, 5, 'b', 456, 123, 101, Float32GetDatum(11.1), Float64GetDatum(12.1), CStringGetDatum(b)}
    };
    bool insertIsNull[insertRowNum][colNum] = {
        {false, false, false, false, false, false, false, false, false, false, false, false},
        {false, false, false, false, false, false, false, false, false, false, false, false}
    };
    for (int i = 0; i < insertRowNum; i++) {
        test.InsertRow(insertValues[i], insertIsNull[i], colNum);
    }

    for (int i = rowNum; i < rowNum + insertRowNum; i++) {
        TestTuple tuple;
        ASSERT_EQ(test.GetRow(i, tuple), SUCCESS);
        for (int j = 0; j < colNum; j++) {
            if (testColTypes[j] == VARCHAROID) {
                char *real = DatumGetCString(insertValues[i - rowNum][j]);
                text *tmp = (text *)tuple.values[j];
                EXPECT_EQ(strcmp(tmp->vl_dat, real), 0);
            } else {
                EXPECT_EQ(tuple.values[j], insertValues[i - rowNum][j]);
            }
        }
    }
}
