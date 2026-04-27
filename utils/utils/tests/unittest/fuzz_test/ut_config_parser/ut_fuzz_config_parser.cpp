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
 * Description:
 * 1. fuzz test config_parser
 *
 * ---------------------------------------------------------------------------------
 */
#include "gtest/gtest.h"
#include <limits.h>
#include "syslog/err_log.h"
#include "syslog/err_log_internal.h"
#include "config_parser/config_parser.h"
#include "ut_fuzz_common.h"
#include "cjson/cJSON.h"

using namespace std;

static u8 *g_update1 = nullptr;
static s32 *g_intValue1 = nullptr;
static double *g_doubleValue1 = nullptr;

static u8 *g_update2 = nullptr;
static s32 *g_intValue2 = nullptr;
static double *g_doubleValue2 = nullptr;

struct ConfigParserHandle {
    cJSON cJson;
    bool isUpdated;
};

typedef struct FuzzPack {
    ConfigParserHandle *configHandleInit1;
    ConfigParserHandle *configHandleInit2;
} FuzzPack;

class ConfigParserFuzzTest : public testing::Test {
public:
    void SetUp() override
    {
        fuzzPack = (FuzzPack *)malloc(sizeof(FuzzPack));
        fuzzPack->configHandleInit1 = (ConfigParserHandle *)malloc(sizeof(ConfigParserHandle));
        fuzzPack->configHandleInit2 = (ConfigParserHandle *)malloc(sizeof(ConfigParserHandle));
    }

    void TearDown() override
    {
        free(fuzzPack->configHandleInit1);
        free(fuzzPack->configHandleInit2);
        free(fuzzPack);
    }
    FuzzPack *fuzzPack;
};

static void GenerateConfigParserHandle(int *i_num, FuzzPack *fuzzPack)
{
    g_update1 = (u8 *)DT_SetGetU8(&g_Element[*i_num], 0x123456);
    (*i_num)++;
    fuzzPack->configHandleInit1->isUpdated = (*g_update1) % 2 ? true : false;
    fuzzPack->configHandleInit1->cJson.child = nullptr;
    fuzzPack->configHandleInit1->cJson.next = nullptr;
    fuzzPack->configHandleInit1->cJson.prev = nullptr;
    fuzzPack->configHandleInit1->cJson.type = cJSON_String;
    fuzzPack->configHandleInit1->cJson.valuestring = DT_SetGetString(&g_Element[*i_num], 2, 10000, "a");
    (*i_num)++;
    g_intValue1 = (s32 *)DT_SetGetS32(&g_Element[*i_num], 0x123456);
    (*i_num)++;
    fuzzPack->configHandleInit1->cJson.valueint = *(int *)g_intValue1;
    g_doubleValue1 = (double *)DT_SetGetDouble(&g_Element[*i_num], 3.1415926);
    (*i_num)++;
    fuzzPack->configHandleInit1->cJson.valuedouble = *g_doubleValue1;
    fuzzPack->configHandleInit1->cJson.string = DT_SetGetString(&g_Element[*i_num], 2, 10000, "a");
    (*i_num)++;

    g_update2 = (u8 *)DT_SetGetU8(&g_Element[*i_num], 0x123456);
    (*i_num)++;
    fuzzPack->configHandleInit2->isUpdated = (*g_update2) % 2 ? true : false;
    fuzzPack->configHandleInit2->cJson.child = nullptr;
    fuzzPack->configHandleInit2->cJson.next = nullptr;
    fuzzPack->configHandleInit2->cJson.prev = nullptr;
    fuzzPack->configHandleInit2->cJson.type = cJSON_String;
    fuzzPack->configHandleInit2->cJson.valuestring = DT_SetGetString(&g_Element[*i_num], 2, 10000, "a");
    (*i_num)++;
    g_intValue2 = (s32 *)DT_SetGetS32(&g_Element[*i_num], 0x123456);
    (*i_num)++;
    fuzzPack->configHandleInit2->cJson.valueint = *(int *)g_intValue2;
    g_doubleValue2 = (double *)DT_SetGetDouble(&g_Element[*i_num], 3.1415926);
    (*i_num)++;
    fuzzPack->configHandleInit2->cJson.valuedouble = *g_doubleValue2;
    fuzzPack->configHandleInit2->cJson.string = DT_SetGetString(&g_Element[*i_num], 2, 10000, "a");
    (*i_num)++;
}

TEST_F(ConfigParserFuzzTest, testForConfigParserLoadFile)
{
    int seed = 0, count = COUNT_NUM;
    DT_Enable_Support_Loop(1);
    DT_Set_Is_Dump_Coverage(1);
    DT_Enable_Leak_Check(0, 0);
    DT_FUZZ_START(seed, count, "testForConfigParserLoadFile", 0)
    {
        int i_num = 0;
        char *configParserLoadFilePara0_0 = DT_SetGetString(&g_Element[i_num++], 2, 10000, "a");
        GenerateConfigParserHandle(&i_num, fuzzPack);
        ConfigParserLoadFile(configParserLoadFilePara0_0, &fuzzPack->configHandleInit1);
    }
    DT_FUZZ_END()
}

TEST_F(ConfigParserFuzzTest, testForConfigParserReloadFile)
{
    int seed = 0, count = COUNT_NUM;
    DT_Enable_Support_Loop(1);
    DT_Set_Is_Dump_Coverage(1);
    DT_FUZZ_START(seed, count, "testForConfigParserReloadFile", 0)
    {
        int i_num = 0;
        char *configParserReloadFilePara0_0 = DT_SetGetString(&g_Element[i_num++], 2, 10000, "a");
        GenerateConfigParserHandle(&i_num, fuzzPack);
        ConfigParserReloadFile(configParserReloadFilePara0_0, fuzzPack->configHandleInit1,
        &fuzzPack->configHandleInit1,
                               NULL);
    }
    DT_FUZZ_END()
}

TEST_F(ConfigParserFuzzTest, testForConfigParserPrintJson)
{
    int seed = 0, count = COUNT_NUM;

    DT_Enable_Support_Loop(1);
    DT_Set_Is_Dump_Coverage(1);
    DT_FUZZ_START(seed, count, "testForConfigParserPrintJson", 0)
    {
        int i_num = 0;
        GenerateConfigParserHandle(&i_num, fuzzPack);
        char *configParserPrintJsonPara1_0 = DT_SetGetString(&g_Element[i_num++], 2, 10000, "a");
        auto **configParserPrintJsonParaPointer1_0 = &configParserPrintJsonPara1_0;
        ConfigParserPrintJson(fuzzPack->configHandleInit1, configParserPrintJsonParaPointer1_0);
    }
    DT_FUZZ_END()
}

TEST_F(ConfigParserFuzzTest, testForConfigParserGetStrValue)
{
    int seed = 0, count = COUNT_NUM;
    DT_Enable_Support_Loop(1);
    DT_Set_Is_Dump_Coverage(1);
    DT_FUZZ_START(seed, count, "testForConfigParserGetStrValue", 0)
    {
        int i_num = 0;
        GenerateConfigParserHandle(&i_num, fuzzPack);
        char *configParserGetStrValuePara1_0 = DT_SetGetString(&g_Element[i_num++], 2, 10000, "a");
        char *configParserGetStrValuePara2_0 = DT_SetGetString(&g_Element[i_num++], 2, 10000, "a");
        auto **configParserGetStrValueParaPointer2_0 = &configParserGetStrValuePara2_0;
        ConfigParserGetStrValue(fuzzPack->configHandleInit1, configParserGetStrValuePara1_0,
                                configParserGetStrValueParaPointer2_0);
    }
    DT_FUZZ_END()
}

TEST_F(ConfigParserFuzzTest, testForConfigParserGetIntValue)
{
    int seed = 0, count = COUNT_NUM;
    DT_Enable_Support_Loop(1);
    DT_Set_Is_Dump_Coverage(1);
    DT_FUZZ_START(seed, count, "testForConfigParserGetIntValue", 0)
    {
        int i_num = 0;
        GenerateConfigParserHandle(&i_num, fuzzPack);
        char *configParserGetIntValuePara1_0 = DT_SetGetString(&g_Element[i_num++], 2, 10000, "a");
        int *configParserGetIntValuePara2_0 = (s32 *)DT_SetGetS32(&g_Element[i_num++], 0x123456);
        if (DT_GET_MutatedValueLen(&g_Element[i_num - 1]) < 4)
            continue;
        ConfigParserGetIntValue(fuzzPack->configHandleInit1, configParserGetIntValuePara1_0,
                                configParserGetIntValuePara2_0);
    }
    DT_FUZZ_END()
}

TEST_F(ConfigParserFuzzTest, testForConfigParserGetDoubleValue)
{
    int seed = 0, count = COUNT_NUM;
    DT_Enable_Support_Loop(1);
    DT_Set_Is_Dump_Coverage(1);
    DT_FUZZ_START(seed, count, "testForConfigParserGetDoubleValue", 0)
    {
        int i_num = 0;
        GenerateConfigParserHandle(&i_num, fuzzPack);
        char *configParserGetDoubleValuePara1_0 = DT_SetGetString(&g_Element[i_num++], 2, 10000, "a");
        double *configParserGetDoubleValuePara2_0 = (double *)DT_SetGetDouble(&g_Element[i_num++], 3.1415926);
        if (DT_GET_MutatedValueLen(&g_Element[i_num - 1]) < 8)
            continue;
        ConfigParserGetDoubleValue(fuzzPack->configHandleInit1, configParserGetDoubleValuePara1_0,
                                   configParserGetDoubleValuePara2_0);
    }
    DT_FUZZ_END()
}

TEST_F(ConfigParserFuzzTest, testForConfigParserGetBoolValue)
{
    int seed = 0, count = COUNT_NUM;
    DT_Enable_Support_Loop(1);
    DT_Set_Is_Dump_Coverage(1);
    DT_FUZZ_START(seed, count, "testForConfigParserGetBoolValue", 0)
    {
        int i_num = 0;
        GenerateConfigParserHandle(&i_num, fuzzPack);
        char *configParserGetBoolValuePara1_0 = DT_SetGetString(&g_Element[i_num++], 2, 10000, "a");
        u8 configParserGetBoolValuePara2_0 = *(u8 *)DT_SetGetU8(&g_Element[i_num++], 0x123456) % 2;
        ConfigParserGetBoolValue(fuzzPack->configHandleInit1, configParserGetBoolValuePara1_0,
                                 (bool *)&configParserGetBoolValuePara2_0);
    }
    DT_FUZZ_END()
}

TEST_F(ConfigParserFuzzTest, testForConfigParserGetNullValue)
{
    int seed = 0, count = COUNT_NUM;
    DT_Enable_Support_Loop(1);
    DT_Set_Is_Dump_Coverage(1);
    DT_FUZZ_START(seed, count, "testForConfigParserGetNullValue", 0)
    {
        int i_num = 0;
        GenerateConfigParserHandle(&i_num, fuzzPack);
        char *configParserGetNullValuePara1_0 = DT_SetGetString(&g_Element[i_num++], 2, 10000, "a");
        u8 configParserGetNullValuePara2_0 = *(u8 *)DT_SetGetU8(&g_Element[i_num++], 0x123456) % 2;
        ConfigParserGetNullValue(fuzzPack->configHandleInit1, configParserGetNullValuePara1_0,
                                 (bool *)&configParserGetNullValuePara2_0);
    }
    DT_FUZZ_END()
}

TEST_F(ConfigParserFuzzTest, testForConfigParserGetArraySize)
{
    int seed = 0, count = COUNT_NUM;
    DT_Enable_Support_Loop(1);
    DT_Set_Is_Dump_Coverage(1);
    DT_FUZZ_START(seed, count, "testForConfigParserGetArraySize", 0)
    {
        int i_num = 0;
        GenerateConfigParserHandle(&i_num, fuzzPack);
        unsigned int *configParserGetArraySizePara1_0 = (u32 *)DT_SetGetU32(&g_Element[i_num++], 0x123456);
        if (DT_GET_MutatedValueLen(&g_Element[i_num - 1]) < 4)
            continue;
        ConfigParserGetArraySize(fuzzPack->configHandleInit1, configParserGetArraySizePara1_0);
    }
    DT_FUZZ_END()
}

TEST_F(ConfigParserFuzzTest, testForConfigParserSetStrValue)
{
    int seed = 0, count = COUNT_NUM;
    DT_Enable_Support_Loop(1);
    DT_Set_Is_Dump_Coverage(1);
    DT_FUZZ_START(seed, count, "testForConfigParserSetStrValue", 0)
    {
        int i_num = 0;
        GenerateConfigParserHandle(&i_num, fuzzPack);
        char *ConfigParserSetStrValuePara1_0 = DT_SetGetString(&g_Element[i_num++], 2, 10000, "a");
        char *ConfigParserSetStrValuePara2_0 = DT_SetGetString(&g_Element[i_num++], 2, 10000, "a");
        ConfigParserSetStrValue(fuzzPack->configHandleInit1, ConfigParserSetStrValuePara1_0,
                                ConfigParserSetStrValuePara2_0);
    }
    DT_FUZZ_END()
}

TEST_F(ConfigParserFuzzTest, testForConfigParserSetIntValue)
{
    int seed = 0, count = COUNT_NUM;
    DT_Enable_Support_Loop(1);
    DT_Set_Is_Dump_Coverage(1);
    DT_FUZZ_START(seed, count, "testForConfigParserSetIntValue", 0)
    {
        int i_num = 0;
        GenerateConfigParserHandle(&i_num, fuzzPack);
        char *ConfigParserSetIntValuePara1_0 = DT_SetGetString(&g_Element[i_num++], 2, 10000, "a");
        int ConfigParserSetIntValuePara2_0 = *(s32 *)DT_SetGetS32(&g_Element[i_num++], 0x123456);
        ConfigParserSetIntValue(fuzzPack->configHandleInit1, ConfigParserSetIntValuePara1_0,
                                ConfigParserSetIntValuePara2_0);
    }
    DT_FUZZ_END()
}

TEST_F(ConfigParserFuzzTest, testForConfigParserSetDoubleValue)
{
    int seed = 0, count = COUNT_NUM;
    DT_Enable_Support_Loop(1);
    DT_Set_Is_Dump_Coverage(1);
    DT_FUZZ_START(seed, count, "testForConfigParserSetDoubleValue", 0)
    {
        int i_num = 0;
        GenerateConfigParserHandle(&i_num, fuzzPack);
        char *ConfigParserSetDoubleValuePara1_0 = DT_SetGetString(&g_Element[i_num++], 2, 10000, "a");
        double ConfigParserSetDoubleValuePara2_0 = *(double *)DT_SetGetDouble(&g_Element[i_num++], 0x123456);
        ConfigParserSetDoubleValue(fuzzPack->configHandleInit1, ConfigParserSetDoubleValuePara1_0,
                                   ConfigParserSetDoubleValuePara2_0);
    }
    DT_FUZZ_END()
}

TEST_F(ConfigParserFuzzTest, testForConfigParserSetBoolValue)
{
    int seed = 0, count = COUNT_NUM;
    DT_Enable_Support_Loop(1);
    DT_Set_Is_Dump_Coverage(1);
    DT_FUZZ_START(seed, count, "testForConfigParserSetBoolValue", 0)
    {
        int i_num = 0;
        GenerateConfigParserHandle(&i_num, fuzzPack);
        char *ConfigParserSetBoolValuePara1_0 = DT_SetGetString(&g_Element[i_num++], 2, 10000, "a");
        u8 *ConfigParserSetBoolValuePara1_0_p = (u8 *)DT_SetGetU8(&g_Element[i_num++], 0x123456);
        bool ConfigParserSetBoolValuePara2_0 = (*ConfigParserSetBoolValuePara1_0_p) % 2 ? true : false;
        ConfigParserSetBoolValue(fuzzPack->configHandleInit1, ConfigParserSetBoolValuePara1_0,
                                 ConfigParserSetBoolValuePara2_0);
    }
    DT_FUZZ_END()
}

TEST_F(ConfigParserFuzzTest, testForConfigParserSetNullValue)
{
    int seed = 0, count = COUNT_NUM;
    DT_Enable_Support_Loop(1);
    DT_Set_Is_Dump_Coverage(1);
    DT_FUZZ_START(seed, count, "testForConfigParserSetNullValue", 0)
    {
        int i_num = 0;
        GenerateConfigParserHandle(&i_num, fuzzPack);
        char *ConfigParserSetNullValuePara1_0 = DT_SetGetString(&g_Element[i_num++], 2, 10000, "a");
        ConfigParserSetNullValue(fuzzPack->configHandleInit1, ConfigParserSetNullValuePara1_0);
    }
    DT_FUZZ_END()
}

TEST_F(ConfigParserFuzzTest, testForConfigParserSetHandle)
{
    int seed = 0, count = COUNT_NUM;
    DT_Enable_Support_Loop(1);
    DT_Set_Is_Dump_Coverage(1);
    DT_FUZZ_START(seed, count, "testForConfigParserSetHandle", 0)
    {
        int i_num = 0;
        char *ConfigParserSetHandlePara1_0 = DT_SetGetString(&g_Element[i_num++], 2, 10000, "a");
        GenerateConfigParserHandle(&i_num, fuzzPack);
        ConfigParserSetHandle(fuzzPack->configHandleInit1, ConfigParserSetHandlePara1_0,
        fuzzPack->configHandleInit2);
    }
    DT_FUZZ_END()
}

TEST_F(ConfigParserFuzzTest, testForConfigParserCreateObjectHandle)
{
    int seed = 0, count = COUNT_NUM;
    DT_Enable_Support_Loop(1);
    DT_Set_Is_Dump_Coverage(1);
    DT_FUZZ_START(seed, count, "testForConfigParserCreateObjectHandle", 0)
    {
        int i_num = 0;
        GenerateConfigParserHandle(&i_num, fuzzPack);
        ConfigParserCreateObjectHandle(&fuzzPack->configHandleInit1);
    }
    DT_FUZZ_END()
}

TEST_F(ConfigParserFuzzTest, testForConfigParserCreateArrayHandle)
{
    int seed = 0, count = COUNT_NUM;
    DT_Enable_Support_Loop(1);
    DT_Set_Is_Dump_Coverage(1);
    DT_FUZZ_START(seed, count, "testForConfigParserCreateArrayHandle", 0)
    {
        int i_num = 0;
        GenerateConfigParserHandle(&i_num, fuzzPack);
        ConfigParserCreateArrayHandle(&fuzzPack->configHandleInit1);
    }
    DT_FUZZ_END()
}

TEST_F(ConfigParserFuzzTest, testForConfigParserHandleGetChild)
{
    int seed = 0, count = COUNT_NUM;
    DT_Enable_Support_Loop(1);
    DT_Set_Is_Dump_Coverage(1);
    DT_FUZZ_START(seed, count, "testForConfigParserHandleGetChild", 0)
    {
        int i_num = 0;
        GenerateConfigParserHandle(&i_num, fuzzPack);
        ConfigParserHandleGetChild(fuzzPack->configHandleInit1);
    }
    DT_FUZZ_END()
}

TEST_F(ConfigParserFuzzTest, testForConfigParserHandleGetNext)
{
    int seed = 0, count = COUNT_NUM;
    DT_Enable_Support_Loop(1);
    DT_Set_Is_Dump_Coverage(1);
    DT_FUZZ_START(seed, count, "testForConfigParserHandleGetNext", 0)
    {
        int i_num = 0;
        GenerateConfigParserHandle(&i_num, fuzzPack);
        ConfigParserHandleGetNext(fuzzPack->configHandleInit1);
    }
    DT_FUZZ_END()
}

TEST_F(ConfigParserFuzzTest, testForConfigParserStoreFile)
{
    int seed = 0, count = COUNT_NUM;
    DT_Enable_Support_Loop(1);
    DT_Set_Is_Dump_Coverage(1);
    DT_Enable_Leak_Check(0, 0);
    DT_FUZZ_START(seed, count, "testForConfigParserStoreFile", 0)
    {
        int i_num = 0;
        char *configParserStoreFilePara1_0 = DT_SetGetString(&g_Element[i_num++], 2, 10000, "a");
        GenerateConfigParserHandle(&i_num, fuzzPack);
        ConfigParserStoreFile(fuzzPack->configHandleInit1, configParserStoreFilePara1_0);
    }
    DT_FUZZ_END()
}

TEST_F(ConfigParserFuzzTest, testForConfigParserLoadString)
{
    int seed = 0, count = COUNT_NUM;
    DT_Enable_Support_Loop(1);
    DT_Set_Is_Dump_Coverage(1);
    DT_Enable_Leak_Check(0, 0);
    DT_FUZZ_START(seed, count, "testForConfigParserLoadString", 0)
    {
        int i_num = 0;
        char *configParserLoadStringPara0_0 = DT_SetGetString(&g_Element[i_num++], 2, 10000, "a");
        GenerateConfigParserHandle(&i_num, fuzzPack);
        ConfigParserLoadString(configParserLoadStringPara0_0, &fuzzPack->configHandleInit1);
    }
    DT_FUZZ_END()
}