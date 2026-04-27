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
#ifndef DSTORE_UT_DSTORE_ENGINE_H
#define DSTORE_UT_DSTORE_ENGINE_H

#include "gtest/gtest.h"

#include "securec.h"

#include <dirent.h>
#include <fcntl.h>
#include <semaphore.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#include <vector>
#include <fstream>

#include "common/log/dstore_log.h"
#include "common/memory/dstore_mctx.h"
#include "common/dstore_datatype.h"
#include "framework/dstore_instance.h"
#include "catalog/dstore_fake_relation.h"
#include "ut_utilities/ut_config_file.h"
#include "page/dstore_page.h"

using namespace DSTORE;

constexpr uint32 GRAND_VERSION_NUM = 97039;

constexpr TablespaceId UNDO_TABLE_SPACE_ID_UT = 4;
constexpr FileId UNDO_TABLE_FILE_ID_UT = 4;

constexpr const char *UT_DEFAULT_STORESPACE_NAME = "ut_default_storespace";

class DSTORETEST : public ::testing::Test {
public:
    /**
     * As we have multiprocess cases, we should take care of their entry/exit to prevent any resource leak.
     */
    static void ProcEnter();

    /**
     * Parent process should exit with 0 normally. However, child processes should have their own exit code to prevent
     * any unwanted operations.
     */
    static void ProcExit(int exitCode);
    
    static void Bootstrap();

    static char *GetTemplatePdbPath();

    static void CopyDataFile();
    static void ReadDataFileImage(PageId pageId, char *outBuffer, size_t bufferSize);
    static bool CmpPages(void *page1, void *page2, size_t pageSize, bool ignorePlsn = false);
    static char **AllocBufferForImage(bool isDataImage, uint32 *blockCount, FileId defaultFileId, FileId undoFileId);
    static void FreeBufferForImage(char **buffer, uint32 blockCount);
    static void InitTenantConfig(StorageGUC *guc);

    DSTORETEST(){};

    virtual ~DSTORETEST()
    {}

    static void SetUpTestCase();

    static void TearDownTestCase();

    static void PrintTestCaseName();

    inline const char *GetTestCaseName() {
        return ::testing::UnitTest::GetInstance()->current_test_info()->test_case_name();
    }

    void SetUp() override;

    void TearDown() override;

    DstoreMemoryContext m_ut_memory_context;
    static StorageGUC m_guc;
    static char pdbTemplatePath[MAXPGPATH];
    static char srcDataFile[MAXPGPATH];
    static char imageDataFile[MAXPGPATH];
    static char undoSrcDataFile[MAXPGPATH];
    static char undoImageDataFile[MAXPGPATH];
    static TenantConfig m_tenantConfig;
    static PdbId m_pdbId;

protected:
    static UTConfigFile m_config;

private:
    static char dstoreDir[MAXPGPATH];
    void ut_memory_context_init()
    {
        m_ut_memory_context =
            DstoreAllocSetContextCreate(nullptr, "UtMemoryContext", ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_INITSIZE, MemoryContextType::SHARED_CONTEXT);
    }

    void ut_memory_context_free()
    {
        DstoreMemoryContextDestroyTop(m_ut_memory_context);
        m_ut_memory_context = nullptr;
    }
};

void create_thread_and_register();
void ut_init_transaction_runtime();
void unregister_thread();
void destory_thread();

template <typename T>
class DSTOREParamTest : public testing::TestWithParam<T> {
public:
    static void SetUpTestCase();

    static void TearDownTestCase();

    void SetUp() override;

    void TearDown() override;

    DstoreMemoryContext m_ut_memory_context;
    static StorageGUC m_guc;
    static TenantConfig m_tenantConfig;

private:
    static char dstoreDir[MAXPGPATH];
    static char pdbTemplatePath[MAXPGPATH];

    void ut_memory_context_init()
    {
        m_ut_memory_context =
            DstoreAllocSetContextCreate(nullptr, "UtMemoryContext", ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_INITSIZE, MemoryContextType::SHARED_CONTEXT);
    }

    void ut_memory_context_free()
    {
        DstoreMemoryContextDestroyTop(m_ut_memory_context);
        m_ut_memory_context = nullptr;
    }
};

class UtInstance : public StorageInstance, public BaseObject
{
public:
    virtual void Install(StorageGUC *guc, DstoreMemoryContext utMctx);
    virtual void InstallPrepare(StorageGUC *guc, DstoreMemoryContext utMctx);
    virtual void InstallFinish();

    virtual void Startup(StorageGUC *guc);
    virtual void Shutdown();
    virtual RetStatus CreatePDB(PdbInfo *pdbInfo);

    virtual void ThreadSetupAndRegister();
    virtual void ThreadUnregisterAndExit();
};

void CreateClusterInfo(StorageGUC *guc, int nodeNum = 0, std::vector<int> nodeType = {}, int startNodeId = 0);
int GetClusterInfoTerm();
void UpdateClusterInfo(int nodeNum, std::vector<int> nodeType, std::vector<int> nodeStatus, int term);
void CreateDefaultVfs(TenantConfig *config);

static void InstallDatabase(StorageGUC *guc, DstoreMemoryContext utMctx)
{
    UtInstance *instance = DstoreNew(utMctx)UtInstance();
    instance->Install(guc, utMctx);
    delete instance;
}

#endif
