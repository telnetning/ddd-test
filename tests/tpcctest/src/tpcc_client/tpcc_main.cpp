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

#include <iostream>
#include <unistd.h>
#include <dirent.h>
#include <assert.h>
#include "common/dstore_datatype.h"
#include "securec.h"
#include "tpcc_client/tpcc_test_client.h"
#include "tpcc_server/tpcc_test_server.h"
#include "common_persistence_handler.h"
#include "config/dstore_vfs_config.h"

const char* TPCC_RUNSTATE_PATH("tpccrunstate");
const char* CLUSTER_INFO_PATH("clusterInfo");

void PrintfTpccRunState(NodeId nodeId, TPCC_BENCHMARK::TpccRunStateMetaData *runStateObj)
{
    std::cout << "==============NodeId: " << nodeId << " PrintfTpccRunState Start==========" << std::endl;
    std::cout << "state " << runStateObj->state << " nodeNum " << runStateObj->nodeNum << " itemRelIsLoadFinished "
              << runStateObj->itemRelIsLoadFinished << " heapCount " << runStateObj->heapCount << " indexCount "
              << runStateObj->indexCount << " loadDataSize " << runStateObj->loadDataSize << std::endl;
    for (uint32_t i = 0; i < runStateObj->nodeNum; ++i) {
        std::cout << "startThreadId " << runStateObj->threadRange[i].startThreadId << " endThreadId "
                  << runStateObj->threadRange[i].endThreadId << std::endl;
    }
    for (uint32_t i = 0; i < runStateObj->nodeNum; ++i) {
        std::cout << "initState[" << i << "] = "  << runStateObj->initState[i] << std::endl;
    }
    for (uint32_t i = 0; i < runStateObj->nodeNum; ++i) {
        std::cout << "startState[" << i << "] = "  << runStateObj->startState[i] << std::endl;
    }
    for (uint32_t i = 0; i < runStateObj->nodeNum; ++i) {
        std::cout << "recoveryState[" << i << "] = "  << runStateObj->recoveryState[i] << std::endl;
    }
    for (uint32_t i = 0; i < runStateObj->nodeNum; ++i) {
        std::cout << "loadDatasState[" << i << "] = "  << runStateObj->loadDatasState[i] << std::endl;
    }
    for (uint32_t i = 0; i < runStateObj->nodeNum; ++i) {
        std::cout << "executeState[" << i << "] = "  << runStateObj->executeState[i] << std::endl;
    }
    for (uint32_t i = 0; i < runStateObj->nodeNum; ++i) {
        std::cout << "checkState[" << i << "] = "  << runStateObj->checkState[i] << std::endl;
    }
    for (uint32_t i = 0; i < runStateObj->nodeNum; ++i) {
        std::cout << "stopState[" << i << "] = "  << runStateObj->stopState[i] << std::endl;
    }
    for (uint32_t index = 0; index < runStateObj->loadDataSize; ++index) {
        std::cout << "index " << index << " startWhId " << runStateObj->loadDatas[index].startWhId << " endWhId "
                  << runStateObj->loadDatas[index].endWhId << " isFinished " << runStateObj->loadDatas[index].isFinished
                  << std::endl;
    }

    std::cout << "==============NodeId: " << nodeId << " PrintfTpccRunState End==========" << std::endl;
}
char* GetTpccRunStatePath()
{
    char *path = (char *)malloc(VFS_FILE_PATH_MAX_LEN);
    assert(path != nullptr);
    char* current_dir = get_current_dir_name();
    if (current_dir == nullptr) {
        free(path);
        return nullptr;
    }
    errno_t rc;
    if (strstr(current_dir, "tpccdir") == NULL) {
        rc = sprintf_s(path, VFS_FILE_PATH_MAX_LEN, "%s/tpccdir/%s", 
                      current_dir, TPCC_RUNSTATE_PATH);
    } else {
        rc = sprintf_s(path, VFS_FILE_PATH_MAX_LEN, "%s/%s", 
                      current_dir, TPCC_RUNSTATE_PATH);
    }
    
    free(current_dir);
    storage_securec_check_ss(rc);
    
    return path;
}
void AllocRunStateInfo(TPCC_BENCHMARK::TpccRunStateMetaData *runStateObj, uint32_t threadNum,
                       const TPCC_BENCHMARK::TpccConfig &tpccCfg)
{
    uint32_t threadNumPerNode = tpccCfg.workThreadNum / runStateObj->nodeNum;
    if (threadNumPerNode == 0) {
        threadNumPerNode = 1;
    }
    uint32_t warehouseNumPerThread = tpccCfg.wareHouseNum / threadNum;
    if (warehouseNumPerThread == 0) {
        warehouseNumPerThread = 1;
    }
    uint32_t lastThreadNum = threadNum - threadNumPerNode * runStateObj->nodeNum;
    int nextStartThreadId = 0;
    for (uint32_t i = 0; i < runStateObj->nodeNum; ++i) {
        runStateObj->threadRange[i].startThreadId = nextStartThreadId;
        if (i < lastThreadNum) {
            runStateObj->threadRange[i].endThreadId = nextStartThreadId + threadNumPerNode;
        } else {
            runStateObj->threadRange[i].endThreadId = nextStartThreadId + threadNumPerNode - 1;
        }
        nextStartThreadId = runStateObj->threadRange[i].endThreadId + 1;
    }

    uint32_t lastWarehousNum = tpccCfg.wareHouseNum - threadNum * warehouseNumPerThread;
    int nextStartWhId = 1;
    for (uint32_t index = 0; index < threadNum; ++index) {
        runStateObj->loadDatas[index].startWhId = nextStartWhId;
        runStateObj->loadDatas[index].endWhId = nextStartWhId + warehouseNumPerThread - 1;
        if (index < lastWarehousNum) {
            runStateObj->loadDatas[index].endWhId += 1;
        }
        runStateObj->loadDatas[index].isFinished = false;
        nextStartWhId = runStateObj->loadDatas[index].endWhId + 1;
    }
}

TPCC_BENCHMARK::TpccRunStateMetaData *CreateTpccRunningFile(CommonPersistentHandler &persistenceHandler,
                                                            const TPCC_BENCHMARK::TpccConfig &tpccCfg, uint32_t nodeNum)
{
    nodeNum = 1;
    char *path = GetTpccRunStatePath();
    assert(persistenceHandler.IsExist(path) == false);
    Size headerSize = sizeof(TPCC_BENCHMARK::TpccRunStateMetaData);
    uint32_t tmpNodeNum = std::min(nodeNum, tpccCfg.wareHouseNum);
    uint32_t threadNum = std::min(tpccCfg.wareHouseNum, tpccCfg.workThreadNum);
    if (threadNum < tmpNodeNum) {
        threadNum = tmpNodeNum;
    }

    Size totalSize = headerSize;
    char *obj = (char *)malloc(totalSize);
    assert(obj != nullptr);

    TPCC_BENCHMARK::TpccRunStateMetaData *runStateObj =
        static_cast<TPCC_BENCHMARK::TpccRunStateMetaData *>(static_cast<void *>(obj));
    assert(runStateObj != nullptr);
    runStateObj->Init(threadNum, tmpNodeNum);
    AllocRunStateInfo(runStateObj, threadNum, tpccCfg);
    persistenceHandler.Create(path, obj, totalSize);
    runStateObj = static_cast<TPCC_BENCHMARK::TpccRunStateMetaData *>(persistenceHandler.GetObject());
    persistenceHandler.Sync();
    std::cout << "nodeId = 1 Create TpccRunState finished " << std::endl;
    PrintfTpccRunState(1, runStateObj);
    free(path);
    return runStateObj;
}

TPCC_BENCHMARK::TpccRunStateMetaData *OpenRunningStatusFile(CommonPersistentHandler &persistenceHandler, int nodeId)
{
    nodeId = 1;
    char *path = GetTpccRunStatePath();
    /* Wait until the file is successfully created on node 0. */
    const int sleepTime = 1000;
    while (!persistenceHandler.IsExist(path)) {
        if (nodeId == 1) {
            free(path);
            return nullptr;
        }
        (void)usleep(sleepTime);
    }
    persistenceHandler.Open(path);
    free(path);
    char *tmpObj = static_cast<char *>(persistenceHandler.GetObject());
    TPCC_BENCHMARK::TpccRunStateMetaData *runStateObj =
        static_cast<TPCC_BENCHMARK::TpccRunStateMetaData *>(static_cast<void *>(tmpObj));
    std::cout << "nodeId = " << nodeId << " Open TpccRunState finished " << std::endl;
    PrintfTpccRunState(nodeId, runStateObj);
    while (runStateObj->state < TPCC_BENCHMARK::TPCC_RUN_STATE_INIT_FINISHED) {
        if (nodeId == 1) {
            free(path);
            return nullptr;
        }
        /* Wait for initdb finished. */
        (void)usleep(sleepTime);
    }

    if (nodeId == 1) {
        runStateObj->ResetState();
    }
    std::cout << "nodeId = " << nodeId << " Wait TpccRunState finished " << std::endl;
    PrintfTpccRunState(nodeId, runStateObj);
    return runStateObj;
}

uint32_t ReadCluserCfgFromFile(FILE *file, char *path, int bufferMaxLen)
{
    char buffer[bufferMaxLen];
    uint32_t nodeCount = 0;
    __attribute__((__unused__)) uint16_t commListenPort = 0;
    __attribute__((__unused__)) uint32_t clusterId = 0;
    __attribute__((__unused__)) uint32_t termId = 0;
    do {
        if ((fgets(buffer, bufferMaxLen, file) == nullptr) ||
            (sscanf_s(buffer, "commListenPort = %hu", &commListenPort) != 1)) {
            (void)printf("cluster information file: %s format %s error\n", path, "commListenPort");
            break;
        }

        if ((fgets(buffer, bufferMaxLen, file) == nullptr) || (sscanf_s(buffer, "clusterId = %u", &clusterId) != 1)) {
            (void)printf("cluster information file: %s format %s error\n", path, "clusterId");
            break;
        }

        if ((fgets(buffer, bufferMaxLen, file) == nullptr) || (sscanf_s(buffer, "termId = %u", &termId) != 1)) {
            (void)printf("cluster information file: %s format %s error\n", path, "termId");
            break;
        }

        if ((fgets(buffer, bufferMaxLen, file) == nullptr) || (sscanf_s(buffer, "nodeCount = %u", &nodeCount) != 1)) {
            (void)printf("cluster information file: %s format %s error\n", path, "nodeCount");
            break;
        }
    } while (false);
    return nodeCount;
}

uint32_t GetClusterNodeCount()
{
    return 1;
}

int main(int argc, char *argv[])
{
    NodeId nodeId = 1;
    DSTORE::StorageInstanceType storageInstanceType = DSTORE::StorageInstanceType::SINGLE;

    /* Step1: Get the Number of Cluster Nodes */
    uint32_t nodeNum = 1;

    /* If the database is not initialized, the initialization process is complete. */
    const int sleepTime = 1000;

    /* Step2: Get the tpcc config param */
    TPCC_BENCHMARK::TpccStorage tpccStorage;
    tpccStorage.Init(nodeId);
    TPCC_BENCHMARK::TpccConfig tpccCfg = tpccStorage.GetTpccConfig();

    /* Step3: Get the TPCC Running Status */
    CommonPersistentHandler persistenceHandler;
    TPCC_BENCHMARK::TpccRunStateMetaData *runStateObj = OpenRunningStatusFile(persistenceHandler, nodeId);

    /* Step3: Check whether the Initdb process is need execute. */
    if (runStateObj == nullptr || runStateObj->state < TPCC_BENCHMARK::TPCC_RUN_STATE_INIT_FINISHED) {
        /* Step3.1: Backup the TPCC Config */
        tpccStorage.BackTpccConfigFile();
        
        if (nodeId == 1) {
            /* Step3.2: Start Initdb process */
            TpccStorageInstance::Init();
            TpccStorageInstance::InitFinished();
            persistenceHandler.Close();
            
            /* Step3.3: Reopen the TpccRunState file and Mark Initdb process is complete */
            runStateObj = CreateTpccRunningFile(persistenceHandler, tpccCfg, nodeNum);
            runStateObj->state = TPCC_BENCHMARK::TPCC_RUN_STATE_INIT_FINISHED;
            persistenceHandler.Sync();
        }
    }
    
    /* Step5: Mark Initdb process is complete and check whether the TPCC configuration file is changed. */
    assert(runStateObj->state >= TPCC_BENCHMARK::TPCC_RUN_STATE_UNKNOW &&
                  runStateObj->state <= TPCC_BENCHMARK::TPCC_RUN_STATE_END);

    if (runStateObj->state == TPCC_BENCHMARK::TPCC_RUN_STATE_LOAD_DATA) {
        if (tpccStorage.CheckTpccConfigIsModify()) {
            /* If the change of the tpcc configuration file occurs during the last loadData process,
             * the process is terminated. Otherwise, only the TPCC configuration file needs to be backed up.
             */
            const std::string TPCC_CONFIG_PATH("config.json");
            const std::string TPCC_CONFIG_BACKUP_PATH("config_back.json");
            std::cout << "Waring: The " << TPCC_CONFIG_PATH
                      << " is Changed. If you want to redo the transaction of the last, please use the "
                      << TPCC_CONFIG_BACKUP_PATH << " file restore the " << TPCC_CONFIG_PATH
                      << " file. \nIf you want to start a new transaction, please remove the tpccdir." << std::endl;
            exit(1);
        } else if (nodeNum != runStateObj->nodeNum) {
            std::cout << "Waring: The " << CLUSTER_INFO_PATH
                      << " is Changed. If you want to redo the transaction of the last, please modify the "
                      << CLUSTER_INFO_PATH
                      << " file. \nIf you want to start a new transaction, please remove the tpccdir." << std::endl;
            exit(1);
        }
    }
    runStateObj->initState[nodeId - 1] = true;
    WaitAllNodeReady(nodeNum, runStateObj->initState);
    InitVfsClientHandles();
    /* Step6: Start StorageInstance on all nodes. */
    TpccStorageInstance::Start(runStateObj->allocedMaxRelOid, storageInstanceType, nodeId);
    runStateObj->startState[nodeId - 1] = true;
    persistenceHandler.Sync();
    WaitAllNodeReady(nodeNum, runStateObj->startState);

    /* CreateTables */
    if (runStateObj->state < TPCC_BENCHMARK::TPCC_RUN_STATE_CREATE_TABLE) {
        runStateObj->state = TPCC_BENCHMARK::TPCC_RUN_STATE_CREATE_TABLE;
        persistenceHandler.Sync();
    }
    if (nodeId == 1 && runStateObj->state <= TPCC_BENCHMARK::TPCC_RUN_STATE_CREATE_TABLE) {
        tpccStorage.CreateTables(&persistenceHandler);
        runStateObj->state = TPCC_BENCHMARK::TPCC_RUN_STATE_LOAD_DATA;
        persistenceHandler.Sync();
    }
    while (runStateObj->state < TPCC_BENCHMARK::TPCC_RUN_STATE_LOAD_DATA) {
        (void)usleep(sleepTime);
    }

    /* Recovery and check Tables*/
    tpccStorage.RecoveryTables(&persistenceHandler);
    tpccStorage.CheckTables(&persistenceHandler);
    runStateObj->recoveryState[nodeId - 1] = true;
    persistenceHandler.Sync();

    WaitAllNodeReady(nodeNum, runStateObj->recoveryState);

    /* LoadDatas */
    if (runStateObj->state == TPCC_BENCHMARK::TPCC_RUN_STATE_LOAD_DATA) {
        tpccStorage.LoadData(&persistenceHandler);

        WaitAllNodeReady(nodeNum, runStateObj->loadDatasState);

        runStateObj->state = TPCC_BENCHMARK::TPCC_RUN_STATE_CREATE_INDEX;
        persistenceHandler.Sync();
        if (tpccStorage.CheckTpccConfigIsModify()) {
            tpccStorage.BackTpccConfigFile();
        }
    }
    /* CreateIndexs */
    bool isCreateIndex = false;
    if (nodeId == 1 && runStateObj->state == TPCC_BENCHMARK::TPCC_RUN_STATE_CREATE_INDEX) {
        tpccStorage.CreateIndexs(&persistenceHandler);
        isCreateIndex = true;
        runStateObj->state = TPCC_BENCHMARK::TPCC_RUN_STATE_EXECUTE;
        persistenceHandler.Sync();
    }
    while (runStateObj->state < TPCC_BENCHMARK::TPCC_RUN_STATE_EXECUTE) {
        (void)usleep(sleepTime);
    }

    /* Recovery and check Index*/
    if (!isCreateIndex) {
        tpccStorage.RecoveryIndexs(&persistenceHandler);
        tpccStorage.CheckIndexs(&persistenceHandler);
    }
    runStateObj->recoveryIndexState[nodeId - 1] = true;
    persistenceHandler.Sync();

    WaitAllNodeReady(nodeNum, runStateObj->recoveryIndexState);
    
    /* Execute */ 
    tpccStorage.Execute(&persistenceHandler);
    WaitAllNodeReady(nodeNum, runStateObj->executeState);

    /* CheckConsistency */ 
    tpccStorage.End(&persistenceHandler);
    runStateObj->checkState[nodeId - 1] = true;

    /* Finished */ 
    WaitAllNodeReady(nodeNum, runStateObj->checkState);

    runStateObj->state = TPCC_BENCHMARK::TPCC_RUN_STATE_END;
    if (nodeNum == 1) {
        persistenceHandler.Close();
        return 0;
    } else {
        TpccStorageInstance::Stop(nodeNum, runStateObj->stopState);
        persistenceHandler.Sync();
        persistenceHandler.Close();
        return 0;
    }
}
