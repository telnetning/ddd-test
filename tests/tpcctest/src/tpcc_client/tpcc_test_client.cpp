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

#include "tpcc_client/tpcc_test_client.h"
#include "tpcc_client/tpcc_table.h"
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <iostream>
#include <fstream>
#include <random>
#include <numeric>
#include <algorithm>
#include <string>
#include <thread>
#include <vector>
#include "cjson/cJSON.h"
#include "table_handler.h"
#include "tuple/dstore_memheap_tuple.h"
#include "transaction/dstore_transaction_interface.h"
#include "framework/dstore_thread_interface.h"
#include "tuple/dstore_tuple_interface.h"
#include "common_persistence_handler.h"
#include "framework/dstore_session_interface.h"
#include "framework/dstore_thread.h"

namespace TPCC_BENCHMARK {

thread_local uint32_t gSeed;

thread_local std::map<std::string, DstoreTableHandler*> localTableHandlers;

thread_local uint32_t thread_local_id = 0;

const std::string TPCC_CONFIG_PATH("config.json");
const std::string TPCC_CONFIG_BACKUP_PATH("config_backup.json");

struct TransactionRunStat {
    std::atomic<uint64_t> nCommitted_;
    std::atomic<uint64_t> nAborted_;
    TransactionRunStat()
    {
        std::atomic_init(&nCommitted_,  (uint64_t)0);
        std::atomic_init(&nAborted_,  (uint64_t)0);
    }
};

struct TpccRunStat {
    TransactionRunStat runstat_[TPCC_TRANSACTION_NUM];
    std::atomic<uint64_t>  nTotalCommitted_;
    std::atomic<uint64_t>  nTotalAborted_;
    TpccRunStat()
    {
        std::atomic_init(&nTotalCommitted_,  (uint64_t)0);
        std::atomic_init(&nTotalAborted_,  (uint64_t)0);
    }
};

static void CreateThreadAndRegister()
{
    g_instance->CreateThreadAndRegister(g_defaultPdbId);
    (void)pthread_setname_np(pthread_self(), "tpcctool");
    volatile uint32_t *interruptHoldoffCount = new uint32_t(0);
    ThreadCore *core = thrd->GetCore();
    core->interruptHoldoffCount = interruptHoldoffCount;
    ThreadContextInterface::GetCurrentThreadContext()->InitTransactionRuntime(g_defaultPdbId, nullptr, nullptr);
    std::random_device sd;
    gSeed = sd();
}

static void UnregisterThread()
{
    ThreadCore *core = thrd->GetCore();
    if (core != nullptr && core->interruptHoldoffCount != nullptr) {
        delete core->interruptHoldoffCount;
        core->interruptHoldoffCount = nullptr;
    }
    g_instance->UnregisterThread();
}

TpccStorage::~TpccStorage()
{
    delete[] m_stats;
}

void TpccStorage::Init(NodeId nodeId)
{
    m_selfNodeId = nodeId;
    std::ifstream configFile(TPCC_CONFIG_PATH);
    if (!configFile.is_open()) {
        std::cout << "Could not open config file: " << TPCC_CONFIG_PATH << std::endl;
        exit(1);
    }

    std::string configMsg((std::istreambuf_iterator<char>(configFile)), std::istreambuf_iterator<char>());
    LoadTpccConfig(configMsg, TPCC_CONFIG_PATH.c_str(), m_tpccConfig);
    configFile.close();

    /* m_stats[m_tpccConfig.workThreadNum] means stat summary */
    m_stats = new TpccRunStat[m_tpccConfig.workThreadNum + 1];
}

void TpccStorage::BackTpccConfigFile()
{
    std::string cmd = std::string("cp -f '") + TPCC_CONFIG_PATH + "' '" + TPCC_CONFIG_BACKUP_PATH + "'";
    system(cmd.c_str());
}

void TpccStorage::LoadData(CommonPersistentHandler *persistentHandler)
{
    std::cout << "--------------------" << "Start Load Data" << "--------------------" << std::endl;
    auto start = std::chrono::system_clock::now();
    TpccRunStateMetaData* runStateObj = static_cast<TpccRunStateMetaData*>(persistentHandler->GetObject());
    StorageAssert(runStateObj != nullptr);
    StorageAssert(runStateObj->loadDatas != nullptr);
    std::vector<uint32_t> unloadDatas;
    ThreadRange threadRange = runStateObj->threadRange[m_selfNodeId - 1];
    for (uint32_t threadId = threadRange.startThreadId; threadId <= threadRange.endThreadId; ++threadId) {
        if (!runStateObj->loadDatas[threadId].isFinished) {
            unloadDatas.emplace_back(threadId);
        }
    }
    Size threadIdCount = unloadDatas.size();
    if (threadIdCount != 0) {
        int idx = 0;
        std::vector<std::thread> workThreads(threadIdCount);
        for (uint32_t threadId : unloadDatas) {
            workThreads[idx++] = std::thread([this, threadId, persistentHandler] {
                CreateThreadAndRegister();
                StorageSession *sc = CreateStorageSession(1ULL);
                ThreadContextInterface *thrdctx = ThreadContextInterface::GetCurrentThreadContext();
                thrdctx->AttachSessionToThread(sc);
                GenDataIntoTable(threadId, persistentHandler);
                thrdctx->DetachSessionFromThread();
                UnregisterThread();
                CleanUpSession(sc);
            });
        }
        for (auto &workThread : workThreads) {
            workThread.join();
        }
    }
    runStateObj->loadDatasState[m_selfNodeId - 1] = true;
    persistentHandler->Sync();

    auto end = std::chrono::system_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    std::cout << "Load Data Costs Time "
              << (double(duration.count()) * std::chrono::microseconds::period::num /
                  std::chrono::microseconds::period::den)
              << "s" << std::endl;
    std::cout << "--------------------" << "Finish Load Data" << "--------------------" << std::endl;
    std::cout << std::endl << std::endl;
}
void TpccStorage::End(CommonPersistentHandler *persistentHandler)
{
    CheckConsistency(persistentHandler);
}

bool TpccStorage::RecoveryTables(CommonPersistentHandler *persistentHandler)
{
    std::cout << "--------------------" << "Start Recovery Table" << "--------------------" << std::endl;
    assert(persistentHandler != nullptr);
    TpccRunStateMetaData *runStateObj = static_cast<TpccRunStateMetaData *>(persistentHandler->GetObject());
    assert(runStateObj != nullptr);

    for (uint32_t i = 0; i < runStateObj->heapCount; ++i) {
        const char *tableName = runStateObj->heapTables[i];
        DstoreTableHandler tableHandler(g_instance);
        int ret = tableHandler.RecoveryTable(tableName);
        if (ret == 1) {
            std::cout << "Recovery " << tableName << " Failed, because the table is not exist!" << std::endl;
        } else {
            std::cout << "Recovery " << tableName << " Success!" << std::endl;
        }
    }

    std::cout << "--------------------" << "Finish Recovery Table" << "--------------------" << std::endl;
    std::cout << std::endl << std::endl;
    return true;
}

bool TpccStorage::RecoveryIndexs(CommonPersistentHandler *persistentHandler)
{
    std::cout << "--------------------" << "Start Recovery Index" << "--------------------" << std::endl;
    assert(persistentHandler != nullptr);
    TpccRunStateMetaData *runStateObj = static_cast<TpccRunStateMetaData *>(persistentHandler->GetObject());
    assert(runStateObj != nullptr);

    for (uint32_t i = 0; i < runStateObj->indexCount; ++i) {
        const char *tableName = runStateObj->indexTables[i];
        int ret = 1;
        for (TableNameType i = TABLE_WAREHOUSE; i < TABLE_MAX_CNT; i = TableNameType(i + 1)) {
            if (strcmp(tableName, TpccTableDesc[i].tableName) == 0) {

                DstoreTableHandler tableHandler(g_instance);
                char *indexName = TableDataGenerator::GenerateIndexName(tableName, TpccTableDesc[i].indexDes->indexCol,
                                                                        TpccTableDesc[i].indexDes->indexAttrNum);

                ret = tableHandler.RecoveryTable(indexName);
                DestroyObject((void**)&indexName);

                break;
            }
        }
        if (ret == 1) {
            std::cout << "Recovery " << tableName << " Failed, because the table is not exist!" << std::endl;
        } else {
            std::cout << "Recovery " << tableName << " Success!" << std::endl;
        }
    }

    std::cout << "--------------------" << "Finish Recovery Index" << "--------------------" << std::endl;
    std::cout << std::endl << std::endl;
    return true;
}

bool TpccStorage::CreateTables(CommonPersistentHandler *persistentHandler)
{
    std::cout << "--------------------" << "Start Create Table" << "--------------------" << std::endl;
    assert(persistentHandler != nullptr);
    TpccRunStateMetaData *runStateObj = static_cast<TpccRunStateMetaData *>(persistentHandler->GetObject());
    assert(runStateObj != nullptr);
    for (TableNameType i = TABLE_WAREHOUSE; i < TABLE_MAX_CNT; i = TableNameType(i + 1)) {
        if (strcmp(runStateObj->heapTables[i], TpccTableDesc[i].tableName) == 0) {
            continue;
        }
        const char *tableName = TpccTableDesc[i].tableName;
        DstoreTableHandler tableHandler(g_instance);
        TableDataGenerator generator(tableName, TpccTableDesc[i].colDefs, TpccTableDesc[i].colNum);
        generator.GenerationTableInfo();
        TableInfo tableInfo = generator.GetTableInfo();
        int ret = tableHandler.CreateTable(tableInfo);
        if(ret == 1) {
            std::cout << "Create " << tableName << " Failed, because the table is exist!" << std::endl;
        } else {
            __attribute__((__unused__)) int rc =
                memcpy_s(runStateObj->heapTables[i], NAME_MAX_LEN, tableName, strlen(tableName) + 1);
            storage_securec_check(rc, "\0", "\0");
            runStateObj->allocedMaxRelOid = simulator->GetCurOid();
            ++runStateObj->heapCount;
            persistentHandler->Sync();
            std::cout << "Create " << tableName << " Success!" << std::endl;
        }
    }

    assert(runStateObj->heapCount == TABLE_MAX_CNT);
    std::cout << "--------------------" << "Finish Create Table" << "--------------------" << std::endl;
    std::cout << std::endl << std::endl;
    return true;
}

bool TpccStorage::CreateIndexs(CommonPersistentHandler *persistentHandler)
{
    std::cout << "--------------------" << "Start Create Index" << "--------------------" << std::endl;
    assert(persistentHandler != nullptr);
    TpccRunStateMetaData *runStateObj = static_cast<TpccRunStateMetaData *>(persistentHandler->GetObject());
    assert(runStateObj != nullptr);
    for (TableNameType i = TABLE_WAREHOUSE; i < TABLE_MAX_CNT; i = TableNameType(i + 1)) {
        if (TpccTableDesc[i].indexDes == nullptr) {
            continue;
        }
        if (strcmp(runStateObj->indexTables[i], TpccTableDesc[i].tableName) == 0 ) {
            continue;
        }
        DstoreTableHandler *tableHandler = simulator->GetTableHandler(TpccTableDesc[i].tableName, nullptr);
        TableDataGenerator generator(TpccTableDesc[i].tableName, TpccTableDesc[i].colDefs, TpccTableDesc[i].colNum);
        TableInfo tableInfo = generator.GetTableInfo();
        tableInfo.indexDesc = TpccTableDesc[i].indexDes;

        TableDataGenerator indexGenerator;
        indexGenerator.GenerationIndexTableInfo(tableInfo);
        TableInfo indexTableInfo = indexGenerator.GetTableInfo();

        TransactionInterface::StartTrxCommand();
        TransactionInterface::SetSnapShot();
        int ret = tableHandler->CreateIndex(indexTableInfo);
        delete tableHandler;
        if (ret == 0) {
            TransactionInterface::CommitTrxCommand();
            __attribute__((__unused__)) int rc =
                memcpy_s(runStateObj->indexTables[i], NAME_MAX_LEN, TpccTableDesc[i].tableName,
                         strlen(TpccTableDesc[i].tableName) + 1);
            storage_securec_check(rc, "\0", "\0");

            runStateObj->allocedMaxRelOid = simulator->GetCurOid();
            ++runStateObj->indexCount;
            persistentHandler->Sync();
            std::cout << "Create " << TpccTableDesc[i].tableName << " Index Success!" << std::endl;
        } else {
            TransactionInterface::AbortTrx();
            std::cout << "Create " << TpccTableDesc[i].tableName << " Index Failed!" << std::endl;
        }
    }
    assert(runStateObj->indexCount <= runStateObj->indexCount);
    std::cout << "--------------------" << "Finish Create Index" << "--------------------" << std::endl;
    std::cout << std::endl << std::endl;
    return true;
}

bool TpccStorage::CheckTables(CommonPersistentHandler* persistentHandler)
{
    std::cout << "--------------------" << "Start Check Table" << "--------------------" << std::endl;
    assert(persistentHandler != nullptr);
    TpccRunStateMetaData *runStateObj = static_cast<TpccRunStateMetaData *>(persistentHandler->GetObject());
    assert(runStateObj != nullptr);
    if (runStateObj->heapCount != 0) {
        for (TableNameType i = TABLE_WAREHOUSE; i < TABLE_MAX_CNT; i = TableNameType(i + 1)) {
            if (strcmp(runStateObj->heapTables[i], TpccTableDesc[i].tableName) != 0) {
                continue;
            }

            const char *tableName = TpccTableDesc[i].tableName;
            TableDataGenerator generator(tableName, TpccTableDesc[i].colDefs, TpccTableDesc[i].colNum);
            generator.GenerationTableInfo();
            TableInfo tableInfo = generator.GetTableInfo();

            DstoreTableHandler *tableHandler = simulator->GetTableHandler(tableName, nullptr);
            assert(tableHandler != nullptr);
            int ret = tableHandler->CheckTable(tableInfo);
            delete tableHandler;
            if (ret == 1) {
                std::cout << "Check " << tableName << " Failed, because the table is not exist!" << std::endl;
            } else {
                std::cout << "Check " << tableName << " Success!" << std::endl;
            }
        }
    }
    std::cout << "--------------------" << "Finish Check Table" << "--------------------" << std::endl;
    std::cout << std::endl << std::endl;
    return true;
}
bool TpccStorage::CheckIndexs(CommonPersistentHandler *persistentHandler)
{
    std::cout << "--------------------" << "Start Check Index" << "--------------------" << std::endl;
    assert(persistentHandler != nullptr);
    TpccRunStateMetaData *runStateObj = static_cast<TpccRunStateMetaData *>(persistentHandler->GetObject());
    assert(runStateObj != nullptr);
    if (runStateObj->indexCount != 0) {
        for (TableNameType i = TABLE_WAREHOUSE; i < TABLE_MAX_CNT; i = TableNameType(i + 1)) {
            if (TpccTableDesc[i].indexDes == nullptr) {
                continue;
            }
            if (strcmp(runStateObj->indexTables[i], TpccTableDesc[i].tableName) != 0) {
                continue;
            }

            const char *tableName = TpccTableDesc[i].tableName;
            TableDataGenerator generator(tableName, TpccTableDesc[i].colDefs, TpccTableDesc[i].colNum);
            generator.GenerationTableInfo();
            TableInfo tableInfo = generator.GetTableInfo();
            tableInfo.indexDesc = TpccTableDesc[i].indexDes;

            TableDataGenerator indexGenerator;
            indexGenerator.GenerationIndexTableInfo(tableInfo);
            TableInfo indexTableInfo = indexGenerator.GetTableInfo();

            char *indexName = TableDataGenerator::GenerateIndexName(tableName, TpccTableDesc[i].indexDes->indexCol,
                                                                    TpccTableDesc[i].indexDes->indexAttrNum);
            DstoreTableHandler *tableHandler = simulator->GetTableHandler(tableName, indexName);
            int ret = tableHandler->CheckIndex(indexTableInfo);
            DestroyObject((void**)&indexName);
            delete tableHandler;

            if (ret == 1) {
                std::cout << "Check " << tableName << " Failed, because the table is not exist!" << std::endl;
            } else {
                std::cout << "Check " << tableName << " Success!" << std::endl;
            }
        }
    }

    std::cout << "--------------------" << "Finish Check Index" << "--------------------" << std::endl;
    std::cout << std::endl << std::endl;
    return true;
}

void TpccStorage::LoadTpccConfig(const std::string& configMsg, const char* fileName, TpccConfig& tpccCfg)
{
    std::cout << "--------------------" << "TPCC Config in " << fileName << "--------------------" << std::endl;
    cJSON *configJson = cJSON_Parse(configMsg.c_str());
    tpccCfg.itemNumPerWareHouse = cJSON_GetObjectItem(configJson, "TpccItemNumPerWareHouse")->valueint;
    std::cout << "TpccItemNumPerWareHouse: " << tpccCfg.itemNumPerWareHouse << std::endl;

    tpccCfg.customerPerDistrict = cJSON_GetObjectItem(configJson, "TpccCustomerPerDistrict")->valueint;
    std::cout << "TpccCustomerPerDistrict: " << tpccCfg.customerPerDistrict << std::endl;

    tpccCfg.orderPerDistrict = cJSON_GetObjectItem(configJson, "TpccOrderPerDistrict")->valueint;
    std::cout << "TpccOrderPerDistrict: " << tpccCfg.orderPerDistrict << std::endl;

    tpccCfg.fixed = cJSON_GetObjectItem(configJson, "fixed")->valueint;
    std::cout << "WarehouseFixed: " << ((tpccCfg.fixed == 1) ? "true" : "false") << std::endl;

    tpccCfg.wareHouseNum = cJSON_GetObjectItem(configJson, "wareHouseNum")->valueint;
    std::cout << "wareHouseNum: " << tpccCfg.wareHouseNum << std::endl;

    tpccCfg.workThreadNum = cJSON_GetObjectItem(configJson, "workThreadNum")->valueint;
    std::cout << "workThreadNum: " << tpccCfg.workThreadNum << std::endl;

    tpccCfg.durationTime = cJSON_GetObjectItem(configJson, "durationTime")->valueint;
    std::cout << "durationTime: " << tpccCfg.durationTime << "s" << std::endl;
    std::cout << std::endl << std::endl;
}

bool TpccStorage::CheckTpccConfigIsModify()
{
    std::ifstream configFileBackup(TPCC_CONFIG_BACKUP_PATH);
    bool isExistBackupFile = configFileBackup.is_open();
    if (isExistBackupFile && configFileBackup.good()) {
        TpccConfig tpccCfgBackup;
        std::string configMsgBackup((std::istreambuf_iterator<char>(configFileBackup)),
                                    std::istreambuf_iterator<char>());
        LoadTpccConfig(configMsgBackup, TPCC_CONFIG_BACKUP_PATH.c_str(), tpccCfgBackup);

        return !m_tpccConfig.IsNotModify(tpccCfgBackup);
    }
    return false;
}

void TpccStorage::Execute(CommonPersistentHandler *persistentHandler)
{
    StorageAssert(persistentHandler != nullptr);
    TpccRunStateMetaData *runStateObj = static_cast<TpccRunStateMetaData *>(persistentHandler->GetObject());
    StorageAssert(runStateObj != nullptr);
    ThreadRange threadRange = runStateObj->threadRange[m_selfNodeId - 1];
    uint32_t threadNum = threadRange.endThreadId - threadRange.startThreadId + 1;
    std::vector<std::thread> workThreads(threadNum);
    int idx = 0;
    for (uint32_t threadId = threadRange.startThreadId; threadId <= threadRange.endThreadId; ++threadId) {
        workThreads[idx++] = std::thread([this, threadId] {
            CreateThreadAndRegister();
            thread_local_id = threadId;
            RunTransactions();
            UnregisterThread();
        });
    }

    for (auto & workThread : workThreads) {
        workThread.join();
    }
    runStateObj->executeState[m_selfNodeId - 1] = true;
    persistentHandler->Sync();

    PrintPerfState();
}

TpccTransactionType TpccStorage::GetTransactionType(double r)
{
    if (r <= 0.45) {
        /* 45% probability */
        return NewOrderType;
    } else if (r > 0.45 && r <= 0.88) {
        /* 43% probability */
        return PaymentType;
    } else if (r > 0.88 && r <= 0.92) {
        /* 4% probability */
        return OrderStatusType;
    } else if (r > 0.92 && r <= 0.96) {
        /* 4% probability */
        return DeliveryType;
    } else if (r > 0.96) {
        /* 4% probability */
        return StockLevelType;
    }
    return InvalidTrxType;
}

void TpccStorage::RunTransactions()
{
    std::default_random_engine e;
    e.seed(gSeed);
    std::uniform_real_distribution<double> u(0, 1);
    bool trxStatus = false;
    TpccTransactionType tpccTrxType;

    auto start = std::chrono::system_clock::now();
    while(true) {
        TransactionInterface::StartTrxCommand();
        TransactionInterface::SetSnapShot();
        tpccTrxType = GetTransactionType(u(e));
        switch (tpccTrxType) {
            case NewOrderType:
                trxStatus = NewOrderTransaction();
                break;
            case PaymentType:
                trxStatus = PaymentTransaction();
                break;
            case OrderStatusType:
                trxStatus = OrderStatusTransaction();
                break;
            case DeliveryType:
                trxStatus = DeliveryTransaction();
                break;
            case StockLevelType:
                trxStatus = StockLevelTransaction();
                break;
            default:
                assert(0);
        }
        if (unlikely(trxStatus == false)) {
            m_stats->runstat_[tpccTrxType].nAborted_.fetch_add(1);
            TransactionInterface::AbortTrx();
        } else {
            m_stats->runstat_[tpccTrxType].nCommitted_.fetch_add(1);
            TransactionInterface::CommitTrxCommand();
        }
        ThreadContextInterface::GetCurrentThreadContext()->ResetQueryMemory();

        auto end = std::chrono::system_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        if (((double(duration.count()) * std::chrono::microseconds::period::num /
             std::chrono::microseconds::period::den) > m_tpccConfig.durationTime)) {
            break;
        }
    }
    ClearLocalTableHandler();
}

void swap(int& v1,int& v2)
{
    int temp = v1;
    v1 = v2;
    v2= temp;
}

/* The New-Order business transaction consists of entering a complete order through a single database transaction. */
bool TpccStorage::NewOrderTransaction()
{
    int all_local = 1;
    int ol_i_id_arr[TPCC_MAX_NUM_ITEMS];
    int ol_supply_w_id_arr[TPCC_MAX_NUM_ITEMS];
    int ol_quantity_arr[TPCC_MAX_NUM_ITEMS];
    double total_amount = 0.0;
    int w_id;
    /* Step 0: Prepare argument for new order transaction */
    if (m_tpccConfig.fixed) {
        w_id = thread_local_id % m_tpccConfig.wareHouseNum + 1;
    } else {
        w_id = RandomNumber(1, m_tpccConfig.wareHouseNum);
    }
    int d_id = RandomNumber(1, TPCC_DISTICT_PER_WAREHOUSE);
    int c_id = NURand(1023, 1, m_tpccConfig.customerPerDistrict);
    /* The number of items in the order (ol_cnt) is random ly selected within [5 .. 15] (an average of 10) */
    int ol_cnt = RandomNumber(5, TPCC_MAX_NUM_ITEMS);
    /* A fixed 1% of the New-Order transactions are chosen at random to simulate user data entry errors and
     * exercise the performance of rolling back transactions. This must be implemented by generating a
     * random number rbk w ithin [1 .. 100].
     */
    int rbk = RandomNumber(1, 100);

    std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
    Timestamp o_entry_d = std::chrono::system_clock::to_time_t(now);

    for (int i = 0; i < ol_cnt; i++) {
        ol_i_id_arr[i] = NURand(8191, 1, m_tpccConfig.itemNumPerWareHouse);
        /* If this is the last item on the order and rbk = 1, then the item number is set to an unused value (value not found
         * in the database such that its use will produce a "not-found " condition within the application program)
         * This condition should result in rolling back the current database transaction
         */
        if ((i == ol_cnt - 1) && (rbk == 1)) {
            ol_i_id_arr[i] = m_tpccConfig.itemNumPerWareHouse + 1;
        }

        /* supplying warehouse number (OL_SUPPLY_W_ID) is selected as the home warehouse 99% of the time and
         * as a remote warehouse 1% of the time.
         */
        if (RandomNumber(1, 100) != 0)
            ol_supply_w_id_arr[i] = w_id;
        else {
            ol_supply_w_id_arr[i] = GetOtherWarehouseId(w_id);
            all_local = 0;
        }
        /* A quantity (OL_QUANTITY) is randomly selected within [1 .. 10] */
        ol_quantity_arr[i] = RandomNumber(1, 10);
    }

    /* When processing the order lines we must select the STOCK rows
     * FOR UPDATE. This can lead to possible deadlocks if two transactions
     * try to lock the same two stock rows in opposite order. To avoid that
     * we process the order lines in the order of the order of ol_supply_w_id, ol_i_id.
     */
    for (int i = 0; i < ol_cnt - 1; i++) {
        for (int j = i + 1; j < ol_cnt; j++) {
            if ((ol_supply_w_id_arr[i] > ol_supply_w_id_arr[j]) || ((ol_supply_w_id_arr[i] == ol_supply_w_id_arr[j]) && (ol_i_id_arr[i] > ol_i_id_arr[j]) )) {
                swap(ol_supply_w_id_arr[i], ol_supply_w_id_arr[j]);
                swap(ol_i_id_arr[i], ol_i_id_arr[j]);
                swap(ol_quantity_arr[i], ol_quantity_arr[j]);
            } else if ((ol_supply_w_id_arr[i] == ol_supply_w_id_arr[j]) && (ol_i_id_arr[i] == ol_i_id_arr[j])){
                ol_i_id_arr[j] = ol_i_id_arr[j] + 1;
            }
        }
    }

    /* Step 1: Retrieve the required data from DISTRICT */
    Datum districtValues[TpccTableDesc[TABLE_DISTRICT].colNum];
    bool districtIsNulls[TpccTableDesc[TABLE_DISTRICT].colNum];
    if (unlikely(SelectDistrictForUpdate(w_id, d_id, districtValues, districtIsNulls) == false)) {
        printf("District for W_ID=%d D_ID=%d not found\n", w_id, d_id);
        assert(0);
        return false;
    }

    double d_tax = districtValues[D_TAX];
    int o_id = districtValues[D_NEXT_O_ID];

    /* Step 2: Retrieve the required data from warehouse and customer */
    Datum warehouseValues[TpccTableDesc[TABLE_WAREHOUSE].colNum];
    bool warehouseIsNulls[TpccTableDesc[TABLE_WAREHOUSE].colNum];
    Datum customerValues[TpccTableDesc[TABLE_CUSTOMER].colNum];
    bool customerIsNulls[TpccTableDesc[TABLE_CUSTOMER].colNum];
    if (unlikely((SelectWarehouseTable(w_id, warehouseValues, warehouseIsNulls) == false) ||
                 (SelectCustomer(w_id, c_id, d_id, customerValues, customerIsNulls) == false))) {
        printf("Warehouse or Customer for W_ID=%d D_ID=%d C_ID=%d not found\n", w_id, d_id, c_id);
        assert(0);
    }
    double w_tax = districtValues[W_TAX];
    double c_discount = customerValues[C_DISCOUNT];

    /* Step 3: Update district set d_next_o_id += 1 */
    if (unlikely(UpdateDistrictTable(w_id, d_id) == false)) {
        assert(0);
    }

    /* Step 4: Insert into orders */
    Datum orderValues[GetTableColMax(TABLE_ORDER)];
    bool orderIsNulls[GetTableColMax(TABLE_ORDER)] = {false};
    orderIsNulls[O_CARRIER_ID] = true;
    orderValues[O_ID] = Int32GetDatum(o_id);
    orderValues[O_D_ID] = Int32GetDatum(d_id);
    orderValues[O_W_ID] = Int32GetDatum(w_id);
    orderValues[O_C_ID] = Int32GetDatum(c_id);
    orderValues[O_CARRIER_ID] = Int32GetDatum(0);
    orderValues[O_OL_CNT] = Int32GetDatum(ol_cnt);
    orderValues[O_ALL_LOCAL] = Int32GetDatum(all_local);
    orderValues[O_ENTRY_D] = TimestampGetDatum(o_entry_d);
    InsertTable(TABLE_ORDER, orderValues, orderIsNulls);

    /* Step 5: Insert into neworder */
    Datum newOrderValues[GetTableColMax(TABLE_NEWORDER)];
    bool newOrderIsNulls[GetTableColMax(TABLE_NEWORDER)] = {false};
    newOrderValues[NO_O_ID] = Int32GetDatum(o_id);
    newOrderValues[NO_D_ID] = Int32GetDatum(d_id);
    newOrderValues[NO_W_ID] = Int32GetDatum(w_id);
    InsertTable(TABLE_NEWORDER, newOrderValues, newOrderIsNulls);

    /* Step 6: Insert into orderline */
    for (int i = 0; i < ol_cnt; i++) {
        int ol_number = i + 1;
        int ol_supply_w_id = ol_supply_w_id_arr[i];
        int ol_i_id = ol_i_id_arr[i];
        int ol_quantity = ol_quantity_arr[i];

        Datum itemValues[GetTableColMax(TABLE_ITEM)];
        bool itemIsNulls[GetTableColMax(TABLE_ITEM)];
        if (unlikely(SelectItem(ol_i_id, itemValues, itemIsNulls) == false)) {
            if ((ol_i_id == int(m_tpccConfig.itemNumPerWareHouse + 1)) && (rbk == 1)) {
                return false;
            }
            printf("ITEM %d not found\n", ol_i_id);
            assert(0);
            return false;
        }

        Datum stockValues[GetTableColMax(TABLE_STOCK)];
        bool stockIsNulls[GetTableColMax(TABLE_STOCK)];
        if (unlikely(SelectStockForUpdate(w_id, ol_i_id, stockValues, stockIsNulls) == false)) {
            printf("STOCK with S_W_ID=%d S_I_ID=%d not found\n", w_id, ol_i_id);
            assert(0);
            return false;
        }

        int s_quantity = stockValues[S_QUANTITY];
        double ol_amount = itemValues[I_PRICE] * ol_quantity;
        total_amount += ol_amount * (1 - c_discount) * (1 + w_tax + d_tax);

        /*Update the STOCK row*/
        int remoteCnt, s_quantity_tmp;
        if (ol_supply_w_id == w_id) {
            remoteCnt = 0;
        } else {
            remoteCnt = 1;
        }
        if (s_quantity >= (ol_quantity + 10)) {
            s_quantity_tmp = s_quantity - ol_quantity;
        } else {
            s_quantity_tmp = s_quantity + 91;
        }
        if (unlikely(UpdateStockTable(ol_supply_w_id, ol_i_id,  remoteCnt, s_quantity_tmp, ol_quantity) == false)) {
            assert(0);
            return false;
        }

        Datum orderLineValues[GetTableColMax(TABLE_ORDERLINE)];
        bool orderLineIsNulls[GetTableColMax(TABLE_ORDERLINE)] = {false};
        orderLineValues[OL_W_ID] = Int32GetDatum(w_id);
        orderLineValues[OL_D_ID] = Int32GetDatum(d_id);
        orderLineValues[OL_O_ID] = Int32GetDatum(o_id);
        orderLineValues[OL_NUMBER] =Int32GetDatum(ol_number);
        orderLineValues[OL_I_ID] = Int32GetDatum(ol_i_id);
        orderLineValues[OL_SUPPLY_W_ID] = Int32GetDatum(ol_supply_w_id);
        orderLineValues[OL_QUANTITY] = Int32GetDatum(ol_quantity);
        orderLineValues[OL_AMOUNT] = Float64GetDatum(ol_amount);
        orderLineIsNulls[OL_DELIVERY_D] = true;
        /* TODO: fill OL_DIST_INFO */
        InsertTable(TABLE_ORDERLINE, orderLineValues, orderLineIsNulls);
    }
    return true;
}

bool TpccStorage::PaymentTransaction()
{
    int w_id;

    if (m_tpccConfig.fixed) {
        w_id = thread_local_id % m_tpccConfig.wareHouseNum + 1;
    } else {
        w_id = RandomNumber(1, m_tpccConfig.wareHouseNum);
    }
    int d_id = RandomNumber(1, TPCC_DISTICT_PER_WAREHOUSE);
    int c_id = NURand(1023, 1, m_tpccConfig.customerPerDistrict);

    double h_amount = RandomNumber(1, 5000);
    /* TODO: 60% select by last name, 40% select by customer id */

    int c_w_id;
    int c_d_id;
    if (RandomNumber(1, 100) <= 85) {
        c_w_id = w_id;
        c_d_id = d_id;
    } else {
        c_w_id = GetOtherWarehouseId(w_id);
        c_d_id = RandomNumber(1, TPCC_DISTICT_PER_WAREHOUSE);
    }

    /* Step1: update distict w_ytd += h_amount */
    if (unlikely(UpdateDistrictTableForAmount(w_id, d_id, h_amount) == false)) {
        assert(0);
        return false;
    }

    /* Step2: select distict */
    Datum districtValues[TpccTableDesc[TABLE_DISTRICT].colNum];
    bool districtIsNulls[TpccTableDesc[TABLE_DISTRICT].colNum];
    if (unlikely(SelectDistrict(w_id, d_id, districtValues, districtIsNulls) == false)) {
        printf("District for W_ID=%d D_ID=%d not found\n", w_id, d_id);
        assert(0);
        return false;
    }

    /* Update the WAREHOUSE */
    UpdateWarehouseTableForAmount(w_id, h_amount);

    /* Select the WAREHOUSE. */
    Datum warehouseValues[TpccTableDesc[TABLE_WAREHOUSE].colNum];
    bool warehouseIsNulls[TpccTableDesc[TABLE_WAREHOUSE].colNum];
    if (unlikely((SelectWarehouseTable(w_id, warehouseValues, warehouseIsNulls) == false))) {
        printf("Warehouse for W_ID=%d not found\n", w_id);
        assert(0);
        return false;
    }
    /* TODO: Get c_id by c_name */

    /* Select the CUSTOMER */
    Datum customerValues[GetTableColMax(TABLE_CUSTOMER)];
    bool customerIsNulls[GetTableColMax(TABLE_CUSTOMER)];
    if (unlikely(SelectCustomer(c_w_id, c_id, c_d_id, customerValues, customerIsNulls) == false)) {
        printf("Customer for C_W_ID=%d C_D_ID=%d C_ID=%d not found\n", w_id, c_d_id, c_id);
        assert(0);
        return false;
    }

    /* Update the CUSTOMER */
    /* TODO: if (payment.c_credit.equals("GC"))*/
    UpdateCustomerTable(c_w_id, c_d_id, c_id, h_amount);

    /* Insert the HISORY row */
    std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
    Timestamp h_date = std::chrono::system_clock::to_time_t(now);
    Datum historyValues[GetTableColMax(TABLE_HISTORY)];
    bool historyIsNulls[GetTableColMax(TABLE_HISTORY)];
    historyValues[H_C_ID] = Int32GetDatum(c_id);
    historyValues[H_C_D_ID] = Int32GetDatum(c_d_id);
    historyValues[H_C_W_ID] = Int32GetDatum(c_w_id);
    historyValues[H_D_ID] = Int32GetDatum(d_id);
    historyValues[H_W_ID] = Int32GetDatum(w_id);
    historyValues[H_AMOUNT] = Float64GetDatum(h_amount);
    historyValues[H_DATE] = TimestampGetDatum(h_date);
    /* TODO:H_DATA */
    historyValues[H_DATA] = PointerGetDatum("mon");
    InsertTable(TABLE_HISTORY, historyValues, historyIsNulls);
    return true;
}

bool TpccStorage::OrderStatusTransaction()
{
    UNUSE_PARAM bool byname;
    char c_last[17];
    memset(c_last, 0, sizeof(c_last));
    int w_id;

    if (m_tpccConfig.fixed) {
        w_id = thread_local_id % m_tpccConfig.wareHouseNum + 1;
    } else {
        w_id = RandomNumber(1, m_tpccConfig.wareHouseNum);
    }
    int d_id = RandomNumber(1, TPCC_DISTICT_PER_WAREHOUSE);
    int c_id = NURand(1023, 1, m_tpccConfig.customerPerDistrict);
    GenerateLastname(NURand(255, 0, 999), c_last);
    /* 60% select by last name, 40% select by customer id */
    byname = (RandomNumber(1, 100) <= 60);

    /* Step 1: Select customer_id, district_id, warehouse_id from customer */
    /* TODO: support select customer by name */
    Datum customerValues[GetTableColMax(TABLE_CUSTOMER)];
    bool customerIsNulls[GetTableColMax(TABLE_CUSTOMER)];
    if (unlikely(SelectCustomer(w_id, c_id, d_id, customerValues, customerIsNulls) == false)) {
        printf("Customer for C_W_ID=%d C_D_ID=%d C_ID=%d not found\n", w_id, d_id, c_id);
        assert(0);
        return false;
    }

    /* Step 2: Select (Max(order_id, customer_id) from order */
    Datum orderValues[GetTableColMax(TABLE_ORDER)];
    bool orderIsNulls[GetTableColMax(TABLE_ORDER)];
    if (unlikely(SelectLastOrderTable(w_id, d_id, orderValues, orderIsNulls) == false)) {
        printf("Last Order for W_ID=%d D_ID=%d C_ID=%d not found\n", w_id, d_id, c_id);
        assert(0);
        return false;
    }

    /* Step 3: Select orderline */
    uint32_t cnt;
    GetDistrictOrderlineCount(w_id, d_id, orderValues[O_ID], cnt);
    assert(cnt > 0 && cnt <= TPCC_MAX_NUM_ITEMS);
    return true;
}

bool TpccStorage::DeliveryTransaction()
{
    int w_id;

    if (m_tpccConfig.fixed) {
        w_id = thread_local_id % m_tpccConfig.wareHouseNum + 1;
    } else {
        w_id = RandomNumber(1, m_tpccConfig.wareHouseNum);
    }
    int o_carrier_id = RandomNumber(1, 10);
    int oldestNewOrderId = -1;
    for (int d_id = 1; d_id <= TPCC_DISTICT_PER_WAREHOUSE; d_id++) {
        oldestNewOrderId = -1;

        /* Step 1: Try to find the oldest undelivered order for this
         * DISTRICT. There may not be one, which is a case
         * that needs to be reportd.
         */
        Datum newOrderValues[GetTableColMax(TABLE_NEWORDER)];
        bool newOrderIsNulls[GetTableColMax(TABLE_NEWORDER)];
        if (SelectOldestNewOrder(w_id, d_id, newOrderValues, newOrderIsNulls) == false) {
            /* No new order to deliver */
            continue;
        }
        oldestNewOrderId = Int32GetDatum(newOrderValues[NO_O_ID]);
        /* Step 2: Delete the oldest new order */
        if (DeleteOldestNewOrder(w_id, d_id, oldestNewOrderId) == false) {
            return false;
        }
        /* Step 3: Update the corresponding order */
        if (UpdateOrderTable(w_id, d_id, oldestNewOrderId, o_carrier_id) == false) {
            return false;
        }

        /* Get the o_c_id from the ORDER. */
        Datum orderValues[GetTableColMax(TABLE_ORDER)];
        bool orderIsNulls[GetTableColMax(TABLE_ORDER)];
        if (SelectOrderTable(w_id, d_id, oldestNewOrderId, orderValues, orderIsNulls) == false) {
            assert(0);
            return false;
        }
        int o_c_id = orderValues[O_C_ID];
        int o_ol_cnt = orderValues[O_OL_CNT];
        /* Step 4: Update orderline */
        std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
        Timestamp ol_delivery_d = std::chrono::system_clock::to_time_t(now);
        if (UpdateOrderlineTable(w_id, d_id, oldestNewOrderId, o_ol_cnt, ol_delivery_d) == false) {
            assert(0);
            return false;
        }

        /* Step 5: Select the sum(ol_amount) from ORDER_LINE. */
        double amountTotal;
        SelectOrderlineTableForSum(w_id, d_id, oldestNewOrderId, amountTotal);

        /* Step 6: Update customer */
        if (UpdateCustomerTableForDelivery(w_id, d_id, o_c_id, amountTotal) == false) {
            assert(0);
            return false;
        }
    }
    return true;
}


bool TpccStorage::StockLevelTransaction()
{
    int w_id;

    if (m_tpccConfig.fixed) {
        w_id = thread_local_id % m_tpccConfig.wareHouseNum + 1;
    } else {
        w_id = RandomNumber(1, m_tpccConfig.wareHouseNum);
    }
    int d_id = RandomNumber(1, TPCC_DISTICT_PER_WAREHOUSE);
    int threshold = RandomNumber(10, 20);

    /* Step 1: get d_next_o_id from district table */
    Datum districtValues[TpccTableDesc[TABLE_DISTRICT].colNum];
    bool districtIsNulls[TpccTableDesc[TABLE_DISTRICT].colNum];
    if (unlikely(SelectDistrict(w_id, d_id, districtValues, districtIsNulls) == false)) {
        printf("District for W_ID=%d D_ID=%d not found\n", w_id, d_id);
        assert(0);
        return false;
    }
    int d_next_o_id = districtValues[D_NEXT_O_ID];

    /* Step 2: select orderline join stock */
    /* [d_next_o_id - 20, d_next_o_id - 1] */
    std::unordered_set<int> olItemIdSet;
    SelectOrderlineTableInRange(w_id, d_id, d_next_o_id - 20, d_next_o_id - 1, olItemIdSet);

    /* Step 3: judge if item under stock level */
    int low_stock = 0;
    for (auto itemId : olItemIdSet) {
        int s_quantity = -1;
        SelectStockTable(w_id, itemId, s_quantity);
        if (s_quantity < threshold) {
            low_stock++;
        }
    }
    assert(low_stock >= 0);
    return true;
}

DstoreTableHandler *TpccStorage::GetTableHandler(TableDesc *tableDesc, bool haveIndex)
{
    char *indexName = nullptr;
    if (haveIndex) {
        indexName = TableDataGenerator::GenerateIndexName(tableDesc->tableName, tableDesc->indexDes->indexCol,
                                                                tableDesc->indexDes->indexAttrNum);
    }

    DstoreTableHandler *tableHandler = nullptr;
    if (localTableHandlers.count(tableDesc->tableName) != 0) {
        tableHandler = localTableHandlers[tableDesc->tableName];
    } else {
        tableHandler = simulator->GetTableHandler(tableDesc->tableName, indexName);
        localTableHandlers[tableDesc->tableName] = tableHandler;
    }
    DestroyObject((void**)&indexName);
    return tableHandler;
}

void TpccStorage::ClearLocalTableHandler()
{

    for (auto itor = localTableHandlers.begin(); itor != localTableHandlers.end();) {
        if (itor->second != nullptr) {
            delete itor->second;
            itor->second = nullptr;
        }
        itor = localTableHandlers.erase(itor);
    }
}
bool TpccStorage::SelectOldestNewOrder(int w_id, int d_id, Datum* values, bool* isNulls)
{
    TableNameType tableType = TABLE_NEWORDER;
    TableDesc *tableDesc = &TpccTableDesc[tableType];
    auto tableHandler =  GetTableHandler(tableDesc);

    Datum indexValues[] = {Int32GetDatum(w_id), Int32GetDatum(d_id)};
    HeapTuple *tuple = nullptr;
    int ret = tableHandler->GetMin(tableDesc->indexDes->indexCol, indexValues, &tuple, 2, sizeof(indexValues) / sizeof(Datum));
    if (ret == 0) {
        assert(tuple != nullptr);
        TupleInterface::DeformHeapTuple(tuple, tableHandler->m_heapRel->attr, values, isNulls);
        DestroyObject((void**)&tuple);
    }
    return ret == 0;
}

bool TpccStorage::DeleteOldestNewOrder(int w_id, int d_id, int o_id)
{
    TableNameType tableType = TABLE_NEWORDER;
    TableDesc *tableDesc = &TpccTableDesc[tableType];
    auto tableHandler =  GetTableHandler(tableDesc);
    Datum indexValues[] = {Int32GetDatum(w_id), Int32GetDatum(d_id), Int32GetDatum(o_id)};
    int ret = tableHandler->Delete(tableDesc->indexDes->indexCol, indexValues, sizeof(indexValues) / sizeof(Datum));

    return ret == 0;
}

bool TpccStorage::SelectNewestOOrder(int w_id, int d_id, Datum* values, bool* isNulls)
{
    TableNameType tableType = TABLE_ORDER;
    TableDesc *tableDesc = &TpccTableDesc[tableType];
    auto *heapRel = simulator->GetHeapRelationEntry(tableDesc->tableName);
    assert(heapRel != nullptr);

    char *indexName = TableDataGenerator::GenerateIndexName(tableDesc->tableName, tableDesc->indexDes->indexCol,
                                                            tableDesc->indexDes->indexAttrNum);
    auto *indexRel = simulator->GetIndexRelationEntry(indexName);
    DestroyObject((void**)&indexName);
    assert(indexRel != nullptr);

    DstoreTableHandler tableHandler(g_instance);
    tableHandler.Init(heapRel, indexRel);
    Datum indexValues[] = {Int32GetDatum(w_id), Int32GetDatum(d_id)};
    HeapTuple *tuple = nullptr;
    tableHandler.GetMax(tableDesc->indexDes->indexCol, indexValues, &tuple, 2, sizeof(indexValues) / sizeof(Datum));
    assert(tuple != nullptr);
    TupleInterface::DeformHeapTuple(tuple, heapRel->attr, values, isNulls);
    DestroyObject((void**)&tuple);
    return true;
}

bool TpccStorage::SelectNewestNewOrder(int w_id, int d_id, Datum* values, bool* isNulls)
{
    TableNameType tableType = TABLE_NEWORDER;
    TableDesc *tableDesc = &TpccTableDesc[tableType];
    auto *heapRel = simulator->GetHeapRelationEntry(tableDesc->tableName);
    assert(heapRel != nullptr);

    char *indexName = TableDataGenerator::GenerateIndexName(tableDesc->tableName, tableDesc->indexDes->indexCol,
                                                            tableDesc->indexDes->indexAttrNum);
    auto *indexRel = simulator->GetIndexRelationEntry(indexName);
    DestroyObject((void**)&indexName);
    assert(indexRel != nullptr);

    DstoreTableHandler tableHandler(g_instance);
    tableHandler.Init(heapRel, indexRel);
    Datum indexValues[] = {Int32GetDatum(w_id), Int32GetDatum(d_id)};
    HeapTuple *tuple = nullptr;
    tableHandler.GetMax(tableDesc->indexDes->indexCol, indexValues, &tuple, 2, sizeof(indexValues) / sizeof(Datum));
    assert(tuple != nullptr);
    TupleInterface::DeformHeapTuple(tuple, heapRel->attr, values, isNulls);
    DestroyObject((void**)&tuple);
    return true;
}

bool TpccStorage::GetDistrictOOrderCount(int w_id, int d_id, uint32_t& cnt)
{
    TableNameType tableType = TABLE_ORDER;
    TableDesc* tableDesc = &TpccTableDesc[tableType];
    auto tableHandler =  GetTableHandler(tableDesc);
    Datum indexValues[] = {Int32GetDatum(w_id), Int32GetDatum(d_id)};
    tableHandler->GetCount(cnt, tableDesc->indexDes->indexCol, indexValues, sizeof(indexValues) / sizeof(Datum));
    return true;
}

bool TpccStorage::GetDistrictNewOrderCount(int w_id, int d_id, uint32_t& cnt)
{
    TableNameType tableType = TABLE_NEWORDER;
    TableDesc* tableDesc = &TpccTableDesc[tableType];
    auto tableHandler =  GetTableHandler(tableDesc);
    Datum indexValues[] = {Int32GetDatum(w_id), Int32GetDatum(d_id)};
    tableHandler->GetCount(cnt, tableDesc->indexDes->indexCol, indexValues, sizeof(indexValues) / sizeof(Datum));
    return true;
}

bool TpccStorage::GetDistrictOrderLineCount(int w_id, int d_id, uint32_t& cnt)
{
    TableNameType tableType = TABLE_ORDERLINE;
    TableDesc* tableDesc = &TpccTableDesc[tableType];
    auto tableHandler =  GetTableHandler(tableDesc);

    Datum indexValues[] = {Int32GetDatum(w_id), Int32GetDatum(d_id)};
    tableHandler->GetCount(cnt, tableDesc->indexDes->indexCol, indexValues, sizeof(indexValues) / sizeof(Datum));
    return true;
}

bool TpccStorage::GetOrderOrderLineCount(int w_id, int d_id, int o_id, uint32_t& cnt)
{
    TableNameType tableType = TABLE_ORDERLINE;
    TableDesc* tableDesc = &TpccTableDesc[tableType];
    auto tableHandler =  GetTableHandler(tableDesc);
    Datum indexValues[] = {Int32GetDatum(w_id), Int32GetDatum(d_id), Int32GetDatum(o_id)};
    tableHandler->GetCount(cnt, tableDesc->indexDes->indexCol, indexValues, sizeof(indexValues) / sizeof(Datum));
    return true;
}

bool TpccStorage::UpdateOrderTable(int w_id, int d_id, int oldestNewOrderId, int o_carrier_id)
{
    TableNameType tableType = TABLE_ORDER;
    TableDesc* tableDesc = &TpccTableDesc[tableType];
    auto tableHandler =  GetTableHandler(tableDesc);
    Datum indexValues[] = {Int32GetDatum(w_id), Int32GetDatum(d_id), Int32GetDatum(oldestNewOrderId)};
    HeapTuple* tuple = nullptr;
    int ret =
        tableHandler->Scan(tableDesc->indexDes->indexCol, indexValues, &tuple, sizeof(indexValues) / sizeof(Datum));
    if (ret == 0) {
        Datum values[tableDesc->colNum];
        bool isNulls[tableDesc->colNum];
        TupleInterface::DeformHeapTuple(tuple, tableHandler->m_heapRel->attr, values, isNulls);
        values[O_CARRIER_ID] = Int32GetDatum(o_carrier_id);
        isNulls[O_CARRIER_ID] = (o_carrier_id == 0);
        tableHandler->Update(tuple->GetCtid(), tableDesc->indexDes->indexCol, values, isNulls);
        DestroyObject((void**)&tuple);
    }
    return true;
}

bool TpccStorage::UpdateOrderlineTable(int w_id, int d_id, int o_id, int o_ol_cnt, Timestamp ol_delivery_d)
{
    TableNameType tableType = TABLE_ORDERLINE;
    TableDesc* tableDesc = &TpccTableDesc[tableType];
    auto tableHandler =  GetTableHandler(tableDesc);

    for(int ol_number = 1; ol_number <= o_ol_cnt; ol_number++){
        Datum indexValues[] = {Int32GetDatum(w_id), Int32GetDatum(d_id), Int32GetDatum(o_id), Int32GetDatum(ol_number)};
        HeapTuple* tuple = nullptr;
        tableHandler->Scan(tableDesc->indexDes->indexCol, indexValues, &tuple, sizeof(indexValues) / sizeof(Datum));
        assert(tuple != nullptr);
        Datum values[tableDesc->colNum];
        bool isNulls[tableDesc->colNum];
        TupleInterface::DeformHeapTuple(tuple, tableHandler->m_heapRel->attr, values, isNulls);
        values[OL_DELIVERY_D] = TimestampGetDatum(ol_delivery_d);
        isNulls[OL_DELIVERY_D] = false;

        tableHandler->Update(tuple->GetCtid(), tableDesc->indexDes->indexCol, values, isNulls);
        DestroyObject((void**)&tuple);
    }
    return true;
}

bool TpccStorage::UpdateCustomerTableForDelivery(int w_id, int d_id, int c_id, double amount)
{
    TableNameType tableType = TABLE_CUSTOMER;
    TableDesc* tableDesc = &TpccTableDesc[tableType];
    auto tableHandler =  GetTableHandler(tableDesc);

    Datum indexValues[] = {Int32GetDatum(w_id), Int32GetDatum(d_id), Int32GetDatum(c_id)};
    HeapTuple* tuple = nullptr;
    tableHandler->Scan(tableDesc->indexDes->indexCol, indexValues, &tuple, sizeof(indexValues) / sizeof(Datum));
    assert(tuple);
    Datum values[tableDesc->colNum];
    bool isNulls[tableDesc->colNum];
    TupleInterface::DeformHeapTuple(tuple, tableHandler->m_heapRel->attr, values, isNulls);
    values[C_BALANCE] = Float64GetDatum(DatumGetFloat64(values[C_BALANCE]) + amount);
    values[C_DELIVERY_CNT] = Int32GetDatum(DatumGetInt32(values[C_DELIVERY_CNT]) + 1);

    tableHandler->Update(tuple->GetCtid(), tableDesc->indexDes->indexCol, values, isNulls);
    DestroyObject((void**)&tuple);
    return true;
}

bool TpccStorage::UpdateCustomerTable(int c_w_id, int c_d_id, int c_id, double h_amount)
{
    TableNameType tableType = TABLE_CUSTOMER;
    TableDesc* tableDesc = &TpccTableDesc[tableType];
    auto tableHandler =  GetTableHandler(tableDesc);
    Datum indexValues[] = {Int32GetDatum(c_w_id), Int32GetDatum(c_d_id), Int32GetDatum(c_id)};
    HeapTuple* tuple = nullptr;
    tableHandler->Scan(tableDesc->indexDes->indexCol, indexValues, &tuple, sizeof(indexValues) / sizeof(Datum));
    assert(tuple != nullptr);
    Datum values[tableDesc->colNum];
    bool isNulls[tableDesc->colNum];
    TupleInterface::DeformHeapTuple(tuple, tableHandler->m_heapRel->attr, values, isNulls);
    values[C_BALANCE] = Float64GetDatum(DatumGetFloat64(values[C_BALANCE]) - h_amount);
    values[C_YTD_PAYMENT] = Float64GetDatum(DatumGetFloat64(values[C_YTD_PAYMENT]) + h_amount);
    values[C_PAYMENT_CNT] = Int32GetDatum(DatumGetInt32(values[C_PAYMENT_CNT]) + 1);

    tableHandler->Update(tuple->GetCtid(), tableDesc->indexDes->indexCol, values, isNulls);
    DestroyObject((void**)&tuple);

    return true;
}

bool TpccStorage::UpdateDistrictTable(int w_id, int d_id)
{
    TableNameType tableType = TABLE_DISTRICT;
    TableDesc* tableDesc = &TpccTableDesc[tableType];
    auto tableHandler =  GetTableHandler(tableDesc);
    Datum indexValues[] = {Int32GetDatum(w_id), Int32GetDatum(d_id)};
    HeapTuple* tuple = nullptr;
    int ret = tableHandler->Scan(tableDesc->indexDes->indexCol, indexValues, &tuple, sizeof(indexValues) / sizeof(Datum));
    if (ret == 0) {
        assert(tuple != nullptr);
        Datum values[tableDesc->colNum];
        bool isNulls[tableDesc->colNum];
        TupleInterface::DeformHeapTuple(tuple, tableHandler->m_heapRel->attr, values, isNulls);
        values[D_NEXT_O_ID] = Int32GetDatum(DatumGetInt32(values[D_NEXT_O_ID]) + 1);
        tableHandler->Update(tuple->GetCtid(), tableDesc->indexDes->indexCol, values, isNulls);

        DestroyObject((void**)&tuple);
    }

    return ret == 0;
}

bool TpccStorage::UpdateDistrictTableForAmount(int w_id, int d_id, double h_amount)
{
    TableNameType tableType = TABLE_DISTRICT;
    TableDesc* tableDesc = &TpccTableDesc[tableType];
    auto tableHandler =  GetTableHandler(tableDesc);
    Datum indexValues[] = {Int32GetDatum(w_id), Int32GetDatum(d_id)};
    HeapTuple* tuple = nullptr;
    tableHandler->Scan(tableDesc->indexDes->indexCol, indexValues, &tuple, sizeof(indexValues) / sizeof(Datum));
    if (tuple == nullptr) {
        return false;
    }

    Datum values[tableDesc->colNum];
    bool isNulls[tableDesc->colNum];
    TupleInterface::DeformHeapTuple(tuple, tableHandler->m_heapRel->attr, values, isNulls);
    values[D_YTD] = Float64GetDatum(DatumGetFloat64(values[D_YTD]) + h_amount);
    tableHandler->Update(tuple->GetCtid(), tableDesc->indexDes->indexCol, values, isNulls);
    DestroyObject((void**)&tuple);
    return true;
}

bool TpccStorage::SelectWarehouseTable(int w_id, Datum* values, bool* isNulls)
{
    TableNameType tableType = TABLE_WAREHOUSE;
    TableDesc* tableDesc = &TpccTableDesc[tableType];
    auto tableHandler =  GetTableHandler(tableDesc);
    Datum indexValues[] = {Int32GetDatum(w_id)};
    HeapTuple* tuple = nullptr;
    int ret = tableHandler->Scan(tableDesc->indexDes->indexCol, indexValues, &tuple, sizeof(indexValues) / sizeof(Datum));
    if (ret == 0) {
        assert(tuple != nullptr);
        TupleInterface::DeformHeapTuple(tuple, tableHandler->m_heapRel->attr, values, isNulls);
        DestroyObject((void**)&tuple);
    }

    return ret == 0;
}

bool TpccStorage::SelectStockTable(int s_w_id, int s_i_id, int& s_quantity)
{
    TableNameType tableType = TABLE_STOCK;
    TableDesc *tableDesc = &TpccTableDesc[tableType];
    auto tableHandler =  GetTableHandler(tableDesc);
    Datum values[] = {Int32GetDatum(s_w_id), Int32GetDatum(s_i_id)};
    HeapTuple *tuple = nullptr;
    int ret = tableHandler->Scan(tableDesc->indexDes->indexCol, values, &tuple, sizeof(values) / sizeof(Datum));
    if (ret == 0) {
        assert(tuple != nullptr);
        Datum stockValues[tableDesc->colNum];
        bool isNulls[tableDesc->colNum];
        TupleInterface::DeformHeapTuple(tuple, tableHandler->m_heapRel->attr, stockValues, isNulls);
        s_quantity = stockValues[S_QUANTITY];
        DestroyObject((void**)&tuple);
    }
    return ret == 0;
}

bool TpccStorage::SelectOrderlineTableInRange(int ol_w_id, int ol_d_id, int rangeStart, int rangeEnd, std::unordered_set<int>& olItemIdSet)
{
    TableNameType tableType = TABLE_ORDERLINE;
    TableDesc* tableDesc = &TpccTableDesc[tableType];
    auto tableHandler =  GetTableHandler(tableDesc);
    for(int ol_o_id = rangeStart; ol_o_id <= rangeEnd; ol_o_id++){
        Datum oOrderValues[GetTableColMax(TABLE_ORDER)];
        bool oOrderIsNulls[GetTableColMax(TABLE_ORDER)];
        SelectOrderTable(ol_w_id, ol_d_id, ol_o_id, oOrderValues, oOrderIsNulls);
        int o_ol_cnt = oOrderValues[O_OL_CNT];
        for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
            Datum indexValues[] = {Int32GetDatum(ol_w_id), Int32GetDatum(ol_d_id), Int32GetDatum(ol_o_id), Int32GetDatum(ol_number)};
            HeapTuple* tuple = nullptr;
            tableHandler->Scan(tableDesc->indexDes->indexCol, indexValues, &tuple, sizeof(indexValues) / sizeof(Datum));
            if (tuple == nullptr) {
                continue;
            }
            Datum values[tableDesc->colNum];
            bool isNulls[tableDesc->colNum];
            TupleInterface::DeformHeapTuple(tuple, tableHandler->m_heapRel->attr, values, isNulls);
            olItemIdSet.insert(values[OL_I_ID]);
            DestroyObject((void**)&tuple);
        }
    }
    return true;
}

bool TpccStorage::SelectOrderlineTableForSum(int w_id, int d_id, int o_id, double &amountTotal)
{
    TableNameType tableType = TABLE_ORDERLINE;
    TableDesc *tableDesc = &TpccTableDesc[tableType];
    auto tableHandler =  GetTableHandler(tableDesc);
    Datum indexValues[] = {Int32GetDatum(w_id), Int32GetDatum(d_id), Int32GetDatum(o_id)};
    tableHandler->Sum(GetIndexDesc(TABLE_ORDERLINE)->indexCol, indexValues, OL_AMOUNT + 1, amountTotal,
                      sizeof(indexValues) / sizeof(Datum));
    return true;
}

bool TpccStorage::UpdateWarehouseTableForAmount(int w_id, double h_amount)
{
    TableNameType tableType = TABLE_WAREHOUSE;
    TableDesc* tableDesc = &TpccTableDesc[tableType];
    auto tableHandler =  GetTableHandler(tableDesc);
    Datum indexValues[] = {Int32GetDatum(w_id)};
    HeapTuple* tuple = nullptr;
    tableHandler->Scan(tableDesc->indexDes->indexCol, indexValues, &tuple, sizeof(indexValues) / sizeof(Datum));
    assert(tuple != nullptr);
    Datum values[tableDesc->colNum];
    bool isNulls[tableDesc->colNum];
    TupleInterface::DeformHeapTuple(tuple, tableHandler->m_heapRel->attr, values, isNulls);
    values[W_YTD] = Float64GetDatum(DatumGetFloat64(values[W_YTD]) + h_amount);
    tableHandler->Update(tuple->GetCtid(), tableDesc->indexDes->indexCol, values, isNulls);
    DestroyObject((void**)&tuple);
    return true;
}

bool TpccStorage::UpdateStockTable(int w_id, int i_id, int remoteCnt, int s_quantity, int s_ytd)
{
    TableNameType tableType = TABLE_STOCK;
    TableDesc *tableDesc = &TpccTableDesc[tableType];
    auto tableHandler =  GetTableHandler(tableDesc);
    Datum indexValues[] = {Int32GetDatum(w_id), Int32GetDatum(i_id)};
    HeapTuple *tuple = nullptr;
    tableHandler->Scan(tableDesc->indexDes->indexCol, indexValues, &tuple, sizeof(indexValues) / sizeof(Datum));
    if (tuple == nullptr) {
        return false;
    }
    Datum values[tableDesc->colNum];
    bool isNulls[tableDesc->colNum];
    TupleInterface::DeformHeapTuple(tuple, tableHandler->m_heapRel->attr, values, isNulls);
    values[S_QUANTITY] = Int32GetDatum(s_quantity);
    values[S_YTD] = Int32GetDatum(s_ytd + DatumGetInt32(values[S_YTD]));
    values[S_ORDER_CNT] = Int32GetDatum(1 + DatumGetInt32(values[S_ORDER_CNT]));
    values[S_REMOTE_CNT] = Int32GetDatum(remoteCnt + DatumGetInt32(values[S_REMOTE_CNT]));
    tableHandler->Update(tuple->GetCtid(), tableDesc->indexDes->indexCol, values, isNulls);
    DestroyObject((void**)&tuple);
    return true;
}

bool TpccStorage::SelectDistrictForUpdate(int w_id, int d_id, Datum *values, bool *isNulls)
{
    TableNameType tableType = TABLE_DISTRICT;
    TableDesc *tableDesc = &TpccTableDesc[tableType];
    auto tableHandler =  GetTableHandler(tableDesc);
    Datum indexValues[] = {Int32GetDatum(w_id), Int32GetDatum(d_id)};
    HeapTuple *tuple = nullptr;
    int ret = tableHandler->LockTuple(tableDesc->indexDes->indexCol, indexValues, &tuple, sizeof(indexValues) / sizeof(Datum));
    if (ret == 0) {
        assert(tuple != nullptr);
        TupleInterface::DeformHeapTuple(tuple, tableHandler->m_heapRel->attr, values, isNulls);
        DestroyObject((void**)&tuple);
    }
    return ret == 0;
}

bool TpccStorage::SelectStockForUpdate(int w_id, int i_id, Datum* values, bool* isNulls)
{
    TableNameType tableType = TABLE_STOCK;
    TableDesc* tableDesc = &TpccTableDesc[tableType];
    auto tableHandler =  GetTableHandler(tableDesc);
    Datum indexValues[] = {Int32GetDatum(w_id), Int32GetDatum(i_id)};
    HeapTuple* tuple = nullptr;
    int ret = tableHandler->LockTuple(tableDesc->indexDes->indexCol, indexValues, &tuple, sizeof(indexValues) / sizeof(Datum));
    if (ret == 0) {
        assert(tuple != nullptr);
        TupleInterface::DeformHeapTuple(tuple, tableHandler->m_heapRel->attr, values, isNulls);
        DestroyObject((void**)&tuple);
    }
    return ret == 0;
}

bool TpccStorage::SelectDistrict(int w_id, int d_id, Datum* values, bool* isNulls)
{
    TableNameType tableType = TABLE_DISTRICT;
    TableDesc* tableDesc = &TpccTableDesc[tableType];
    auto tableHandler =  GetTableHandler(tableDesc);
    Datum indexValues[] = {Int32GetDatum(w_id), Int32GetDatum(d_id)};
    HeapTuple* tuple = nullptr;
    int ret = tableHandler->Scan(tableDesc->indexDes->indexCol, indexValues, &tuple, sizeof(indexValues) / sizeof(Datum));
    if (ret == 0) {
        assert(tuple != nullptr);
        TupleInterface::DeformHeapTuple(tuple, tableHandler->m_heapRel->attr, values, isNulls);
        DestroyObject((void**)&tuple);
    }

    return ret == 0;
}

bool TpccStorage::SelectItem(int i_id, Datum* values, bool* isNulls)
{
    TableNameType tableType = TABLE_ITEM;
    TableDesc* tableDesc = &TpccTableDesc[tableType];
    auto tableHandler =  GetTableHandler(tableDesc);
    Datum indexValues[] = {Int32GetDatum(i_id)};
    HeapTuple* tuple = nullptr;
    int ret = tableHandler->Scan(tableDesc->indexDes->indexCol, indexValues, &tuple, sizeof(indexValues) / sizeof(Datum));
    if (ret == 0) {
        assert(tuple != nullptr);
        TupleInterface::DeformHeapTuple(tuple, tableHandler->m_heapRel->attr, values, isNulls);
        DestroyObject((void**)&tuple);
    }

    return ret == 0;
}

bool TpccStorage::SelectCustomer(int w_id, int c_id, int d_id, Datum *values, bool *isNulls)
{
    TableNameType tableType = TABLE_CUSTOMER;
    TableDesc *tableDesc = &TpccTableDesc[tableType];
    auto tableHandler =  GetTableHandler(tableDesc);
    Datum indexValues[] = {Int32GetDatum(w_id), Int32GetDatum(d_id), Int32GetDatum(c_id)};
    HeapTuple *tuple = nullptr;
    int ret = tableHandler->Scan(tableDesc->indexDes->indexCol, indexValues, &tuple, sizeof(indexValues) / sizeof(Datum));
    if (ret == 0) {
        assert(tuple != nullptr);
        TupleInterface::DeformHeapTuple(tuple, tableHandler->m_heapRel->attr, values, isNulls);
        DestroyObject((void**)&tuple);
    }

    return ret == 0;
}

bool TpccStorage::SelectOrderTable(int w_id, int d_id, int o_id, Datum *values, bool *isNulls)
{
    TableNameType tableType = TABLE_ORDER;
    TableDesc *tableDesc = &TpccTableDesc[tableType];
    auto tableHandler =  GetTableHandler(tableDesc);
    Datum indexValues[] = {Int32GetDatum(w_id), Int32GetDatum(d_id), Int32GetDatum(o_id)};
    HeapTuple *tuple = nullptr;
    int ret = tableHandler->Scan(tableDesc->indexDes->indexCol, indexValues, &tuple, sizeof(indexValues) / sizeof(Datum));
    if (ret == 0) {
        assert(tuple != nullptr);
        TupleInterface::DeformHeapTuple(tuple, tableHandler->m_heapRel->attr, values, isNulls);
        DestroyObject((void**)&tuple);
    }

    return ret == 0;
}

bool TpccStorage::SelectNewOrderTable(int w_id, int d_id, int o_id, Datum *values, bool *isNulls)
{
    TableNameType tableType = TABLE_NEWORDER;
    TableDesc *tableDesc = &TpccTableDesc[tableType];
    auto tableHandler =  GetTableHandler(tableDesc);
    Datum indexValues[] = {Int32GetDatum(w_id), Int32GetDatum(d_id), Int32GetDatum(o_id)};
    HeapTuple *tuple = nullptr;
    int ret = tableHandler->Scan(tableDesc->indexDes->indexCol, indexValues, &tuple);
    if (ret == 0) {
        assert(tuple != nullptr);
        TupleInterface::DeformHeapTuple(tuple, tableHandler->m_heapRel->attr, values, isNulls);
        DestroyObject((void**)&tuple);
    }
    return ret == 0;
}

bool TpccStorage::SelectLastOrderTable(int w_id, int d_id, Datum* values, bool* isNulls)
{
    TableNameType tableType = TABLE_ORDER;
    TableDesc* tableDesc = &TpccTableDesc[tableType];
    auto tableHandler =  GetTableHandler(tableDesc);
    Datum indexValues[] = {Int32GetDatum(w_id), Int32GetDatum(d_id)};
    HeapTuple *tuple = nullptr;
    int ret = tableHandler->GetMax(tableDesc->indexDes->indexCol, indexValues, &tuple, 2, sizeof(indexValues) / sizeof(Datum));
    if (ret == 0) {
        assert(tuple != nullptr);
        TupleInterface::DeformHeapTuple(tuple, tableHandler->m_heapRel->attr, values, isNulls);
        DestroyObject((void**)&tuple);
    }

    return ret == 0;
}

bool TpccStorage::GetDistrictOrderlineCount(int w_id, int d_id, int o_id, uint32_t& cnt)
{
    TableNameType tableType = TABLE_ORDERLINE;
    TableDesc* tableDesc = &TpccTableDesc[tableType];
    auto tableHandler =  GetTableHandler(tableDesc);
    Datum indexValues[] = {Int32GetDatum(w_id), Int32GetDatum(d_id), Int32GetDatum(o_id)};
    int ret = tableHandler->GetCount(cnt, tableDesc->indexDes->indexCol, indexValues, sizeof(indexValues) / sizeof(Datum));
    return ret == 0;
}

bool TpccStorage::SelectOrderlineTable(int w_id, int d_id, int o_id, int ol_number, Datum *values, bool *isNulls)
{
    TableNameType tableType = TABLE_ORDERLINE;
    TableDesc* tableDesc = &TpccTableDesc[tableType];
    auto tableHandler =  GetTableHandler(tableDesc);
    Datum indexValues[] = {Int32GetDatum(w_id), Int32GetDatum(d_id), Int32GetDatum(o_id), Int32GetDatum(ol_number)};
    HeapTuple *tuple = nullptr;
    int ret = tableHandler->Scan(tableDesc->indexDes->indexCol, indexValues, &tuple);
    if (ret == 0) {
        assert(tuple != nullptr);
        TupleInterface::DeformHeapTuple(tuple, tableHandler->m_heapRel->attr, values, isNulls);
        DestroyObject((void**)&tuple);
    }
    return ret == 0;
}

void TpccStorage::CalRunStat()
{
    TpccRunStat* summary = &m_stats[m_tpccConfig.workThreadNum];
    summary->nTotalCommitted_ = 0;
    summary->nTotalAborted_ = 0;
    for (int i = 0; i < TPCC_TRANSACTION_NUM; i++) {
        summary->runstat_[i].nCommitted_ = 0;
        summary->runstat_[i].nAborted_ = 0;
        for (uint32_t k = 0; k < m_tpccConfig.workThreadNum; k++) {
            TpccRunStat &wid = m_stats[k];
            TransactionRunStat &stat = wid.runstat_[i];
            summary->runstat_[i].nCommitted_.fetch_add(stat.nCommitted_);
            summary->runstat_[i].nAborted_.fetch_add(stat.nAborted_);

            summary->nTotalCommitted_.fetch_add(stat.nCommitted_);
            summary->nTotalAborted_.fetch_add(stat.nAborted_);
        }
    }
}

void TpccStorage::PrintPerfState()
{
    uint32_t run_time = m_tpccConfig.durationTime;
    TpccRunStat* summary = &m_stats[m_tpccConfig.workThreadNum];
    CalRunStat();
    uint64_t total = summary->nTotalCommitted_.load() + summary->nTotalAborted_.load();

    printf("==> Committed TPS: %lu, TPMc: %lu\n\n", summary->nTotalCommitted_ / run_time,
           summary->runstat_[NewOrderType].nCommitted_.load() / run_time * 60);

    printf("%-20s | %-20s | %-20s | %-20s | %-20s | %-20s\n","tran", "#totaltran", "%ratio", "#committed", "#aborted", "%abort");
    printf("%-20s | %-20s | %-20s | %-20s | %-20s | %-20s\n", "----", "--------", "------", "----------", "--------", "------");
    for (int i = 0; i < 5; i++) {
        const TransactionRunStat &stat = summary->runstat_[i];
        uint64_t totalpert = stat.nCommitted_.load() + stat.nAborted_.load();
        printf("%-30s %-25lu %-20.1f %-20lu %-20lu %-20.1f\n", TpccTableDesc[i].tableName, totalpert,
               (totalpert * 100.0) / total, stat.nCommitted_.load(), stat.nAborted_.load(), (stat.nAborted_.load() * 100.0) / totalpert);
    }
    printf("\n");
    printf("%s        %11lu      %6.1f%%      %10lu      %9lu      %6.1f%%\n", "Total", total, 100.0,
           (uint64_t)summary->nTotalCommitted_, (uint64_t)summary->nTotalAborted_, ((uint64_t)summary->nTotalAborted_ * 100.0) / total);
    printf("-----         ----------       ------      ----------       --------       ------\n");
}

GenDataIntoTableFunc TpccStorage::GetGenDataFunc(TableNameType tableNameType)
{
    switch (tableNameType) {
        case TABLE_WAREHOUSE:
            return &TpccStorage::GenDataIntoWarehouse;
        case TABLE_DISTRICT:
            return &TpccStorage::GenDataIntoDistrict;
        case TABLE_STOCK:
            return &TpccStorage::GenDataIntoStock;
        case TABLE_ITEM:
            return &TpccStorage::GenDataIntoItem;
        case TABLE_CUSTOMER:
            return &TpccStorage::GenDataIntoCustomer;
        case TABLE_ORDER:
            return &TpccStorage::GenDataIntoOrder;
        case TABLE_NEWORDER:
        case TABLE_ORDERLINE:
        case TABLE_HISTORY:
        case TABLE_MAX_CNT:
            assert(0);
    }
    return nullptr;
}

void TpccStorage::GenDataIntoTable(uint32_t threadId, CommonPersistentHandler* persistentHandler)
{
    TpccRunStateMetaData* runStateObj = static_cast<TpccRunStateMetaData*>(persistentHandler->GetObject());
    bool isItemLoadFinished = false;
    TransactionInterface::StartTrxCommand();
    TransactionInterface::SetSnapShot();
    if (threadId == 0 && m_selfNodeId == 1 && !runStateObj->itemRelIsLoadFinished) {
        printf("Worker %d Loading Item\n", threadId);
        (this->*GetGenDataFunc(TABLE_ITEM))(0);
        isItemLoadFinished = true;
        printf("Worker %d Loading Item Done\n", threadId);
    }
    StorageAssert(threadId < runStateObj->loadDataSize);
    WarehouseInterval warehouse = runStateObj->loadDatas[threadId];
    for (uint32_t i = warehouse.startWhId; i <= warehouse.endWhId; i++) {
        printf("NodeId %d worker %d Loading WareHouse \t %d\n", m_selfNodeId, threadId, i);
        for (TableNameType j = TABLE_WAREHOUSE; j <= TABLE_ORDER; j = (TableNameType)(j + 1)) {
            if (j == TABLE_ITEM) {
                continue;
            }
            (this->*GetGenDataFunc(j))(i);
        }
        printf("NodeId %d worker %d Loading WareHouse %d \t Done\n", m_selfNodeId, threadId, i);
    }

    TransactionInterface::CommitTrxCommand();
    m_runStateMutex.lock();
    runStateObj->itemRelIsLoadFinished = isItemLoadFinished;
    runStateObj->loadDatas[threadId].isFinished = true;
    persistentHandler->Sync();
    m_runStateMutex.unlock();
    ClearLocalTableHandler();
}

void TpccStorage::GenDataIntoWarehouse(uint32_t whId)
{
    std::default_random_engine e;
    e.seed(gSeed);
    std::uniform_real_distribution<double> u(0, 2000);
    TableNameType tableType = TABLE_WAREHOUSE;
    TableDesc* tableDesc = &TpccTableDesc[tableType];
    auto tableHandler = GetTableHandler(tableDesc, false);
    TableDataGenerator dataGenerator(tableDesc->colDefs, tableDesc->colNum);

    TestTuple *testTuples = dataGenerator.GenerateDataAndGetTestTuples(1);
    testTuples[0].values[W_ID] = Int32GetDatum(whId);
    testTuples[0].values[W_TAX] = Float64GetDatum(u(e) / 10000.0);
    testTuples[0].values[W_YTD] = Float64GetDatum(300000.0);
    tableHandler->Insert(testTuples[0].values, testTuples[0].isNulls);
    dataGenerator.Reset();
}

/*
 *For each WAREHOUSE there are 10 DISTRICT rows.
 */
void TpccStorage::GenDataIntoDistrict(uint32_t whId)
{
    std::default_random_engine e;
    e.seed(gSeed);
    std::uniform_real_distribution<double> u(0, 2000);

    TableNameType tableType = TABLE_DISTRICT;
    TableDesc* tableDesc = &TpccTableDesc[tableType];
    auto tableHandler = GetTableHandler(tableDesc, false);

    TableDataGenerator dataGenerator(tableDesc->colDefs, tableDesc->colNum);
    TestTuple *testTuples = dataGenerator.GenerateDataAndGetTestTuples(TPCC_DISTICT_PER_WAREHOUSE);
    for (int i = 1; i <= TPCC_DISTICT_PER_WAREHOUSE; i++) {
        testTuples[i - 1].values[D_W_ID] = Int32GetDatum(whId);
        testTuples[i - 1].values[D_ID] = Int32GetDatum(i);
        testTuples[i - 1].values[D_TAX] = Float64GetDatum(u(e) / 10000.0);
        testTuples[i - 1].values[D_YTD] = Float64GetDatum(30000.0);
        testTuples[i - 1].values[D_NEXT_O_ID] = Int32GetDatum(m_tpccConfig.orderPerDistrict+1);
        tableHandler->Insert(testTuples[i - 1].values, testTuples[i - 1].isNulls);
    }
    dataGenerator.Reset();
}

/*
 * For each WAREHOUSE there are m_tpccConfig.itemNumPerWareHouse STOCK rows.
 */
void TpccStorage::GenDataIntoStock(uint32_t whId)
{
    std::default_random_engine e;
    e.seed(gSeed);
    std::uniform_int_distribution<unsigned> u(10, 100);
    TableNameType tableType = TABLE_STOCK;
    TableDesc* tableDesc = &TpccTableDesc[tableType];
    auto tableHandler = GetTableHandler(tableDesc, false);

    TableDataGenerator dataGenerator(tableDesc->colDefs, tableDesc->colNum);
    uint32_t loop = 100;
    for (uint32_t i = 0; i < m_tpccConfig.itemNumPerWareHouse / loop; i++) {
        TestTuple *testTuples = dataGenerator.GenerateDataAndGetTestTuples(loop);
        for (uint32_t j = 1; j <= loop; j++) {
            testTuples[j - 1].values[S_I_ID] = Int32GetDatum(i * loop + j);
            testTuples[j - 1].values[S_W_ID] = Int32GetDatum(whId);
            int s_quantity = Int32GetDatum(u(e));
            testTuples[j - 1].values[S_QUANTITY] = s_quantity;
            testTuples[j - 1].values[S_YTD] = Int32GetDatum(0);
            testTuples[j - 1].values[S_ORDER_CNT] = Int32GetDatum(0);
            testTuples[j - 1].values[S_REMOTE_CNT] = Int32GetDatum(0);
            tableHandler->Insert(testTuples[j - 1].values, testTuples[j - 1].isNulls);
        }
        dataGenerator.Reset();
    }
}

void TpccStorage::GenDataIntoItem(UNUSE_PARAM uint32_t whId)
{
    TableNameType tableType = TABLE_ITEM;
    TableDesc* tableDesc = &TpccTableDesc[tableType];
    auto tableHandler = GetTableHandler(tableDesc, false);
    TableDataGenerator dataGenerator(tableDesc->colDefs, tableDesc->colNum);

    TestTuple *testTuples = nullptr;
    uint32_t loop = 100;
    for (uint32_t i = 0; i < (m_tpccConfig.itemNumPerWareHouse / loop) ; i++) {
        testTuples = dataGenerator.GenerateDataAndGetTestTuples(loop);
        for (uint32_t j = 1; j <= loop; j++) {
            testTuples[j - 1].values[I_ID] = Int32GetDatum(i * loop + j);
            tableHandler->Insert(testTuples[j - 1].values, testTuples[j - 1].isNulls);
        }
        dataGenerator.Reset();
    }
}

/*
 *Within each DISTRICT there are 3,000 CUSTOMERs
 */
void TpccStorage::GenDataIntoCustomer(uint32_t whId)
{
    std::default_random_engine e;
    e.seed(gSeed);
    std::uniform_real_distribution<double> u(0, 5000);
    TableNameType tableType = TABLE_CUSTOMER;
    TableDesc* tableDesc = &TpccTableDesc[tableType];
    auto tableHandler = GetTableHandler(tableDesc, false);
    TableDataGenerator dataGenerator(tableDesc->colDefs, tableDesc->colNum);

    for (int i = 0; i < TPCC_DISTICT_PER_WAREHOUSE; i++) {
        TestTuple *testTuples = dataGenerator.GenerateDataAndGetTestTuples(m_tpccConfig.customerPerDistrict);
        for (uint32_t j = 1; j <= m_tpccConfig.customerPerDistrict; j++) {
            testTuples[j - 1].values[C_ID]   = Int32GetDatum(j);
            testTuples[j - 1].values[C_W_ID] = Int32GetDatum(whId);
            testTuples[j - 1].values[C_D_ID] = Int32GetDatum(i + 1);
            testTuples[j - 1].values[C_CREDIT_LIM] = Float64GetDatum(50000.00);
            testTuples[j - 1].values[C_DISCOUNT] = Float64GetDatum(u(e) / 10000.0);
            testTuples[j - 1].values[C_BALANCE] = Float64GetDatum(-10.00);
            testTuples[j - 1].values[C_YTD_PAYMENT] = Float64GetDatum(10.00);
            testTuples[j - 1].values[C_PAYMENT_CNT] = Int32GetDatum(1);
            testTuples[j - 1].values[C_DELIVERY_CNT] = Int32GetDatum(1);
            tableHandler->Insert(testTuples[j - 1].values, testTuples[j - 1].isNulls);
            /* For each CUSTOMER there is one row in HISTORY.*/
            GenDataIntoHistroy(whId, i + 1, j);
        }
        dataGenerator.Reset();
    }
}

void TpccStorage::GenDataIntoHistroy(uint32_t whid, uint32_t d_id, uint32_t c_id)
{
    TableNameType tableType = TABLE_HISTORY;
    TableDesc* tableDesc = &TpccTableDesc[tableType];
    auto tableHandler = GetTableHandler(tableDesc, false);
    TableDataGenerator dataGenerator(tableDesc->colDefs, tableDesc->colNum);

    TestTuple *testTuples = dataGenerator.GenerateDataAndGetTestTuples(1);
    testTuples[0].values[H_ID] = ((whid - 1) * 30000 + (d_id - 1) * 3000 + c_id);
    testTuples[0].values[H_C_W_ID] = Int32GetDatum(whid);
    testTuples[0].values[H_W_ID] = Int32GetDatum(whid);
    testTuples[0].values[H_C_ID] = Int32GetDatum(c_id);
    testTuples[0].values[H_C_D_ID] = Int32GetDatum(d_id);
    testTuples[0].values[H_D_ID] = Int32GetDatum(d_id);
    testTuples[0].values[H_D_ID] = Float64GetDatum(10.00);
    tableHandler->Insert(testTuples[0].values, testTuples[0].isNulls);
    dataGenerator.Reset();
}

/*
 * For the ORDER table the TPC-C specification demands that they
 * are generated using a random permutation of all TpccCustomerPerDistrict customers.
 * To do that we set up an array with all C_IDs and then randomly shuffle it.
 */
void ShufferCustomerId(std::vector<uint32_t>& customerVec)
{
    std::iota(customerVec.begin(), customerVec.end(), 1);
    std::shuffle(customerVec.begin(), customerVec.end(), std::default_random_engine(gSeed));
}


void TpccStorage::GenDataIntoOrderLine(uint32_t w_id, uint32_t d_id, uint32_t o_id, uint32_t o_ol_cnt)
{
    std::default_random_engine e;
    e.seed(gSeed);
    std::uniform_int_distribution<unsigned> u(1, m_tpccConfig.itemNumPerWareHouse);
    std::uniform_real_distribution<double> uf(1, 999999);
    TableNameType tableType = TABLE_ORDERLINE;
    TableDesc* tableDesc = &TpccTableDesc[tableType];
    auto tableHandler = GetTableHandler(tableDesc, false);
    TableDataGenerator dataGenerator(tableDesc->colDefs, tableDesc->colNum);

    std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
    Timestamp ol_delivery_d = std::chrono::system_clock::to_time_t(now);
    TestTuple *testTuples = dataGenerator.GenerateDataAndGetTestTuples(o_ol_cnt);
    for (uint32_t i = 1; i <= o_ol_cnt; i++) {
        testTuples[i - 1].values[OL_W_ID] = Int32GetDatum(w_id);
        testTuples[i - 1].values[OL_SUPPLY_W_ID] = Int32GetDatum(w_id);
        testTuples[i - 1].values[OL_D_ID] = Int32GetDatum(d_id);
        testTuples[i - 1].values[OL_O_ID] = Int32GetDatum(o_id);
        testTuples[i - 1].values[OL_NUMBER] = Int32GetDatum(i);
        testTuples[i - 1].values[OL_I_ID] = Int32GetDatum(u(e));
        if (o_id > 0.7 * m_tpccConfig.orderPerDistrict){
            testTuples[i - 1].values[OL_AMOUNT] = Float64GetDatum(uf(e) / 100.0);
            testTuples[i - 1].values[OL_DELIVERY_D] = Int32GetDatum(0);
            testTuples[i - 1].isNulls[OL_DELIVERY_D] = true;
        }
        else {
            testTuples[i - 1].values[OL_AMOUNT] = Float64GetDatum(0.00);
            testTuples[i - 1].values[OL_DELIVERY_D] = ol_delivery_d;
            testTuples[i - 1].isNulls[OL_DELIVERY_D] = false;
        }
        testTuples[i - 1].values[OL_QUANTITY] = Int32GetDatum(5);
        tableHandler->Insert(testTuples[i - 1].values, testTuples[i - 1].isNulls);
    }
    dataGenerator.Reset();
}

void TpccStorage::GenDataIntoOrder(uint32_t whId)
{
    std::default_random_engine e;
    e.seed(gSeed);
    std::uniform_int_distribution<unsigned> u(5, 15);
    TableDesc* orderTableDesc = &TpccTableDesc[TABLE_ORDER];
    DstoreTableHandler *orderTableHandler = simulator->GetTableHandler(orderTableDesc->tableName, nullptr);
    TableDataGenerator oldDataGenerator(orderTableDesc->colDefs, orderTableDesc->colNum);

    TableDesc* newOrderTableDesc = &TpccTableDesc[TABLE_NEWORDER];
    DstoreTableHandler *newOrdertableHandler = simulator->GetTableHandler(newOrderTableDesc->tableName, nullptr);
    TableDataGenerator newDataGenerator(newOrderTableDesc->colDefs, newOrderTableDesc->colNum);


    std::vector<uint32_t> customerVec(m_tpccConfig.customerPerDistrict);
    uint32_t o_ol_cnt;
    for (uint32_t i = 1; i <= TPCC_DISTICT_PER_WAREHOUSE; i++) {
        ShufferCustomerId(customerVec);
        TestTuple *orderTestTuples = oldDataGenerator.GenerateDataAndGetTestTuples(m_tpccConfig.orderPerDistrict);
        uint32_t newOrderCnt = 0.3 * m_tpccConfig.orderPerDistrict;
        TestTuple *newOrderTestTuples = newDataGenerator.GenerateDataAndGetTestTuples(newOrderCnt);
        for (uint32_t j = 1; j <= m_tpccConfig.orderPerDistrict; j++) {
            orderTestTuples[j - 1].values[O_W_ID] = Int32GetDatum(whId);
            orderTestTuples[j - 1].values[O_D_ID] = Int32GetDatum(i);
            orderTestTuples[j - 1].values[O_ID] = Int32GetDatum(j);
            orderTestTuples[j - 1].values[O_C_ID] = Int32GetDatum(customerVec[(j - 1) % m_tpccConfig.customerPerDistrict]);
            o_ol_cnt = u(e);
            orderTestTuples[j - 1].values[O_OL_CNT] = Int32GetDatum(o_ol_cnt);
            orderTestTuples[j - 1].values[O_ALL_LOCAL] = Int32GetDatum(1);

            /* mark 30% orders in ordertable as new order */
            if (j > 0.7 * m_tpccConfig.orderPerDistrict){
                orderTestTuples[j - 1].values[O_CARRIER_ID] = Int32GetDatum(0);
                orderTestTuples[j - 1].isNulls[O_CARRIER_ID] = true;
                int index = j - 0.7 * m_tpccConfig.orderPerDistrict -1;
                newOrderTestTuples[index].values[NO_W_ID] = Int32GetDatum(whId);
                newOrderTestTuples[index].values[NO_D_ID] = Int32GetDatum(i);
                newOrderTestTuples[index].values[NO_O_ID] = Int32GetDatum(j);
                newOrdertableHandler->Insert(newOrderTestTuples[index].values, newOrderTestTuples[index].isNulls);
            }
            else {
                orderTestTuples[j - 1].values[O_CARRIER_ID] = Int32GetDatum(i);
                orderTestTuples[j - 1].isNulls[O_CARRIER_ID] = false;
            }
            orderTableHandler->Insert(orderTestTuples[j - 1].values, orderTestTuples[j - 1].isNulls);

            /* Create the ORDER_LINE rows for this ORDER. */
            GenDataIntoOrderLine(whId, i, j, o_ol_cnt);
        }
        oldDataGenerator.Reset();
        newDataGenerator.Reset();
    }
    delete orderTableHandler;
    delete newOrdertableHandler;
}

void TpccStorage::InsertTable(TableNameType tableNameType, Datum *values, bool *isNulls)
{
    TableDesc *tableDesc = &TpccTableDesc[tableNameType];
    auto tableHandler = GetTableHandler(tableDesc, tableDesc->indexDes != nullptr);
    tableHandler->Insert(values, isNulls, tableDesc->indexDes->indexCol);
}

void TpccStorage::GetSplitRange(uint32_t size, uint32_t seq, uint32_t &start, uint32_t &end)
{
    assert(seq < m_tpccConfig.workThreadNum);
    uint32_t range = size / m_tpccConfig.workThreadNum;
    if(range < 1) range = 1;
    start = range * seq + 1;
    end = start + range - 1;
    if (seq == m_tpccConfig.workThreadNum - 1)
        end = size;
    assert(end >= start);
}

inline int fast_rand(void) {
    gSeed = (214013*gSeed+2531011);
    return (gSeed>>16)&0x7FFF;
}

int TpccStorage::RandomNumber(int min, int max)
{
    return min + (fast_rand() % ((max - min) + 1));
}

int TpccStorage::NURand(unsigned A, unsigned x, unsigned y)
{
    static int first = 1;
    unsigned C = 0, C_255 = 0, C_1023 = 0, C_8191 = 0;

    if (first) {
        C_255 = RandomNumber(0, 255);
        C_1023 = RandomNumber(0, 1023);
        C_8191 = RandomNumber(0, 8191);
        first = 0;
    }

    switch (A) {
        case 255: {
            C = C_255;
            break;
        }
        case 1023: {
            C = C_1023;
            break;
        }
        case 8191: {
            C = C_8191;
            break;
        }
        default:
            ErrLog(DSTORE_PANIC, MODULE_FRAMEWORK, ErrMsg("A : %u", A));
    }

    return (int)(((RandomNumber(0, A) | RandomNumber(x, y)) + C) % (y-x+1)) + x;
}

int TpccStorage::GetOtherWarehouseId(int w_id)
{
    if (m_tpccConfig.wareHouseNum == 1) {
        return 1;
    }
    int tmp;
    do {
        tmp = RandomNumber(1, m_tpccConfig.wareHouseNum);
    } while (tmp == w_id);
    return tmp;
}

void TpccStorage::GenerateLastname(int num, char *name)
{
    static const char *n[] =
        {"BAR", "OUGHT", "ABLE", "PRI", "PRES",
         "ESE", "ANTI", "CALLY", "ATION", "EING"};

    strcpy(name,n[num/100]);
    strcat(name,n[(num/10)%10]);
    strcat(name,n[num%10]);

    return;
}
/* check database consistency -
 * http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-c_v5.11.0.pdf
 */
void TpccStorage::CheckConsistency(CommonPersistentHandler *persistentHandler)
{
    StorageAssert(persistentHandler != nullptr);
    TpccRunStateMetaData *runStateObj = static_cast<TpccRunStateMetaData *>(persistentHandler->GetObject());
    StorageAssert(runStateObj != nullptr);

    /* consistency check divided to 7 conditions */
    std::cout << "Start consistency check" << std::endl;
    const uint32_t conditions[] = {1, 2, 3, 4, 5, 6, 7};
    /* Step 1 to 7 */
    ThreadRange threadRange = runStateObj->threadRange[m_selfNodeId - 1];
    uint32_t threadNum = threadRange.endThreadId - threadRange.startThreadId + 1;
    for (auto i : conditions) {
        CheckConsistency(i, threadNum, threadRange);
    }
    std::cout << "Finish all consistency check" << std::endl;
}

void TpccStorage::CheckConsistency(uint32_t condition, __attribute__((__unused__)) uint32_t threadNum,
                                   __attribute__((__unused__)) ThreadRange threadRange)
{
    std::vector<std::thread> workThreads(m_tpccConfig.wareHouseNum);
    uint32_t threadId = 0;
    
    for (auto &workThread : workThreads) {
        workThread = std::thread([this, condition, threadId] {
            uint32_t startWhId;
            uint32_t endWhId;
            GetSplitRange(m_tpccConfig.wareHouseNum, threadId, startWhId, endWhId);
            CreateThreadAndRegister();
            TransactionInterface::StartTrxCommand();
            TransactionInterface::SetSnapShot();
            switch (condition) {
                case 1:
                    CheckCondition1(startWhId, endWhId);
                    break;
                case 2:
                    CheckCondition2(startWhId, endWhId);
                    break;
                case 3:
                    CheckCondition3(startWhId, endWhId);
                    break;
                case 4:
                    CheckCondition4(startWhId, endWhId);
                    break;
                case 5:
                    CheckCondition5(startWhId, endWhId);
                    break;
                case 6:
                    CheckCondition6(startWhId, endWhId);
                    break;
                case 7:
                    CheckCondition7(startWhId, endWhId);
                    break;
                default: {
                    return;
                }
            }
            TransactionInterface::CommitTrxCommand();
            ClearLocalTableHandler();
            UnregisterThread();
        });
        threadId++;
    }

    for (auto & workThread : workThreads) {
        workThread.join();
    }
}

/*
 * Consistency Condition 1
 * Entries in the WAREHOUSE and DISTRICT tables must satisfy the relationship:
 * W_YTD = sum(D_YTD)
 * for each warehouse defined by (W_ID = D_W_ID).
 */
void TpccStorage::CheckCondition1(int wh_start, int wh_end) {
    for (int w_id = wh_start; w_id <= wh_end; w_id++) {
        /*Step 1: Get warehouse w_ytd*/
        Datum warehouseValues[GetTableColMax(TABLE_WAREHOUSE)];
        bool warehouseIsNulls[GetTableColMax(TABLE_WAREHOUSE)];
        SelectWarehouseTable(w_id, warehouseValues, warehouseIsNulls);
        float64 w_ytd = DatumGetFloat64(warehouseValues[W_YTD]);
        /*Step 2: Get sum of district d_ytd*/
        float64 sum_ytd = 0;
        for (int d_id = 1; d_id <= TPCC_DISTICT_PER_WAREHOUSE; d_id++) {
            Datum districtValues[GetTableColMax(TABLE_DISTRICT)];
            bool districtIsNulls[GetTableColMax(TABLE_DISTRICT)];
            SelectDistrict(w_id, d_id, districtValues, districtIsNulls);
            sum_ytd += DatumGetFloat64(districtValues[D_YTD]);
        }
        /*Step 3: Check*/
        if (std::abs(w_ytd - sum_ytd) > 0.01) {
            std::cout << "Consistency Condition 1 Failed!" << std::endl;
            return;
        }
    }
}

/*
 * Consistency Condition 2
 * Entries in the DISTRICT, ORDER, and NEW-ORDER tables must satisfy the relationship:
 * D_NEXT_O_ID - 1 = max(O_ID) = max(NO_O_ID)
 * for each district defined by (D_W_ID = O_W_ID = NO_W_ID) and (D_ID = O_D_ID = NO_D_ID). This condition
 * does not apply to the NEW-ORDER table for any districts which have no outstanding new orders (i.e., the numbe r of
 * rows is zero).
 */
void TpccStorage::CheckCondition2(int wh_start, int wh_end) {
    for (int w_id = wh_start; w_id <= wh_end; w_id++) {
        for (int d_id = 1; d_id <= TPCC_DISTICT_PER_WAREHOUSE; d_id++) {
            /* Step 1: Get next order id */
            Datum districtValues[GetTableColMax(TABLE_DISTRICT)];
            bool districtIsNulls[GetTableColMax(TABLE_DISTRICT)];
            SelectDistrict(w_id, d_id, districtValues, districtIsNulls);
            int d_next_o_id = districtValues[D_NEXT_O_ID];
            /* Step 2: Select newest order */
            uint32_t orderCnt;
            GetDistrictOOrderCount(w_id, d_id, orderCnt);
            if (orderCnt == 0) continue;
            Datum oOrderValues[GetTableColMax(TABLE_ORDER)];
            bool oOrderIsNulls[GetTableColMax(TABLE_ORDER)];
            if (SelectNewestOOrder(w_id, d_id, oOrderValues, oOrderIsNulls)) {
                int n_o_id = oOrderValues[O_ID];
                if (d_next_o_id - n_o_id != 1) {
                    std::cout << "Consistency Condition 2 Failed!" << std::endl;
                    return;
                }
            }
            /* Step 3: Select newest new order */
            uint32_t neworderCnt;
            GetDistrictNewOrderCount(w_id, d_id, neworderCnt);
            if(neworderCnt == 0) continue;
            Datum oldestNewOrderValues[GetTableColMax(TABLE_NEWORDER)];
            bool oldestNewOrderIsNulls[GetTableColMax(TABLE_NEWORDER)];
            SelectOldestNewOrder(w_id, d_id, oldestNewOrderValues, oldestNewOrderIsNulls);
            UNUSE_PARAM int oldestNewOrderId = Int32GetDatum(oldestNewOrderValues[NO_O_ID]);
            Datum newOrderValues[GetTableColMax(TABLE_NEWORDER)];
            bool newOrderIsNulls[GetTableColMax(TABLE_NEWORDER)];
            if (SelectNewestNewOrder(w_id, d_id, newOrderValues, newOrderIsNulls)) {
                int n_no_o_id = newOrderValues[NO_O_ID];
                if (d_next_o_id - n_no_o_id != 1) {
                    std::cout << "Consistency Condition 2 Failed!" << std::endl;
                    return;
                }
            }
        }
    }
}

/*
 * Consistency Condition 3
 * Entries in the NEW-ORDER table must satisfy the relationship:
 * max(NO_O_ID) - min(NO_O_ID) + 1 = [number of rows in the NEW-ORDER table for this district]
 * for each district defined by NO_W_ID and NO_D_ID. This condition does not apply to any districts which have no
 * outstanding new orders (i.e., the number of rows is zero).
 */
void TpccStorage::CheckCondition3(int wh_start, int wh_end) {
    for (int w_id = wh_start; w_id <= wh_end; w_id++) {
        for (int d_id = 1; d_id <= TPCC_DISTICT_PER_WAREHOUSE; d_id++) {
            /* Step 1: Get district neworder count */
            uint32_t neworderCnt;
            GetDistrictNewOrderCount(w_id, d_id, neworderCnt);
            if (neworderCnt == 0) continue;
            /* Step 2: Select newest new order */
            Datum newOrderValues[GetTableColMax(TABLE_NEWORDER)];
            bool newOrderIsNulls[GetTableColMax(TABLE_NEWORDER)];
            SelectNewestNewOrder(w_id, d_id, newOrderValues, newOrderIsNulls);
            int n_no_o_id = newOrderValues[NO_O_ID];
            /* Step 3: Select oldest new order */
            SelectOldestNewOrder(w_id, d_id, newOrderValues, newOrderIsNulls);
            int o_no_o_id = newOrderValues[NO_O_ID];
            if (n_no_o_id - o_no_o_id + 1 != int(neworderCnt)) {
                std::cout << "Consistency Condition 3 Failed!" << std::endl;
                return;
            }
        }
    }
}

/*
 * Consistency Condition 4
 * Entries in the ORDER and ORDER-LINE tables must satisfy the relationship:
 * sum(O_OL_CNT) = [number of rows in the ORDER-LINE table for this district]
 * for each district defined by (O_W_ID = OL_W_ID) and (O_D_ID = OL_D_ID).
 */
void TpccStorage::CheckCondition4(int wh_start, int wh_end) {
    for (int w_id = wh_start; w_id <= wh_end; w_id++) {
        for (int d_id = 1; d_id <= TPCC_DISTICT_PER_WAREHOUSE; d_id++) {
            /* Step 1: Get next order id */
            Datum districtValues[GetTableColMax(TABLE_DISTRICT)];
            bool districtIsNulls[GetTableColMax(TABLE_DISTRICT)];
            SelectDistrict(w_id, d_id, districtValues, districtIsNulls);
            int d_next_o_id = districtValues[D_NEXT_O_ID];
            /* Step 2: Get sum of o_ol_cnt*/
            Datum oOrderValues[GetTableColMax(TABLE_ORDER)];
            bool oOrderIsNulls[GetTableColMax(TABLE_ORDER)];
            int sum_o_ol_cnt = 0;
            for (int o_id = 1; o_id < d_next_o_id; o_id++) {
                if (!SelectOrderTable(w_id, d_id, o_id, oOrderValues, oOrderIsNulls)) {
                    printf("TpccStorage::CheckCondition4 w_id:%d, d_id:%d, o_id:%d \n", w_id, d_id, o_id);
                    assert(0);
                }
                int o_ol_cnt = oOrderValues[O_OL_CNT];
                sum_o_ol_cnt += o_ol_cnt;
            }
            /* Step 3: Get district orderline count */
            uint32_t orderlineCnt;
            GetDistrictOrderLineCount(w_id, d_id, orderlineCnt);
            if (sum_o_ol_cnt != int(orderlineCnt)) {
                std::cout << "Consistency Condition 4 Failed!" << std::endl;
                return;
            }
        }
    }
}

/*
 * Consistency Condition 5
 * For any row in the ORDER table, O_CARRIER_ID is set to a null value if and only if there is a corresponding row in
 * the NEW-ORDER table defined by (O_W_ID, O_D_ID, O_ID) = (NO_W_ID, NO_D_ID, NO_O_ID).
 */
void TpccStorage::CheckCondition5(int wh_start, int wh_end) {
    for (int w_id = wh_start; w_id <= wh_end; w_id++) {
        for (int d_id = 1; d_id <= TPCC_DISTICT_PER_WAREHOUSE; d_id++) {
            /* Step 1: Get next order id */
            Datum districtValues[GetTableColMax(TABLE_DISTRICT)];
            bool districtIsNulls[GetTableColMax(TABLE_DISTRICT)];
            SelectDistrict(w_id, d_id, districtValues, districtIsNulls);
            int d_next_o_id = districtValues[D_NEXT_O_ID];
            Datum oOrderValues[GetTableColMax(TABLE_ORDER)];
            bool oOrderIsNulls[GetTableColMax(TABLE_ORDER)];
            Datum newOrderValues[GetTableColMax(TABLE_NEWORDER)];
            bool newOrderIsNulls[GetTableColMax(TABLE_NEWORDER)];
            for(int o_id = 1; o_id < d_next_o_id; o_id++) {
                /* Step 2: Get whether the order is deliveried */
                if (!SelectOrderTable(w_id, d_id, o_id, oOrderValues, oOrderIsNulls)) {
                    assert(0);
                }
                bool carrierIdIsNull = (oOrderValues[O_CARRIER_ID] == 0) ? true : false;
                /* Step 3: Get whether the order is in the newordertable */
                bool find = SelectNewOrderTable(w_id, d_id, o_id, newOrderValues, newOrderIsNulls);
                if (carrierIdIsNull != find) {
                    std::cout << "Consistency Condition 5 Failed!" << std::endl;
                    return;
                }
            }
        }
    }
}

/*
 * Consistency Condition 6
 * For any row in the ORDER table, O_OL_CNT must equal the number of rows in the ORDER-LINE table for the
 * corresponding order defined by (O_W_ID, O_D_ID, O_ID) = (OL_W_ID, OL_D_ID, OL_O_ID).
 */
void TpccStorage::CheckCondition6(int wh_start, int wh_end) {
    for (int w_id = wh_start; w_id <= wh_end; w_id++) {
        for (int d_id = 1; d_id <= TPCC_DISTICT_PER_WAREHOUSE; d_id++) {
            /* Step 1: Get next order id */
            Datum districtValues[GetTableColMax(TABLE_DISTRICT)];
            bool districtIsNulls[GetTableColMax(TABLE_DISTRICT)];
            SelectDistrict(w_id, d_id, districtValues, districtIsNulls);
            int d_next_o_id = districtValues[D_NEXT_O_ID];
            Datum oOrderValues[GetTableColMax(TABLE_ORDER)];
            bool oOrderIsNulls[GetTableColMax(TABLE_ORDER)];
            for(int o_id = 1; o_id < d_next_o_id; o_id++) {
                /* Step 2: Get table o_ol_cnt */
                if (!SelectOrderTable(w_id, d_id, o_id, oOrderValues, oOrderIsNulls)) {
                    assert(0);
                }
                int o_ol_cnt = oOrderValues[O_OL_CNT];
                /* Step 3: Get orderline count */
                uint32_t orderlineCnt;
                GetOrderOrderLineCount(w_id, d_id, o_id, orderlineCnt);
                if (o_ol_cnt != int(orderlineCnt)) {
                    std::cout << "Consistency Condition 6 Failed!" << std::endl;
                    return;
                }
            }
        }
    }
}

/*
 * Consistency Condition 7
 * For any row in the ORDER-LINE table, OL_DELIVERY_D is set to a null date/ time if and only if the corresponding
 * row in the ORDER table defined by (O_W_ID, O_D_ID, O_ID) = (OL_W_ID, OL_D_ID, OL_O_ID) has
 * O_CARRIER_ID set to a null value.
 */
void TpccStorage::CheckCondition7(int wh_start, int wh_end) {
    for (int w_id = wh_start; w_id <= wh_end; w_id++) {
        for (int d_id = 1; d_id <= TPCC_DISTICT_PER_WAREHOUSE; d_id++) {
            /* Step 1: Get next order id */
            Datum districtValues[TpccTableDesc[TABLE_DISTRICT].colNum];
            bool districtIsNulls[TpccTableDesc[TABLE_DISTRICT].colNum];
            SelectDistrict(w_id, d_id, districtValues, districtIsNulls);
            int d_next_o_id = districtValues[D_NEXT_O_ID];
            Datum oOrderValues[GetTableColMax(TABLE_ORDER)];
            bool oOrderIsNulls[GetTableColMax(TABLE_ORDER)];
            Datum orderlineValues[GetTableColMax(TABLE_ORDERLINE)];
            bool orderlineIsNulls[GetTableColMax(TABLE_ORDERLINE)];
            for(int o_id = 1; o_id < d_next_o_id; o_id++) {
                /* Step 2: Get whether the carrier id is null */
                if (!SelectOrderTable(w_id, d_id, o_id, oOrderValues, oOrderIsNulls)) {
                    assert(0);
                }
                bool carrierIdIsNull = (oOrderValues[O_CARRIER_ID] == 0) ? true : false;
                int o_ol_cnt = oOrderValues[O_OL_CNT];
                /* Step 3: Get whether orderline delivery date is null */
                for(int ol_number = 1; ol_number <= o_ol_cnt; ol_number++){
                    SelectOrderlineTable(w_id, d_id, o_id, ol_number, orderlineValues, orderlineIsNulls);
                    bool deliveryDIsNull = orderlineIsNulls[OL_DELIVERY_D];
                    if (carrierIdIsNull != deliveryDIsNull) {
                        std::cout << "Consistency Condition 7 Failed!" << std::endl;
                        return;
                    }
                }
            }
        }
    }
}

} /* The end of TPCC_BENCHMARK */