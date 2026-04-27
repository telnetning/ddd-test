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
 * ---------------------------------------------------------------------------------------
 *
 * dstore_logical_decode_worker.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_LOGICAL_DECODE_WORKER_H
#define DSTORE_LOGICAL_DECODE_WORKER_H

#include <thread>

#include "common/dstore_datatype.h"
#include "common/algorithm/dstore_ilist.h"
#include "catalog/dstore_fake_namespace.h"
#include "common/dstore_common_utils.h"
#include "wal/dstore_wal.h"
#include "dstore_logical_queue.h"
#include "dstore_decode_plugin.h"
#include "dstore_decode_dict.h"
#include "logical_replication/dstore_logical_replication_struct.h"

namespace DSTORE {

#ifdef ENABLE_LOGICAL_REPL
/* namespace info updated in trx */
struct TrxInternalNsp {
    Oid nspId;
    DstoreNameData nspName;
    CommandId cid;
    dlist_node nspNode;
    bool isDelete;
};

/* attr info updated in trx */
struct TrxInternalAttr {
    dlist_node attrNode;
    FormData_pg_attribute attr;
};

/* internal table info updated in trx */
struct TrxInternalTblInfo {
    Oid tableOid;
    DstoreNameData relName;
    bool relHasOids;
    Oid nspId;
    DstoreNameData nspName;
    CommandId cid;
    bool isDelete;
    int natts;
    dlist_head attrs;   /* TrxInternalAttrs list */
    dlist_node tableInfoNode;

    void UpdateRelationInfo(DecodeRelationData *relationData)
    {
        StorageAssert(tableOid == DSTORE_INVALID_OID || tableOid == relationData->tableOid);
        tableOid = relationData->tableOid;
        nspId = relationData->nspId;
        errno_t rc = memcpy_s(relName.data, NAME_DATA_LEN, relationData->relName.data, NAME_DATA_LEN);
        storage_securec_check(rc, "\0", "\0");
        natts = relationData->natts;
        relHasOids = relationData->relHasOids;
    }

    void CopyData(TrxInternalTblInfo *copyTo)
    {
        copyTo->tableOid = tableOid;
        errno_t rc = memcpy_s(copyTo->relName.data, NAME_DATA_LEN, relName.data, NAME_DATA_LEN);
        storage_securec_check(rc, "\0", "\0");
        copyTo->nspId = nspId;
        rc = memcpy_s(copyTo->nspName.data, NAME_DATA_LEN, nspName.data, NAME_DATA_LEN);
        storage_securec_check(rc, "\0", "\0");
        copyTo->cid = cid;
        copyTo->isDelete = isDelete;
        copyTo->natts = natts;
        DListInit(&copyTo->attrs);
        dlist_iter iter;
        dlist_foreach(iter, &attrs) {
            TrxInternalAttr *baseAttr = dlist_container(TrxInternalAttr, attrNode, iter.cur);
            TrxInternalAttr *copyAttr = static_cast<TrxInternalAttr *>(DstorePalloc(sizeof(TrxInternalAttr)));
            StorageReleasePanic(copyAttr == nullptr, MODULE_LOGICAL_REPLICATION,
                                ErrMsg("CopyAttr alloc memory failed."));
            rc = memcpy_s(&copyAttr->attr, ATTRIBUTE_FIXED_SIZE,
                          &baseAttr->attr, ATTRIBUTE_FIXED_SIZE);
            storage_securec_check(rc, "", "");
            DListPushTail(&copyTo->attrs, &copyAttr->attrNode);
        }
    }

    bool DeleteAttr(const int attrNum)
    {
        dlist_mutable_iter iter;
        dlist_foreach_modify(iter, &attrs) {
            TrxInternalAttr *baseAttr = dlist_container(TrxInternalAttr, attrNode, iter.cur);
            if (baseAttr->attr.attnum == attrNum) {
                DListDelete(iter.cur);
                DstorePfree(baseAttr);
                return true;
            }
        }
        return false;
    }

    TrxInternalAttr* FindAttr(int attrNum)
    {
        dlist_iter iter;
        dlist_foreach(iter, &attrs) {
            TrxInternalAttr *baseAttr = dlist_container(TrxInternalAttr, attrNode, iter.cur);
            if (baseAttr->attr.attnum == attrNum) {
                return baseAttr;
            }
        }
        return nullptr;
    }

    DecodeTableInfo* ConvertToDecodeTableInfo()
    {
        DecodeTableInfo *tableInfo = DecodeTableInfo::Create(natts);
        DecodeTableInfoStatus status = (isDelete ? DecodeTableInfoStatus::DELETE : DecodeTableInfoStatus::COLLECTED);
        tableInfo->Init(tableOid, INVALID_CSN, status);
        tableInfo->InitDesc(natts, relHasOids);
        tableInfo->SetRelName(relName.data);
        tableInfo->SetNsp(nspId, nspName.data);
        int attrCount = 0;         /* for check */
        dlist_mutable_iter iter;
        dlist_foreach_modify (iter, &attrs) {
            TrxInternalAttr *attrInfo = dlist_container(TrxInternalAttr, attrNode, iter.cur);
            errno_t rc = memcpy_s(tableInfo->fakeDescData.attrs[attrInfo->attr.attnum], MAXALIGN(ATTRIBUTE_FIXED_SIZE),
                                  &(attrInfo->attr),  ATTRIBUTE_FIXED_SIZE);
            storage_securec_check(rc, "\0", "\0");
            attrCount += 1;
        }
        StorageAssert(attrCount == natts);
        return tableInfo;
    }
};

/* constructed decodeTableInfo in trx (normal decode tableInfo + cid) */
struct TrxInternalDecodeTblInfo {
    CommandId cid;
    DecodeTableInfo *tableInfo;
    dlist_node tableInfoNode;
};

struct TrxInternalDecodeTblInfoEnt {
    Oid tableOid;
    dlist_node *tableInfoNode;
};

struct ParallelDecodeWorkerInitParam {
    int workerId;
    DstoreMemoryContext decodeMctx;
    DecodeOptions *decodeOptions;
    DecodePlugin *decodePlugin;
    DecodeDict *decodeDict;
};
 
class ParallelDecodeWorker : public BaseObject {
public:
    explicit ParallelDecodeWorker(const ParallelDecodeWorkerInitParam &initParam, const PdbId pdbId);
    ~ParallelDecodeWorker() = default;
    RetStatus Init();
    void Destroy();
    void Run();
    void Stop() noexcept;
    inline bool IsRunning()
    {
        return m_isRunningFlag.load();
    }
    void Report() const;
    inline void SetWalDispatcher(class WalDispatcher *walDispacher)
    {
        m_walDispatcher = walDispacher;
    }
    void QueueTrx(TrxChangeCtx *trx);
    TrxLogicalLog* PopTrxLogicalLog();
private:
    void WorkerMain(PdbId pdbId);
    RetStatus DecodeTrx(TrxLogicalLog *trxOut, TrxChangeCtx *trxChange);
    RetStatus DecodeTrxWithoutDDLInternal(TrxLogicalLog *trxOut, TrxChangeCtx *trxChange);
    RetStatus DecodeTrxWithDDLInternal(TrxLogicalLog *trxOut, TrxChangeCtx *trxChange);
    DecodeTableInfo* GetTableInfoFromDecodeDict(RowChange *rowChange) const;
    void CollectTableInfoFromTuples(TrxChangeCtx *trxChange);
    void ConvertTrxInternalTblInfoToDecodeTableInfo(TrxChangeCtx *trxChange);
    void ConvertDecodeTableInfoToTrxInternalTableInfo(DecodeTableInfo *decodeTableInfo,
                                                      TrxInternalTblInfo *trxInternalTableInfo) const;
    void FindCreateTableOpAndAllocMem(TrxChangeCtx *trxChange);
    void BuildNameSpaceInfo(TrxChangeCtx *trxChange, RowChange *rowChange);
    void BuildRelationInfo(TrxChangeCtx *trxChange, RowChange *rowChange);
    void CollectRelationChangeToTrxInternalTblInfo(RowChange *rowChange, TrxInternalTblInfo *internalTableInfo);
    void CollectSysRelChange(RowChange *rowChange, TrxInternalTblInfo *internalTableInfo);
    void CollectSysAttrChange(RowChange *rowChange, TrxInternalTblInfo *internalTableInfo) const;
    void InitDecodeTableInfoHash(TrxChangeCtx *trxChange);
    void UpdateDecodeTableInfoHash(TrxChangeCtx *trxChange, RowChange *rowChange) const;
    void SetNspInfoVisible(TrxChangeCtx *trxChange, const Oid nspId,
                           const DstoreNameData nspName, const CommandId cid) const;
    TrxInternalTblInfo* FindNewestTrxInternalTblInfo(TrxChangeCtx *trxChange,
                                                     Oid tableOid, CommandId cid = INVALID_CID) const;
    TrxInternalNsp* FindNewestTrxInternalNsp(TrxChangeCtx *trxChange, Oid nspId, CommandId cid = INVALID_CID) const;
    void UpdateDecodeDict(TrxChangeCtx *trxChange);
    Oid GetDDLTableOid(RowChange *rowChange) const;
    void GetNspInfo(RowChange *rowChange, Oid &nspId, DstoreNameData &nspName) const;

    DstoreMemoryContext m_privateDecodeMctx;
    PdbId m_pdbId;
    int m_workerId;
    std::atomic<bool> m_isRunningFlag;
    std::atomic_bool m_needStopFlag;
    std::thread *m_workerThrd;

    /* To be decoded queue, organized in transaction unit */
    LogicalQueue *m_trxChangesQueue;
    /* Decoded results queue, organized in transaction unit */
    LogicalQueue *m_trxLogicalLogQueue;
    /* WalDispatcher pointer, to release trxChange memory etc */
    class WalDispatcher *m_walDispatcher;

    /* decode dependency */
    DecodeDict *m_decodeDict;
    DecodeOptions *m_decodeOptions;
    DecodePlugin *m_decodePlugin;
};

#endif
}
#endif