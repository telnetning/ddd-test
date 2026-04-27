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
 *
 * IDENTIFICATION
 *        storage/include/logical_replication/dstore_logical_meta.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_DECODE_DICT_H
#define DSTORE_DECODE_DICT_H

#include "common/dstore_common_utils.h"
#include "logical_replication/dstore_logical_replication_struct.h"
#include "framework/dstore_vfs_adapter.h"
#include "dstore_decode_dict_file.h"
#include "lock/dstore_lwlock.h"

#ifdef UT
#ifndef ENABLE_LOGICAL_REPL
/* LOGICAL REPL is not supported for now. */
#define ENABLE_LOGICAL_REPL
#endif
#endif

namespace DSTORE {

#ifdef ENABLE_LOGICAL_REPL
/* catalogs that decode dict is intersted in */
constexpr Oid DECODE_DICT_COLLECT_SYSTABLE_OID[] = {SYSTABLE_RELATION_OID, SYSTABLE_ATTRIBUTE_OID, PG_NAMESPACE_OID};
extern bool IsLogicalDecodeDictNeeded(Oid tableId);

constexpr const uint32 DEFAULT_TABLES_PER_DECODE_DICT = 50;

/* control the catalog info collection */
struct SyncCatalogOpts {
};

/* data in pg_class tuple which decode based on. i.e, collect from sys_relation tuple */
struct DecodeRelationData {
    Oid tableOid;
    DstoreNameData relName;
    bool relHasOids;
    Oid nspId;
    int natts;
};

/* struct used for logical decoding */
struct DecodeTableInfo {
    Oid tableOid;
    DstoreNameData relName;
    Oid nspId;
    DstoreNameData nspName;
    CommitSeqNo csn;           /* DecodeTableInfo version */
    DecodeTableInfoStatus status;
    TupleDescData fakeDescData;

    inline void Init(Oid relId, CommitSeqNo ddlCsn, DecodeTableInfoStatus curStatus)
    {
        tableOid = relId;
        csn = ddlCsn;
        status = curStatus;
        relName.data[0] = '\0';
        nspName.data[0] = '\0';
        nspId = DSTORE_INVALID_OID;
    }

    inline void InitDesc(int natts, bool relHasOids)
    {
        fakeDescData.natts = natts;
        fakeDescData.tdisredistable = false;
        fakeDescData.tdtypeid = RECORDOID;
        fakeDescData.tdtypmod = -1;
        fakeDescData.tdhasuids = false;
        fakeDescData.tdrefcount = -1;
        fakeDescData.initdefvals = nullptr;
        fakeDescData.tdhasoid = relHasOids;
    }

    DecodeTableInfo* Copy()
    {
        DecodeTableInfo *copyTableInfo = Create(fakeDescData.natts);
        copyTableInfo->Init(tableOid, csn, status);
        copyTableInfo->InitDesc(fakeDescData.natts, fakeDescData.tdhasoid);
        copyTableInfo->SetRelName(relName.data);
        copyTableInfo->SetNsp(nspId, nspName.data);
        errno_t rc;
        for (int i = 0; i < fakeDescData.natts; i++) {
            rc = memcpy_s(copyTableInfo->fakeDescData.attrs[i], MAXALIGN(ATTRIBUTE_FIXED_SIZE),
                          fakeDescData.attrs[i], MAXALIGN(ATTRIBUTE_FIXED_SIZE));
            storage_securec_check(rc, "\0", "\0");
        }
        return copyTableInfo;
    }

    inline TupleDesc GetTupleDesc()
    {
        return &fakeDescData;
    }

    inline const char *GetNspName() const
    {
        return nspName.data;
    }

    inline void SetNsp(const Oid spaceId, const char *spaceName)
    {
        nspId = spaceId;
        errno_t rc = memcpy_s(nspName.data, NAME_DATA_LEN, spaceName, NAME_DATA_LEN);
        storage_securec_check(rc, "\0", "\0");
    }

    inline const char *GetRelName() const
    {
        return relName.data;
    }

    inline void SetRelName(const char *relname)
    {
        errno_t rc = memcpy_s(relName.data, NAME_DATA_LEN, relname, NAME_DATA_LEN);
        storage_securec_check(rc, "\0", "\0");
    }

    int GetAttrNums() const
    {
        return fakeDescData.natts;
    }

    static DecodeTableInfo *Create(int natts)
    {
        Size attrPointerSize = static_cast<uint32>(natts) * sizeof(Form_pg_attribute);
        Size decodeTblSize = MAXALIGN(sizeof(DecodeTableInfo) + attrPointerSize);
        Size attrSize = static_cast<uint32>(natts) * MAXALIGN(ATTRIBUTE_FIXED_SIZE);
        char *stg = static_cast<char *>(DstorePalloc0(decodeTblSize + attrSize));
        StorageReleasePanic(stg == nullptr, MODULE_LOGICAL_REPLICATION,
                            ErrMsg("Allocate space OOM, allocate stg fail."));
        DecodeTableInfo *tableInfo = static_cast<DecodeTableInfo *>(static_cast<void *>(stg));
        tableInfo->fakeDescData.attrs = static_cast<Form_pg_attribute *>(
            static_cast<void *>(stg + sizeof(DecodeTableInfo)));

        char *attrOffset = stg + decodeTblSize;
        /* make fakeDescData.attrs pointer valid */
        for (int i = 0; i < natts; i++) {
            tableInfo->fakeDescData.attrs[i] = static_cast<Form_pg_attribute>(static_cast<void *>(attrOffset));
            attrOffset += MAXALIGN(ATTRIBUTE_FIXED_SIZE);
        }
        return tableInfo;
    }

    static DecodeTableInfo* ConvertFromItem(DecodeTableInfoDiskData *item)
    {
        if (unlikely(item == nullptr)) {
            return nullptr;
        }
        DecodeTableInfo *tableInfo = Create(item->natts);
        if (unlikely(tableInfo == nullptr)) {
            return nullptr;
        }
        tableInfo->Init(item->tableOid, item->csn, item->status);
        tableInfo->InitDesc(item->natts, item->relHasOids);
        tableInfo->SetRelName(item->relName);
        tableInfo->SetNsp(item->nspId, item->nspName);

        char *attr = item->attrsData;
        for (int i = 0; i < item->natts; i++) {
            Form_pg_attribute cur = static_cast<Form_pg_attribute>(static_cast<void *>(attr));
            StorageAssert(cur->attrelid == item->tableOid);
            UNUSED_VARIABLE(cur);
            errno_t rc = memcpy_s(tableInfo->fakeDescData.attrs[i], MAXALIGN(ATTRIBUTE_FIXED_SIZE),
                                  attr,  ATTRIBUTE_FIXED_SIZE);
            storage_securec_check(rc, "\0", "\0");
            attr += ATTRIBUTE_FIXED_SIZE;
        }
        return tableInfo;
    }

    static DecodeTableInfoDiskData* ConvertToItem(DecodeTableInfo* tableInfo)
    {
        if (unlikely(tableInfo == nullptr)) {
            return nullptr;
        }
        uint16 itemSz = static_cast<uint16>(sizeof(DecodeTableInfoDiskData)) +
            static_cast<uint16>(tableInfo->fakeDescData.natts) * static_cast<uint16>(ATTRIBUTE_FIXED_SIZE);
        DecodeTableInfoDiskData *itemData = static_cast<DecodeTableInfoDiskData *>(DstorePalloc0(itemSz));
        if (unlikely(itemData == nullptr)) {
            return nullptr;
        }
        itemData->size = itemSz;
        itemData->tableOid = tableInfo->tableOid;
        errno_t rc = memcpy_s(itemData->relName, NAME_DATA_LEN, tableInfo->relName.data, NAME_DATA_LEN);
        storage_securec_check(rc, "\0", "\0");
        itemData->nspId = tableInfo->nspId;
        rc = memcpy_s(itemData->nspName, NAME_DATA_LEN, tableInfo->nspName.data, NAME_DATA_LEN);
        storage_securec_check(rc, "\0", "\0");
        itemData->csn = tableInfo->csn;
        itemData->status = tableInfo->status;
        itemData->relHasOids = tableInfo->fakeDescData.tdhasoid;
        itemData->natts = tableInfo->fakeDescData.natts;
        int offset = 0;
        for (int i = 0; i < tableInfo->fakeDescData.natts; i++) {
            rc = memcpy_s(itemData->attrsData + offset, ATTRIBUTE_FIXED_SIZE,
                          static_cast<void *>(tableInfo->fakeDescData.attrs[i]), ATTRIBUTE_FIXED_SIZE);
            storage_securec_check(rc, "\0", "\0");
            offset += ATTRIBUTE_FIXED_SIZE;
        }
        return itemData;
    }
};

struct DecodeTableInfoPos {
    DecodeTableInfo *tableInfo;
    BlockNumber block;
    dlist_node posNode;

    void Init(DecodeTableInfo *initInfo, BlockNumber initBlock)
    {
        tableInfo = initInfo;
        block = initBlock;
        DListNodeInit(&posNode);
    }
};

struct DecodeTableInfoEntry {
    Oid tableOid;
    int versionNum;
    dlist_head head; /* DecodeTableInfoPos list, sort by csn */

    void Init(Oid tableId)
    {
        tableOid = tableId;
        versionNum = 0;
        DListInit(&head);
    }
};

class DecodeDict : public BaseObject {
public:
    explicit DecodeDict(PdbId pdbId, DstoreMemoryContext mctx);
    virtual ~DecodeDict();
    virtual RetStatus Init();
    void Destroy();

    /**
     * Synchronize Catalog info, the function is called by logical slot creation process(after find consistency point).
     * @param Form_pg_class tableDef.
     * @param TupleDesc TupleDesc formed by outside.
     * @param Form_pg_namespace nspDef.
     * @param tableInfoCsn this tableInfo csn.
     * @return
     */
    virtual RetStatus SynchronizeCatalog(CatalogInfo *rawCatalog);

    /**
     * catalog change csn, concurrent called with ddl trx.
     * @param tableOid Oid in sys_realtion
     * @param dictChangeCsn
     * @return
     */
    virtual RetStatus CollectDecodeDictChange(Oid tableOid, CommitSeqNo dictChangeCsn);

    /**
     * Collect relation change from walrecord to keep decode dict fresh.
     * @param tableInfo catalog change.
     * @return
     */
    virtual RetStatus UpdateDecodeDictChangeFromWal(DecodeTableInfo *tableInfo);

    /**
     * Get corresponding tableInfo version, called by logical decoding.
     * @param tableOid Oid in sys_relation
     * @param snapshotCsn snapshot csn in walrecord.
     * @return nullptr if the DecodeTableInfo has not been decoeded,
     * this only happens on multi-walstream condition or parallel decode worker with much difference in speed.
     */
    DecodeTableInfo* GetVisibleDecodeTableInfo(Oid tableOid, CommitSeqNo snapshotCsn);
    /**
     * Get useful decode info in sys_relation tuple.
     * @param sysRelTup sys_relation tuple
     * @return relationData
     */
    void GetInfoFromSysRelationTuple(const HeapTuple &sysRelTup, DecodeRelationData &relationData) const;
    void GetALLDecodeTableInfoVersion(Oid tableOid, StringInfo buf);
#ifndef UT
protected:
#endif
    void LoadDecodeTableInfoFromDisk();
    DecodeTableInfoEntry* FindDecodeTableInfoPos(Oid tableOid, CommitSeqNo ddlCsn, bool forUpdate,
        DecodeTableInfoPos* &outPos);
    RetStatus CreateDecodeTableInfo(DecodeTableInfo *tableInfo);
    PdbId m_pdbId;
    DstoreMemoryContext m_memoryContext;
    DecodeDictFile *m_decodeDictFile;
    HTAB *m_tableInfo; /* for efficiency */
    LWLock m_bufferLock; /* single node lock, this will be also used in ditribute decode dict */
};
#endif

}
#endif