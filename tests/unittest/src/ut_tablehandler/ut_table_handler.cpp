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
#include "ut_tablehandler/ut_table_handler.h"
#include "ut_tablespace/ut_segment.h"
#include "heap/dstore_heap_scan.h"
#include "table/dstore_table_interface.h"
#include "heap/dstore_heap_interface.h"
#include "errorcode/dstore_page_error_code.h"
#include "systable/dstore_relation.h"
#include "tablespace/dstore_index_temp_segment.h"

namespace DSTORE {

thread_local UTTableHandler *ThdUtTableHandler = nullptr;

/* private constructor */
UTTableHandler::UTTableHandler(DstoreMemoryContext memCxt, bool allocateNew) :
    m_tablespaceId(INVALID_TABLESPACE_ID),
    m_heapSegmentPageId(INVALID_PAGE_ID),
    m_lobSegmentPageId(INVALID_PAGE_ID),
    m_indexSegmentPageId(INVALID_PAGE_ID),
    m_heapTestContext(nullptr),
    m_btreeTestContext(nullptr),
    m_utTableHandlerMemCxt(memCxt),
    m_allocated(allocateNew)
{}

/* nattsCustomized and attTypePidListCustomized used for customizing TableHandler,
 * caller must define the columns num (attrCustomized) and atttypid of each column,
 * and need to process the heap tuple data by self */
UTTableHandler *UTTableHandler::CreateTableHandler(PdbId pdbId, DstoreMemoryContext memCxt, bool needLob, bool isTempTableSpace,
                                                   int nattsCustomized, Oid *attTypePidListCustomized)
{
    /* DO NOT share UTTableHandler between threads!!! */
    UTTableHandler *utTableHandler = UTTableHandler::Create(pdbId, memCxt, true, isTempTableSpace);
    utTableHandler->m_lobSegmentPageId = INVALID_PAGE_ID;
    if (!isTempTableSpace) {
        /* IndexNormalSegment would be create when calling CreateBtreeContext or CreateIndex */
        HeapNormalSegment *heapSegment =
            (HeapNormalSegment*)SegmentTest::UTAllocSegment(pdbId, utTableHandler->m_tablespaceId, g_storageInstance->GetBufferMgr(), SegmentType::HEAP_SEGMENT_TYPE);
        utTableHandler->m_heapSegmentPageId = heapSegment->GetSegmentMetaPageId();
        /* Must delete DataSegment here because *StorageMmgr would init a new one */
        delete heapSegment;

        if (needLob) {
            HeapNormalSegment *lobSegment =
                (HeapNormalSegment*)SegmentTest::UTAllocSegment(pdbId, utTableHandler->m_tablespaceId, g_storageInstance->GetBufferMgr(), SegmentType::HEAP_SEGMENT_TYPE);
            utTableHandler->m_lobSegmentPageId = lobSegment->GetSegmentMetaPageId();
            delete lobSegment;
        }
        
    } else {
        HeapTempSegment *heapSegment =
            (HeapTempSegment*)SegmentTest::UTAllocSegment(pdbId, utTableHandler->m_tablespaceId, thrd->GetTmpLocalBufMgr(), SegmentType::HEAP_TEMP_SEGMENT_TYPE);
        utTableHandler->m_heapSegmentPageId = heapSegment->GetSegmentMetaPageId();
        delete heapSegment;

        if (needLob) {
            HeapTempSegment *lobSegment =
                (HeapTempSegment*)SegmentTest::UTAllocSegment(pdbId, utTableHandler->m_tablespaceId, thrd->GetTmpLocalBufMgr(), SegmentType::HEAP_TEMP_SEGMENT_TYPE);
            utTableHandler->m_lobSegmentPageId = lobSegment->GetSegmentMetaPageId();
            /* Must delete DataSegment here because *StorageMmgr would init a new one */
            delete lobSegment;
        }
    }
    utTableHandler->CreateTableContext(pdbId, nattsCustomized, attTypePidListCustomized, needLob);
    return utTableHandler;
}

UTTableHandler *UTTableHandler::GetTableHandler(PdbId pdbId, DstoreMemoryContext memCxt, PageId heapSegPageId,
    PageId lobSegPageId, PageId indexSegPageId, bool isTempTableSpace, int nattsCustomized,
    Oid *attTypePidListCustomized)
{
    /* DO NOT share UTTableHandler between threads!!! */
    UTTableHandler *utTableHandler = UTTableHandler::Create(pdbId, memCxt, false, isTempTableSpace);

    utTableHandler->m_heapSegmentPageId = heapSegPageId;
    utTableHandler->m_lobSegmentPageId = lobSegPageId;
    utTableHandler->m_indexSegmentPageId = indexSegPageId;

    utTableHandler->CreateTableContext(
        pdbId, nattsCustomized, attTypePidListCustomized, lobSegPageId != INVALID_PAGE_ID);
    /* IndexNormalSegment would be create when calling CreateBtreeContext or CreateIndex */
    return utTableHandler;
}

UTTableHandler *UTTableHandler::CreatePartitionTableHandler(PdbId pdbId, DstoreMemoryContext memCxt, int numPartition)
{
    /* DO NOT share UTTableHandler between threads!!! */
    EXPECT_GT(numPartition, 1);
    UTTableHandler *utTableHandler = UTTableHandler::Create(pdbId, memCxt, true, false);
    utTableHandler->CreatePartitionedTableContext(numPartition);
    /* IndexNormalSegment would be create when calling CreateBtreeContext or CreateIndex */
    return utTableHandler;
}

UTTableHandler *UTTableHandler::Create(PdbId pdbId, DstoreMemoryContext memCxt, bool allocateNew, bool isTempTableSpace)
{
    DstoreMemoryContext utMemCxt = DstoreAllocSetContextCreate(memCxt, "UTTableHandlerMemCxt", ALLOCSET_DEFAULT_SIZES);
    UTTableHandler *utTableHandler = DstoreNew(memCxt) UTTableHandler(utMemCxt, allocateNew);

    utTableHandler->m_tablespaceId = isTempTableSpace ? static_cast<TablespaceId>(TBS_ID::TEMP_TABLE_SPACE_ID)
                                                      : static_cast<TablespaceId>(TBS_ID::DEFAULT_TABLE_SPACE_ID);
    utTableHandler->m_pdbId = pdbId;

    return utTableHandler;
}

void UTTableHandler::Destroy(UTTableHandler *utTableHandler)
{
    if (unlikely(utTableHandler == nullptr)) {
        return;
    }
    if (utTableHandler->m_allocated) {
        if (utTableHandler->m_heapTestContext && utTableHandler->m_heapTestContext->tableRel) {
            if (utTableHandler->m_heapTestContext->numHeapPartition > 1) {
                StorageRelation *heapRels = (StorageRelation *)(utTableHandler->m_heapTestContext->tableRel);
                for (int i = 0; i < utTableHandler->m_heapTestContext->numHeapPartition; i++) {
                    if (heapRels[i]->tableSmgr != nullptr) {
                        heapRels[i]->tableSmgr->m_segment->DropSegment();
                    }
                    if (heapRels[i]->lobTableSmgr != nullptr) {
                        heapRels[i]->lobTableSmgr->m_segment->DropSegment();
                    }
                    DstorePfreeExt(heapRels[i]);
                }
            } else {
                StorageRelation heapRel = utTableHandler->m_heapTestContext->tableRel;
                heapRel->tableSmgr->m_segment->DropSegment();
                if (heapRel->lobTableSmgr != nullptr) {
                    heapRel->lobTableSmgr->m_segment->DropSegment();
                }
            }
            DstorePfreeExt(utTableHandler->m_heapTestContext->tableRel);
            DstorePfreeExt(utTableHandler->m_heapTestContext->tupleDesc);
            DstorePfreeExt(utTableHandler->m_heapTestContext);
        }
        if (utTableHandler->m_btreeTestContext && utTableHandler->m_btreeTestContext->indexRel->btreeSmgr) {
            StorageTableInterface::DestroyBtreeSmgr(utTableHandler->m_btreeTestContext->indexRel->btreeSmgr);
            DstorePfreeExt(utTableHandler->m_btreeTestContext->indexRel);
        }
    }

    DstoreMemoryContext memCxt = utTableHandler->m_utTableHandlerMemCxt;
    DstoreMemoryContextDelete(memCxt);
    delete utTableHandler;
}

void UTTableHandler::Clear()
{
    if (m_heapTestContext && m_heapTestContext->tableRel != nullptr) {
        if (m_heapTestContext->numHeapPartition > 1) {
            StorageRelation *heapRels = (StorageRelation *)(m_heapTestContext->tableRel);
            for (int i = 0; i < m_heapTestContext->numHeapPartition; i++) {
                /* Do not drop! just clear memory */
                delete heapRels[i]->tableSmgr;
                delete heapRels[i]->lobTableSmgr;
                DstorePfreeExt(heapRels[i]);
            }
        } else {
            delete m_heapTestContext->tableRel->tableSmgr;
            delete m_heapTestContext->tableRel->lobTableSmgr;
        }
        DstorePfreeExt(m_heapTestContext->tableRel);
        DstorePfreeExt(m_heapTestContext->tupleDesc);
        DstorePfreeExt(m_heapTestContext);
    }
    if (m_btreeTestContext && m_btreeTestContext->indexRel->btreeSmgr) {
        m_btreeTestContext->indexRel->btreeSmgr->m_segment->DropSegment();
        DstorePfreeExt(m_btreeTestContext->indexRel);
        DstorePfreeExt(m_btreeTestContext->scanKeyInfo);
        DstorePfreeExt(m_btreeTestContext->indexBuildInfo);
        DstorePfreeExt(m_btreeTestContext);
    }

    m_heapTestContext = nullptr;
    m_btreeTestContext = nullptr;
}

/* nattsCustomized and attTypePidListCustomized used for customizing TableHandler */
void UTTableHandler::CreateTableContext(PdbId pdbId, int nattsCustomized, Oid *attTypePidListCustomized, bool needLob)
{
    RelationPersistence persistenceMod;
    TableSpace *tablespace =
        (TableSpace *)UtMockModule::UtGetTableSpace(m_tablespaceId, nullptr, DSTORE_NO_LOCK);
    if (!tablespace->IsTempTbs()) {
        persistenceMod = SYS_RELPERSISTENCE_PERMANENT;
    } else {
        persistenceMod = SYS_RELPERSISTENCE_GLOBAL_TEMP;
    }

    /* create table storage manager for heap table */
    TableStorageMgr *tableSmgr = DstoreNew(m_utTableHandlerMemCxt) TableStorageMgr(pdbId,
        DEFAULT_HEAP_FILLFACTOR,
        GenerateFakeHeapTupDesc(nattsCustomized, attTypePidListCustomized),
        persistenceMod);
    tableSmgr->Init(m_heapSegmentPageId, m_tablespaceId, m_utTableHandlerMemCxt);

    TableStorageMgr *lobTableSmgr = nullptr;
    if (needLob) {
        lobTableSmgr = DstoreNew(m_utTableHandlerMemCxt)
            TableStorageMgr(pdbId, DEFAULT_HEAP_FILLFACTOR, GenerateFakeLobTupDesc(), persistenceMod);
        lobTableSmgr->Init(m_lobSegmentPageId, m_tablespaceId, m_utTableHandlerMemCxt);
    }
    UtMockModule::UtDropTableSpace(tablespace, DSTORE_NO_LOCK);

    AutoMemCxtSwitch autoMemCxtSwitch{m_utTableHandlerMemCxt};
    /* Init heapTestContext */
    m_heapTestContext = (HeapTestContext *)DstorePalloc(sizeof(HeapTestContext));
    m_heapTestContext->tableRel = (StorageRelation)DstorePalloc(sizeof(StorageRelationData));
    m_heapTestContext->tableRel->Init();
    m_heapTestContext->tableRel->tableSmgr = tableSmgr;
    m_heapTestContext->tableRel->lobTableSmgr = lobTableSmgr;
    m_heapTestContext->tableRel->m_pdbId = pdbId;
    m_heapTestContext->tupleDesc = tableSmgr->GetTupleDesc()->Copy();
    m_heapTestContext->numHeapPartition = 1; /* non-partition table */
}

void UTTableHandler::CreatePartitionedTableContext(int numPartition)
{
    AutoMemCxtSwitch autoMemCxtSwitch{m_utTableHandlerMemCxt};
    /* create tableSmgr for partitions */
    TableStorageMgr *heapSmgr = nullptr;
    PageId heapSegmentPageId = INVALID_PAGE_ID;
    StorageRelation *heapRels = (StorageRelation *)DstorePalloc(sizeof(StorageRelation *) * numPartition);
    for (int i = 0; i < numPartition; i++) {
        HeapNormalSegment *heapSegment =
            (HeapNormalSegment*)SegmentTest::UTAllocSegment(m_pdbId, m_tablespaceId, g_storageInstance->GetBufferMgr(), SegmentType::HEAP_SEGMENT_TYPE);
        EXPECT_NE(heapSegment->GetSegmentMetaPageId(), INVALID_PAGE_ID);
        EXPECT_NE(heapSegmentPageId, heapSegment->GetSegmentMetaPageId());
        heapSegmentPageId = heapSegment->GetSegmentMetaPageId();
        delete heapSegment;
        heapSmgr = DstoreNew(m_utTableHandlerMemCxt)
            TableStorageMgr(m_pdbId, DEFAULT_HEAP_FILLFACTOR, GenerateFakeHeapTupDesc(), SYS_RELPERSISTENCE_PERMANENT);
        heapSmgr->Init(heapSegmentPageId, m_tablespaceId, m_utTableHandlerMemCxt);
        heapRels[i] = (StorageRelation)DstorePalloc(sizeof(StorageRelationData));
        heapRels[i]->Init();
        heapRels[i]->tableSmgr = heapSmgr;
        heapRels[i]->m_pdbId = m_pdbId;
    }

    /* Init heapTestContext */
    m_heapTestContext = (HeapTestContext *)DstorePalloc(sizeof(HeapTestContext));
    m_heapTestContext->tupleDesc = GenerateFakeHeapTupDesc();
    m_heapTestContext->numHeapPartition = numPartition;
    m_heapTestContext->tableRel = (StorageRelation)heapRels;
}

void UTTableHandler::CreateBtreeContext(int *colNums, int numKeyAttrs, bool unique, int fillfactor, int numAttr)
{
    AutoMemCxtSwitch autoMemCxtSwitch{m_utTableHandlerMemCxt};
    if (m_allocated) {
        TableSpace *tablespace = (TableSpace *)UtMockModule::UtGetTableSpace(m_tablespaceId);
        if (!tablespace->IsTempTbs()) {
            IndexNormalSegment *indexSegment = (IndexNormalSegment*)SegmentTest::UTAllocSegment(m_pdbId,
                m_tablespaceId, g_storageInstance->GetBufferMgr(), SegmentType::INDEX_SEGMENT_TYPE);
            m_indexSegmentPageId = indexSegment->GetSegmentMetaPageId();
            delete indexSegment;
        } else {
            IndexTempSegment *indexSegment = (IndexTempSegment*)SegmentTest::UTAllocSegment(m_pdbId, m_tablespaceId,
                thrd->GetTmpLocalBufMgr(), SegmentType::INDEX_SEGMENT_TYPE);
            m_indexSegmentPageId = indexSegment->GetSegmentMetaPageId();
            delete indexSegment;
        }
        UtMockModule::UtDropTableSpace(tablespace);
    } else {
        ASSERT_NE(m_indexSegmentPageId, INVALID_PAGE_ID);
    }

    RelationPersistence persistenceMod;
    TableSpace *tablespace = (TableSpace *)UtMockModule::UtGetTableSpace(m_tablespaceId);
    if (!tablespace->IsTempTbs()) {
        persistenceMod = SYS_RELPERSISTENCE_PERMANENT;
    } else {
        persistenceMod = SYS_RELPERSISTENCE_GLOBAL_TEMP;
    }

    BtreeStorageMgr *btrSmgr = DstoreNew(m_utTableHandlerMemCxt) BtreeStorageMgr(m_pdbId, fillfactor, persistenceMod);
    btrSmgr->Init(m_indexSegmentPageId, m_tablespaceId, m_utTableHandlerMemCxt);
    UtMockModule::UtDropTableSpace(tablespace);

    /* Create IndexBuildInfo */
    numAttr = (numAttr == 0) ? numKeyAttrs : numAttr;
    StorageAssert(numAttr >= numKeyAttrs);
    IndexBuildInfo *buildInfo = GenerateIndexBuildInfo(numKeyAttrs, numAttr, colNums, unique);

    /* Create scankey */
    ScanKey keyInfos = (ScanKey)DstorePalloc0(MAXALIGN(sizeof(ScanKeyData)) * numKeyAttrs);
    for (int i = 0; i < buildInfo->baseInfo.indexKeyAttrsNum; i++) {
        keyInfos[i].skAttno = static_cast<AttrNumber>(i + 1);
        /* get scanKey func by attribute type */
        GetScanFuncByType(&keyInfos[i], buildInfo->baseInfo.opcinType[i]);
    }

    /* construct BtreeTestContext */
    StorageAssert(m_btreeTestContext == nullptr);
    m_btreeTestContext = (BtreeTestContext *)DstorePalloc(sizeof(BtreeTestContext));
    m_btreeTestContext->indexRel = (StorageRelation)DstorePalloc(sizeof(StorageRelationData));
    m_btreeTestContext->indexRel->m_pdbId = m_pdbId;
    m_btreeTestContext->indexRel->btreeSmgr = btrSmgr;
    m_btreeTestContext->scanKeyInfo = keyInfos;
    m_btreeTestContext->indexBuildInfo = buildInfo;
}

void UTTableHandler::CreateIndex(int *colNums, int numKeyAttrs, bool unique, int fillfactor, int numAttr)
{
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();
    CreateBtreeContext(colNums, numKeyAttrs, unique, fillfactor, numAttr);
    BtreeBuild btreeBuild(m_btreeTestContext->indexRel,
                          m_btreeTestContext->indexBuildInfo, m_btreeTestContext->scanKeyInfo);
    EXPECT_EQ(btreeBuild.BuildIndex(), DSTORE_SUCC);
    txn->Commit();
}

void UTTableHandler::CreateIndexParallel(int *colNums, int numKeyAttrs, bool unique, int fillfactor, int parallelworkers)
{
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();
    CreateBtreeContext(colNums, numKeyAttrs, unique, fillfactor);
    BtreeBuild btreeBuild(m_btreeTestContext->indexRel,
                          m_btreeTestContext->indexBuildInfo, m_btreeTestContext->scanKeyInfo);
    EXPECT_EQ(btreeBuild.BuildIndexParallel(parallelworkers), DSTORE_SUCC);
    txn->Commit();
}

IndexBuildInfo *UTTableHandler::GenerateIndexBuildInfo(int numKeyAttrs, int numAttr, int *colNums, bool unique)
{
    AutoMemCxtSwitch autoMemCxtSwitch{m_utTableHandlerMemCxt};

    StorageAssert(numAttr <= INDEX_MAX_KEY_NUM);
    int numHeapPartition = m_heapTestContext->numHeapPartition;
    TupleDesc heapTupleDesc = m_heapTestContext->tupleDesc->Copy();

    /* generate IndexBuildInfo */
    Size buildInfoSize = MAXALIGN(sizeof(IndexBuildInfo));
    Size indoptionSize = MAXALIGN(sizeof(int16) * numAttr);
    Size opcintypeSize = MAXALIGN(sizeof(Oid) * numAttr);
    Size partOidsSize = numHeapPartition == 1 ? Size(0) : MAXALIGN(sizeof(Oid) * numHeapPartition);
    Size partTuplesSize = numHeapPartition == 1 ? Size(0) : MAXALIGN(sizeof(double) * numHeapPartition);
    char *buildInfoPointer = (char *)(IndexBuildInfo *)DstorePalloc0(buildInfoSize + indoptionSize + opcintypeSize + partOidsSize + partTuplesSize);
    IndexBuildInfo *buildInfo = (IndexBuildInfo *)buildInfoPointer;

    buildInfo->heapRelationOid = DSTORE_INVALID_OID;
    buildInfo->heapAttributes = heapTupleDesc;
    buildInfo->heapRelNum = numHeapPartition;

    buildInfo->heapRels = static_cast<StorageRelation*>(DstorePalloc(numHeapPartition * sizeof(StorageRelation)));
    if (numHeapPartition > 1) {
        StorageRelation *heapRels = (StorageRelation *)m_heapTestContext->tableRel;
        /* partitioned table */
        for (int i = 0; i < numHeapPartition; i++) {
            buildInfo->heapRels[i] = heapRels[i];
        }
    } else {
        buildInfo->heapRels[0] = m_heapTestContext->tableRel;
    }

    /* Construct index tuple descriptor from heap heapTupleDesc */
    GenerateFakeIndexTupDesc(buildInfo, numAttr, colNums);
    buildInfo->baseInfo.relKind = numHeapPartition == 1 ? SYS_RELKIND_INDEX : SYS_RELKIND_GLOBAL_INDEX;
    buildInfo->baseInfo.isUnique = unique;
    buildInfo->baseInfo.indexAttrsNum = buildInfo->baseInfo.attributes->natts;
    buildInfo->baseInfo.indexKeyAttrsNum = numKeyAttrs;
    buildInfo->baseInfo.indexOption = (int16*)(buildInfoPointer + buildInfoSize);
    buildInfo->baseInfo.opcinType = (Oid *)((char *)buildInfo->baseInfo.indexOption + indoptionSize);

    if (partOidsSize == 0) {
        buildInfo->allPartOids = nullptr;
        buildInfo->allPartTuples = nullptr;
    } else {
        /* we used DstorePalloc0 that oids are initiated as 0 (DSTORE_INVALID_OID) */
        buildInfo->allPartOids = (Oid *)((char *)buildInfo->baseInfo.opcinType + opcintypeSize);
        buildInfo->allPartTuples = (double *)((char *)buildInfo->allPartOids + partOidsSize);
    }

    buildInfo->heapTuples = 0;
    buildInfo->indexTuples = 0;

    for (int i = 0; i < numAttr; i++) {
        /* get attribute number of key */
        buildInfo->indexAttrOffset[i] = (AttrNumber)colNums[i] + 1;
        buildInfo->baseInfo.opcinType[i] = buildInfo->baseInfo.attributes->attrs[i]->atttypid;
    }
    return buildInfo;
}

PageId UTTableHandler::GetBtreeRootPageId()
{
    BufferDesc *btrMetaBuf = INVALID_BUFFER_DESC;
    BtrMeta *btrMeta = GetBtreeSmgr()->GetBtrMeta(LW_SHARED, &btrMetaBuf);
    if (unlikely(btrMetaBuf == INVALID_BUFFER_DESC || btrMeta == nullptr)) {
        return INVALID_PAGE_ID;
    }
    PageId rootPageId = btrMeta->GetRootPageId();
    g_storageInstance->GetBufferMgr()->UnlockAndRelease(btrMetaBuf);
    return rootPageId;
}

TupleDesc UTTableHandler::GenerateEmptyTupDescTemplate(int natts)
{
    AutoMemCxtSwitch autoMemCxtSwitch{m_utTableHandlerMemCxt};

    Size descSize = MAXALIGN((uint32)(sizeof(TupleDescData) + natts * sizeof(Form_pg_attribute)));
    Size attrsSize = natts * MAXALIGN((uint32)sizeof(FormData_pg_attribute));
    char *offset = (char *)DstorePalloc0(descSize + attrsSize);
    TupleDesc desc = (TupleDesc)offset;

    desc->natts = natts;
    desc->tdisredistable = false;
    desc->attrs = (Form_pg_attribute *)(offset + sizeof(TupleDescData));
    desc->initdefvals = nullptr;
    desc->tdtypeid = RECORDOID;
    desc->tdtypmod = -1;
    desc->tdhasoid = false;
    desc->tdrefcount = -1;
    desc->tdhasuids = false;
    desc->tdhaslob = false;

    offset += descSize;
    for (int i = 0; i < desc->natts; i++) {
        desc->attrs[i] = (Form_pg_attribute)offset;
        offset += MAXALIGN((uint32)sizeof(FormData_pg_attribute));
    }

    return desc;
}

/* nattsCustomized and attTypePidListCustomized used for customizing TableHandler,
 * when no parameter is transferred, we generate default tuple desc from TYPE_CACHE_TABLE */
TupleDesc UTTableHandler::GenerateFakeHeapTupDesc(int nattsCustomized, Oid *attTypePidListCustomized)
{
    if (nattsCustomized <= 0) {
        /* Generate default heap tuple desc */
        TupleDesc desc = GenerateEmptyTupDescTemplate(TYPE_CACHE_NUM);
        for (int i = 0; i < TYPE_CACHE_NUM; i++) {
            desc->attrs[i]->attrelid = DSTORE_INVALID_OID;
            errno_t rc = memcpy_s(desc->attrs[i]->attname.data, NAME_DATA_LEN, TYPE_CACHE_TABLE[i].name, NAME_DATA_LEN);
            storage_securec_check(rc, "\0", "\0");
            desc->attrs[i]->atttypid = TYPE_CACHE_TABLE[i].type;
            desc->attrs[i]->attnum = i;
            desc->attrs[i]->attnotnull = true;
            desc->attrs[i]->attlen = TYPE_CACHE_TABLE[i].attlen;
            desc->attrs[i]->attbyval = TYPE_CACHE_TABLE[i].attbyval;
            desc->attrs[i]->attalign = TYPE_CACHE_TABLE[i].attalign;
            desc->attrs[i]->attcacheoff = -1;
            desc->attrs[i]->atthasdef = false;
            desc->attrs[i]->atttypmod = -1;
            desc->attrs[i]->attstorage = 'p';
            if (AttIsLob(desc->attrs[i])) {
                desc->tdhaslob = true;
            }
        }
        return desc;
    } else {
        /* Generate customized heap tuple desc */
        TupleDesc desc = GenerateEmptyTupDescTemplate(nattsCustomized);
        for (int i = 0; i < nattsCustomized; i++) {
            TypeCache typeCache = INVALID_TYPE_CACHE;
            for (int j = 0; j < TYPE_CACHE_NUM; j++) {
                if (TYPE_CACHE_TABLE[j].type == attTypePidListCustomized[i]) {
                    typeCache = TYPE_CACHE_TABLE[j];
                    break;
                }
            }
            StorageAssert(strncmp("invalid_type", typeCache.name, 12));

            desc->attrs[i]->attrelid = DSTORE_INVALID_OID;
            errno_t rc = memcpy_s(desc->attrs[i]->attname.data, NAME_DATA_LEN, typeCache.name, NAME_DATA_LEN);
            storage_securec_check(rc, "\0", "\0");
            desc->attrs[i]->atttypid = typeCache.type;
            desc->attrs[i]->attnum = i;
            desc->attrs[i]->attnotnull = true;
            desc->attrs[i]->attlen = typeCache.attlen;
            desc->attrs[i]->attbyval = typeCache.attbyval;
            desc->attrs[i]->attalign = typeCache.attalign;
            desc->attrs[i]->attcacheoff = -1;
            desc->attrs[i]->atthasdef = false;
            desc->attrs[i]->atttypmod = -1;
            desc->attrs[i]->attstorage = 'p';
            if (AttIsLob(desc->attrs[i])) {
                desc->tdhaslob = true;
            }
        }

        return desc;
    }

    StorageAssert(0);
    return nullptr;
}

TupleDesc UTTableHandler::GenerateFakeLobTupDesc()
{
    TupleDesc desc = GenerateEmptyTupDescTemplate(1);
    desc->attrs[0]->attrelid = DSTORE_INVALID_OID;
    errno_t rc = memcpy_s(desc->attrs[0]->attname.data, NAME_DATA_LEN, TYPE_CACHE_TABLE[BLOB_IDX].name, NAME_DATA_LEN);
    storage_securec_check(rc, "\0", "\0");
    desc->attrs[0]->atttypid = TYPE_CACHE_TABLE[BLOB_IDX].type;
    desc->attrs[0]->attnum = 0;
    desc->attrs[0]->attnotnull = true;
    desc->attrs[0]->attlen = TYPE_CACHE_TABLE[BLOB_IDX].attlen;
    desc->attrs[0]->attbyval = TYPE_CACHE_TABLE[BLOB_IDX].attbyval;
    desc->attrs[0]->attalign = TYPE_CACHE_TABLE[BLOB_IDX].attalign;
    desc->attrs[0]->attcacheoff = -1;
    desc->attrs[0]->atthasdef = false;
    desc->attrs[0]->atttypmod = -1;
    desc->attrs[0]->attstorage = 'p';
    desc->tdhaslob = false;
    return desc;
}

TupleDesc UTTableHandler::GenerateFakeLogicalDecodeTupDesc()
{
    std::vector<int> decodeSupport{0, 1, 2, 3, 9, 10, 12};
    TupleDesc desc = GenerateEmptyTupDescTemplate(decodeSupport.size());
    for (int i = 0; i < decodeSupport.size(); i++) {
        desc->attrs[i]->attrelid = DSTORE_INVALID_OID;
        errno_t rc = memcpy_s(desc->attrs[i]->attname.data, NAME_DATA_LEN,
                              TYPE_CACHE_TABLE[decodeSupport[i]].name, NAME_DATA_LEN);
        storage_securec_check(rc, "\0", "\0");
        desc->attrs[i]->atttypid = TYPE_CACHE_TABLE[decodeSupport[i]].type;
        desc->attrs[i]->attnum = i + 1;
        desc->attrs[i]->attnotnull = true;
        desc->attrs[i]->attlen = TYPE_CACHE_TABLE[decodeSupport[i]].attlen;
        desc->attrs[i]->attbyval = TYPE_CACHE_TABLE[decodeSupport[i]].attbyval;
        desc->attrs[i]->attalign = TYPE_CACHE_TABLE[decodeSupport[i]].attalign;
        desc->attrs[i]->attcacheoff = -1;
        desc->attrs[i]->atthasdef = false;
        desc->attrs[i]->atttypmod = -1;
        desc->attrs[i]->attstorage = 'p';
    }
    return desc;
}

void UTTableHandler::GenerateFakeIndexTupDesc(IndexBuildInfo *buildInfo, int numAttrs, int *colIndex)
{
    TupleDesc indexTupDesc = GenerateEmptyTupDescTemplate(numAttrs);

    for (int i = 0; i < numAttrs; i++) {
        errno_t rc = memcpy_s(indexTupDesc->attrs[i], sizeof(FormData_pg_attribute),
                              buildInfo->heapAttributes->attrs[colIndex[i]], sizeof(FormData_pg_attribute));
        storage_securec_check(rc, "\0", "\0");

        indexTupDesc->attrs[i]->attrelid = DSTORE_INVALID_OID;
        indexTupDesc->attrs[i]->attnum = i + 1;
    }

    buildInfo->baseInfo.attributes = indexTupDesc;
}

Datum UTTableHandler::GenerateRandomDataByType(Form_pg_attribute attr)
{
    int32 random = (int32)rand();
    Oid type = attr->atttypid;
    switch (type) {
        case BOOLOID:
            return Int32GetDatum(random % 2);
        case INT2OID:
            return Int16GetDatum(random);
        case OIDOID:
        case DATEOID:
        case INT4OID:
            return Int32GetDatum(random);
        case TIMESTAMPOID:
        case TIMESTAMPTZOID:
        case CASHOID:
        case INT8OID:
            return Int64GetDatum(random);
        case CHAROID:
            return Int32GetDatum((int32) ((uint8) random % 128));
        case CSTRINGOID:
        case NAMEOID: {
            /* len should be within [1, NAME_DATA_LEN-1] to reserve space for null char at the end */
            int32 len = random % (NAME_DATA_LEN - 2) + 1;
            DstoreName namePtr = (DstoreName) DstorePalloc(NAME_DATA_LEN);
            for (int i = 0; i < len; i++) {
                namePtr->data[i] = (char) (rand() % 26 + 'A');
            }
            namePtr->data[len] = '\0';
            return PointerGetDatum(namePtr);
        }
        case FLOAT4OID:
            return Float32GetDatum(random);
        case FLOAT8OID:
            return Float64GetDatum(random);
        case VARCHAROID:
        case TEXTOID:
        case BLOBOID:
        case CLOBOID:
            return PointerGetDatum(GenerateRandomDatumPtr(TEXTOID, random));
        case ANYARRAYOID:
            return PointerGetDatum(GenerateRandomDatumPtr(ANYARRAYOID, random));
        case TIMEOID:
            return TimeADTGetDatum((int64)random % ((int64)STORAGE_SECS_PER_DAY * STORAGE_USECS_PER_SEC));
        case BPCHAROID:
            return PointerGetDatum(GenerateRandomDatumPtr(BPCHAROID, random));
        case NUMERICOID:
            return PointerGetDatum(GenerateRandomDatumPtr(NUMERICOID, random));
        case MACADDROID:
            return PointerGetDatum(GenerateRandomDatumPtr(MACADDROID, random));
        case INETOID:
            return PointerGetDatum(GenerateRandomDatumPtr(INETOID, random));
        default:
            return (Datum)0;
    }
}

void UTTableHandler::GenerateRandomData(Datum *values, bool *isnulls, TupleDesc tupleDesc)
{
    int numAttrs = tupleDesc->natts;
    for (int i = 0; i < numAttrs; i++) {
        isnulls[i] = false;
        values[i] = UTTableHandler::GenerateRandomDataByType(tupleDesc->attrs[i]);
        if (tupleDesc->attrs[i]->attlen == -1 && values[i] == (Datum)0) {
            isnulls[i] = true;
        }
    }
}
void UTTableHandler::GenerateRandomData(Datum *values, bool *isnulls)
{
    TupleDesc desc = m_heapTestContext->tupleDesc;
    UTTableHandler::GenerateRandomData(values, isnulls, desc);
}

/* Use TupleDesc by Caller input */
HeapTuple *UTTableHandler::GenerateRandomHeapTuple(TupleDesc tupleDesc)
{
    int nAttrs = tupleDesc->natts;
    Datum *values = (Datum *)DstorePalloc(nAttrs * sizeof(Datum));
    bool *isnulls = (bool *)DstorePalloc(nAttrs * sizeof(bool));

    UTTableHandler::GenerateRandomData(values, isnulls, tupleDesc);
    HeapTuple *heapTuple = HeapTuple::FormTuple(tupleDesc, values, isnulls);

    DstorePfree(values);
    DstorePfree(isnulls);

    return heapTuple;
}

/* Use TupleDesc saved in UTTableHandler */
HeapTuple *UTTableHandler::GenerateRandomHeapTuple()
{
    TupleDesc heapTupleDesc = m_heapTestContext->tupleDesc;
    int nAttrs = heapTupleDesc->natts;
    Datum *values = (Datum *)DstorePalloc(nAttrs * sizeof(Datum));
    bool *isnulls = (bool *)DstorePalloc(nAttrs * sizeof(bool));

    GenerateRandomData(values, isnulls);
    HeapTuple *heapTuple = HeapTuple::FormTuple(heapTupleDesc, values, isnulls);

    DstorePfree(values);
    DstorePfree(isnulls);

    return heapTuple;
}

HeapTuple *UTTableHandler::GenerateRandomHeapTuple(Datum *values, bool *isnulls)
{
    GenerateRandomData(values, isnulls);
    return HeapTuple::FormTuple(m_heapTestContext->tupleDesc, values, isnulls);
}

BtreeTestInsertContext *UTTableHandler::GenerateRandomIndexTuple()
{
    int numAttrs = m_btreeTestContext->indexBuildInfo->heapAttributes->natts;
    int numIndexAttrs = m_btreeTestContext->indexBuildInfo->baseInfo.indexKeyAttrsNum;

    Size indexCtxSize = sizeof(BtreeTestInsertContext);
    Size valuesSize = sizeof(Datum) * numIndexAttrs;
    Size isnullSize = sizeof(bool) * numIndexAttrs;

    char *cp = (char *)DstorePalloc(indexCtxSize + valuesSize + isnullSize);
    BtreeTestInsertContext *insertContext = (BtreeTestInsertContext *)cp;
    insertContext->values = (Datum *)(cp + indexCtxSize);
    insertContext->isnull = (bool *)(cp + indexCtxSize + valuesSize);

    /* generate a random heap tuple */
    Datum values[numAttrs];
    bool isnull[numAttrs];
    HeapTuple *heapTuple = GenerateRandomHeapTuple(values, isnull);

    /* substitute the following steps with Btree::form_datum later */
    for (int i = 0; i < numIndexAttrs; i++) {
        int keyIdx = m_btreeTestContext->indexBuildInfo->indexAttrOffset[i] - 1;
        insertContext->values[i] = values[keyIdx];
        insertContext->isnull[i] = isnull[keyIdx];
    }
    /* Btree::form_datum end */

    IndexTuple *indexTuple = IndexTuple::FormTuple(m_btreeTestContext->indexBuildInfo->baseInfo.attributes,
                                                   insertContext->values, insertContext->isnull);

    insertContext->heapTuple = heapTuple;
    insertContext->indexTuple = indexTuple;

    return insertContext;
}

IndexTuple *UTTableHandler::InsertRandomIndexTuple(bool alreadyXactStart)
{
    BtreeTestInsertContext *insertContext = GenerateRandomIndexTuple();

    /* insert heap tuple */
    ItemPointerData ctid = InsertHeapTupAndCheckResult(insertContext->heapTuple, alreadyXactStart);

    /* construct btree insert context */
    BtreeInsert btreeInsert(m_btreeTestContext->indexRel, &m_btreeTestContext->indexBuildInfo->baseInfo,
                            m_btreeTestContext->scanKeyInfo);
    if (alreadyXactStart) {
        RetStatus status = btreeInsert.InsertTuple(insertContext->values, insertContext->isnull, &ctid);
        StorageAssert(status == DSTORE::DSTORE_SUCC);
        thrd->GetActiveTransaction()->IncreaseCommandCounter();
    } else {
        Transaction *txn = thrd->GetActiveTransaction();
        txn->Start();
        RetStatus status = btreeInsert.InsertTuple(insertContext->values, insertContext->isnull, &ctid);
        txn->Commit();
        StorageAssert(status == DSTORE::DSTORE_SUCC);
    }

    IndexTuple *indexTuple = insertContext->indexTuple;
    DstorePfreeExt(insertContext->heapTuple);
    DstorePfreeExt(insertContext);
    indexTuple->SetHeapCtid(&ctid);
    return indexTuple;
}

/* delete heap tuple from heap corresponding of the index tuple,
 * and do not delete index tuple from btree when isOnlyDeleteHeap is true. */
RetStatus UTTableHandler::DeleteWithIndexTuple(IndexTuple *indexTuple, bool alreadyXactStart, bool isOnlyDeleteHeap)
{
    Transaction *txn = thrd->GetActiveTransaction();
    if (!alreadyXactStart) {
        txn->Start();
        txn->SetSnapshotCsn();
    }
    IndexInfo *indexInfo = &m_btreeTestContext->indexBuildInfo->baseInfo;
    Datum *values = (Datum*) DstorePalloc(indexInfo->indexKeyAttrsNum * sizeof(Datum));
    bool *isNulls = (bool*) DstorePalloc(indexInfo->indexKeyAttrsNum * sizeof (bool));
    indexTuple->DeformTuple(indexInfo->attributes, values, isNulls);
    ItemPointerData heapCtid = indexTuple->GetHeapCtid();

    /* Heap delete */
    HeapDeleteContext heapDeleteContext;
    heapDeleteContext.ctid = heapCtid;
    heapDeleteContext.snapshot = *(txn->GetSnapshot());
    heapDeleteContext.cid = txn->GetCurCid();
    HeapDeleteHandler *heapDelete = DstoreNew(g_dstoreCurrentMemoryContext)
        HeapDeleteHandler(g_storageInstance, thrd, m_heapTestContext->tableRel);
    RetStatus status = heapDelete->Delete(&heapDeleteContext);
    if (STORAGE_FUNC_FAIL(status)) {
        if (!alreadyXactStart) {
            txn->Abort();
        }
        return DSTORE_FAIL;
    }

    delete heapDelete;
    heapDelete = nullptr;
    if (isOnlyDeleteHeap) {
        if (!alreadyXactStart) {
            txn->Commit();
        } else {
            txn->IncreaseCommandCounter();
        }
        return status;
    }

    /* Index delete */
    BtreeDelete btreeDelete(m_btreeTestContext->indexRel, &m_btreeTestContext->indexBuildInfo->baseInfo,
                            m_btreeTestContext->scanKeyInfo);
    status = btreeDelete.DeleteTuple(values, isNulls, &heapCtid);
    m_btreeTestContext->lastWorkingPageId = btreeDelete.GetCurPageId();
    if (!alreadyXactStart) {
        if (STORAGE_FUNC_FAIL(status)) {
            /* We must assert error code before abort(), or error code maybe overwritten. */
            StorageAssert(StorageGetErrorCode() == PAGE_ERROR_NO_SPACE_FOR_TD);
            txn->Abort();
        } else {
            txn->Commit();
        }
    } else {
        txn->IncreaseCommandCounter();
    }
    return status;
}

RetStatus UTTableHandler::DeleteWithIndexTuple(BtreePagePayload &pagePayload, OffsetNumber delOff, IndexTuple *delTuple,
                                               bool alreadyXactStart)
{
    Transaction *txn = thrd->GetActiveTransaction();
    if (!alreadyXactStart) {
        txn->Start();
        txn->SetSnapshotCsn();
    }

    /* Heap delete */
    HeapDeleteContext heapDeleteContext;
    heapDeleteContext.ctid = delTuple->GetHeapCtid();
    heapDeleteContext.snapshot = *(txn->GetSnapshot());
    heapDeleteContext.cid = txn->GetCurCid();
    HeapDeleteHandler heapDelete(g_storageInstance, thrd, m_heapTestContext->tableRel);
    RetStatus status = heapDelete.Delete(&heapDeleteContext);
    if (STORAGE_FUNC_FAIL(status)) {
        if (!alreadyXactStart) {
            txn->Abort();
        }
        return DSTORE_FAIL;
    }

    /* Index delete */
    BtreeDelete btreeDelete(m_btreeTestContext->indexRel, &m_btreeTestContext->indexBuildInfo->baseInfo,
                            m_btreeTestContext->scanKeyInfo);
    status = btreeDelete.DeleteTupleByOffset(pagePayload.GetBuffDesc(), delOff);
    if (!alreadyXactStart) {
        if (STORAGE_FUNC_FAIL(status)) {
            /* We must assert error code before abort(), or error code maybe overwritten. */
            StorageAssert(StorageGetErrorCode() == PAGE_ERROR_NO_SPACE_FOR_TD);
            txn->Abort();
        } else {
            txn->Commit();
        }
    } else {
        txn->IncreaseCommandCounter();
    }
    return status;
}

IndexTuple *UTTableHandler::UpdateWithIndexTuple(IndexTuple *indexTuple, bool alreadyXactStart)
{
    Transaction *txn = thrd->GetActiveTransaction();
    if (!alreadyXactStart) {
        txn->Start();
        txn->SetSnapshotCsn();
    }
    IndexInfo *indexInfo = &m_btreeTestContext->indexBuildInfo->baseInfo;
    Datum *values = (Datum*) DstorePalloc(indexInfo->indexKeyAttrsNum * sizeof(Datum));
    bool *isNulls = (bool*) DstorePalloc(indexInfo->indexKeyAttrsNum * sizeof (bool));
    indexTuple->DeformTuple(indexInfo->attributes, values, isNulls);
    ItemPointerData heapCtid = indexTuple->GetHeapCtid();

    /* Delete old index tuple. */
    BtreeDelete btreeDelete(m_btreeTestContext->indexRel, &m_btreeTestContext->indexBuildInfo->baseInfo,
                            m_btreeTestContext->scanKeyInfo);
    RetStatus status = btreeDelete.DeleteTuple(values, isNulls, &heapCtid);
    if (STORAGE_FUNC_FAIL(status)) {
        /* We must assert error code before abort(), or error code maybe overwritten. */
        StorageAssert(StorageGetErrorCode() == PAGE_ERROR_NO_SPACE_FOR_TD);
        if (!alreadyXactStart) {
            txn->Abort();
        }
        return nullptr;
    }

    /* Generate a new heap & index tuple */
    BtreeTestInsertContext *insertContext = GenerateRandomIndexTuple();

    /* Heap update */
    HeapUpdateContext updateContext;
    updateContext.oldCtid = heapCtid;
    updateContext.newTuple = insertContext->heapTuple;
    updateContext.snapshot = *(txn->GetSnapshot());
    updateContext.cid = txn->GetCurCid();
    HeapUpdateHandler heapUpdate(g_storageInstance, thrd, m_heapTestContext->tableRel);
    status = heapUpdate.Update(&updateContext);
    if (STORAGE_FUNC_FAIL(status)) {
        /* We must assert error code before abort(), or error code maybe overwritten. */
        StorageAssert(StorageGetErrorCode() == PAGE_ERROR_NO_SPACE_FOR_TD);
        if (!alreadyXactStart) {
            txn->Abort();
        }
        return nullptr;
    }

    /* Insert new index tuple */
    BtreeInsert btreeInsert(m_btreeTestContext->indexRel, &m_btreeTestContext->indexBuildInfo->baseInfo,
                            m_btreeTestContext->scanKeyInfo);
    status = btreeInsert.InsertTuple(insertContext->values, insertContext->isnull, &heapCtid);
    StorageAssert(status == DSTORE::DSTORE_SUCC);
    if (alreadyXactStart) {

        thrd->GetActiveTransaction()->IncreaseCommandCounter();
    } else {
        txn->Commit();
    }

    IndexTuple *newIndexTuple = insertContext->indexTuple;
    DstorePfreeExt(insertContext->heapTuple);
    DstorePfreeExt(insertContext);
    newIndexTuple->SetHeapCtid(&heapCtid);
    return newIndexTuple;
}

RetStatus UTTableHandler::InsertIndexTupleOnly(DefaultRowDef *rowDef, bool *nullbitmap, ItemPointer heapCtid)
{
    TupleDesc indexTupleDesc = GetIndexTupleDesc();
    int numAttrs = m_btreeTestContext->indexBuildInfo->baseInfo.indexAttrsNum;
    Datum values[numAttrs];
    for (int i = 0; i < numAttrs; i++) {
        values[i] = GetSpecificDataByType(rowDef, indexTupleDesc->attrs[i]->atttypid);
    }

    BtreeInsert btreeInsert(m_btreeTestContext->indexRel, &m_btreeTestContext->indexBuildInfo->baseInfo,
                            m_btreeTestContext->scanKeyInfo);
    RetStatus status = btreeInsert.InsertTuple(values, nullbitmap, heapCtid);
    StorageAssert(status == DSTORE_SUCC);
    return status;
}

RetStatus UTTableHandler::DeleteIndexTupleOnly(DefaultRowDef *rowDef, bool *nullbitmap, ItemPointer heapCtid)
{
    TupleDesc indexTupleDesc = GetIndexTupleDesc();
    int numAttrs = m_btreeTestContext->indexBuildInfo->baseInfo.indexAttrsNum;
    Datum values[numAttrs];
    for (int i = 0; i < numAttrs; i++) {
        values[i] = GetSpecificDataByType(rowDef, indexTupleDesc->attrs[i]->atttypid);
    }

    BtreeDelete btreeDelete(m_btreeTestContext->indexRel, &m_btreeTestContext->indexBuildInfo->baseInfo,
                            m_btreeTestContext->scanKeyInfo);
    /* Failure is allowded */
    return btreeDelete.DeleteTuple(values, nullbitmap, heapCtid);
}

void UTTableHandler::FillTableWithSpecificData(DefaultRowDef *tableDef, int rowNum, bool isTxnStarted)
{
    EXPECT_EQ(m_heapTestContext->numHeapPartition, 1);
    TupleDesc desc = m_heapTestContext->tupleDesc;

    Datum values[desc->natts];
    bool isnulls[desc->natts];
    for (int i = 0; i < rowNum; i++) {
        HeapTuple *heapTuple = GetSpecificHeapTuple(tableDef + i, (Datum *)values, (bool *)isnulls);
        InsertHeapTupAndCheckResult(heapTuple, isTxnStarted);
        DstorePfreeExt(heapTuple);
    }
}

void UTTableHandler::FillPartitionTableWithSpecificData(int partitionNo, DefaultRowDef *tableDef, int rowNum, bool isTxnStarted)
{
    ASSERT_GT(m_heapTestContext->numHeapPartition, partitionNo);
    StorageRelation *heapRels = (StorageRelation *)m_heapTestContext->tableRel;

    TupleDesc tupleDesc = m_heapTestContext->tupleDesc;
    Datum values[tupleDesc->natts];
    bool isnulls[tupleDesc->natts];
    thrd->GetActiveTransaction()->Start();
    StorageRelation tableRel = m_heapTestContext->tableRel;
    m_heapTestContext->tableRel = heapRels[partitionNo];
    for (int i = 0; i < rowNum; i++) {
        HeapTuple *heapTuple = GetSpecificHeapTuple(tableDef + i, (Datum *)values, (bool *)isnulls);
        InsertHeapTupAndCheckResult(heapTuple, isTxnStarted);
        DstorePfreeExt(heapTuple);
    }
    m_heapTestContext->tableRel = tableRel;
    thrd->GetActiveTransaction()->Commit();
}

Datum UTTableHandler::GetSpecificDataByType(DefaultRowDef *tableDef, Oid type)
{
    switch (type) {
        case BOOLOID:
            return Int32GetDatum(tableDef->column_bool);
        case INT2OID:
            return Int16GetDatum(tableDef->column_int16);
        case INT4OID:
            return Int32GetDatum(tableDef->column_int32);
        case INT8OID:
            return Int64GetDatum(tableDef->column_int64);
        case CHAROID:
            return Int32GetDatum(tableDef->column_char);
        case NAMEOID:
            return PointerGetDatum(tableDef->column_name.data);
        case OIDOID:
            return ObjectIdGetDatum(tableDef->column_oid);
        case TIMESTAMPOID:
            return TimestampGetDatum(tableDef->column_timestamp);
        case TIMESTAMPTZOID:
            return TimestampTzGetDatum(tableDef->column_timestamptz);
        case FLOAT4OID:
            return Float32GetDatum(tableDef->column_float32);
        case FLOAT8OID:
            return Float64GetDatum(tableDef->column_float64);
        case TEXTOID:
            return PointerGetDatum(tableDef->column_text);
        case VARCHAROID:
            return PointerGetDatum(tableDef->column_varchar);
        case BLOBOID:
            return PointerGetDatum(tableDef->column_blob);
        case CLOBOID:
            return PointerGetDatum(tableDef->column_clob);
        case CSTRINGOID:
            return CStringGetDatum(tableDef->column_cstring);
        case DATEOID:
            return DateADTGetDatum(tableDef->column_date);
        case CASHOID:
            return CashGetDatum(tableDef->column_money);
        case TIMEOID:
            return TimeADTGetDatum(tableDef->column_time);
        case MACADDROID:
            return PointerGetDatum(tableDef->column_macaddr);
        case INETOID:
            return PointerGetDatum(tableDef->column_inet);
        default:
            return (Datum)0;
    }
}

HeapTuple *UTTableHandler::GetSpecificHeapTuple(DefaultRowDef *tableDef, Datum *values, bool *isnulls, bool *nullbitmap)
{
    TupleDesc tupleDesc = m_heapTestContext->tupleDesc;
    int numAttrs = tupleDesc->natts;
    for (int i = 0; i < numAttrs; i++) {
        if (nullbitmap[i]) {
            isnulls[i] = true;
            values[i] = (Datum)0;
        } else {
            isnulls[i] = false;
            values[i] = GetSpecificDataByType(tableDef, tupleDesc->attrs[i]->atttypid);
        }
    }
    HeapTuple *heapTuple = HeapTuple::FormTuple(tupleDesc, values, isnulls);
    return heapTuple;
}

HeapTuple *UTTableHandler::GenerateSpecificHeapTuple(const std::string &rawData)
{
    int dataLen = (int)strlen(rawData.c_str());
    int diskTupleLen = HEAP_DISK_TUP_HEADER_SIZE + dataLen;

    HeapTuple *heapTuple = (HeapTuple*)DstorePalloc0(sizeof(HeapTuple) + diskTupleLen);
    heapTuple->m_diskTuple = (HeapDiskTuple*)((char*)heapTuple + sizeof(HeapTuple));
    errno_t ret = memcpy_s((char *)heapTuple->m_diskTuple->GetData(), dataLen, rawData.c_str(), dataLen);
    storage_securec_check(ret, "\0", "\0");
    heapTuple->SetDiskTupleSize(diskTupleLen);

    return heapTuple;
}

void *UTTableHandler::GenerateTextWithFixedLen(int len)
{
    text *textPtr = (text*) DstorePalloc(sizeof(varlena) + len * sizeof(char));
    DstoreSetVarSize(textPtr, VARHDRSZ + len * sizeof(char));
    for (int i = 0; i < len; i++) {
        textPtr->vl_dat[i] = ('A' + rand() % 26);
    }
    return textPtr;
}

void *UTTableHandler::GenerateSpecificDatumPtr(Oid typeOid, int seed)
{
    switch (typeOid) {
        case BLOBOID:
        case CLOBOID:
        case TEXTOID: {
            int TEXT_LEN = 26;
            int32 len = seed % (TEXT_LEN - 1) + 1;
            text *textPtr = (text*) DstorePalloc(sizeof(varlena) + len * sizeof(char));
            DstoreSetVarSize(textPtr, VARHDRSZ + len * sizeof(char));
            for (int i = 0; i < len; i++) {
                textPtr->vl_dat[i] = ('A' + seed % 26);
            }
            return textPtr;
        }
    }
}

void *UTTableHandler::GenerateRandomDatumPtr(Oid typeOid, int random)
{
    switch (typeOid) {
        case BPCHAROID:
        case BLOBOID:
        case CLOBOID:
        case VARCHAROID:
        case TEXTOID: {
            int TEXT_LEN = 16;
            int32 len = random % (TEXT_LEN - 1) + 1;
            text *textPtr = (text*) DstorePalloc(sizeof(varlena) + len * sizeof(char));
            DstoreSetVarSize(textPtr, VARHDRSZ + len * sizeof(char));
            for (int i = 0; i < len; i++) {
                textPtr->vl_dat[i] = ('A' + rand() % 26);
            }
            return textPtr;
        }
        case ANYARRAYOID: {
            int ARRAY_LEN = 16;
            int32 num = random % (ARRAY_LEN - 1) + 1;

            /* Create a new int array with room for "num" elements */
            ArrayType* arrayTypeP = NULL;
            int nbytes = ARR_OVERHEAD_NONULLS(1) + sizeof(int) * num;
            arrayTypeP = (ArrayType*)DstorePalloc0(nbytes);

            DstoreSetVarSize(arrayTypeP, nbytes);
            ARR_NDIM(arrayTypeP) = 1;
            arrayTypeP->dataoffset = 0; /* marker for no null bitmap */
            ARR_ELEMTYPE(arrayTypeP) = INT4OID;
            ARR_DIMS(arrayTypeP)[0] = num;
            ARR_LBOUND(arrayTypeP)[0] = 1;

            for (int i = 0; i < num; i++) {
                ARR_DATA_PTR(arrayTypeP)[i] = rand();
            }
            return arrayTypeP;
        }
        case MACADDROID: {
            MacAddr * macAddrPtr = (MacAddr *) DstorePalloc(sizeof(MacAddr));
            macAddrPtr->a = random % 256;
            macAddrPtr->b = rand() % 256;
            macAddrPtr->c = rand() % 256;
            macAddrPtr->d = rand() % 256;
            macAddrPtr->e = rand() % 256;
            macAddrPtr->f = rand() % 256;
            return macAddrPtr;
        }
        case INETOID: {
            Inet * inetPtr = (Inet *) DstorePalloc(sizeof(Inet));
            inetPtr->inetData.family = rand() % 2 ? DSTORE_AF_INET : DSTORE_AF_INET6;
            inetPtr->inetData.bits = rand() % DstoreInetGetIpMaxBits(inetPtr) + 1;
            for (int i = 0; i < DstoreInetGetIpMaxBits(inetPtr) / 8; i++) {
                inetPtr->inetData.ipAddr[0] = rand() % 256;
            }
            DstoreSetVarSize(inetPtr, sizeof(Inet));
            return inetPtr;
        }
        default:
            return (Datum *) DstorePalloc0(VARHDRSZ);
    }
}

DefaultRowDef UTTableHandler::GetDefaultRowDef()
{
    DefaultRowDef rowDef = {true,   /* column_bool */
                            0,      /* column_int16 */
                            0,      /* column_int32 */
                            0,      /* column_int64 */
                            'a',    /* column_char */
                            "abcd", /* column_name */
                            0,      /* column_oid */
                            0,      /* column_timestamp */
                            0,      /* column_timestamptz */
                            0,      /* column_float32 */
                            0,      /* column_float64 */
                            (text *) GenerateRandomDatumPtr(TEXTOID),
                            (VarChar *) GenerateRandomDatumPtr(VARCHAROID),
                            (char *) GenerateRandomDatumPtr(CSTRINGOID),
                            0,      /* column_date */
                            0,      /* column_money */
                            0,      /* column_time */
                            (MacAddr *) GenerateRandomDatumPtr(MACADDROID),
                            (Inet *) GenerateRandomDatumPtr(INETOID),
                            (varlena *) GenerateRandomDatumPtr(BLOBOID),
                            (varlena *) GenerateRandomDatumPtr(CLOBOID)};
    return rowDef;
}

void UTTableHandler::FillTableWithRandomData(int rowNum)
{
    ASSERT_EQ(m_heapTestContext->numHeapPartition, 1);

    FillTableWithRandomData(m_heapTestContext->tableRel, rowNum);
}

void UTTableHandler::FillTableWithRandomData(int partitionNo, int rowNum)
{
    ASSERT_GT(m_heapTestContext->numHeapPartition, partitionNo);
    StorageRelation *heapRels = (StorageRelation *)m_heapTestContext->tableRel;

    FillTableWithRandomData(heapRels[partitionNo], rowNum);
}

void UTTableHandler::FillTableWithRandomData(StorageRelation tableRel, int rowNum)
{
    TupleDesc tupleDesc = m_heapTestContext->tupleDesc;
    HeapInsertHandler heapInsert(g_storageInstance, thrd, tableRel);
    HeapInsertContext insertContext;
    insertContext.cid = thrd->GetActiveTransaction()->GetCurCid();
    Datum values[tupleDesc->natts];
    bool isnulls[tupleDesc->natts];
    thrd->GetActiveTransaction()->Start();
    for (int rec = 0; rec < rowNum; rec++) {
        HeapTuple *heapTuple = GenerateRandomHeapTuple(values, isnulls);
        insertContext.heapTuple = heapTuple;
        int retVal = heapInsert.Insert(&insertContext);
        StorageAssert(retVal == DSTORE_SUCC);
        DstorePfreeExt(heapTuple);
    }
    thrd->GetActiveTransaction()->Commit();
}

ItemPointerData UTTableHandler::InsertHeapTupAndCheckResult(HeapTuple *heapTuple, bool alreadyXactStart,
    SnapshotData *snapshot, bool addTxnCid, bool checkResult)
{
    HeapInsertHandler heapInsert(g_storageInstance, thrd, m_heapTestContext->tableRel);
    HeapInsertContext insertContext;
    insertContext.heapTuple = heapTuple;

    if (!alreadyXactStart) {
        thrd->GetActiveTransaction()->Start();
        if (snapshot == INVALID_SNAPSHOT) {
            thrd->GetActiveTransaction()->SetSnapshotCsn();
        } else {
            thrd->GetActiveTransaction()->SetSnapshotCsnForFlashback(snapshot->GetCsn());
        }
        insertContext.cid = thrd->GetActiveTransaction()->GetCurCid();
        int retVal = heapInsert.Insert(&insertContext);
        StorageAssert(retVal == DSTORE_SUCC);
        thrd->GetActiveTransaction()->Commit();
    } else {
        if (snapshot == INVALID_SNAPSHOT) {
            thrd->GetActiveTransaction()->SetSnapshotCsn();
        } else {
            thrd->GetActiveTransaction()->SetSnapshotCsnForFlashback(snapshot->GetCsn());
        }
        insertContext.cid = thrd->GetActiveTransaction()->GetCurCid();
        int retVal = heapInsert.Insert(&insertContext);
        if (addTxnCid) {
            thrd->GetActiveTransaction()->IncreaseCommandCounter();
        }
        StorageAssert(retVal == DSTORE_SUCC);
    }
    /* Check result */
    if (checkResult) {
        HeapTuple *resTuple = FetchHeapTuple(&insertContext.ctid, snapshot, alreadyXactStart);
        EXPECT_TRUE(resTuple->GetDiskTupleSize() == heapTuple->GetDiskTupleSize());
        int32 dataSize = heapTuple->GetDiskTupleSize() - HEAP_DISK_TUP_HEADER_SIZE;
        EXPECT_EQ(memcmp((void*)resTuple->GetDiskTuple()->GetData(), heapTuple->GetDiskTuple()->GetData(), dataSize), 0);
        DstorePfreeExt(resTuple);
    }

    return insertContext.ctid;
}

HeapTuple *UTTableHandler::FetchHeapTuple(ItemPointerData *ctid, SnapshotData *snapshot, bool alreadyStartXact)
{
    HeapTuple *tuple = nullptr;
    if (!alreadyStartXact) {
        thrd->GetActiveTransaction()->Start();
        if (snapshot == INVALID_SNAPSHOT) {
            thrd->GetActiveTransaction()->SetSnapshotCsn();
        } else if (snapshot->GetSnapshotType() == SnapshotType::SNAPSHOT_NOW) {
            thrd->GetActiveTransaction()->SetSnapshotCsn(true);
        } else {
            thrd->GetActiveTransaction()->SetSnapshotCsnForFlashback(snapshot->GetCsn());
        }
        {
            HeapScanHandler heap_scan(g_storageInstance, thrd, m_heapTestContext->tableRel);
            HeapInterface::BeginScan(&heap_scan, thrd->GetActiveTransaction()->GetSnapshot());
            tuple = heap_scan.FetchTuple(*ctid);
            heap_scan.EndFetch();
        }
        thrd->GetActiveTransaction()->Commit();
    } else {
        if (snapshot == INVALID_SNAPSHOT) {
            thrd->GetActiveTransaction()->SetSnapshotCsn();
        } else if (snapshot->GetSnapshotType() == SnapshotType::SNAPSHOT_NOW) {
            thrd->GetActiveTransaction()->SetSnapshotCsn(true);
        } else {
            thrd->GetActiveTransaction()->SetSnapshotCsnForFlashback(snapshot->GetCsn());
        }
        {
            HeapScanHandler heap_scan(g_storageInstance, thrd, m_heapTestContext->tableRel);
            HeapInterface::BeginScan(&heap_scan, thrd->GetActiveTransaction()->GetSnapshot());
            tuple = heap_scan.FetchTuple(*ctid);
            heap_scan.EndFetch();
        }
        thrd->GetActiveTransaction()->IncreaseCommandCounter();
    }

    return tuple;
}

void UTTableHandler::BatchInsertHeapTupsAndCheckResult(HeapTuple **heapTuples, uint16 nTuples, ItemPointerData *ctids,
                                                       SnapshotData *snapshot, bool alreadyXactStart)
{
    HeapInsertHandler heapInsert(g_storageInstance, thrd, m_heapTestContext->tableRel);

    HeapInsertContext contexts[nTuples];
    for (uint16 i = 0; i < nTuples; i++) {
        HeapInsertContext context;
        context.heapTuple = heapTuples[i];
        context.ctid = INVALID_ITEM_POINTER;
        contexts[i] = context;
    }

    HeapBacthInsertContext batchConext{contexts, nTuples};

    if (!alreadyXactStart) {
        thrd->GetActiveTransaction()->Start();
        if (snapshot == INVALID_SNAPSHOT) {
            thrd->GetActiveTransaction()->SetSnapshotCsn();
        } else {
            thrd->GetActiveTransaction()->SetSnapshotCsnForFlashback(snapshot->GetCsn());
        }
        for (uint16 i = 0; i < nTuples; i++) {
            contexts[i].cid = thrd->GetActiveTransaction()->GetCurCid();
        }
        int retVal = heapInsert.BatchInsert(&batchConext);
        StorageAssert(retVal == DSTORE_SUCC);
        thrd->GetActiveTransaction()->Commit();
    } else {
        if (snapshot == INVALID_SNAPSHOT) {
            thrd->GetActiveTransaction()->SetSnapshotCsn();
        } else {
            thrd->GetActiveTransaction()->SetSnapshotCsnForFlashback(snapshot->GetCsn());
        }
        for (uint16 i = 0; i < nTuples; i++) {
            contexts[i].cid = thrd->GetActiveTransaction()->GetCurCid();
        }
        int retVal = heapInsert.BatchInsert(&batchConext);
        thrd->GetActiveTransaction()->IncreaseCommandCounter();
        StorageAssert(retVal == DSTORE_SUCC);
    }

    for (int i = 0; i < nTuples; i++) {
        HeapInsertContext *context = &contexts[i];
        HeapTuple *heapTuple = context->heapTuple;
        ctids[i] = context->ctid;
        /* Check result */
        HeapTuple *resTuple = FetchHeapTuple(&(context->ctid), snapshot, alreadyXactStart);
        EXPECT_TRUE(resTuple->GetDiskTupleSize() == heapTuple->GetDiskTupleSize());
        int32 dataSize = heapTuple->GetDiskTupleSize() - HEAP_DISK_TUP_HEADER_SIZE;
        EXPECT_EQ(memcmp((void *)resTuple->GetDiskTuple()->GetData(), heapTuple->GetDiskTuple()->GetData(), dataSize),
                  0);
        DstorePfreeExt(resTuple);
    }
}

HeapTuple *UTTableHandler::FetchHeapTupleWithCsn(ItemPointerData *ctid, bool alreadyStartXact, CommitSeqNo csn)
{
    HeapTuple *tuple = nullptr;
    if (!alreadyStartXact) {
        thrd->GetActiveTransaction()->Start();
        thrd->GetActiveTransaction()->SetSnapshotCsnForFlashback(csn);
        {
            HeapScanHandler heap_scan(g_storageInstance, thrd, m_heapTestContext->tableRel);
            heap_scan.Begin(thrd->GetActiveTransaction()->GetSnapshot());
            tuple = heap_scan.FetchTuple(*ctid);
            heap_scan.EndFetch();
        }
        thrd->GetActiveTransaction()->Commit();
    } else {
        thrd->GetActiveTransaction()->SetSnapshotCsnForFlashback(csn);
        {
            HeapScanHandler heap_scan(g_storageInstance, thrd, m_heapTestContext->tableRel);
            heap_scan.Begin(thrd->GetActiveTransaction()->GetSnapshot());
            tuple = heap_scan.FetchTuple(*ctid);
            heap_scan.EndFetch();
        }
        thrd->GetActiveTransaction()->IncreaseCommandCounter();
    }

    return tuple;
}

void UTTableHandler::FetchHeapTupAndCheckResult(HeapTuple *expecTup, ItemPointerData *ctid, SnapshotData *snapshot,
                                                bool alreadyStartXact)
{
    HeapTuple *resTup = FetchHeapTuple(ctid, snapshot, alreadyStartXact);
    EXPECT_TRUE(resTup != nullptr);
    ASSERT_EQ(memcmp((char *)resTup->GetDiskTuple()->GetData(), expecTup->GetDiskTuple()->GetData(),
        resTup->GetDiskTupleSize() - HEAP_DISK_TUP_HEADER_SIZE), 0);
    DstorePfreeExt(resTup);
}

IndexTuple *UTTableHandler::GenerateSpecificIndexTuple(std::string rawData, ItemPointerData tid)
{
    int dataLen = (int)strlen(rawData.c_str()) + 1;
    int tupleLen = sizeof(IndexTuple) + dataLen;

    IndexTuple *indexTuple = (IndexTuple *)DstorePalloc0(tupleLen);

    errno_t ret = memcpy_s((char *)indexTuple->GetValues(), dataLen, rawData.c_str(), dataLen);
    storage_securec_check(ret, "\0", "\0");

    indexTuple->SetHeapCtid(&tid);
    indexTuple->SetSize(tupleLen);

    return indexTuple;
}

int UTTableHandler::CompareDatum(Datum a, Datum b, Oid typeOid)
{
    switch (typeOid) {
        case INT2OID:
            return Int2FastCmp(a, b, nullptr);
        case INT4OID:
            return Int4FastCmp(a, b, nullptr);
        case INT8OID:
            return Int8FastCmp(a, b, nullptr);
        case CHAROID:
            return CharFastCmp(a, b, nullptr);
        case VARCHAROID:
        case TEXTOID:
        case BLOBOID:
        case CLOBOID:
            return TextFastCmp(a, b, nullptr);
        case NAMEOID:
            return NameFastCmp(a, b, nullptr);
        case OIDOID:
            return OidFastCmp(a, b, nullptr);
        case TIMESTAMPOID:
            return TimestampFastCmp(a, b, nullptr);
        case FLOAT4OID:
            return Float4FastCmp(a, b, nullptr);
        case FLOAT8OID:
            return Float8FastCmp(a, b, nullptr);
        default:
            return Int4FastCmp(a, b, nullptr);
    }
}

void UTTableHandler::GetScanKeyByTypeKeyCache(ScanKey scanKey, Pointer cachePointer)
{
    FuncCache *typeKeyCache= (FuncCache *)cachePointer;
    scanKey->skFunc.fnAddr = typeKeyCache->fnAddr;
    scanKey->skFunc.fnOid = typeKeyCache->fnOid;
    scanKey->skFunc.fnNargs = 2;
    scanKey->skFunc.fnStrict = true;
    scanKey->skFunc.fnRetset = false;
    scanKey->skFunc.fnMcxt = m_utTableHandlerMemCxt;
    scanKey->skCollation = DEFAULT_COLLATION_OID;
}

void UTTableHandler::GetScanFuncByFnOid(ScanKey scanKey, Oid fnOid)
{
    FuncCache cache = g_storageInstance->GetCacheHashMgr()->GetFuncCacheFromFnOid(fnOid);
    GetScanKeyByTypeKeyCache(scanKey, (Pointer)&cache);
}

void UTTableHandler::GetScanFuncByType(ScanKey scanKey, Oid leftType, Oid rightType)
{
    FuncCache cache = g_storageInstance->GetCacheHashMgr()->GetFuncCacheFromArgType(
            leftType, rightType, MAINTAIN_ORDER);
    GetScanKeyByTypeKeyCache(scanKey, (Pointer)&cache);
}

void UTTableHandler::GetScanFuncByType(ScanKey scanKey, Oid type)
{
    GetScanFuncByType(scanKey, type, type);
}

} /* namespace DSTORE */
