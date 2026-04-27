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
 * dstore_systable.h
 *
 * IDENTIFICATION
 *        include/systable/dstore_systable.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_SYSTABLE_H
#define DSTORE_SYSTABLE_H


#include "systable/dstore_systable_struct.h"
#include "common/memory/dstore_mctx.h"
#include "heap/dstore_heap_interface.h"
#include "systable/dstore_relation.h"
#include "index/dstore_index_interface.h"
#include "common/algorithm/dstore_ilist.h"
namespace DSTORE {

struct SelectColumn {
    uint16_t columnNum;
    Oid *typeOid;
    int16_t *attNo;
    int16_t *attLen;
};

struct SysScanDescData {
    bool isSort;
    StorageRelation heapRel;                 /* catalog being scanned */
    HeapScanHandler *heapScan;               /* only valid in heap-scan case */
    IndexScanHandler *indexScan;             /* only valid in index-scan case */
    ScanDirection indexScanDir;              /* only valid in index-scan case */
    bool recheck;                            /* T means scan keys must be rechecked, will be delete */
    IndexInfo idxInfo;                       /* Index info will be delete, indexRel include idxInfo */
    SelectColumn selectColumn;
};
struct StorageRelationNode {
    dlist_node node;
    StorageRelation relation;
    HeapTuple *tuple;
};

class Systable : public BaseObject {
public:
    explicit Systable(StorageRelation relation);
    Systable();
    virtual ~Systable() = default;
    DISALLOW_COPY_AND_MOVE(Systable);

    SysScanDesc BeginScan(int nkeys, ScanKey key, Snapshot snapshot);
    HeapTuple *GetNext(SysScanDesc sysscan);
    void EndScan(SysScanDesc sysscan) const;

    StorageRelation m_systableReldesc;
};
}  // namespace DSTORE

#endif