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
 * IDENTIFICATION
 *        src/systable/dstore_systable.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "systable/dstore_systable.h"
#include "common/dstore_datatype.h"
#include "common/dstore_common_utils.h"
#include "heap/dstore_heap_scan.h"
#include "heap/dstore_heap_interface.h"
#include "common/memory/dstore_mctx.h"
#include "transaction/dstore_transaction_interface.h"

namespace DSTORE {
Systable::Systable(StorageRelation relation) : m_systableReldesc(relation)
{
}
Systable::Systable() : m_systableReldesc(nullptr)
{
}

SysScanDesc Systable::BeginScan(int nkeys, ScanKey key, Snapshot snapshot)
{
    StorageAssert(DstoreRelationIsValid(m_systableReldesc));
    /* Step1: Create a SysScanDesc. */
    SysScanDesc sysscan = (SysScanDesc)DstorePalloc0(sizeof(SysScanDescData));
    if (unlikely(sysscan == nullptr)) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        ErrLog(DSTORE_ERROR, MODULE_SYSTABLE, ErrMsg("Alloc scan desc memory failed."));
        return nullptr;
    }

    /* Step2: Set heap scan infos. */
    sysscan->heapRel = m_systableReldesc;
    sysscan->heapScan = HeapInterface::CreateHeapScanHandler(m_systableReldesc);
    if (unlikely(sysscan->heapScan == nullptr)) {
        DstorePfreeExt(sysscan);
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        ErrLog(DSTORE_ERROR, MODULE_SYSTABLE, ErrMsg("Create heapscan desc failed."));
        return nullptr;
    }
    sysscan->indexScan = nullptr;
    if (STORAGE_FUNC_FAIL(HeapInterface::BeginScan(sysscan->heapScan, snapshot))) {
        ErrLog(DSTORE_ERROR, MODULE_SYSTABLE, ErrMsg("Begin heapscan failed."));
        DstorePfreeExt(sysscan);
        return nullptr;
    }
    if (nkeys > 0) {
        HeapInterface::SetScanKey(sysscan->heapScan, m_systableReldesc->attr, nkeys, key);
    }
    return sysscan;
}

void Systable::EndScan(SysScanDesc sysscan) const
{
    HeapInterface::EndScan(sysscan->heapScan);
    HeapInterface::DestroyHeapScanHandler(sysscan->heapScan);
    sysscan->heapScan = nullptr;
    DstorePfreeExt(sysscan->selectColumn.typeOid);
    DstorePfreeExt(sysscan->selectColumn.attNo);
    DstorePfreeExt(sysscan->selectColumn.attLen);
    DstorePfreeExt(sysscan);
}

HeapTuple *Systable::GetNext(SysScanDesc sysscan)
{
    if (unlikely(sysscan == nullptr)) {
        return nullptr;
    }
    HeapTuple *heapTuple = nullptr;
    heapTuple = HeapInterface::SeqScan(sysscan->heapScan);
    if (heapTuple != nullptr && sysscan != nullptr && sysscan->heapRel != nullptr) {
        heapTuple->SetTableOid(sysscan->heapRel->relOid);
    }
    return heapTuple;
}
}  // namespace DSTORE
