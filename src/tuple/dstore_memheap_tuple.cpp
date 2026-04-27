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
 * dstore_memheap_tuple.cpp
 *
 * IDENTIFICATION
 *        src/tuple/dstore_memheap_tuple.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "securec.h"
#include "framework/dstore_instance.h"
#include "systable/systable_attribute.h"
#include "framework/dstore_thread.h"
#include "catalog/dstore_fake_attribute.h"
#include "catalog/dstore_fake_type.h"
#include "errorcode/dstore_tuple_error_code.h"
#include "transaction/dstore_transaction_types.h"
#include "buffer/dstore_buf.h"
#include "buffer/dstore_buf_mgr.h"
#include "page/dstore_heap_page.h"
#include "heap/dstore_heap_interface.h"
#include "tuple/dstore_tupledesc.h"
#include "tuple/dstore_heap_tuple.h"
#include "tuple/dstore_disk_tuple_interface.h"
#include "tuple/dstore_memheap_tuple.h"

namespace DSTORE {

Datum HeapTuple::GetSysattr(int attNum, bool *isNull)
{
    Datum result;

    /* Currently, no sys attribute ever reads as NULL. */
    *isNull = false;

    switch (static_cast<HeapTupleSystemAttr>(attNum)) {
        case HeapTupleSystemAttr::DSTORE_SELF_ITEM_POINTER_ATTRIBUTE_NUMBER:
            /* pass-by-reference datatype */
            result = PointerGetDatum(GetCtid());
            break;
        case HeapTupleSystemAttr::DSTORE_OBJECT_ID_ATTRIBUTE_NUMBER:
            result = ObjectIdGetDatum(GetOid());
            break;
        case HeapTupleSystemAttr::DSTORE_TABLE_OID_ATTRIBUTE_NUMBER:
            result = ObjectIdGetDatum(GetTableOid());
            StorageAssert(result != ObjectIdGetDatum(DSTORE_INVALID_OID));
            break;
        case HeapTupleSystemAttr::DSTORE_TRX_INSERT_XID_ATTRIBUTE_NUMBER:
            result = UInt64GetDatum(GetXid().m_placeHolder);
            break;
        case HeapTupleSystemAttr::DSTORE_TRX_DELETE_XID_ATTRIBUTE_NUMBER:
            result = UInt64GetDatum(GetDeleteXidForDebug().m_placeHolder);
            break;
        default:
            storage_set_error(TUPLE_ERROR_UNDEFINED_COLUMN, attNum);
            result = 0; /* keep compiler quiet */
            break;
    }
    return result;
}

void HeapTuple::CalculateOffset(int attNum, int natts, Form_pg_attribute *att, int& off)
{
    m_diskTuple->CalculateOffset<HeapDiskTuple>(attNum, natts, att, off);
}

void HeapTuple::CalculateOffsetSlow(CalculateOffsetSlowContext &context)
{
    m_diskTuple->CalculateOffsetSlow<HeapDiskTuple>(context);
}

bool HeapTuple::CheckHasNull(int attNum, bool *isNull, bool& slow) const
{
    if (HasNull()) {
        /*
         * there's a null somewhere in the tuple
         * check to see if any preceding bits are null...
         *
         * If destinated attr is null, return directly
         */
        if (AttrIsNull(attNum)) {
            *isNull = true;
            return true;
        }

        for (int i = 0; i < attNum; i++) {
            if (AttrIsNull(i)) {
                slow = true;
                break;
            }
        }
    }
    return false;
}

void HeapTuple::CheckHasVarAtt(int attNum, bool& slow, Form_pg_attribute *att) const
{
    /*
     * Otherwise, check for non-fixed-length attrs up to and including
     * target. If there aren't any, it's safe to cheaply initialize the
     * cached offsets for these attrs.
     */
    if (HasVariable()) {
        for (int i = 0; i <= attNum; i++) {
            if (att[i]->attlen <= 0) {
                slow = true;
                break;
            }
        }
    }
}

Datum HeapTuple::GetAttr(int attNum, TupleDesc desc, bool *isNull, bool forceReturnLobLocator)
{
    /* Step 0. check if we are fetching system attributes */
    if (unlikely(attNum <= 0)) {
        return GetSysattr(attNum, isNull);
    }

    /* Mainly for alter table .. add column */
    if (unlikely((uint32)attNum > m_diskTuple->GetNumColumn())) {
        return GetTupInitDefVal(attNum, desc, isNull);
    }

    char *tp = GetValues();         /* ptr to tuple data */
    *isNull = false;
    int off = 0;                /* offset in tuple data */
    bool slow = false;      /* can we use/set attcacheoff? */
    bool needCheckLobValue = false;
    int loboff = 0;
    Datum value = static_cast<Datum>(0);
    Form_pg_attribute *att = desc->attrs;

    /* Step 1. get array offset of current attNum by minus one here */
    attNum--;
    if (AttIsLob(att[attNum]) && !forceReturnLobLocator) {
        HeapDiskTuple *diskTuple = GetDiskTuple();
        if (diskTuple->HasInlineLobValue()) {
            needCheckLobValue = true;
        }
    }
    /* Step 2. Check if we have nulls in current tuple */
    if (CheckHasNull(attNum, isNull, slow)) {
        return value;
    }

    /* Step 3. Check if we could use cached offset to fetch value here */
    if (!slow) {
        /*
         * If we get here, there are no nulls up to and including the target
         * attribute.  If we have a cached offset, we can use it.
         */
        if (att[attNum]->attcacheoff >= 0 &&
            GetValuesOffset() + static_cast<uint32>(att[attNum]->attcacheoff) < GetDiskTupleSize()) {
            off = att[attNum]->attcacheoff;
            value = FetchAtt(att[attNum], tp + off);
            if ((!needCheckLobValue) || (!DstoreVarAttIsExternalDlob(tp + off))) {
                return value;
            }
        }
        /* Slow will be set true if has var att */
        CheckHasVarAtt(attNum, slow, att);
    }

    /* Step 4. Calculate offset (and cache them if we could) to fetch the correct attr */
    CalculateOffsetSlowContext context = {attNum, desc->natts, att, off, tp, needCheckLobValue, loboff};
    slow ? CalculateOffsetSlow(context) : CalculateOffset(attNum, desc->natts, att, off);
    if (GetValuesOffset() + static_cast<uint32>(off) >= GetDiskTupleSize()) {
        /* off exceeds tuple size, should be null of default value if any */
        return GetTupInitDefVal(attNum + 1, desc, isNull);
    }

    /* Step 5. fetch the value of required attr using calculated offset */
    value = FetchAtt(desc->attrs[attNum], tp + off);
    if (needCheckLobValue && DstoreVarAttIsExternalDlob(tp + off)) {
        VarattLobLocator *lobLocator = STATIC_CAST_PTR_TYPE(VarData1BE(tp + off), VarattLobLocator *);
        if (lobLocator->relid == DSTORE_INVALID_OID) {
            char *lobValues = STATIC_CAST_PTR_TYPE(GetDiskTuple(), char *) + GetDiskTupleSize() + loboff;
            value = PointerGetDatum(lobValues);
        }
    }

    return value;
}

/*
 * heap_form_tuple
 *		construct a tuple from the given values[] and isnull[] arrays,
 *		which are of the length indicated by tupleDescriptor->natts
 *
 * The result is allocated in the current memory context.
 */
HeapTuple *HeapTuple::FormTuple(TupleDesc tupleDesc, Datum *values, bool *isnull, AllocMemFunc allocMem)
{
#ifndef UT
    AutoMemCxtSwitch autoMemCxtSwitch{thrd->GetQueryMemoryContext()};
#endif
    bool hasNull = false;
    int attributeNum = tupleDesc->natts;

    /* Check if we have too many attributes */
    if (attributeNum > MAX_TUPLE_ATTR) {
        storage_set_error(TUPLE_ERROR_NATTRS_EXCEEDS_LIMIT);
        return nullptr;
    }

    /* Check if we have NULL columns */
    for (int i = 0; i < attributeNum; i++) {
        if (isnull[i]) {
            hasNull = true;
            break;
        }
    }

    /* Determine total space needed */
    Size valuesOffset = static_cast<Size>(HeapDiskTuple::GetValuesOffset(static_cast<uint16>(attributeNum), hasNull,
                                                                         tupleDesc->tdhasoid, false));
    Size lobSize = 0;
    Size dataSize = DataTuple::ComputeDataSize(tupleDesc, values, isnull, lobSize);
    Size diskTupleSize = valuesOffset + dataSize;
    Size totalSize = sizeof(HeapTuple) + diskTupleSize + lobSize;

    /* Alloc enough spaces for the new tuple */
    char *tuplePointer = static_cast<char *>((allocMem != nullptr) ? allocMem(totalSize) : DstorePalloc0(totalSize));
    if (unlikely(tuplePointer == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("DstorePalloc0 failed when FormTuple."));
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        return nullptr;
    }
    HeapTuple *memTuple = static_cast<HeapTuple *>(static_cast<void *>(tuplePointer));
    HeapDiskTuple* diskTuple = static_cast<HeapDiskTuple *>(static_cast<void *>(tuplePointer + sizeof(HeapTuple)));

    /* Set information of diskTuple correctly */
    diskTuple->SetNumColumn(static_cast<uint32>(attributeNum));
    if (hasNull) {
        diskTuple->SetHasNull();
    }
    if (tupleDesc->tdhasoid) {
        diskTuple->SetHasOid();
    }
    memTuple->SetDiskTupleSize(static_cast<uint32>(diskTupleSize));

    /* Now, fill this tuple by given data */
    if (likely(!hasNull)) {
        DataTuple::AssembleData<false, HeapDiskTuple>(tupleDesc, values, isnull,
            static_cast<char *>(static_cast<void *>(diskTuple)), diskTupleSize);
    } else {
        DataTuple::AssembleData<true, HeapDiskTuple>(tupleDesc, values, isnull,
            static_cast<char *>(static_cast<void *>(diskTuple)), diskTupleSize);
    }

    if (allocMem != nullptr) {
        memTuple->SetExternalMem();
    }
    diskTuple->SetDatumTypeId(tupleDesc->tdtypeid);
    diskTuple->SetDatumTypeMod(tupleDesc->tdtypmod);
    memTuple->m_diskTuple = diskTuple;
    memTuple->SetDatumVarSize(static_cast<uint32>(diskTupleSize));

    return memTuple;
}

void HeapTuple::DeformTuple(TupleDesc tupleDesc, Datum *values, bool *isNulls)
{
    char *tupleValues = GetDiskTuple()->GetValues();
    char *lobValues = STATIC_CAST_PTR_TYPE(GetDiskTuple(), char *) + GetDiskTupleSize();
    char *nullBits = static_cast<char *>(static_cast<void *>(GetDiskTuple()->GetNullBitmap()));
    int natts = GetNumAttrs();
    int end = DstoreMin(tupleDesc->natts, natts);

    /* refact "template" later */
    UNUSE_PARAM TupleAttrContext attrContext = {tupleDesc, values, isNulls, 0, false};
    DisassembleDataContext context = {
        attrContext, 0, end, tupleValues, nullBits, lobValues
    };
    if (HasNull()) {
        DataTuple::DisassembleData<true>(context);
    } else {
        DataTuple::DisassembleData<false>(context);
    }

    /* If tuple doesn't have all the atts indicated by tupleDesc, read the rest as default value */
    for (; natts < tupleDesc->natts; ++natts) {
        values[natts] = GetTupInitDefVal(natts + 1, tupleDesc, &isNulls[natts]);
    }
}

Datum HeapTuple::DeformColumnData(TupleDesc tupleDesc, Form_pg_attribute att, char *tupleValues, char *&lobValues)
{
    return DataTuple::DisassembleColumnData(tupleDesc, att, tupleValues, lobValues);
}

void HeapTuple::DeformTuplePart(TupleAttrContext &attrContext, int start, int end)
{
    char *tupleValue = GetDiskTuple()->GetValues();
    char *lobValue = STATIC_CAST_PTR_TYPE(GetDiskTuple(), char *) + GetDiskTupleSize();
    char *nullValue = reinterpret_cast<char *>(GetDiskTuple()->GetNullBitmap());

    /* refact "template" later */
    DisassembleDataContext context = {
        attrContext, start, end, tupleValue, nullValue, lobValue
    };
    if (HasNull()) {
        DataTuple::DisassembleData<true>(context);
    } else {
        DataTuple::DisassembleData<false>(context);
    }
}

HeapTuple* HeapTuple::Copy(AllocMemFunc allocMem)
{
    UNUSE_PARAM AutoMemCxtSwitch autoMemCxtSwitch(thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_QUERY));
    HeapTuple *newtuple;
    char *tuplePointer;
    errno_t rc;

    /* return nullptr if we have no disktuple to copy */
    if (this->m_diskTuple == nullptr) {
        return nullptr;
    }
    /* allocate enough space for new tuple */
    uint32 totalSize = m_head.len + sizeof(HeapTuple);
    tuplePointer = static_cast<char *>((allocMem != nullptr) ? allocMem(totalSize) : DstorePalloc(totalSize));
    if (STORAGE_VAR_NULL(tuplePointer)) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        ErrLog(DSTORE_ERROR, MODULE_CATALOG,
            ErrMsg("DstorePalloc0 fail size(%u) when allocating tuplePointer in HeapTuple Copy.", totalSize));
        return nullptr;
    }
    newtuple = static_cast<HeapTuple *>(static_cast<void *>(tuplePointer));
    newtuple->m_diskTuple = static_cast<HeapDiskTuple *>(static_cast<void *>(tuplePointer + sizeof(HeapTuple)));

    /* copy fields of m_head */
    newtuple->m_head = m_head;
    newtuple->m_head.type = DSTORE_TUPLE_TYPE;
    if (allocMem != nullptr) {
        newtuple->SetExternalMem();
    }
    /* then, memcopy the information of m_diskTuple directly */
    rc = memcpy_s(static_cast<char *>(static_cast<void *>(newtuple->m_diskTuple)), m_head.len,
                  static_cast<char *>(static_cast<void *>(m_diskTuple)), m_head.len);
    storage_securec_check(rc, "\0", "\0");

    return newtuple;
}

void HeapTuple::Copy(HeapTuple *destTup, HeapTuple *srcTup, bool isExternalMem)
{
    StorageAssert(srcTup != nullptr);
    StorageAssert(srcTup->m_diskTuple != nullptr);

    destTup->m_head = srcTup->m_head;
    destTup->m_head.type = DSTORE_TUPLE_TYPE;
    isExternalMem ? destTup->SetExternalMem() : destTup->SetInternalMem();

    destTup->m_diskTuple = reinterpret_cast<HeapDiskTuple *>(destTup + 1);
    errno_t rc = memcpy_s(reinterpret_cast<char *>(destTup->m_diskTuple), destTup->m_head.len,
                          reinterpret_cast<char *>(srcTup->m_diskTuple), srcTup->m_head.len);
    storage_securec_check(rc, "\0", "\0");
}

Xid HeapTuple::GetTupleXid() const
{
    return GetXid();
}

TupleDescData *TupleDescData::Copy()
{
    /*
     * Allocate enough memory for the tuple descriptor, including the
     * attribute rows, and set up the attribute row pointers.
     *
     * Note: we assume that sizeof(struct tupleDesc) is a multiple of the
     * struct pointer alignment requirement, and hence we don't need to insert
     * alignment padding between the struct and the array of attribute row
     * pointers.
     *
     * Note: Only the fixed part of pg_attribute rows is included in tuple
     * descriptors, so we only need ATTRIBUTE_FIXED_PART_SIZE space per attr.
     * That might need alignment padding, however.
     */
    char *stg = nullptr;
    uint32 attroffset =
        static_cast<uint32>(sizeof(TupleDescData) + static_cast<uint32>(natts) * sizeof(Form_pg_attribute));
    attroffset = MAXALIGN(attroffset);
    stg = static_cast<char *>(DstorePalloc(
        attroffset + static_cast<uint32>(natts) * static_cast<uint32>(MAXALIGN(ATTRIBUTE_FIXED_PART_SIZE))));
    if (STORAGE_VAR_NULL(stg)) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        ErrLog(DSTORE_ERROR, MODULE_CATALOG,
            ErrMsg("DstorePalloc0 fail size(%u) when allocating stg in TupleDescData Copy.",
                attroffset + static_cast<uint32>(natts) * static_cast<uint32>(MAXALIGN(ATTRIBUTE_FIXED_PART_SIZE))));
        return nullptr;
    }

    TupleDescData *copyDesc = static_cast<TupleDescData *>(static_cast<void *>(stg));
    *copyDesc = *this;

    if (natts > 0) {
        copyDesc->attrs = static_cast<Form_pg_attribute *>(static_cast<void *>(stg + sizeof(TupleDescData)));
        stg += attroffset;
        for (int i = 0; i < natts; i++) {
            copyDesc->attrs[i] = static_cast<Form_pg_attribute>(static_cast<void *>(stg));
            stg += MAXALIGN(ATTRIBUTE_FIXED_PART_SIZE);
        }
    } else {
        copyDesc->attrs = nullptr;
    }

    for (int i = 0; i < copyDesc->natts; i++) {
        errno_t rc = memcpy_s(copyDesc->attrs[i], ATTRIBUTE_FIXED_PART_SIZE, attrs[i], ATTRIBUTE_FIXED_PART_SIZE);
        storage_securec_check(rc, "\0", "\0");
    }

    copyDesc->initdefvals = nullptr;
    /* set copyDesc->constr = nullptr */

    /*
     * Also, assume the destination is not to be ref-counted.  (Copying the
     * source's refcount would be wrong in any case.)
     */
    copyDesc->tdtypeid = tdtypeid;
    copyDesc->tdtypmod = tdtypmod;
    copyDesc->tdisredistable = tdisredistable;

    return copyDesc;
}

}

namespace TupleInterface {
using namespace DSTORE;
HeapTuple *FormHeapTuple(TupleDesc tupleDescriptor, Datum *values, bool *isnull, AllocMemFunc allocMem)
{
    return HeapTuple::FormTuple(tupleDescriptor, values, isnull, allocMem);
}

Datum GetHeapSysAttr(HeapTuple *tuple, int attnum, bool *isNull)
{
    return tuple->GetSysattr(attnum, isNull);
}

Datum GetHeapAttr(HeapTuple *tuple, int attnum, TupleDesc desc, bool *isNull, bool forceReturnLobLocator)
{
    return tuple->GetAttr(attnum, desc, isNull, forceReturnLobLocator);
}

/*
 * heap_modify_tuple
 *		form a new tuple from an old tuple and a set of replacement values.
 *
 * The replValues, replIsnull, and doReplace arrays must be of the length
 * indicated by tupleDesc->natts.  The new tuple is constructed using the data
 * from replValues/replIsnull at columns where doReplace is true, and using
 * the data from the old tuple at columns where doReplace is false.
 *
 * The result is allocated in the current memory context.
 */
HeapTuple *ModifyTuple(HeapTuple *tuple, const TupleAttrContext &context, const bool *doReplace, AllocMemFunc allocMem)
{
    uint32 numberOfAttributes = static_cast<uint32>(context.tupleDesc->natts);
    Datum *values = nullptr;
    bool *isnull = nullptr;
    HeapTuple *newTuple;

    /* Step 1. Allocate and fill values and isnull arrays from either the tuple or repl info */
    values = static_cast<Datum *>(DstorePalloc(numberOfAttributes * sizeof(Datum)));
    if (unlikely(values == nullptr)) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("DstorePalloc fail size(%lu).",
                numberOfAttributes * sizeof(Datum)));
        return nullptr;
    }

    isnull = static_cast<bool *>(DstorePalloc(numberOfAttributes * sizeof(bool)));
    if (unlikely(isnull == nullptr)) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("DstorePalloc fail size(%lu).",
                numberOfAttributes * sizeof(bool)));
        return nullptr;
    }

    /* Step 1.1 Deform the old tuple first */
    DeformHeapTuple(tuple, context.tupleDesc, values, isnull);

    /* Step 1.2 Replace values/isnull elements by given repl info if needed */
    for (uint32 attoff = 0; attoff < numberOfAttributes; attoff++) {
        if (doReplace[attoff]) {
            values[attoff] = context.values[attoff];
            isnull[attoff] = context.isNulls[attoff];
        }
    }

    /* Step 2. Create a new tuple from the values and isnull arrays */
    newTuple = FormHeapTuple(context.tupleDesc, values, isnull, allocMem);

    /* Step 3: Clean up resources */
    DstorePfree(values);
    DstorePfree(isnull);

    /* Step 4: Copy identification information from the old tuple */
    if (newTuple == nullptr && StorageGetErrorCode() == COMMON_ERROR_MEMORY_ALLOCATION) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Allocate memory failed when modify tuple."));
        return nullptr;
    }
    StorageAssert(newTuple != nullptr);
    newTuple->SetCtid(*tuple->GetCtid());
    newTuple->SetTableOid(tuple->GetTableOid());
    newTuple->SetTdId(tuple->GetTdId());
    /* when modify tuple, set old tuple xid */
    newTuple->SetXid(tuple->GetXid());
    if (context.tupleDesc->tdhasoid) {
        newTuple->SetOid(tuple->GetOid());
    }

    return newTuple;
}


void ModifyLobLocatorRelid(DSTORE::ModifyLobLocatorRelidContext &context)
{
    DSTORE::HeapTuple *tuple = context.tuple;
    FormData_pg_attribute **attrs = context.tupleDesc->attrs;
    int numAttrs = context.tupleDesc->natts;
    context.needUpdateTuple = false;
    bool isNull = false;

    for (int i = 0; i < numAttrs; i++) {
        if (!AttIsLob(attrs[i])) {
            continue;
        }

        void *tp = DatumGetPointer(tuple->GetAttr(i + 1, context.tupleDesc, &isNull, true));
        if (isNull || !DstoreVarAttIsExternalDlob(tp)) {
            continue;
        }

        VarattLobLocator *lobLocator = STATIC_CAST_PTR_TYPE(VarData1BE(tp), VarattLobLocator *);
        StorageReleasePanic(lobLocator->relid == DSTORE_INVALID_OID, MODULE_HEAP,
                            ErrMsg("relid of lob column is invalid when swap partition."));
        if (lobLocator->relid != context.targetTableOid) {
            lobLocator->relid = context.targetTableOid;
            context.needUpdateTuple = true;
        }
    }
}

void DeformHeapTuple(HeapTuple *tuple, TupleDesc tupleDesc, Datum *values, bool *isNulls)
{
    return tuple->DeformTuple(tupleDesc, values, isNulls);
}

Datum DeformHeapColumnData(HeapTuple *tuple, TupleDesc tupleDesc, Form_pg_attribute att,
    char *tupleValues, char *&lobValues)
{
    return tuple->DeformColumnData(tupleDesc, att, tupleValues, lobValues);
}

void DeformHeapTuplePart(HeapTuple *tuple, TupleAttrContext &context, int start, int end)
{
    return tuple->DeformTuplePart(context, start, end);
}

HeapTuple *CopyHeapTuple(HeapTuple *tuple, AllocMemFunc allocMem)
{
    return tuple->Copy(allocMem);
}

void CopyHeapTupleToDest(HeapTuple *destTup, HeapTuple *srcTup, bool isExternalMem)
{
    HeapTuple::Copy(destTup, srcTup, isExternalMem);
}

uint64 GetXid(HeapTuple *tuple)
{
    return tuple->GetTupleXid().m_placeHolder;
}

bool HasNull(HeapTuple *tuple)
{
    return tuple->HasNull();
}

bool HasExternal(HeapTuple *tuple)
{
    return tuple->HasExternal();
}
bool HasInlineLob(DSTORE::HeapTuple *tuple)
{
    return tuple->HasInlineLob();
}

char *GetValues(HeapTuple *tuple)
{
    return tuple->GetValues();
}
uint32 GetValuesOffset(HeapTuple *tuple)
{
    return tuple->GetValuesOffset();
}

Oid GetOid(HeapTuple *tuple)
{
    return tuple->GetOid();
}
void SetOid(HeapTuple *tuple, Oid oid)
{
    tuple->SetOid(oid);
}
bool HasOid(HeapTuple *tuple)
{
    return tuple->HasOid();
}

void TupleStructInit(DSTORE::HeapTuple *tuple)
{
    tuple->Init();
}

size_t GetTupleStructSize()
{
    constexpr size_t tupleSize = MAXALIGN(sizeof(HeapTuple));
    return tupleSize;
}

Oid GetTableOid(HeapTuple *tuple)
{
    return tuple->GetTableOid();
}

void SetTableOid(HeapTuple *tuple, Oid oid)
{
    tuple->SetTableOid(oid);
}

void SetLobTargetOid(HeapTuple *tuple, Oid oid)
{
    tuple->SetLobTargetOid(oid);
}

ItemPointer GetCtidPtr(HeapTuple *tuple)
{
    return tuple->GetCtid();
}

ItemPointerData GetCtid(HeapTuple *tuple)
{
    return *tuple->GetCtid();
}

void SetCtid(HeapTuple *tuple, ItemPointerData ctid)
{
    tuple->SetCtid(ctid);
}

uint16 GetNumAttrs(HeapTuple *tuple)
{
    return tuple->GetNumAttrs();
}
bool AttrIsNull(HeapTuple *tuple, int attrIdx)
{
    return tuple->AttrIsNull(attrIdx);
}

char *GetStruct(HeapTuple *tuple)
{
    return tuple->GetStruct();
}

uint32 GetTupleTotalSize(HeapTuple *tuple)
{
    return tuple->GetDiskTupleSize() + sizeof(HeapTuple);
}

uint32 GetDiskTupleSize(HeapTuple *tuple)
{
    return tuple->GetDiskTupleSize();
}
void SetDiskTupleSize(HeapTuple *tuple, uint32_t size)
{
    tuple->SetDiskTupleSize(size);
}
void *GetDiskTuple(HeapTuple *tuple)
{
    return tuple->GetDiskTuple();
}
void SetDiskTuple(DSTORE::HeapTuple *tuple, void *diskTuple)
{
    tuple->SetDiskTuple(static_cast<HeapDiskTuple *>(diskTuple));
}
uint8 GetDiskTupleHeaderSize()
{
    return HEAP_DISK_TUP_HEADER_SIZE;
}
void SetDiskTuple(HeapTuple *tuple, char* diskTuple)
{
    tuple->m_diskTuple = static_cast<HeapDiskTuple *>(static_cast<void *>(diskTuple));
}
char *GetDiskTupleData(HeapTuple *tuple)
{
    return tuple->GetDiskTuple()->GetData();
}
uint32 GetSizeOfHeapTuple()
{
    return sizeof(HeapTuple);
}
void DiskTupleSetTupleSize(HeapTuple *tuple, uint16 size)
{
    return tuple->GetDiskTuple()->SetTupleSize(size);
}
void ResetInfo(HeapTuple *tuple)
{
    return tuple->GetDiskTuple()->ResetInfo();
}
void SetHasOid(HeapTuple *tuple)
{
    return tuple->GetDiskTuple()->SetHasOid();
}
void SetHasExternal(HeapTuple *tuple)
{
    return tuple->GetDiskTuple()->SetHasExternal();
}
void SetHasInlineLobValue(HeapTuple *tuple)
{
    return tuple->GetDiskTuple()->SetHasInlineLobValue();
}
void SetHasVariable(HeapTuple *tuple)
{
    return tuple->GetDiskTuple()->SetHasVariable();
}
void SetNumColumn(HeapTuple *tuple, uint32 nattrs)
{
    return tuple->GetDiskTuple()->SetNumColumn(nattrs);
}
void SetHasNull(HeapTuple *tuple)
{
    return tuple->GetDiskTuple()->SetHasNull();
}
void SetExternalMem(HeapTuple *tuple)
{
    return tuple->SetExternalMem();
}
Oid GetDatumTypeId(HeapTuple *tuple)
{
    return tuple->GetDatumTypeId();
}
void SetDatumTypeId(HeapTuple *tuple, Oid typeId)
{
    tuple->SetDatumTypeId(typeId);
}

int32 GetDatumTypeMod(HeapTuple *tuple)
{
    return tuple->GetDatumTypeMod();
}
void SetDatumTypeMod(HeapTuple *tuple, int32_t typeMod)
{
    tuple->SetDatumTypeMod(typeMod);
}

Form_pg_attribute GetHeapTupleDescAttr(TupleDesc tupdesc, int i)
{
    return DstoreTupleDescAttr(tupdesc, i);
}

template<typename OffsetType>
OffsetType HeapAttAlignNominal(OffsetType currOffset, char attAlign)
{
    return AttAlignNominal<OffsetType>(currOffset, attAlign);
}
template int32_t HeapAttAlignNominal(int32_t currOffset, char attAlign);

template<typename OffsetType>
OffsetType HeapAttAlignPointer(OffsetType currOffset, char attAlign, int16_t attLen, const char *attPtr)
{
    return AttAlignPointer<OffsetType>(currOffset, attAlign, attLen, attPtr);
}
template int32_t HeapAttAlignPointer(int32_t currOffset, char attAlign, int16 attLen, const char *attPtr);

template<typename OffsetType>
OffsetType HeapAttAddLength(OffsetType currOffset, int16_t attLen, char *attPtr)
{
    return AttAddLength<OffsetType>(currOffset, attLen, attPtr);
}
template int32_t HeapAttAddLength(int32_t currOffset, int16 attLen, char *attPtr);

Datum GetHeapTupInitDefVal(int attNum, TupleDesc tupleDesc, bool *isNull)
{
    return GetTupInitDefVal(attNum, tupleDesc, isNull);
}

void CreateHeapTuple(uint32_t diskTupSize, void *diskTup, HeapTuple *tuple)
{
    tuple->SetDiskTupleSize(diskTupSize);
    tuple->SetDiskTuple(static_cast<HeapDiskTuple *>(diskTup));
    tuple->SetCtid(INVALID_ITEM_POINTER);
}

void DestroyTuple(HeapTuple *tuple, FreeMemFunc freeMem)
{
    if (unlikely(tuple == nullptr)) {
        return;
    }

    if (tuple->IsExternalMem()) {
        StorageAssert(freeMem != nullptr);
        freeMem(tuple);
    } else {
        DstorePfree(tuple);
    }
}

bool IsTupleEqual(HeapTuple *newTuple, HeapTuple *oldTuple)
{
    /* if the tuple payload is the same ... */
    if (newTuple->GetDiskTupleSize() == oldTuple->GetDiskTupleSize() &&
        newTuple->GetDiskTuple()->GetNumColumn() == oldTuple->GetDiskTuple()->GetNumColumn() &&
        newTuple->GetDiskTuple()->HasNull() == oldTuple->GetDiskTuple()->HasNull() &&
        memcmp(((char *)(newTuple->GetDiskTuple())) + newTuple->GetDiskTuple()->GetHeaderSize(),
            ((char *)(oldTuple->GetDiskTuple())) + oldTuple->GetDiskTuple()->GetHeaderSize(),
            newTuple->GetDiskTupleSize() - newTuple->GetDiskTuple()->GetHeaderSize()) == 0) {
        return true;
    }
    return false;
}

}  // namespace TupleInterface
