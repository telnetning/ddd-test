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
 * dstore_data_tuple.cpp
 *     This file defines the building blocks that bg page writer needs.
 *
 * IDENTIFICATION
 *        src/tuple/dstore_data_tuple.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "common/log/dstore_log.h"
#include "catalog/dstore_fake_type.h"
#include "systable/systable_attribute.h"
#include "tuple/dstore_heap_tuple.h"
#include "tuple/dstore_index_tuple.h"
#include "tuple/dstore_tuple_struct.h"
#include "tuple/dstore_data_tuple.h"

namespace DSTORE {
template<typename TupleType>
void DataTuple::CalculateOffset(int attNum, int natts, Form_pg_attribute *att, int& off)
{
    int j = 1;

    /*
     * If we get here, we have a tuple with no nulls or var-widths up to
     * and including the target attribute, so we can use the cached offset
     * ... only we don't have it yet, or we'd not have got here.  Since
     * it's cheap to compute offsets for fixed-width columns, we take the
     * opportunity to initialize the cached offsets for *all* the leading
     * fixed-width columns, in hope of avoiding future visits to this
     * routine.
     */
    att[0]->attcacheoff = 0;

    /* we might have set some offsets in the slow path previously */
    while (j < natts && att[j]->attcacheoff > 0) {
        j++;
    }

    /* calculate offset for all leading columns */
    off = att[j - 1]->attcacheoff + att[j - 1]->attlen;
    for (; j < natts; j++) {
        if (att[j]->attlen <= 0) {
            break;
        }

        off = AttAlignNominal(off, att[j]->attalign);
        att[j]->attcacheoff = off;
        off += att[j]->attlen;
    }

    /* Sanity check here */
    StorageAssert(j > attNum);

    /* Use calculated cached offset to fetch the attr value later */
    off = att[attNum]->attcacheoff;
}

template <typename TupleType>
void DataTuple::CalculateOffsetSlow(CalculateOffsetSlowContext &context)
{
    bool usecache = true;
    /*
     * Now we know that we have to walk the tuple CAREFULLY.  But we still
     * might be able to cache some offsets for next time.
     *
     * Note - This loop is a little tricky.  For each non-null attribute,
     * we have to first account for alignment padding before the attr,
     * then advance over the attr based on its length.      Nulls have no
     * storage and no alignment padding either.  We can use/set
     * attcacheoff until we reach either a null or a var-width attribute.
     */
    int off = 0;
    int loboff = 0;
    int natts = context.natts;
    int attNum = context.attNum;
    Form_pg_attribute *att = context.att;
    char* tp = context.tp;
    bool needCheckLobValue = context.needCheckLobValue;
    TupleType *diskTuple = static_cast<TupleType *>(static_cast<void *>(this));
    /* this loop exits at "break" */
    for (int i = 0; i < natts; i++) {
        if (diskTuple->HasNull() && diskTuple->AttrIsNull(i)) {
            usecache = false;
            continue; /* this cannot be the target att; otherwise, it returns already */
        }

        /* If we know the next offset, we can skip the rest of following steps */
        if (usecache && att[i]->attcacheoff >= 0) {
            off = att[i]->attcacheoff;
        } else if (att[i]->attlen == ATT_VAR_LEN_TYPE) {
            /*
             * We can only cache the offset for a varlena attribute if the
             * offset is already suitably aligned, so that there would be
             * no pad bytes in any case: then the offset will be valid for
             * either an aligned or unaligned value.
             */
            if (usecache && off == AttAlignNominal(off, att[i]->attalign)) {
                att[i]->attcacheoff = off;
            } else {
                off = AttAlignPointer(off, att[i]->attalign, -1, tp + off);
                usecache = false;
            }
        } else {
            /* not varlena, so safe to use att_align_nominal here */
            off = AttAlignNominal(off, att[i]->attalign);

            if (usecache) {
                att[i]->attcacheoff = off;
            }
        }

        /* We have reached the required attr, break here */
        if (i == attNum) {
            context.off = off;
            context.loboff = loboff;
            break;
        }
        if (needCheckLobValue && AttIsLob(att[i])) {
            void *temp_tp = tp + off;
            if (DstoreVarAttIsExternalDlob(temp_tp)) {
                VarattLobLocator *lobLocator = STATIC_CAST_PTR_TYPE(VarData1BE(temp_tp), VarattLobLocator *);
                loboff += lobLocator->rawsize;
            }
        }
        /* Otherwise, keep calculating offset by adding data length of current attr to it */
        off = AttAddLength(off, att[i]->attlen, tp + off);

        if (usecache && att[i]->attlen <= 0) {
            usecache = false; /* can't use attcacheoff anymore */
        }
    }
}

Size DataTuple::ComputeDataSize(TupleDesc desc, const Datum *values, const bool *isnull)
{
    Size lobSize = 0;
    return ComputeDataSize(desc, values, isnull, lobSize);
}

Size DataTuple::ComputeDataSize(TupleDesc desc, const Datum *values, const bool *isnull, Size &lobSize)
{
    Size dataLength = 0;
    int numberOfAttributes = desc->natts;
    Form_pg_attribute *att = desc->attrs;

    for (int i = 0; i < numberOfAttributes; i++) {
        char *val;

        if (isnull[i]) {
            continue;
        }

        val = DatumGetPointer(values[i]);
        if (AttIsPackable(att[i]) && DSTORE_VARATT_CAN_MAKE_SHORT(val)) {
            /*
             * we're anticipating converting to a short varlena header, so
             * adjust length and don't count any alignment
             */
            dataLength += DstoreVarAttConvertedShortSize(val);
        } else if (desc->tdhaslob && AttIsLob(att[i]) &&
                   VarAttIs4B(val) && DstoreVarSize4B(val) > MAX_INLINE_LOB_SIZE) {
            dataLength += VARHDRSZ_EXTERNAL + sizeof(VarattLobLocator);
            lobSize += DstoreVarSize4B(val);
        } else if (att[i]->attlen == ATT_VAR_LEN_TYPE && DstoreVarAttIsExternalExpanded(val)) {
            dataLength = AttAlignNominal(dataLength, att[i]->attalign);
            dataLength += DstoreExpandedVarSize(val);
        } else {
            dataLength = AttAlignDatum(dataLength, att[i]->attalign, att[i]->attlen, val);
            dataLength = static_cast<Size>(AttAddLength(dataLength, att[i]->attlen, val));
        }
    }

    return dataLength;
}

template <bool hasnulls>
void DataTuple::DisassembleData(DisassembleDataContext &context)
{
    bool slow = context.attrContext.slow;
    int32 offset = static_cast<int32>(context.attrContext.offset); /* offset in tuple data */
    Datum *values = context.attrContext.values;
    bool *isNulls = context.attrContext.isNulls;
    TupleDescData *tupleDesc = context.attrContext.tupleDesc;
    char *nullBits = context.nullBits;
    char *tupleValues = context.tupleValues;
    char *lobValues = context.lobValues;
    int start = context.start;
    int end = context.end;
    Form_pg_attribute thisatt;

    for (int attnum = start; attnum < end; attnum++) {
        thisatt = DstoreTupleDescAttr(tupleDesc, attnum);
        if (hasnulls && DataTupleAttrIsNull(static_cast<uint32>(attnum), nullBits)) {
            values[attnum] = static_cast<Datum>(0);
            isNulls[attnum] = true;
            slow = true;
            continue;
        }

        isNulls[attnum] = false;

        if (!slow && thisatt->attcacheoff >= 0) {
            offset = thisatt->attcacheoff;
        } else if (thisatt->attlen == ATT_VAR_LEN_TYPE) {
            /*
             * We can only cache the offset for a varlena attribute if the
             * offset is already suitably aligned, so that there would be no
             * pad bytes in any case: then the offset will be valid for either
             * an aligned or unaligned value.
             */
            if (!slow && offset == AttAlignNominal(offset, thisatt->attalign)) {
                thisatt->attcacheoff = offset;
            } else {
                offset = AttAlignPointer(offset, thisatt->attalign, -1, tupleValues + offset);
                slow = true;
            }
        } else {
            offset = AttAlignNominal(offset, thisatt->attalign);
            if (!slow) {
                thisatt->attcacheoff = offset;
            }
        }

        values[attnum] = DisassembleColumnData(tupleDesc, thisatt, tupleValues + offset, lobValues);
        ErrLog(DSTORE_DEBUG1, MODULE_HEAP, ErrMsg("DisassembleData AttAddLength currOffset: %u, attLen: %d, attPtr: %s",
                                                  offset, thisatt->attlen, tupleValues + offset));
        offset = AttAddLength(offset, thisatt->attlen, tupleValues + offset);
        if (thisatt->attlen <= 0) {
            slow = true; /* can't use attcacheoff anymore */
        }
    }

    context.attrContext.slow = slow;
    context.attrContext.offset = offset;
}

Datum DataTuple::DisassembleColumnData(TupleDesc tupleDesc, Form_pg_attribute att,
                                       char *tupleValues, char *&lobValues)
{
    Datum result;
    if (tupleDesc->tdhaslob && AttIsLob(att) &&
        DstoreVarAttIsExternalDlob(tupleValues) &&
        lobValues) {
        VarattLobLocator *locator = STATIC_CAST_PTR_TYPE(
            VarData1BE(tupleValues), VarattLobLocator *);
        if (locator->ctid == INVALID_ITEM_POINTER.m_placeHolder) {
            result = FetchAtt(att, lobValues);
            lobValues += DstoreVarSize4B(lobValues);
        } else {
            result = FetchAtt(att, tupleValues);
        }
    } else {
        result = FetchAtt(att, tupleValues);
    }

    return result;
}

template <typename TupleType>
Size DataTuple::AssembleColumnData(AssembleDataContext<TupleType> &context)
{
    Size dataLength;
    errno_t rc = EOK;
    if (context.att->attbyval) {
        /* pass-by-value */
        context.tupleValues = AttAlignNominal(context.tupleValues, context.att->attalign);
        StoreAttByVal(context.tupleValues, context.value, context.att->attlen);
        dataLength = static_cast<uint16>(context.att->attlen);
    } else if (context.att->attlen == ATT_VAR_LEN_TYPE) {
        /* varlena */
        Pointer val = DatumGetPointer(context.value);
        context.diskTuple->SetHasVariable();
        if (DstoreVarAttIsExternal(val)) {
            if (DstoreVarAttIsExternalExpanded(val)) {
                /*
                 * we want to flatten the expanded value so that the
                 * constructed tuple doesn't depend on it
                 */
                context.tupleValues = AttAlignNominal(context.tupleValues, context.att->attalign);
                dataLength = DstoreExpandedVarSize(val);
                DstoreformExpandedVar(val, context.tupleValues, dataLength);
            } else {
                context.diskTuple->SetHasExternal();
                /* no alignment, since it's short by definition */
                dataLength = DstoreVarSizeExternal(val);
                rc = memcpy_s(context.tupleValues, context.remainLength, val, dataLength);
                storage_securec_check(rc, "\0", "\0");
            }
        } else if (DstoreVarAttIsShort(val)) {
            /* no alignment for short varlenas */
            dataLength = DstoreVarSizeShort(val);
            rc = memcpy_s(context.tupleValues, context.remainLength, val, dataLength);
            storage_securec_check(rc, "\0", "\0");
        } else if (AttIsPackable(context.att) && DSTORE_VARATT_CAN_MAKE_SHORT(val)) {
            /* convert to short varlena -- no alignment */
            dataLength = DstoreVarAttConvertedShortSize(val);
            DstoreSetVarSizeShort(context.tupleValues, static_cast<uint32>(dataLength));
            if (dataLength > 1) {
                rc = memcpy_s(context.tupleValues + 1,
                              context.remainLength - 1,
                              static_cast<void*>(VarData(val)),
                              dataLength - 1);
                storage_securec_check(rc, "\0", "\0");
            }
        } else if (context.hasLob && AttIsLob(context.att) &&
                   VarAttIs4B(val) && DstoreVarSize4B(val) > MAX_INLINE_LOB_SIZE) {
            /* store invalid lob locators in-line as placeholders */
            context.diskTuple->SetHasExternal();
            context.diskTuple->SetHasInlineLobValue();
            dataLength = VARHDRSZ_EXTERNAL + sizeof(VarattLobLocator);
            DstoreSetVarTag1BE(context.tupleValues, VartagExternal::VARTAG_DLOB_LOCATOR);
            VarattLobLocator *lobLocator = STATIC_CAST_PTR_TYPE(VarData1BE(context.tupleValues), VarattLobLocator *);
            lobLocator->relid = DSTORE_INVALID_OID;
            lobLocator->rawsize = (int32_t)(DstoreVarSize4B(val));
            lobLocator->extsize = (int32_t)DstoreVarSize4B(val);
            lobLocator->ctid = INVALID_ITEM_POINTER.m_placeHolder;
            /* append lob values */
            Size lobValueSize = DstoreVarSize4B(val);
            rc = memcpy_s(context.lobValues, lobValueSize, val, lobValueSize);
            storage_securec_check(rc, "\0", "\0");
            context.lobValues += lobValueSize;
        } else {
            /* full 4-byte header varlena */
            context.tupleValues = AttAlignNominal(context.tupleValues, context.att->attalign);
            dataLength = DstoreVarSize(val);
            rc = memcpy_s(context.tupleValues, context.remainLength, val, dataLength);
            storage_securec_check(rc, "\0", "\0");
        }
    } else if (context.att->attlen == ATT_CSTR_LEN_TYPE) {
        /* cstring ... never needs alignment */
        context.diskTuple->SetHasVariable();
        StorageAssert(context.att->attalign == 'c');
        dataLength = strlen(DatumGetCString(context.value)) + 1;
        rc = memcpy_s(context.tupleValues, context.remainLength, DatumGetPointer(context.value), dataLength);
        storage_securec_check(rc, "\0", "\0");
    } else {
        /* fixed-length pass-by-reference */
        context.tupleValues = AttAlignNominal(context.tupleValues, context.att->attalign);
        StorageAssert(context.att->attlen > 0);
        dataLength = static_cast<uint16>(context.att->attlen);
        rc = memcpy_s(context.tupleValues, context.remainLength, DatumGetPointer(context.value), dataLength);
        storage_securec_check(rc, "\0", "\0");
    }
    return dataLength;
}

template <bool hasNull, typename TupleType>
void DataTuple::AssembleData(TupleDesc tupleDesc, Datum *values, const bool *isnull, char *tuple, Size dataSize)
{
    uint8 *nullBit = nullptr;
    Form_pg_attribute *att = tupleDesc->attrs;

    char *begin = tuple;
    char *lobValues = tuple + dataSize;
    StorageAssert(lobValues != nullptr);
    TupleType *diskTuple = static_cast<TupleType *>(static_cast<void *>(tuple));

#ifdef DSTORE_USE_ASSERT_CHECKING
    char *start = tuple;
#endif
    const uint32 byteBitNum = 8;
    uint32 bitmapLen = DataTuple::GetBitmapLen(tupleDesc->natts);
    if (hasNull) {
        nullBit = diskTuple->GetNullBitmap();
        StorageAssert(nullBit != nullptr);
        errno_t rc = memset_s(nullBit, bitmapLen, 0, bitmapLen);
        storage_securec_check(rc, "\0", "\0");
    }

    char *tupleValues = diskTuple->GetValues();
    diskTuple->ResetAttrInfo();

    for (uint32 i = 0; i < static_cast<uint32>(tupleDesc->natts); i++) {
        Size remainLength = dataSize - static_cast<size_t>(tupleValues - begin);
        if (hasNull && nullBit != nullptr) {
            if (isnull[i]) {
                diskTuple->SetHasNull();
                continue;
            }
            StorageAssert((nullBit != nullptr && (i / byteBitNum < bitmapLen)));
            nullBit[i / byteBitNum] |= (static_cast<uint32>(1) << (i % byteBitNum));
        }

        /*
         * XXX we use the att_align macros on the pointer value itself, not on
         * an offset.  This is a bit of a hack.
         */
        AssembleDataContext<TupleType> context = {
            values[i], diskTuple, att[i], tupleValues, remainLength, tupleDesc->tdhaslob, lobValues
        };
        Size dataLength = AssembleColumnData<TupleType>(context);

        tupleValues += dataLength;
    }

    StorageAssert(static_cast<size_t>(tupleValues - start) == dataSize);
}

template void DataTuple::CalculateOffset<HeapDiskTuple>(int attNum, int natts, Form_pg_attribute *att, int &off);
template void DataTuple::CalculateOffset<IndexTuple>(int attNum, int natts, Form_pg_attribute *att, int &off);
template void DataTuple::CalculateOffsetSlow<HeapDiskTuple>(CalculateOffsetSlowContext &context);
template void DataTuple::CalculateOffsetSlow<IndexTuple>(CalculateOffsetSlowContext &context);

template void DataTuple::DisassembleData<true>(DisassembleDataContext &);
template void DataTuple::DisassembleData<false>(DisassembleDataContext &);

template Size DataTuple::AssembleColumnData<HeapDiskTuple>(AssembleDataContext<HeapDiskTuple> &);
template Size DataTuple::AssembleColumnData<IndexTuple>(AssembleDataContext<IndexTuple> &);

template void DataTuple::AssembleData<true, HeapDiskTuple>(TupleDesc, Datum *, const bool *, char *, Size);
template void DataTuple::AssembleData<false, HeapDiskTuple>(TupleDesc, Datum *, const bool *, char *, Size);
template void DataTuple::AssembleData<true, IndexTuple>(TupleDesc, Datum *, const bool *, char *, Size);
template void DataTuple::AssembleData<false, IndexTuple>(TupleDesc, Datum *, const bool *, char *, Size);

}  // namespace DSTORE
