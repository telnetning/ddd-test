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
 * dstore_fake_attribute.h
 *
 * IDENTIFICATION
 *        dstore/include/catalog/dstore_fake_attribute.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_GAUSSKERNEL_INCLUDE_CATALOG_DSTORE_FAKE_ATTRIBUTE_H
#define SRC_GAUSSKERNEL_INCLUDE_CATALOG_DSTORE_FAKE_ATTRIBUTE_H

#include "common/error/dstore_error.h"
#include "common/dstore_datatype.h"
#include "errorcode/dstore_common_error_code.h"
#include "errorcode/dstore_catalog_error_code.h"
#include "page/dstore_itemptr.h"
#include "tuple/dstore_tuple_struct.h"
#include "systable/systable_attribute.h"
#include "dstore_fake_type.h"

namespace DSTORE {
#ifdef CLOUD_NATIVE_DB1
#pragma pack (push, 1)
struct FormData_pg_attribute {
    Oid         attrelid;       /* OID of relation containing this attribute */
    DstoreNameData    attname;        /* name of attribute */

    /*
     * atttypid is the OID of the instance in Catalog Class pg_type that
     * defines the data type of this attribute (e.g. int32).  Information in
     * that instance is redundant with the attlen, attbyval, and attalign
     * attributes of this instance, so they had better match or openGauss will
     * fail.
     */
    Oid         atttypid;

    /*
     * attstattarget is the target number of statistics datapoints to collect
     * during VACUUM ANALYZE of this column.  A zero here indicates that we do
     * not wish to collect any stats about this column. A "-1" here indicates
     * that no value has been explicitly set for this column, so ANALYZE
     * should use the default setting.
     */
    int32        attstattarget;

    /*
     * attlen is a copy of the typlen field from pg_type for this attribute.
     * See atttypid comments above.
     */
    int16        attlen;

    /*
     * attnum is the "attribute number" for the attribute:  A value that
     * uniquely identifies this attribute within its class. For user
     * attributes, Attribute numbers are greater than 0 and not greater than
     * the number of attributes in the class. I.e. if the Class pg_class says
     * that Class XYZ has 10 attributes, then the user attribute numbers in
     * Class pg_attribute must be 1-10.
     *
     * System attributes have attribute numbers less than 0 that are unique
     * within the class, but not constrained to any particular range.
     *
     * Note that (attnum - 1) is often used as the index to an array.
     */
    int16        attnum;

    /*
     * attndims is the declared number of dimensions, if an array type,
     * otherwise zero.
     */
    int32        attndims;

    /*
     * fastgetattr() uses attcacheoff to cache byte offsets of attributes in
     * heap tuples.  The value actually stored in pg_attribute (-1) indicates
     * no cached value.  But when we copy these tuples into a tuple
     * descriptor, we may then update attcacheoff in the copies. This speeds
     * up the attribute walking process.
     *
     * Important: this is only for uncompressed tuples, both cached and updated.
     * And it cann't be applied to compressed tuples. Each attribute within
     * compressed tuple should be accessed one by one, step by step.
     */
    int32        attcacheoff;

    /*
     * atttypmod records type-specific data supplied at table creation time
     * (for example, the max length of a varchar field).  It is passed to
     * type-specific input and output functions as the third argument. The
     * value will generally be -1 for types that do not need typmod.
     */
    int32        atttypmod;

    /*
     * attbyval is a copy of the typbyval field from pg_type for this
     * attribute.  See atttypid comments above.
     */
    bool        attbyval;

    /* ----------
     * attstorage tells for VARLENA attributes, what the heap access
     * methods can do to it if a given tuple doesn't fit into a page.
     * Possible values are
     *      'p': Value must be stored plain always
     *      'e': Value can be stored in "secondary" relation (if relation
     *           has one, see pg_class.reltoastrelid)
     *      'm': Value can be stored compressed inline
     *      'x': Value can be stored compressed inline or in "secondary"
     * Note that 'm' fields can also be moved out to secondary storage,
     * but only as a last resort ('e' and 'x' fields are moved first).
     * ----------
     */
    char        attstorage;

    /*
     * attalign is a copy of the typalign field from pg_type for this
     * attribute.  See atttypid comments above.
     */
    char        attalign;

    /* This flag represents the "NOT NULL" constraint */
    bool        attnotnull;

    /* Has DEFAULT value or not */
    bool        atthasdef;

    /* Is dropped (ie, logically invisible) or not */
    bool        attisdropped;

    /* Has a local definition (hence, do not drop when attinhcount is 0) */
    bool        attislocal;

    /* Compression Mode for this attribute
     * its size is 1Byte, and the 7 fields before are also 1Btye wide, so place it here;
     * its valid value is:  CMPR_NONE ~ CMPR_NUMSTR. see also pagecompress.h
     */
    int8        attcmprmode;

    /* Number of times inherited from direct parent relation(s) */
    int32        attinhcount;

    /* attribute's collation */
    Oid         attcollation;

#ifdef CATALOG_VARLEN                    /* variable-length fields start here */
    /* NOTE: The following fields are not present in tuple descriptors. */

    /* Column-level access permissions */
    aclitem     attacl[1];

    /* Column-level options */
    text        attoptions[1];

    /* Column-level FDW options */
    text        attfdwoptions[1];

    /* the value is not null only when ALTER TABLE ... ADD COLUMN call */
    bytea       attinitdefval;

    /* the attribute type for kv storage: tag(1), field(2), time(3), hide(4) or default(0) */
    int8        attkvtype;
#endif
};
#pragma pack (pop)
#endif
/*
 * ATTRIBUTE_FIXED_PART_SIZE is the size of the fixed-layout,
 * guaranteed-not-null part of a pg_attribute row.  This is in fact as much
 * of the row as gets copied into tuple descriptors, so don't expect you
 * can access fields beyond attcollation except in a real tuple!
 */
constexpr int ATTRIBUTE_FIXED_PART_SIZE = (offsetof(FormData_pg_attribute, attcollation) + sizeof(Oid));
const int16 ATT_VAR_LEN_TYPE = -1;
const int16 ATT_CSTR_LEN_TYPE = -2;

inline bool AttIsPackable(Form_pg_attribute att)
{
    return att->attlen == ATT_VAR_LEN_TYPE && att->attstorage != 'p';
}

inline bool VarlenaAttIsPackable(Form_pg_attribute att)
{
    return att->attstorage != 'p';
}

inline bool AttIsLob(Form_pg_attribute att)
{
    return (att)->attlen == ATT_VAR_LEN_TYPE && ((att)->atttypid == BLOBOID || (att)->atttypid == CLOBOID);
}

/*
 * FetchAtt, different getDatum macro for attrs with different length
 * undefined length
 */
inline Datum FetchAtt(void *value, bool attByVal, int16 attLen)
{
    if (unlikely(!attByVal)) {
        return PointerGetDatum(value);
    }
    Datum attDatum = static_cast<Datum>(0);
    switch (attLen) {
        case  sizeof(int64):
            attDatum = Int64GetDatum(*static_cast<int64 *>(value));
            break;
        case sizeof(int32):
            attDatum = Int32GetDatum(*static_cast<int32 *>(value));
            break;
        case sizeof(int16):
            attDatum = Int16GetDatum(*static_cast<int16 *>(value));
            break;
        case sizeof(char):
            attDatum = CharGetDatum(*static_cast<char *>(value));
            break;
        default:
            storage_set_error(ATTRIBUTE_ERROR_UNSUPPORTED_BYVAL_LENGTH, attLen);
    }
    return attDatum;
}

inline Datum FetchAtt(Form_pg_attribute att, void *value)
{
    return FetchAtt(value, att->attbyval, att->attlen);
}

/*
 * AttAlignNominal aligns the given offset as needed for a datum of alignment
 * requirement attalign, ignoring any consideration of packed varlena datums.
 * There are three main use cases for using this macro directly:
 *  * we know that the att in question is not varlena (attlen != -1);
 *    in this case it is cheaper than the above macros and just as good.
 *  * we need to estimate alignment padding cost abstractly, ie without
 *    reference to a real tuple.  We must assume the worst case that
 *    all varlenas are aligned.
 *  * within arrays, we unconditionally align varlenas (XXX this should be
 *    revisited, probably).
 *
 * The attalign cases are tested in what is hopefully something like their
 * frequency of occurrence.
 *
 * The alignment of diskTuple colums in DSTORE has been removed.
 */
template<typename OffsetType>
inline OffsetType AttAlignNominal(OffsetType currOffset, char attAlign)
{
    (void) attAlign;
    return currOffset;
}
inline char *AttAlignNominal(char *currOffset, char attAlign)
{
    return reinterpret_cast<char *>(AttAlignNominal(reinterpret_cast<uintptr_t>(currOffset), attAlign));
}

/*
 * AttAlignDatum aligns the given offset as needed for a datum of alignment
 * requirement attalign and typlen attlen.  attdatum is the Datum variable
 * we intend to pack into a tuple (it's only accessed if we are dealing with
 * a varlena type).  Note that this assumes the Datum will be stored as-is;
 * callers that are intending to convert non-short varlena datums to short
 * format have to account for that themselves.
 */
inline Size AttAlignDatum(Size curOffset, char attAlign, int16 attLen, const char *attValue)
{
    if ((attLen == ATT_VAR_LEN_TYPE) && (DstoreVarAttIsShort(attValue))) {
        return curOffset;
    }
    return static_cast<Size>(AttAlignNominal(curOffset, attAlign));
}

/*
 * AttAlignPointer performs the same calculation as AttAlignDatum,
 * but is used when walking a tuple.  attptr is the current actual data
 * pointer; when accessing a varlena field we have to "peek" to see if we
 * are looking at a pad byte or the first byte of a 1-byte-header datum.
 * (A zero byte must be either a pad byte, or the first byte of a correctly
 * aligned 4-byte length word; in either case we can align safely.  A non-zero
 * byte must be either a 1-byte length word, or the first byte of a correctly
 * aligned 4-byte length word; in either case we need not align.)
 *
 * Note: some callers pass a "char *" pointer for cur_offset.  This is
 * a bit of a hack but should work all right as long as intptr_t is the
 * correct width.
 */
template<typename OffsetType>
inline OffsetType AttAlignPointer(OffsetType currOffset, char attAlign, int16 attLen, const char *attPtr)
{
    if (attLen == ATT_VAR_LEN_TYPE && VarAttNotPadByte(attPtr)) {
        return currOffset;
    }
    return AttAlignNominal(currOffset, attAlign);
}

/*
 * AttAddLength increments the given offset by the space needed for
 * the given Datum variable.  attDatum is only accessed if we are dealing
 * with a variable-length attribute.
 */
template<typename OffsetType>
inline OffsetType AttAddLength(OffsetType currOffset, int16 attLen, char *attPtr)
{
    OffsetType newOffset = static_cast<OffsetType>(0);
    if (attLen > 0) {
        newOffset = currOffset + attLen;
    } else if (attLen == ATT_VAR_LEN_TYPE) {
        StorageAssert(attPtr != nullptr);
        newOffset = currOffset + DstoreVarSizeAny(attPtr);
    } else if (attLen == ATT_CSTR_LEN_TYPE) {
        StorageAssert(attPtr != nullptr);
        newOffset = currOffset + (strlen(attPtr) + 1);
    } else {
        storage_set_error(ATTRIBUTE_ERROR_UNSUPPORTED_BYVAL_LENGTH, attLen);
    }
    return newOffset;
}

/*
 * StoreAttByVal is a partial inverse of FetchAtt: store a given Datum
 * value into a tuple data area at the specified address.  However, it only
 * handles the byval case, because in typical usage the caller needs to
 * distinguish by-val and by-ref cases anyway, and so a do-it-all macro
 * wouldn't be convenient.
 */
inline void StoreAttByVal(void *attValue, Datum newDatum, int16 attLen)
{
    StorageAssert(attValue != nullptr);
    switch (attLen) {
        case sizeof(char):
            *static_cast<char *>(attValue) = DatumGetChar(newDatum);
            break;
        case sizeof(int16):
            *static_cast<int16 *>(attValue) = DatumGetInt16(newDatum);
            break;
        case sizeof(int32):
            *static_cast<int32 *>(attValue) = DatumGetInt32(newDatum);
            break;
        case sizeof(Datum):
            *static_cast<Datum *>(attValue) = newDatum;
            break;
        default:
            storage_set_error(ATTRIBUTE_ERROR_UNSUPPORTED_BYVAL_LENGTH, attLen);
    }
}

}
#endif /* SRC_GAUSSKERNEL_INCLUDE_CATALOG_STORAGE_FAKE_ATTRIBUTE_H */
