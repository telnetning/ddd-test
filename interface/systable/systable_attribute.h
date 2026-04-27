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
 * Description:
 * The systable_attribute is one of core system tables.
 */
#ifndef DSTORE_SYSTABLE_ATTRIBUTE_H
#define DSTORE_SYSTABLE_ATTRIBUTE_H
#include <cstdint>
#include "common/dstore_common_utils.h"
#include "index/dstore_scankey.h"

namespace DSTORE {
constexpr char SYSTABLE_ATTRIBUTE_NAME[] = "sys_attribute";
constexpr int SYSTABLE_ATTRIBUTE_OID = 1249;
constexpr int SYSTABLE_ATTRIBUTE_ROWTYPE = 75;
constexpr int NATTS_SYS_ATTRIBUTE = 25;
constexpr int ANUM_SYS_ATTRIBUTE_ATTRELID = 1;
constexpr int ANUM_SYS_ATTRIBUTE_ATTNAME = 2;
constexpr int ANUM_SYS_ATTRIBUTE_ATTTYPID = 3;
constexpr int ANUM_SYS_ATTRIBUTE_ATTSTATTARGET = 4;
constexpr int ANUM_SYS_ATTRIBUTE_ATTLEN = 5;
constexpr int ANUM_SYS_ATTRIBUTE_ATTNUM = 6;
constexpr int ANUM_SYS_ATTRIBUTE_ATTNDIMS = 7;
constexpr int ANUM_SYS_ATTRIBUTE_ATTCACHEOFF = 8;
constexpr int ANUM_SYS_ATTRIBUTE_ATTTYPMOD = 9;
constexpr int ANUM_SYS_ATTRIBUTE_ATTBYVAL = 10;
constexpr int ANUM_SYS_ATTRIBUTE_ATTSTORAGE = 11;
constexpr int ANUM_SYS_ATTRIBUTE_ATTALIGN = 12;
constexpr int ANUM_SYS_ATTRIBUTE_ATTNOTNULL = 13;
constexpr int ANUM_SYS_ATTRIBUTE_ATTHASDEF = 14;
constexpr int ANUM_SYS_ATTRIBUTE_ATTISDROPPED = 15;
constexpr int ANUM_SYS_ATTRIBUTE_ATTISLOCAL = 16;
constexpr int ANUM_SYS_ATTRIBUTE_ATTCMPRMODE = 17;
constexpr int ANUM_SYS_ATTRIBUTE_ATTINHCOUNT = 18;
constexpr int ANUM_SYS_ATTRIBUTE_ATTCOLLATION = 19;
/* variable-length fields start here */
constexpr int ANUM_SYS_ATTRIBUTE_ATTACL = 20;
constexpr int ANUM_SYS_ATTRIBUTE_ATTOPTIONS = 21;
constexpr int ANUM_SYS_ATTRIBUTE_ATTFDWOPTIONS = 22;
constexpr int ANUM_SYS_ATTRIBUTE_ATTINITDEFVAL = 23;
/* variable-length fields end here */
constexpr int ANUM_SYS_ATTRIBUTE_ATTKVTYPE = 24;
constexpr int ANUM_SYS_ATTRIBUTE_ATTIDENTITY = 25;

#pragma pack (push, 1)
struct SysAttributeTupDef {
    Oid attrelid;     /* OID of relation containing this attribute */
    DstoreNameData attname; /* name of attribute */

    /*
     * atttypid is the OID of the instance in Catalog Class SysTableType that
     * defines the data type of this attribute (e.g. int32_t).  Information in
     * that instance is redundant with the attlen, attbyval, and attalign
     * attributes of this instance, so they had better match or openGauss will fail.
     */
    Oid atttypid;

    /*
     * attstattarget is the target number of statistics datapoints to collect
     * during VACUUM ANALYZE of this column.  A zero here means that we do
     * not wish to collect any stats about this column. A "-1" here indicates
     * that no value has been explicitly set for this column, so ANALYZE
     * should use the default setting.
     */
    int32_t attstattarget;

    /*
     * attlen is a copy of the typlen field from SysTableType for this attribute.
     * See atttypid comments above.
     */
    int16_t attlen;

    /*
     * attnum is the "attribute number" for the attribute:	A value that
     * uniquely identifies this attribute within its class. For user
     * attributes, Attribute numbers are greater than 0 and not greater than
     * the number of attributes in the class. I.e. if the Class SystableRelation says
     * that Class XYZ has 10 attributes, then the user attribute numbers in
     * Class SysTableAttribute must be 1-10.
     *
     * System attributes have attribute numbers less than 0 that are unique
     * within the class, but not constrained to any particular range.
     *
     * Note that (attnum - 1) is often used as the index to an array.
     */
    int16_t attnum;

    /*
     * attndims is the declared number of dimensions, if an array type,
     * otherwise zero.
     */
    int32_t attndims;

    /*
     * fastgetattr() uses attcacheoff to cache byte offsets of attributes in
     * heap tuples.  The value actually stored in pg_attribute (-1) indicates
     * no cached value.  But when we copy these tuples into a tuple descriptor,
     * we may then update attcacheoff in the copies. This speeds
     * up the attribute walking process.
     *
     * Important: this is only for uncompressed tuples, both cached and updated.
     * And it can't be applied to compressed tuples. Each attribute within
     * compressed tuple should be accessed one by one, step by step.
     */
    int32_t attcacheoff;

    /*
     * atttypmod records type-specific data supplied at table creation time
     * (for example, the max length of a varchar field).  It is passed to
     * type-specific input and output functions as the third argument. The
     * value will generally be -1 for types that do not need typmod.
     */
    int32_t atttypmod;

    /*
     * attbyval is a copy of the typbyval field from pg_type for this
     * attribute.  See atttypid comments above.
     */
    bool attbyval;

    /* ----------
     * attstorage tells for VARLENA attributes, what the heap access
     * methods can do to it if a given tuple doesn't fit into a page.
     * Possible values are:
     *    'p': Value must be stored plain always
     *    'e': Value can be stored in "secondary" relation (if relation
     *        has one, see pg_class.reltoastrelid)
     *    'm': Value can be stored compressed inline
     *    'x': Value can be stored compressed inline or in "secondary"
     * Note that 'm' fields can also be moved to secondary storage,
     * but only as a last resort ('e' and 'x' fields are moved first).
     * ----------
     */
    char attstorage;

    /*
     * attalign is a copy of the typalign field in pg_type for this
     * attribute.  See atttypid comments above.
     */
    char attalign;

    /* This flag represents the "NOT NULL" constraint */
    bool attnotnull;

    /* Has DEFAULT value or not */
    bool atthasdef;

    /* Is dropped (ie. logically invisible) or not */
    bool attisdropped;

    /* Has a local definition (hence, do not drop when attinhcount is 0) */
    bool attislocal;

    /* Compression Mode for this attribute
     * its size is 1Byte, and the 7 fields before are also 1Btye wide, so place it here;
     * its valid value range is:  CMPR_NONE ~ CMPR_NUMSTR. see also pagecompress.h
     */
    int8_t attcmprmode;

    /* Number of times inherited from direct parent relation(s) */
    int32_t attinhcount;

    /* attribute's collation */
    Oid attcollation;

#ifdef CATALOG_VARLEN /* variable-length fields start here */
    /* NOTE: The following fields are not present in tuple descriptor */

    /* column-level access permissions */
    aclitem attacl[1];

    /* column-level options */
    text attoptions[1];

    /* column-level FDW options */
    text attfdwoptions[1];

    /* the value is not null only when ALTER TABLE ... ADD COLUMN call */
    bytea attinitdefval;

    /* the attribute type for kv storage: tag(1), field(2), time(3), hide(4) or default(0) */
    int8_t attkvtype;
    char attidentity;
#endif
    Oid GetAttrType() const
    {
        return atttypid;
    }
};
#pragma pack (pop)

using FormData_pg_attribute = SysAttributeTupDef;
using Form_pg_attribute = SysAttributeTupDef *;
}  // namespace DSTORE
#endif