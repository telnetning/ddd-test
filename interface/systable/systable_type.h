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
 * The systable_type is one of core system tables.
 */

#ifndef DSTORE_SYSTABLE_TYPE_H
#define DSTORE_SYSTABLE_TYPE_H

#include <cstdint>
#include "common/dstore_common_utils.h"
#include "tuple/dstore_tuple_struct.h"
#include "index/dstore_scankey.h"
namespace DSTORE {
constexpr char SYSTABLE_TYPE_NAME[] = "sys_type";
constexpr int SYSTABLE_TYPE_OID = 1247;
constexpr int SYSTABLE_TYPE_ROWTYPE = 71;
constexpr int NATTS_SYS_TYPE = 31;
constexpr int ANUM_SYS_TYPE_TYPNAME = 1;
constexpr int ANUM_SYS_TYPE_TYPNAMESPACE = 2;
constexpr int ANUM_SYS_TYPE_TYPOWNER = 3;
constexpr int ANUM_SYS_TYPE_TYPLEN = 4;
constexpr int ANUM_SYS_TYPE_TYPBYVAL = 5;
constexpr int ANUM_SYS_TYPE_TYPTYPE = 6;
constexpr int ANUM_SYS_TYPE_TYPCATEGORY = 7;
constexpr int ANUM_SYS_TYPE_TYPISPREFERRED = 8;
constexpr int ANUM_SYS_TYPE_TYPISDEFINED = 9;
constexpr int ANUM_SYS_TYPE_TYPDELIM = 10;
constexpr int ANUM_SYS_TYPE_TYPRELID = 11;
constexpr int ANUM_SYS_TYPE_TYPELEM = 12;
constexpr int ANUM_SYS_TYPE_TYPARRAY = 13;
constexpr int ANUM_SYS_TYPE_TYPINPUT = 14;
constexpr int ANUM_SYS_TYPE_TYPOUTPUT = 15;
constexpr int ANUM_SYS_TYPE_TYPRECEIVE = 16;
constexpr int ANUM_SYS_TYPE_TYPSEND = 17;
constexpr int ANUM_SYS_TYPE_TYPMODIN = 18;
constexpr int ANUM_SYS_TYPE_TYPMODOUT = 19;
constexpr int ANUM_SYS_TYPE_TYPANALYZE = 20;
constexpr int ANUM_SYS_TYPE_TYPALIGN = 21;
constexpr int ANUM_SYS_TYPE_TYPSTORAGE = 22;
constexpr int ANUM_SYS_TYPE_TYPNOTNULL = 23;
constexpr int ANUM_SYS_TYPE_TYPBASETYPE = 24;
constexpr int ANUM_SYS_TYPE_TYPTYPMOD = 25;
constexpr int ANUM_SYS_TYPE_TYPNDIMS = 26;
constexpr int ANUM_SYS_TYPE_TYPCOLLATION = 27;
/* variable-length fields start here */
constexpr int ANUM_SYS_TYPE_TYPDEFAULTBIN = 28;
constexpr int ANUM_SYS_TYPE_TYPDEFAULT = 29;
constexpr int ANUM_SYS_TYPE_TYPACL = 30;
constexpr int ANUM_SYS_TYPE_ELEM_TYPTYPMOD = 31;
/* variable-length fields end here */

constexpr char SYS_DEFAULT_TYPDELIM = ',';

constexpr Oid SYS_BOOLOID = 16;
constexpr Oid SYS_BYTEAOID = 17;
constexpr Oid SYS_CHAROID = 18;
constexpr Oid SYS_NAMEOID = 19;
constexpr Oid SYS_INT8OID = 20;
constexpr Oid SYS_INT2OID = 21;
constexpr Oid SYS_INT2VECTOROID = 22;
constexpr Oid SYS_INT4OID = 23;
constexpr Oid SYS_REGPROCOID = 24;
constexpr Oid SYS_TEXTOID = 25;
constexpr Oid SYS_OIDOID = 26;
constexpr Oid SYS_TIDOID = 27;
constexpr Oid SYS_XIDOID = 28;
constexpr Oid SYS_CIDOID = 29;
constexpr Oid SYS_OIDVECTOROID = 30;
constexpr Oid SYS_SHORTXIDOID = 31;
constexpr Oid SYS_OIDVECTOREXTENDOID = 32;
constexpr Oid SYS_INT16OID = 34;

constexpr Oid SYS_PGNODETREEOID = 194;
constexpr Oid SYS_FLOAT4OID = 700;
constexpr Oid SYS_FLOAT8OID = 701;
constexpr Oid SYS_UNKNOWNOID = 705;
constexpr Oid SYS_INETOID = 869;
constexpr Oid SYS_CHARARRAYOID = 1002;
constexpr Oid SYS_NAMEARRAYOID = 1003;
constexpr Oid SYS_INT2ARRAYOID = 1005;
constexpr Oid SYS_TEXTARRAYOID = 1009;
constexpr Oid SYS_INT8ARRAYOID = 1016;
constexpr Oid SYS_FLOAT4ARRAYOID = 1021;
constexpr Oid SYS_OIDARRAYOID = 1028;
constexpr Oid SYS_ACLITEMOID = 1033;
constexpr Oid SYS_ACLITEMARRAYOID = 1034;
constexpr Oid SYS_TIMESTAMPOID = 1114;
constexpr Oid SYS_TIMESTAMPTZOID = 1184;
constexpr Oid SYS_ANYARRAYOID = 2277;
constexpr Oid SYS_RECORDOID = 2249;

constexpr Oid SYS_INT1OID = 5545;
constexpr Oid SYS_HASH16OID = 5801;
constexpr Oid SYS_HASH32OID = 5802;

enum class SysTypeKind : char {
    SYS_TYPTYPE_BASE = 'b',      /* base type (ordinary scalar type) */
    SYS_TYPTYPE_COMPOSITE = 'c', /* composite (e.g., table's rowtype) */
    SYS_TYPTYPE_DOMAIN = 'd',    /* domain over another type */
    SYS_TYPTYPE_ENUM = 'e',      /* enumerated type */
    SYS_TYPTYPE_PSEUDO = 'p',    /* pseudo-type */
    SYS_TYPTYPE_RANGE = 'r',     /* range type */
    SYS_TYPTYPE_TABLEOF = 'o',   /* table of type */
};

enum class SysTypeCategory : char {
    SYS_TYPCATEGORY_INVALID = '\0', /* not an allowed category */
    SYS_TYPCATEGORY_ARRAY = 'A',
    SYS_TYPCATEGORY_BOOLEAN = 'B',
    SYS_TYPCATEGORY_COMPOSITE = 'C',
    SYS_TYPCATEGORY_DATETIME = 'D',
    SYS_TYPCATEGORY_ENUM = 'E',
    SYS_TYPCATEGORY_GEOMETRIC = 'G',
    SYS_TYPCATEGORY_NETWORK = 'I', /* think INET */
    SYS_TYPCATEGORY_NUMERIC = 'N',
    SYS_TYPCATEGORY_PSEUDOTYPE = 'P',
    SYS_TYPCATEGORY_RANGE = 'R',
    SYS_TYPCATEGORY_STRING = 'S',
    SYS_TYPCATEGORY_TIMESPAN = 'T',
    SYS_TYPCATEGORY_USER = 'U',
    SYS_TYPCATEGORY_BITSTRING = 'V', /* er ... "varbit"? */
    SYS_TYPCATEGORY_UNKNOWN = 'X',
    SYS_TYPCATEGORY_TABLEOF = 'O',         /* table of type */
    SYS_TYPCATEGORY_TABLEOF_VARCHAR = 'Q', /* table of type, index by varchar */
    SYS_TYPCATEGORY_TABLEOF_INTEGER = 'F', /* table of type, index by integer */
};

#pragma pack (push, 1)
struct SysTypeTupDef {
    DstoreNameData typname; /* type name */
    Oid typnamespace; /* OID of namespace containing this type */
    Oid typowner;     /* type owner */

    /*
     * For a fixed-size type, typlen is the number of bytes we use to
     * represent a value of this type, e.g. 4 for an int32_t.    But for a
     * variable-length type, typlen is negative.  We use -1 to indicate a
     * "varlena" type (one that has a length word), -2 to indicate a
     * null-terminated C string.
     */
    int16_t typlen;

    /*
     * typbyval determines whether internal openGauss routines pass a value of
     * this type by value or by reference. typbyval is preferably FALSE if
     * the length is not 1, 2, or 4 (or 8 on 8-byte-Datum machines).
     * Variable-length types are always passed by reference. Note that
     * typbyval can be false even if the length would allow pass-by-value;
     * for example, this is currently true for type float4.
     */
    bool typbyval;

    /*
     * typtype is 'b' for a base type, 'c' for a composite type (e.g., a
     * table's rowtype), 'd' for a domain, 'e' for an enum type, 'p' for a
     * pseudo-type, or 'r' for a range type. (Use the TYPTYPE macros below.)
     *
     * If typtype is 'c', typrelid is the OID of the class' entry in pg_class.
     * typtype is 'o' for a table of type.
     */
    char typtype;

    /*
     * typcategory and typispreferred help the parser distinguish between preferred
     * and non-preferred coercions.  The category can be any single ASCII
     * character (but not \0).    The categories used for built-in types are
     * identified by the TYPCATEGORY macros below.
     */
    char typcategory; /* arbitrary type classification */

    bool typispreferred; /* is type "preferred" within its category? */

    /*
     * If typisdefined is false, the entry is simply a placeholder (forward
     * reference).    We know the type name, but not yet anything else about it.
     */
    bool typisdefined;

    char typdelim; /* delimiter for arrays of this type */

    Oid typrelid; /* 0 if not a composite type */

    /*
     * If typelem is not 0, it identifies another row in pg_type. The
     * current type can then be subscripted like an array yielding values of
     * type typelem. A non-zero typelem does not guarantee this type to be a
     * "real" array type; some ordinary fixed-length types can also be
     * subscripted (e.g., point, name). Variable-length types can *not* be
     * turned into pseudo-arrays like that. Hence, the way to determine
     * whether a type is a "true" array type is if:
     *
     * typelem != 0 and typlen == -1.
     *
     * if typtype is 'o' (e.g table of A),  typelem means the Oid of A in pg_type
     */
    Oid typelem;

    /*
     * If there is a "true" array type having this type as element type,
     * typarray links to it.  Zero if no associated "true" array type.
     */
    Oid typarray;

    /*
     * I/O conversion procedures for the datatype.
     */
    Oid typinput; /* text format (required) */
    Oid typoutput;
    Oid typreceive; /* binary format (optional) */
    Oid typsend;

    /*
     * I/O functions for optional type modifiers.
     */
    Oid typmodin;
    Oid typmodout;

    /*
     * Custom ANALYZE procedure for the datatype (0 selects the default).
     */
    Oid typanalyze;

    /* ----------------
     * typalign is the alignment required when storing a value of this
     * type.  It applies to storage on disk as well as most
     * representations of the value inside openGauss.  When multiple values
     * are stored consecutively, such as in the representation of a
     * complete row on disk, padding is inserted before a datum of this
     * type so that it start at the specified boundary.  The alignment
     * reference is the beginning of the first datum in the sequence.
     *
     * 'c' = CHAR alignment, ie no alignment needed.
     * 's' = SHORT alignment (2 bytes on most machines).
     * 'i' = INT alignment (4 bytes on most machines).
     * 'd' = DOUBLE alignment (8 bytes on many machines, but by no means all).
     *
     * For the macros that compute these alignment requirements, see include/access/tupmacs.h.
     * Also Note also that we allow the nominal alignment
     * to be violated when storing "packed" varlenas; the TOAST mechanism
     * takes care of hiding that from most code.
     *
     * NOTE: for types used in system tables, it is critical that the size
     * and alignment defined in pg_type consistent with the way that the
     * compiler will lay out the field in a struct representing a table row.
     * ----------------
     */
    char typalign;

    /* ----------------
     * typstorage tells whether the type is prepared for toasting and what
     * the default strategy for attributes of this type should be.
     *
     * 'p' PLAIN      type not prepared for toasting
     * 'e' EXTERNAL   external storage possible, don't try to compress
     * 'x' EXTENDED   try to compress and store external if required
     * 'm' MAIN          like 'x' but try to keep in main tuple
     * ----------------
     */
    char typstorage;

    /*
     * This flag indicates a "NOT NULL" constraint against this datatype.
     *
     * If true, the attnotnull column for a corresponding table column using
     * this datatype will always enforce the NOT NULL constraint.
     *
     * Used primarily for domain types.
     */
    bool typnotnull;

    /*
     * Domains use typbasetype to show the base (or domain) type on which the
     * domain is based on.    Zero if the type is not a domain.
     */
    Oid typbasetype;

    /*
     * Domains use typtypmod to record the typmod to be applied to their base
     * type (-1 if the base type does not use a typmod).  -1 if this type is not a
     * domain.
     */
    int32_t typtypmod;

    /*
     * typndims is the declared number of dimensions for an array domain type
     * (i.e., typbasetype is an array type).  Otherwise zero.
     */
    int32_t typndims;

    /*
     * Collation: 0 if type cannot use collations, DEFAULT_COLLATION_OID for
     * collatable base types, possibly other OID for domains
     */
    Oid typcollation;
    /* Variable-length fields are not defined here */
};
#pragma pack (pop)

using FormData_pg_type = SysTypeTupDef;
using Form_pg_type = FormData_pg_type *;

struct SysTypeTupDefCache {
    Oid oid;
    SysTypeTupDef tupDef;
};

}  // namespace DSTORE

#endif