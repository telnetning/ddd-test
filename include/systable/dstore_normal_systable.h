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
 * The strorage_normal_systable is the definition of normal system tables.
 */
#ifndef DSTORE_NORMAL_SYSTABLE_H
#define DSTORE_NORMAL_SYSTABLE_H

#include <cstdint>
#include "common/dstore_common_utils.h"
#include "systable/dstore_systable_struct.h"
#include "systable/systable_type.h"

namespace DSTORE {
constexpr char PG_PARTITION_NAME[] = "pg_partition";
constexpr Oid PG_PARTITION_OID =  9016;
constexpr Oid PG_PARTITION_ROWTYPE_OID = 3790;
const TableColumn PG_PARTITION_COLS[] = {
    {"relname", SYS_NAMEOID},
    {"parttype", SYS_CHAROID},
    {"parentid", SYS_OIDOID},
    {"rangenum", SYS_INT4OID},
    {"intervalnum", SYS_INT4OID},
    {"partstrategy", SYS_CHAROID},
    {"relfilenode", SYS_OIDOID},
    {"relfileid", SYS_INT2OID},
    {"relblknum", SYS_OIDOID},
    {"rellobfileid", SYS_INT2OID},
    {"rellobblknum", SYS_OIDOID},
    {"reltablespace", SYS_OIDOID},
    {"relpages", SYS_FLOAT8OID},
    {"reltuples", SYS_FLOAT8OID},
    {"relallvisible", SYS_INT4OID},
    {"reltoastrelid", SYS_OIDOID},
    {"reltoastidxid", SYS_OIDOID},
    {"indextblid", SYS_OIDOID},
    {"indisusable", SYS_BOOLOID},
    {"reldeltarelid", SYS_OIDOID},
    {"reldeltaidx", SYS_OIDOID},
    {"relcudescrelid", SYS_OIDOID},
    {"relcudescidx", SYS_OIDOID},
    {"relfrozenxid", SYS_SHORTXIDOID},
    {"intspnum", SYS_INT4OID},
    {"partkey", SYS_INT2VECTOROID},
    {"intervaltablespace", SYS_OIDVECTOROID},
    {"interval", SYS_TEXTARRAYOID},
    {"boundaries", SYS_TEXTARRAYOID},
    {"transit", SYS_TEXTARRAYOID},
    {"reloptions", SYS_TEXTARRAYOID},
    {"relfrozenxid64", SYS_XIDOID},
    {"relminmxid", SYS_XIDOID},
};
constexpr int PG_PARTITION_COLS_CNT = sizeof(PG_PARTITION_COLS) / sizeof(PG_PARTITION_COLS[0]);
constexpr char PG_ATTRDEF_NAME[] = "pg_attrdef";
constexpr Oid PG_ATTRDEF_OID =  2604;
constexpr Oid PG_ATTRDEF_ROWTYPE_OID = 16384;
const TableColumn PG_ATTRDEF_COLS[] = {
    {"adrelid", SYS_OIDOID},
    {"adnum", SYS_INT2OID},
    {"adbin", SYS_PGNODETREEOID},
    {"adsrc", SYS_TEXTOID},
    {"adgencol", SYS_CHAROID},
};
constexpr int PG_ATTRDEF_COLS_CNT = sizeof(PG_ATTRDEF_COLS) / sizeof(PG_ATTRDEF_COLS[0]);
constexpr char PG_CONSTRAINT_NAME[] = "pg_constraint";
constexpr Oid PG_CONSTRAINT_OID =  2606;
constexpr Oid PG_CONSTRAINT_ROWTYPE_OID = 16385;
const TableColumn PG_CONSTRAINT_COLS[] = {
    {"conname", SYS_NAMEOID},
    {"connamespace", SYS_OIDOID},
    {"contype", SYS_CHAROID},
    {"condeferrable", SYS_BOOLOID},
    {"condeferred", SYS_BOOLOID},
    {"convalidated", SYS_BOOLOID},
    {"conrelid", SYS_OIDOID},
    {"contypid", SYS_OIDOID},
    {"conindid", SYS_OIDOID},
    {"confrelid", SYS_OIDOID},
    {"confupdtype", SYS_CHAROID},
    {"confdeltype", SYS_CHAROID},
    {"confmatchtype", SYS_CHAROID},
    {"conislocal", SYS_BOOLOID},
    {"coninhcount", SYS_INT4OID},
    {"connoinherit", SYS_BOOLOID},
    {"consoft", SYS_BOOLOID},
    {"conopt", SYS_BOOLOID},
    {"conkey", SYS_INT2ARRAYOID},
    {"confkey", SYS_INT2ARRAYOID},
    {"conpfeqop", SYS_OIDARRAYOID},
    {"conppeqop", SYS_OIDARRAYOID},
    {"conffeqop", SYS_OIDARRAYOID},
    {"conexclop", SYS_OIDARRAYOID},
    {"conbin", SYS_PGNODETREEOID},
    {"consrc", SYS_TEXTOID},
    {"conincluding", SYS_INT2ARRAYOID},
};
constexpr int PG_CONSTRAINT_COLS_CNT = sizeof(PG_CONSTRAINT_COLS) / sizeof(PG_CONSTRAINT_COLS[0]);
constexpr char PG_INHERITS_NAME[] = "pg_inherits";
constexpr Oid PG_INHERITS_OID =  2611;
constexpr Oid PG_INHERITS_ROWTYPE_OID = 16386;
const TableColumn PG_INHERITS_COLS[] = {
    {"inhrelid", SYS_OIDOID},
    {"inhparent", SYS_OIDOID},
    {"inhseqno", SYS_INT4OID},
};
constexpr int PG_INHERITS_COLS_CNT = sizeof(PG_INHERITS_COLS) / sizeof(PG_INHERITS_COLS[0]);
constexpr char PG_INDEX_NAME[] = "pg_index";
constexpr Oid PG_INDEX_OID =  2610;
constexpr Oid PG_INDEX_ROWTYPE_OID = 16387;
const TableColumn PG_INDEX_COLS[] = {
    {"indexrelid", SYS_OIDOID},
    {"indrelid", SYS_OIDOID},
    {"indnatts", SYS_INT2OID},
    {"indisunique", SYS_BOOLOID},
    {"indisprimary", SYS_BOOLOID},
    {"indisexclusion", SYS_BOOLOID},
    {"indimmediate", SYS_BOOLOID},
    {"indisclustered", SYS_BOOLOID},
    {"indisusable", SYS_BOOLOID},
    {"indisvalid", SYS_BOOLOID},
    {"indcheckxmin", SYS_BOOLOID},
    {"indisready", SYS_BOOLOID},
    {"indkey", SYS_INT2VECTOROID},
    {"indcollation", SYS_OIDVECTOROID},
    {"indclass", SYS_OIDVECTOROID},
    {"indoption", SYS_INT2VECTOROID},
    {"indexprs", SYS_PGNODETREEOID},
    {"indpred", SYS_PGNODETREEOID},
    {"indisreplident", SYS_BOOLOID},
    {"indnkeyatts", SYS_INT2OID},
    {"indcctmpid", SYS_OIDOID},
    {"indisvisible", SYS_BOOLOID},
};
constexpr int PG_INDEX_COLS_CNT = sizeof(PG_INDEX_COLS) / sizeof(PG_INDEX_COLS[0]);
constexpr char PG_OPERATOR_NAME[] = "pg_operator";
constexpr Oid PG_OPERATOR_OID =  2617;
constexpr Oid PG_OPERATOR_ROWTYPE_OID = 16388;
const TableColumn PG_OPERATOR_COLS[] = {
    {"oprname", SYS_NAMEOID},
    {"oprnamespace", SYS_OIDOID},
    {"oprowner", SYS_OIDOID},
    {"oprkind", SYS_CHAROID},
    {"oprcanmerge", SYS_BOOLOID},
    {"oprcanhash", SYS_BOOLOID},
    {"oprleft", SYS_OIDOID},
    {"oprright", SYS_OIDOID},
    {"oprresult", SYS_OIDOID},
    {"oprcom", SYS_OIDOID},
    {"oprnegate", SYS_OIDOID},
    {"oprcode", SYS_REGPROCOID},
    {"oprrest", SYS_REGPROCOID},
    {"oprjoin", SYS_REGPROCOID},
};
constexpr int PG_OPERATOR_COLS_CNT = sizeof(PG_OPERATOR_COLS) / sizeof(PG_OPERATOR_COLS[0]);
constexpr char PG_OPFAMILY_NAME[] = "pg_opfamily";
constexpr Oid PG_OPFAMILY_OID =  2753;
constexpr Oid PG_OPFAMILY_ROWTYPE_OID = 16389;
const TableColumn PG_OPFAMILY_COLS[] = {
    {"opfmethod", SYS_OIDOID},
    {"opfname", SYS_NAMEOID},
    {"opfnamespace", SYS_OIDOID},
    {"opfowner", SYS_OIDOID},
};
constexpr int PG_OPFAMILY_COLS_CNT = sizeof(PG_OPFAMILY_COLS) / sizeof(PG_OPFAMILY_COLS[0]);
constexpr char PG_OPCLASS_NAME[] = "pg_opclass";
constexpr Oid PG_OPCLASS_OID =  2616;
constexpr Oid PG_OPCLASS_ROWTYPE_OID = 16390;
const TableColumn PG_OPCLASS_COLS[] = {
    {"opcmethod", SYS_OIDOID},
    {"opcname", SYS_NAMEOID},
    {"opcnamespace", SYS_OIDOID},
    {"opcowner", SYS_OIDOID},
    {"opcfamily", SYS_OIDOID},
    {"opcintype", SYS_OIDOID},
    {"opcdefault", SYS_BOOLOID},
    {"opckeytype", SYS_OIDOID},
};
constexpr int PG_OPCLASS_COLS_CNT = sizeof(PG_OPCLASS_COLS) / sizeof(PG_OPCLASS_COLS[0]);
constexpr char PG_AM_NAME[] = "pg_am";
constexpr Oid PG_AM_OID =  2601;
constexpr Oid PG_AM_ROWTYPE_OID = 16599;
const TableColumn PG_AM_COLS[] = {
    {"amname", SYS_NAMEOID},
    {"amstrategies", SYS_INT2OID},
    {"amsupport", SYS_INT2OID},
    {"amcanorder", SYS_BOOLOID},
    {"amcanorderbyop", SYS_BOOLOID},
    {"amcanbackward", SYS_BOOLOID},
    {"amcanunique", SYS_BOOLOID},
    {"amcanmulticol", SYS_BOOLOID},
    {"amoptionalkey", SYS_BOOLOID},
    {"amsearcharray", SYS_BOOLOID},
    {"amsearchnulls", SYS_BOOLOID},
    {"amstorage", SYS_BOOLOID},
    {"amclusterable", SYS_BOOLOID},
    {"ampredlocks", SYS_BOOLOID},
    {"amkeytype", SYS_OIDOID},
    {"aminsert", SYS_REGPROCOID},
    {"ambeginscan", SYS_REGPROCOID},
    {"amgettuple", SYS_REGPROCOID},
    {"amgetbitmap", SYS_REGPROCOID},
    {"amrescan", SYS_REGPROCOID},
    {"amendscan", SYS_REGPROCOID},
    {"ammarkpos", SYS_REGPROCOID},
    {"amrestrpos", SYS_REGPROCOID},
    {"ammerge", SYS_REGPROCOID},
    {"ambuild", SYS_REGPROCOID},
    {"ambuildempty", SYS_REGPROCOID},
    {"ambulkdelete", SYS_REGPROCOID},
    {"amvacuumcleanup", SYS_REGPROCOID},
    {"amcanreturn", SYS_REGPROCOID},
    {"amcostestimate", SYS_REGPROCOID},
    {"amoptions", SYS_REGPROCOID},
};
constexpr int PG_AM_COLS_CNT = sizeof(PG_AM_COLS) / sizeof(PG_AM_COLS[0]);
constexpr char PG_AMOP_NAME[] = "pg_amop";
constexpr Oid PG_AMOP_OID =  2602;
constexpr Oid PG_AMOP_ROWTYPE_OID = 16600;
const TableColumn PG_AMOP_COLS[] = {
    {"amopfamily", SYS_OIDOID},
    {"amoplefttype", SYS_OIDOID},
    {"amoprighttype", SYS_OIDOID},
    {"amopstrategy", SYS_INT2OID},
    {"amoppurpose", SYS_CHAROID},
    {"amopopr", SYS_OIDOID},
    {"amopmethod", SYS_OIDOID},
    {"amopsortfamily", SYS_OIDOID},
};
constexpr int PG_AMOP_COLS_CNT = sizeof(PG_AMOP_COLS) / sizeof(PG_AMOP_COLS[0]);
constexpr char PG_AMPROC_NAME[] = "pg_amproc";
constexpr Oid PG_AMPROC_OID =  2603;
constexpr Oid PG_AMPROC_ROWTYPE_OID = 17368;
const TableColumn PG_AMPROC_COLS[] = {
    {"amprocfamily", SYS_OIDOID},
    {"amproclefttype", SYS_OIDOID},
    {"amprocrighttype", SYS_OIDOID},
    {"amprocnum", SYS_INT2OID},
    {"amproc", SYS_REGPROCOID},
};
constexpr int PG_AMPROC_COLS_CNT = sizeof(PG_AMPROC_COLS) / sizeof(PG_AMPROC_COLS[0]);
constexpr char PG_LANGUAGE_NAME[] = "pg_language";
constexpr Oid PG_LANGUAGE_OID =  2612;
constexpr Oid PG_LANGUAGE_ROWTYPE_OID = 17857;
const TableColumn PG_LANGUAGE_COLS[] = {
    {"lanname", SYS_NAMEOID},
    {"lanowner", SYS_OIDOID},
    {"lanispl", SYS_BOOLOID},
    {"lanpltrusted", SYS_BOOLOID},
    {"lanplcallfoid", SYS_OIDOID},
    {"laninline", SYS_OIDOID},
    {"lanvalidator", SYS_OIDOID},
    {"lanacl", SYS_ACLITEMARRAYOID},
};
constexpr int PG_LANGUAGE_COLS_CNT = sizeof(PG_LANGUAGE_COLS) / sizeof(PG_LANGUAGE_COLS[0]);
constexpr char PG_LARGEOBJECT_METADATA_NAME[] = "pg_largeobject_metadata";
constexpr Oid PG_LARGEOBJECT_METADATA_OID =  2995;
constexpr Oid PG_LARGEOBJECT_METADATA_ROWTYPE_OID = 17858;
const TableColumn PG_LARGEOBJECT_METADATA_COLS[] = {
    {"lomowner", SYS_OIDOID},
    {"lomacl", SYS_ACLITEMARRAYOID},
};
constexpr int PG_LARGEOBJECT_METADATA_COLS_CNT =
    sizeof(PG_LARGEOBJECT_METADATA_COLS) / sizeof(PG_LARGEOBJECT_METADATA_COLS[0]);
constexpr char PG_LARGEOBJECT_NAME[] = "pg_largeobject";
constexpr Oid PG_LARGEOBJECT_OID =  2613;
constexpr Oid PG_LARGEOBJECT_ROWTYPE_OID = 17859;
const TableColumn PG_LARGEOBJECT_COLS[] = {
    {"loid", SYS_OIDOID},
    {"pageno", SYS_INT4OID},
    {"data", SYS_BYTEAOID},
};
constexpr int PG_LARGEOBJECT_COLS_CNT = sizeof(PG_LARGEOBJECT_COLS) / sizeof(PG_LARGEOBJECT_COLS[0]);
constexpr char PG_AGGREGATE_NAME[] = "pg_aggregate";
constexpr Oid PG_AGGREGATE_OID =  2600;
constexpr Oid PG_AGGREGATE_ROWTYPE_OID = 17860;
const TableColumn PG_AGGREGATE_COLS[] = {
    {"aggfnoid", SYS_REGPROCOID},
    {"aggtransfn", SYS_REGPROCOID},
    {"aggcollectfn", SYS_REGPROCOID},
    {"aggfinalfn", SYS_REGPROCOID},
    {"aggsortop", SYS_OIDOID},
    {"aggtranstype", SYS_OIDOID},
    {"agginitval", SYS_TEXTOID},
    {"agginitcollect", SYS_TEXTOID},
    {"aggkind", SYS_CHAROID},
    {"aggnumdirectargs", SYS_INT2OID},
};
constexpr int PG_AGGREGATE_COLS_CNT = sizeof(PG_AGGREGATE_COLS) / sizeof(PG_AGGREGATE_COLS[0]);
constexpr char PG_STATISTIC_NAME[] = "pg_statistic";
constexpr Oid PG_STATISTIC_OID =  2619;
constexpr Oid PG_STATISTIC_ROWTYPE_OID = 17861;
const TableColumn PG_STATISTIC_COLS[] = {
    {"starelid", SYS_OIDOID},
    {"starelkind", SYS_CHAROID},
    {"staattnum", SYS_INT2OID},
    {"stainherit", SYS_BOOLOID},
    {"stanullfrac", SYS_FLOAT4OID},
    {"stawidth", SYS_INT4OID},
    {"stadistinct", SYS_FLOAT4OID},
    {"stakind1", SYS_INT2OID},
    {"stakind2", SYS_INT2OID},
    {"stakind3", SYS_INT2OID},
    {"stakind4", SYS_INT2OID},
    {"stakind5", SYS_INT2OID},
    {"staop1", SYS_OIDOID},
    {"staop2", SYS_OIDOID},
    {"staop3", SYS_OIDOID},
    {"staop4", SYS_OIDOID},
    {"staop5", SYS_OIDOID},
    {"stanumbers1", SYS_FLOAT4ARRAYOID},
    {"stanumbers2", SYS_FLOAT4ARRAYOID},
    {"stanumbers3", SYS_FLOAT4ARRAYOID},
    {"stanumbers4", SYS_FLOAT4ARRAYOID},
    {"stanumbers5", SYS_FLOAT4ARRAYOID},
    {"stavalues1", SYS_ANYARRAYOID},
    {"stavalues2", SYS_ANYARRAYOID},
    {"stavalues3", SYS_ANYARRAYOID},
    {"stavalues4", SYS_ANYARRAYOID},
    {"stavalues5", SYS_ANYARRAYOID},
    {"stadndistinct", SYS_FLOAT4OID},
    {"staextinfo", SYS_TEXTOID},
};
constexpr int PG_STATISTIC_COLS_CNT = sizeof(PG_STATISTIC_COLS) / sizeof(PG_STATISTIC_COLS[0]);
constexpr char PG_REWRITE_NAME[] = "pg_rewrite";
constexpr Oid PG_REWRITE_OID =  2618;
constexpr Oid PG_REWRITE_ROWTYPE_OID = 17862;
const TableColumn PG_REWRITE_COLS[] = {
    {"rulename", SYS_NAMEOID},
    {"ev_class", SYS_OIDOID},
    {"ev_attr", SYS_INT2OID},
    {"ev_type", SYS_CHAROID},
    {"ev_enabled", SYS_CHAROID},
    {"is_instead", SYS_BOOLOID},
    {"ev_qual", SYS_PGNODETREEOID},
    {"ev_action", SYS_PGNODETREEOID},
};
constexpr int PG_REWRITE_COLS_CNT = sizeof(PG_REWRITE_COLS) / sizeof(PG_REWRITE_COLS[0]);
constexpr char PG_TRIGGER_NAME[] = "pg_trigger";
constexpr Oid PG_TRIGGER_OID =  2620;
constexpr Oid PG_TRIGGER_ROWTYPE_OID = 17863;
const TableColumn PG_TRIGGER_COLS[] = {
    {"tgrelid", SYS_OIDOID},
    {"tgname", SYS_NAMEOID},
    {"tgfoid", SYS_OIDOID},
    {"tgtype", SYS_INT2OID},
    {"tgenabled", SYS_CHAROID},
    {"tgisinternal", SYS_BOOLOID},
    {"tgconstrrelid", SYS_OIDOID},
    {"tgconstrindid", SYS_OIDOID},
    {"tgconstraint", SYS_OIDOID},
    {"tgdeferrable", SYS_BOOLOID},
    {"tginitdeferred", SYS_BOOLOID},
    {"tgnargs", SYS_INT2OID},
    {"tgattr", SYS_INT2VECTOROID},
    {"tgargs", SYS_BYTEAOID},
    {"tgqual", SYS_PGNODETREEOID},
    {"tgowner", SYS_OIDOID},
};
constexpr int PG_TRIGGER_COLS_CNT = sizeof(PG_TRIGGER_COLS) / sizeof(PG_TRIGGER_COLS[0]);
constexpr char PG_DESCRIPTION_NAME[] = "pg_description";
constexpr Oid PG_DESCRIPTION_OID =  2609;
constexpr Oid PG_DESCRIPTION_ROWTYPE_OID = 17864;
const TableColumn PG_DESCRIPTION_COLS[] = {
    {"objoid", SYS_OIDOID},
    {"classoid", SYS_OIDOID},
    {"objsubid", SYS_INT4OID},
    {"description", SYS_TEXTOID},
};
constexpr int PG_DESCRIPTION_COLS_CNT = sizeof(PG_DESCRIPTION_COLS) / sizeof(PG_DESCRIPTION_COLS[0]);
constexpr char PG_CAST_NAME[] = "pg_cast";
constexpr Oid PG_CAST_OID =  2605;
constexpr Oid PG_CAST_ROWTYPE_OID = 17865;
const TableColumn PG_CAST_COLS[] = {
    {"castsource", SYS_OIDOID},
    {"casttarget", SYS_OIDOID},
    {"castfunc", SYS_OIDOID},
    {"castcontext", SYS_CHAROID},
    {"castmethod", SYS_CHAROID},
};
constexpr int PG_CAST_COLS_CNT = sizeof(PG_CAST_COLS) / sizeof(PG_CAST_COLS[0]);
constexpr char PG_ENUM_NAME[] = "pg_enum";
constexpr Oid PG_ENUM_OID =  3501;
constexpr Oid PG_ENUM_ROWTYPE_OID = 18219;
const TableColumn PG_ENUM_COLS[] = {
    {"enumtypid", SYS_OIDOID},
    {"enumsortorder", SYS_FLOAT4OID},
    {"enumlabel", SYS_NAMEOID},
};
constexpr int PG_ENUM_COLS_CNT = sizeof(PG_ENUM_COLS) / sizeof(PG_ENUM_COLS[0]);
constexpr char PG_NAMESPACE_NAME[] = "pg_namespace";
constexpr Oid PG_NAMESPACE_OID =  2615;
constexpr Oid PG_NAMESPACE_ROWTYPE_OID = 18220;
const TableColumn PG_NAMESPACE_COLS[] = {
    {"nspname", SYS_NAMEOID},
    {"nspowner", SYS_OIDOID},
    {"nsptimeline", SYS_INT8OID},
    {"nspacl", SYS_ACLITEMARRAYOID},
    {"in_redistribution", SYS_CHAROID},
    {"nspblockchain", SYS_BOOLOID},
};
constexpr int PG_NAMESPACE_COLS_CNT = sizeof(PG_NAMESPACE_COLS) / sizeof(PG_NAMESPACE_COLS[0]);
constexpr char PG_CONVERSION_NAME[] = "pg_conversion";
constexpr Oid PG_CONVERSION_OID =  2607;
constexpr Oid PG_CONVERSION_ROWTYPE_OID = 18221;
const TableColumn PG_CONVERSION_COLS[] = {
    {"conname", SYS_NAMEOID},
    {"connamespace", SYS_OIDOID},
    {"conowner", SYS_OIDOID},
    {"conforencoding", SYS_INT4OID},
    {"contoencoding", SYS_INT4OID},
    {"conproc", SYS_REGPROCOID},
    {"condefault", SYS_BOOLOID},
};
constexpr int PG_CONVERSION_COLS_CNT = sizeof(PG_CONVERSION_COLS) / sizeof(PG_CONVERSION_COLS[0]);
constexpr char PG_DEPEND_NAME[] = "pg_depend";
constexpr Oid PG_DEPEND_OID =  2608;
constexpr Oid PG_DEPEND_ROWTYPE_OID = 18222;
const TableColumn PG_DEPEND_COLS[] = {
    {"classid", SYS_OIDOID},
    {"objid", SYS_OIDOID},
    {"objsubid", SYS_INT4OID},
    {"refclassid", SYS_OIDOID},
    {"refobjid", SYS_OIDOID},
    {"refobjsubid", SYS_INT4OID},
    {"deptype", SYS_CHAROID},
};
constexpr int PG_DEPEND_COLS_CNT = sizeof(PG_DEPEND_COLS) / sizeof(PG_DEPEND_COLS[0]);
constexpr char PG_DATABASE_NAME[] = "pg_database";
constexpr Oid PG_DATABASE_OID =  1262;
constexpr Oid PG_DATABASE_ROWTYPE_OID = 1248;
const TableColumn PG_DATABASE_COLS[] = {
    {"datname", SYS_NAMEOID},
    {"datdba", SYS_OIDOID},
    {"encoding", SYS_INT4OID},
    {"datcollate", SYS_NAMEOID},
    {"datctype", SYS_NAMEOID},
    {"datistemplate", SYS_BOOLOID},
    {"datallowconn", SYS_BOOLOID},
    {"datconnlimit", SYS_INT4OID},
    {"datlastsysoid", SYS_OIDOID},
    {"datfrozenxid", SYS_SHORTXIDOID},
    {"dattablespace", SYS_OIDOID},
    {"vfsname", SYS_NAMEOID},
    {"datcompatibility", SYS_NAMEOID},
    {"datacl", SYS_ACLITEMARRAYOID},
    {"datfrozenxid64", SYS_XIDOID},
    {"datminmxid", SYS_XIDOID},
};
constexpr int PG_DATABASE_COLS_CNT = sizeof(PG_DATABASE_COLS) / sizeof(PG_DATABASE_COLS[0]);
constexpr char PG_DB_ROLE_SETTING_NAME[] = "pg_db_role_setting";
constexpr Oid PG_DB_ROLE_SETTING_OID =  2964;
constexpr Oid PG_DB_ROLE_SETTING_ROWTYPE_OID = 18223;
const TableColumn PG_DB_ROLE_SETTING_COLS[] = {
    {"setdatabase", SYS_OIDOID},
    {"setrole", SYS_OIDOID},
    {"setconfig", SYS_TEXTARRAYOID},
};
constexpr int PG_DB_ROLE_SETTING_COLS_CNT = sizeof(PG_DB_ROLE_SETTING_COLS) / sizeof(PG_DB_ROLE_SETTING_COLS[0]);
constexpr char PG_TABLESPACE_NAME[] = "pg_tablespace";
constexpr Oid PG_TABLESPACE_OID =  1213;
constexpr Oid PG_TABLESPACE_ROWTYPE_OID = 18224;
const TableColumn PG_TABLESPACE_COLS[] = {
    {"spcname", SYS_NAMEOID},
    {"spcowner", SYS_OIDOID},
    {"spcacl", SYS_ACLITEMARRAYOID},
    {"spcoptions", SYS_TEXTARRAYOID},
    {"spcmaxsize", SYS_TEXTOID},
    {"relative", SYS_BOOLOID},
};
constexpr int PG_TABLESPACE_COLS_CNT = sizeof(PG_TABLESPACE_COLS) / sizeof(PG_TABLESPACE_COLS[0]);
constexpr char PG_PLTEMPLATE_NAME[] = "pg_pltemplate";
constexpr Oid PG_PLTEMPLATE_OID =  1136;
constexpr Oid PG_PLTEMPLATE_ROWTYPE_OID = 18225;
const TableColumn PG_PLTEMPLATE_COLS[] = {
    {"tmplname", SYS_NAMEOID},
    {"tmpltrusted", SYS_BOOLOID},
    {"tmpldbacreate", SYS_BOOLOID},
    {"tmplhandler", SYS_TEXTOID},
    {"tmplinline", SYS_TEXTOID},
    {"tmplvalidator", SYS_TEXTOID},
    {"tmpllibrary", SYS_TEXTOID},
    {"tmplacl", SYS_ACLITEMARRAYOID},
};
constexpr int PG_PLTEMPLATE_COLS_CNT = sizeof(PG_PLTEMPLATE_COLS) / sizeof(PG_PLTEMPLATE_COLS[0]);
constexpr char PG_AUTHID_NAME[] = "pg_authid";
constexpr Oid PG_AUTHID_OID =  1260;
constexpr Oid PG_AUTHID_ROWTYPE_OID = 2842;
const TableColumn PG_AUTHID_COLS[] = {
    {"rolname", SYS_NAMEOID},
    {"rolsuper", SYS_BOOLOID},
    {"rolinherit", SYS_BOOLOID},
    {"rolcreaterole", SYS_BOOLOID},
    {"rolcreatedb", SYS_BOOLOID},
    {"rolcatupdate", SYS_BOOLOID},
    {"rolcanlogin", SYS_BOOLOID},
    {"rolreplication", SYS_BOOLOID},
    {"rolauditadmin", SYS_BOOLOID},
    {"rolsystemadmin", SYS_BOOLOID},
    {"rolconnlimit", SYS_INT4OID},
    {"rolpassword", SYS_TEXTOID},
    {"rolvalidbegin", SYS_TIMESTAMPTZOID},
    {"rolvaliduntil", SYS_TIMESTAMPTZOID},
    {"rolrespool", SYS_NAMEOID},
    {"roluseft", SYS_BOOLOID},
    {"rolparentid", SYS_OIDOID},
    {"roltabspace", SYS_TEXTOID},
    {"rolkind", SYS_CHAROID},
    {"rolnodegroup", SYS_OIDOID},
    {"roltempspace", SYS_TEXTOID},
    {"rolspillspace", SYS_TEXTOID},
    {"rolexcpdata", SYS_TEXTOID},
    {"rolmonitoradmin", SYS_BOOLOID},
    {"roloperatoradmin", SYS_BOOLOID},
    {"rolpolicyadmin", SYS_BOOLOID},
};
constexpr int PG_AUTHID_COLS_CNT = sizeof(PG_AUTHID_COLS) / sizeof(PG_AUTHID_COLS[0]);
constexpr char PG_AUTH_MEMBERS_NAME[] = "pg_auth_members";
constexpr Oid PG_AUTH_MEMBERS_OID =  1261;
constexpr Oid PG_AUTH_MEMBERS_ROWTYPE_OID = 2843;
const TableColumn PG_AUTH_MEMBERS_COLS[] = {
    {"roleid", SYS_OIDOID},
    {"member", SYS_OIDOID},
    {"grantor", SYS_OIDOID},
    {"admin_option", SYS_BOOLOID},
};
constexpr int PG_AUTH_MEMBERS_COLS_CNT = sizeof(PG_AUTH_MEMBERS_COLS) / sizeof(PG_AUTH_MEMBERS_COLS[0]);
constexpr char PG_SHDEPEND_NAME[] = "pg_shdepend";
constexpr Oid PG_SHDEPEND_OID =  1214;
constexpr Oid PG_SHDEPEND_ROWTYPE_OID = 18226;
const TableColumn PG_SHDEPEND_COLS[] = {
    {"dbid", SYS_OIDOID},
    {"classid", SYS_OIDOID},
    {"objid", SYS_OIDOID},
    {"objsubid", SYS_INT4OID},
    {"refclassid", SYS_OIDOID},
    {"refobjid", SYS_OIDOID},
    {"deptype", SYS_CHAROID},
    {"objfile", SYS_TEXTOID},
};
constexpr int PG_SHDEPEND_COLS_CNT = sizeof(PG_SHDEPEND_COLS) / sizeof(PG_SHDEPEND_COLS[0]);
constexpr char PG_SHDESCRIPTION_NAME[] = "pg_shdescription";
constexpr Oid PG_SHDESCRIPTION_OID =  2396;
constexpr Oid PG_SHDESCRIPTION_ROWTYPE_OID = 18227;
const TableColumn PG_SHDESCRIPTION_COLS[] = {
    {"objoid", SYS_OIDOID},
    {"classoid", SYS_OIDOID},
    {"description", SYS_TEXTOID},
};
constexpr int PG_SHDESCRIPTION_COLS_CNT = sizeof(PG_SHDESCRIPTION_COLS) / sizeof(PG_SHDESCRIPTION_COLS[0]);
constexpr char PG_TS_CONFIG_NAME[] = "pg_ts_config";
constexpr Oid PG_TS_CONFIG_OID =  3602;
constexpr Oid PG_TS_CONFIG_ROWTYPE_OID = 18228;
const TableColumn PG_TS_CONFIG_COLS[] = {
    {"cfgname", SYS_NAMEOID},
    {"cfgnamespace", SYS_OIDOID},
    {"cfgowner", SYS_OIDOID},
    {"cfgparser", SYS_OIDOID},
    {"cfoptions", SYS_TEXTARRAYOID},
};
constexpr int PG_TS_CONFIG_COLS_CNT = sizeof(PG_TS_CONFIG_COLS) / sizeof(PG_TS_CONFIG_COLS[0]);
constexpr char PG_TS_CONFIG_MAP_NAME[] = "pg_ts_config_map";
constexpr Oid PG_TS_CONFIG_MAP_OID =  3603;
constexpr Oid PG_TS_CONFIG_MAP_ROWTYPE_OID = 18229;
const TableColumn PG_TS_CONFIG_MAP_COLS[] = {
    {"mapcfg", SYS_OIDOID},
    {"maptokentype", SYS_INT4OID},
    {"mapseqno", SYS_INT4OID},
    {"mapdict", SYS_OIDOID},
};
constexpr int PG_TS_CONFIG_MAP_COLS_CNT = sizeof(PG_TS_CONFIG_MAP_COLS) / sizeof(PG_TS_CONFIG_MAP_COLS[0]);
constexpr char PG_TS_DICT_NAME[] = "pg_ts_dict";
constexpr Oid PG_TS_DICT_OID =  3600;
constexpr Oid PG_TS_DICT_ROWTYPE_OID = 18230;
const TableColumn PG_TS_DICT_COLS[] = {
    {"dictname", SYS_NAMEOID},
    {"dictnamespace", SYS_OIDOID},
    {"dictowner", SYS_OIDOID},
    {"dicttemplate", SYS_OIDOID},
    {"dictinitoption", SYS_TEXTOID},
};
constexpr int PG_TS_DICT_COLS_CNT = sizeof(PG_TS_DICT_COLS) / sizeof(PG_TS_DICT_COLS[0]);
constexpr char PG_TS_PARSER_NAME[] = "pg_ts_parser";
constexpr Oid PG_TS_PARSER_OID =  3601;
constexpr Oid PG_TS_PARSER_ROWTYPE_OID = 18231;
const TableColumn PG_TS_PARSER_COLS[] = {
    {"prsname", SYS_NAMEOID},
    {"prsnamespace", SYS_OIDOID},
    {"prsstart", SYS_REGPROCOID},
    {"prstoken", SYS_REGPROCOID},
    {"prsend", SYS_REGPROCOID},
    {"prsheadline", SYS_REGPROCOID},
    {"prslextype", SYS_REGPROCOID},
};
constexpr int PG_TS_PARSER_COLS_CNT = sizeof(PG_TS_PARSER_COLS) / sizeof(PG_TS_PARSER_COLS[0]);
constexpr char PG_TS_TEMPLATE_NAME[] = "pg_ts_template";
constexpr Oid PG_TS_TEMPLATE_OID =  3764;
constexpr Oid PG_TS_TEMPLATE_ROWTYPE_OID = 18232;
const TableColumn PG_TS_TEMPLATE_COLS[] = {
    {"tmplname", SYS_NAMEOID},
    {"tmplnamespace", SYS_OIDOID},
    {"tmplinit", SYS_REGPROCOID},
    {"tmpllexize", SYS_REGPROCOID},
};
constexpr int PG_TS_TEMPLATE_COLS_CNT = sizeof(PG_TS_TEMPLATE_COLS) / sizeof(PG_TS_TEMPLATE_COLS[0]);
constexpr char PG_AUTH_HISTORY_NAME[] = "pg_auth_history";
constexpr Oid PG_AUTH_HISTORY_OID =  3457;
constexpr Oid PG_AUTH_HISTORY_ROWTYPE_OID = 18233;
const TableColumn PG_AUTH_HISTORY_COLS[] = {
    {"roloid", SYS_OIDOID},
    {"passwordtime", SYS_TIMESTAMPTZOID},
    {"rolpassword", SYS_TEXTOID},
};
constexpr int PG_AUTH_HISTORY_COLS_CNT = sizeof(PG_AUTH_HISTORY_COLS) / sizeof(PG_AUTH_HISTORY_COLS[0]);
constexpr char PG_USER_STATUS_NAME[] = "pg_user_status";
constexpr Oid PG_USER_STATUS_OID =  3460;
constexpr Oid PG_USER_STATUS_ROWTYPE_OID = 3463;
const TableColumn PG_USER_STATUS_COLS[] = {
    {"roloid", SYS_OIDOID},
    {"failcount", SYS_INT4OID},
    {"locktime", SYS_TIMESTAMPTZOID},
    {"rolstatus", SYS_INT2OID},
    {"permspace", SYS_INT8OID},
    {"tempspace", SYS_INT8OID},
    {"passwordexpired", SYS_INT2OID},
};
constexpr int PG_USER_STATUS_COLS_CNT = sizeof(PG_USER_STATUS_COLS) / sizeof(PG_USER_STATUS_COLS[0]);
constexpr char PG_EXTENSION_NAME[] = "pg_extension";
constexpr Oid PG_EXTENSION_OID =  3079;
constexpr Oid PG_EXTENSION_ROWTYPE_OID = 18234;
const TableColumn PG_EXTENSION_COLS[] = {
    {"extname", SYS_NAMEOID},
    {"extowner", SYS_OIDOID},
    {"extnamespace", SYS_OIDOID},
    {"extrelocatable", SYS_BOOLOID},
    {"extversion", SYS_TEXTOID},
    {"extconfig", SYS_OIDARRAYOID},
    {"extcondition", SYS_TEXTARRAYOID},
};
constexpr int PG_EXTENSION_COLS_CNT = sizeof(PG_EXTENSION_COLS) / sizeof(PG_EXTENSION_COLS[0]);
constexpr char PG_OBSSCANINFO_NAME[] = "pg_obsscaninfo";
constexpr Oid PG_OBSSCANINFO_OID =  5679;
constexpr Oid PG_OBSSCANINFO_ROWTYPE_OID = 18235;
const TableColumn PG_OBSSCANINFO_COLS[] = {
    {"query_id", SYS_INT8OID},
    {"user_id", SYS_TEXTOID},
    {"table_name", SYS_TEXTOID},
    {"file_type", SYS_TEXTOID},
    {"time_stamp", SYS_TIMESTAMPTZOID},
    {"actual_time", SYS_FLOAT8OID},
    {"file_scanned", SYS_INT8OID},
    {"data_size", SYS_FLOAT8OID},
    {"billing_info", SYS_TEXTOID},
};
constexpr int PG_OBSSCANINFO_COLS_CNT = sizeof(PG_OBSSCANINFO_COLS) / sizeof(PG_OBSSCANINFO_COLS[0]);
constexpr char PG_FOREIGN_DATA_WRAPPER_NAME[] = "pg_foreign_data_wrapper";
constexpr Oid PG_FOREIGN_DATA_WRAPPER_OID =  2328;
constexpr Oid PG_FOREIGN_DATA_WRAPPER_ROWTYPE_OID = 18236;
const TableColumn PG_FOREIGN_DATA_WRAPPER_COLS[] = {
    {"fdwname", SYS_NAMEOID},
    {"fdwowner", SYS_OIDOID},
    {"fdwhandler", SYS_OIDOID},
    {"fdwvalidator", SYS_OIDOID},
    {"fdwacl", SYS_ACLITEMARRAYOID},
    {"fdwoptions", SYS_TEXTARRAYOID},
};
constexpr int PG_FOREIGN_DATA_WRAPPER_COLS_CNT =
    sizeof(PG_FOREIGN_DATA_WRAPPER_COLS) / sizeof(PG_FOREIGN_DATA_WRAPPER_COLS[0]);
constexpr char PG_FOREIGN_SERVER_NAME[] = "pg_foreign_server";
constexpr Oid PG_FOREIGN_SERVER_OID =  1417;
constexpr Oid PG_FOREIGN_SERVER_ROWTYPE_OID = 18237;
const TableColumn PG_FOREIGN_SERVER_COLS[] = {
    {"srvname", SYS_NAMEOID},
    {"srvowner", SYS_OIDOID},
    {"srvfdw", SYS_OIDOID},
    {"srvtype", SYS_TEXTOID},
    {"srvversion", SYS_TEXTOID},
    {"srvacl", SYS_ACLITEMARRAYOID},
    {"srvoptions", SYS_TEXTARRAYOID},
};
constexpr int PG_FOREIGN_SERVER_COLS_CNT = sizeof(PG_FOREIGN_SERVER_COLS) / sizeof(PG_FOREIGN_SERVER_COLS[0]);
constexpr char PG_USER_MAPPING_NAME[] = "pg_user_mapping";
constexpr Oid PG_USER_MAPPING_OID =  1418;
constexpr Oid PG_USER_MAPPING_ROWTYPE_OID = 18238;
const TableColumn PG_USER_MAPPING_COLS[] = {
    {"umuser", SYS_OIDOID},
    {"umserver", SYS_OIDOID},
    {"umoptions", SYS_TEXTARRAYOID},
};
constexpr int PG_USER_MAPPING_COLS_CNT = sizeof(PG_USER_MAPPING_COLS) / sizeof(PG_USER_MAPPING_COLS[0]);
constexpr char PGXC_CLASS_NAME[] = "pgxc_class";
constexpr Oid PGXC_CLASS_OID =  9001;
constexpr Oid PGXC_CLASS_ROWTYPE_OID = 18239;
const TableColumn PGXC_CLASS_COLS[] = {
    {"pcrelid", SYS_OIDOID},
    {"pclocatortype", SYS_CHAROID},
    {"pchashalgorithm", SYS_INT2OID},
    {"pchashbuckets", SYS_INT2OID},
    {"pgroup", SYS_NAMEOID},
    {"redistributed", SYS_CHAROID},
    {"redis_order", SYS_INT4OID},
    {"pcattnum", SYS_INT2VECTOROID},
    {"nodeoids", SYS_OIDVECTOREXTENDOID},
    {"options", SYS_TEXTOID},
};
constexpr int PGXC_CLASS_COLS_CNT = sizeof(PGXC_CLASS_COLS) / sizeof(PGXC_CLASS_COLS[0]);
constexpr char PGXC_NODE_NAME[] = "pgxc_node";
constexpr Oid PGXC_NODE_OID =  9015;
constexpr Oid PGXC_NODE_ROWTYPE_OID = 11649;
const TableColumn PGXC_NODE_COLS[] = {
    {"node_name", SYS_NAMEOID},
    {"node_type", SYS_CHAROID},
    {"node_port", SYS_INT4OID},
    {"node_host", SYS_NAMEOID},
    {"node_port1", SYS_INT4OID},
    {"node_host1", SYS_NAMEOID},
    {"hostis_primary", SYS_BOOLOID},
    {"nodeis_primary", SYS_BOOLOID},
    {"nodeis_preferred", SYS_BOOLOID},
    {"node_id", SYS_INT4OID},
    {"sctp_port", SYS_INT4OID},
    {"control_port", SYS_INT4OID},
    {"sctp_port1", SYS_INT4OID},
    {"control_port1", SYS_INT4OID},
    {"nodeis_central", SYS_BOOLOID},
    {"nodeis_active", SYS_BOOLOID},
};
constexpr int PGXC_NODE_COLS_CNT = sizeof(PGXC_NODE_COLS) / sizeof(PGXC_NODE_COLS[0]);
constexpr char PGXC_GROUP_NAME[] = "pgxc_group";
constexpr Oid PGXC_GROUP_OID =  9014;
constexpr Oid PGXC_GROUP_ROWTYPE_OID = 18240;
const TableColumn PGXC_GROUP_COLS[] = {
    {"group_name", SYS_NAMEOID},
    {"in_redistribution", SYS_CHAROID},
    {"group_members", SYS_OIDVECTOREXTENDOID},
    {"group_buckets", SYS_TEXTOID},
    {"is_installation", SYS_BOOLOID},
    {"group_acl", SYS_ACLITEMARRAYOID},
    {"group_kind", SYS_CHAROID},
    {"group_parent", SYS_OIDOID},
};
constexpr int PGXC_GROUP_COLS_CNT = sizeof(PGXC_GROUP_COLS) / sizeof(PGXC_GROUP_COLS[0]);
constexpr char PG_RESOURCE_POOL_NAME[] = "pg_resource_pool";
constexpr Oid PG_RESOURCE_POOL_OID =  3450;
constexpr Oid PG_RESOURCE_POOL_ROWTYPE_OID = 3466;
const TableColumn PG_RESOURCE_POOL_COLS[] = {
    {"respool_name", SYS_NAMEOID},
    {"mem_percent", SYS_INT4OID},
    {"cpu_affinity", SYS_INT8OID},
    {"control_group", SYS_NAMEOID},
    {"active_statements", SYS_INT4OID},
    {"max_dop", SYS_INT4OID},
    {"memory_limit", SYS_NAMEOID},
    {"parentid", SYS_OIDOID},
    {"io_limits", SYS_INT4OID},
    {"io_priority", SYS_NAMEOID},
    {"nodegroup", SYS_NAMEOID},
    {"is_foreign", SYS_BOOLOID},
    {"max_worker", SYS_INT4OID},
};
constexpr int PG_RESOURCE_POOL_COLS_CNT = sizeof(PG_RESOURCE_POOL_COLS) / sizeof(PG_RESOURCE_POOL_COLS[0]);
constexpr char PG_WORKLOAD_GROUP_NAME[] = "pg_workload_group";
constexpr Oid PG_WORKLOAD_GROUP_OID =  3451;
constexpr Oid PG_WORKLOAD_GROUP_ROWTYPE_OID = 3467;
const TableColumn PG_WORKLOAD_GROUP_COLS[] = {
    {"workload_gpname", SYS_NAMEOID},
    {"respool_oid", SYS_OIDOID},
    {"act_statements", SYS_INT4OID},
};
constexpr int PG_WORKLOAD_GROUP_COLS_CNT = sizeof(PG_WORKLOAD_GROUP_COLS) / sizeof(PG_WORKLOAD_GROUP_COLS[0]);
constexpr char PG_APP_WORKLOADGROUP_MAPPING_NAME[] = "pg_app_workloadgroup_mapping";
constexpr Oid PG_APP_WORKLOADGROUP_MAPPING_OID =  3464;
constexpr Oid PG_APP_WORKLOADGROUP_MAPPING_ROWTYPE_OID = 3468;
const TableColumn PG_APP_WORKLOADGROUP_MAPPING_COLS[] = {
    {"appname", SYS_NAMEOID},
    {"workload_gpname", SYS_NAMEOID},
};
constexpr int PG_APP_WORKLOADGROUP_MAPPING_COLS_CNT =
    sizeof(PG_APP_WORKLOADGROUP_MAPPING_COLS) / sizeof(PG_APP_WORKLOADGROUP_MAPPING_COLS[0]);
constexpr char PG_FOREIGN_TABLE_NAME[] = "pg_foreign_table";
constexpr Oid PG_FOREIGN_TABLE_OID =  3118;
constexpr Oid PG_FOREIGN_TABLE_ROWTYPE_OID = 18241;
const TableColumn PG_FOREIGN_TABLE_COLS[] = {
    {"ftrelid", SYS_OIDOID},
    {"ftserver", SYS_OIDOID},
    {"ftwriteonly", SYS_BOOLOID},
    {"ftoptions", SYS_TEXTARRAYOID},
};
constexpr int PG_FOREIGN_TABLE_COLS_CNT = sizeof(PG_FOREIGN_TABLE_COLS) / sizeof(PG_FOREIGN_TABLE_COLS[0]);
constexpr char PG_RLSPOLICY_NAME[] = "pg_rlspolicy";
constexpr Oid PG_RLSPOLICY_OID =  3254;
constexpr Oid PG_RLSPOLICY_ROWTYPE_OID = 18242;
const TableColumn PG_RLSPOLICY_COLS[] = {
    {"polname", SYS_NAMEOID},
    {"polrelid", SYS_OIDOID},
    {"polcmd", SYS_CHAROID},
    {"polpermissive", SYS_BOOLOID},
    {"polroles", SYS_OIDARRAYOID},
    {"polqual", SYS_PGNODETREEOID},
};
constexpr int PG_RLSPOLICY_COLS_CNT = sizeof(PG_RLSPOLICY_COLS) / sizeof(PG_RLSPOLICY_COLS[0]);
constexpr char PG_DEFAULT_ACL_NAME[] = "pg_default_acl";
constexpr Oid PG_DEFAULT_ACL_OID =  826;
constexpr Oid PG_DEFAULT_ACL_ROWTYPE_OID = 18243;
const TableColumn PG_DEFAULT_ACL_COLS[] = {
    {"defaclrole", SYS_OIDOID},
    {"defaclnamespace", SYS_OIDOID},
    {"defaclobjtype", SYS_CHAROID},
    {"defaclacl", SYS_ACLITEMARRAYOID},
};
constexpr int PG_DEFAULT_ACL_COLS_CNT = sizeof(PG_DEFAULT_ACL_COLS) / sizeof(PG_DEFAULT_ACL_COLS[0]);
constexpr char PG_SECLABEL_NAME[] = "pg_seclabel";
constexpr Oid PG_SECLABEL_OID =  3596;
constexpr Oid PG_SECLABEL_ROWTYPE_OID = 18244;
const TableColumn PG_SECLABEL_COLS[] = {
    {"objoid", SYS_OIDOID},
    {"classoid", SYS_OIDOID},
    {"objsubid", SYS_INT4OID},
    {"provider", SYS_TEXTOID},
    {"label", SYS_TEXTOID},
};
constexpr int PG_SECLABEL_COLS_CNT = sizeof(PG_SECLABEL_COLS) / sizeof(PG_SECLABEL_COLS[0]);
constexpr char PG_SHSECLABEL_NAME[] = "pg_shseclabel";
constexpr Oid PG_SHSECLABEL_OID =  3592;
constexpr Oid PG_SHSECLABEL_ROWTYPE_OID = 18245;
const TableColumn PG_SHSECLABEL_COLS[] = {
    {"objoid", SYS_OIDOID},
    {"classoid", SYS_OIDOID},
    {"provider", SYS_TEXTOID},
    {"label", SYS_TEXTOID},
};
constexpr int PG_SHSECLABEL_COLS_CNT = sizeof(PG_SHSECLABEL_COLS) / sizeof(PG_SHSECLABEL_COLS[0]);
constexpr char PG_COLLATION_NAME[] = "pg_collation";
constexpr Oid PG_COLLATION_OID =  3456;
constexpr Oid PG_COLLATION_ROWTYPE_OID = 18246;
const TableColumn PG_COLLATION_COLS[] = {
    {"collname", SYS_NAMEOID},
    {"collnamespace", SYS_OIDOID},
    {"collowner", SYS_OIDOID},
    {"collencoding", SYS_INT4OID},
    {"collcollate", SYS_NAMEOID},
    {"collctype", SYS_NAMEOID},
};
constexpr int PG_COLLATION_COLS_CNT = sizeof(PG_COLLATION_COLS) / sizeof(PG_COLLATION_COLS[0]);
constexpr char PG_RANGE_NAME[] = "pg_range";
constexpr Oid PG_RANGE_OID =  3541;
constexpr Oid PG_RANGE_ROWTYPE_OID = 18247;
const TableColumn PG_RANGE_COLS[] = {
    {"rngtypid", SYS_OIDOID},
    {"rngsubtype", SYS_OIDOID},
    {"rngcollation", SYS_OIDOID},
    {"rngsubopc", SYS_OIDOID},
    {"rngcanonical", SYS_REGPROCOID},
    {"rngsubdiff", SYS_REGPROCOID},
};
constexpr int PG_RANGE_COLS_CNT = sizeof(PG_RANGE_COLS) / sizeof(PG_RANGE_COLS[0]);
constexpr char GS_POLICY_LABEL_NAME[] = "gs_policy_label";
constexpr Oid GS_POLICY_LABEL_OID =  9500;
constexpr Oid GS_POLICY_LABEL_ROWTYPE_OID = 18248;
const TableColumn GS_POLICY_LABEL_COLS[] = {
    {"labelname", SYS_NAMEOID},
    {"labeltype", SYS_NAMEOID},
    {"fqdnnamespace", SYS_OIDOID},
    {"fqdnid", SYS_OIDOID},
    {"relcolumn", SYS_NAMEOID},
    {"fqdntype", SYS_NAMEOID},
};
constexpr int GS_POLICY_LABEL_COLS_CNT = sizeof(GS_POLICY_LABEL_COLS) / sizeof(GS_POLICY_LABEL_COLS[0]);
constexpr char GS_AUDITING_POLICY_NAME[] = "gs_auditing_policy";
constexpr Oid GS_AUDITING_POLICY_OID =  9510;
constexpr Oid GS_AUDITING_POLICY_ROWTYPE_OID = 18249;
const TableColumn GS_AUDITING_POLICY_COLS[] = {
    {"polname", SYS_NAMEOID},
    {"polcomments", SYS_NAMEOID},
    {"modifydate", SYS_TIMESTAMPOID},
    {"polenabled", SYS_BOOLOID},
};
constexpr int GS_AUDITING_POLICY_COLS_CNT = sizeof(GS_AUDITING_POLICY_COLS) / sizeof(GS_AUDITING_POLICY_COLS[0]);
constexpr char GS_AUDITING_POLICY_ACCESS_NAME[] = "gs_auditing_policy_access";
constexpr Oid GS_AUDITING_POLICY_ACCESS_OID =  9520;
constexpr Oid GS_AUDITING_POLICY_ACCESS_ROWTYPE_OID = 18250;
const TableColumn GS_AUDITING_POLICY_ACCESS_COLS[] = {
    {"accesstype", SYS_NAMEOID},
    {"labelname", SYS_NAMEOID},
    {"policyoid", SYS_OIDOID},
    {"modifydate", SYS_TIMESTAMPOID},
};
constexpr int GS_AUDITING_POLICY_ACCESS_COLS_CNT =
    sizeof(GS_AUDITING_POLICY_ACCESS_COLS) / sizeof(GS_AUDITING_POLICY_ACCESS_COLS[0]);
constexpr char GS_AUDITING_POLICY_PRIVILEGES_NAME[] = "gs_auditing_policy_privileges";
constexpr Oid GS_AUDITING_POLICY_PRIVILEGES_OID =  9530;
constexpr Oid GS_AUDITING_POLICY_PRIVILEGES_ROWTYPE_OID = 18251;
const TableColumn GS_AUDITING_POLICY_PRIVILEGES_COLS[] = {
    {"privilegetype", SYS_NAMEOID},
    {"labelname", SYS_NAMEOID},
    {"policyoid", SYS_OIDOID},
    {"modifydate", SYS_TIMESTAMPOID},
};
constexpr int GS_AUDITING_POLICY_PRIVILEGES_COLS_CNT =
    sizeof(GS_AUDITING_POLICY_PRIVILEGES_COLS) / sizeof(GS_AUDITING_POLICY_PRIVILEGES_COLS[0]);
constexpr char GS_AUDITING_POLICY_FILTERS_NAME[] = "gs_auditing_policy_filters";
constexpr Oid GS_AUDITING_POLICY_FILTERS_OID =  9540;
constexpr Oid GS_AUDITING_POLICY_FILTERS_ROWTYPE_OID = 18252;
const TableColumn GS_AUDITING_POLICY_FILTERS_COLS[] = {
    {"filtertype", SYS_NAMEOID},
    {"labelname", SYS_NAMEOID},
    {"policyoid", SYS_OIDOID},
    {"modifydate", SYS_TIMESTAMPOID},
    {"logicaloperator", SYS_TEXTOID},
};
constexpr int GS_AUDITING_POLICY_FILTERS_COLS_CNT =
    sizeof(GS_AUDITING_POLICY_FILTERS_COLS) / sizeof(GS_AUDITING_POLICY_FILTERS_COLS[0]);
constexpr char GS_MASKING_POLICY_NAME[] = "gs_masking_policy";
constexpr Oid GS_MASKING_POLICY_OID =  9610;
constexpr Oid GS_MASKING_POLICY_ROWTYPE_OID = 18253;
const TableColumn GS_MASKING_POLICY_COLS[] = {
    {"polname", SYS_NAMEOID},
    {"polcomments", SYS_NAMEOID},
    {"modifydate", SYS_TIMESTAMPOID},
    {"polenabled", SYS_BOOLOID},
};
constexpr int GS_MASKING_POLICY_COLS_CNT = sizeof(GS_MASKING_POLICY_COLS) / sizeof(GS_MASKING_POLICY_COLS[0]);
constexpr char GS_MASKING_POLICY_ACTIONS_NAME[] = "gs_masking_policy_actions";
constexpr Oid GS_MASKING_POLICY_ACTIONS_OID =  9650;
constexpr Oid GS_MASKING_POLICY_ACTIONS_ROWTYPE_OID = 18254;
const TableColumn GS_MASKING_POLICY_ACTIONS_COLS[] = {
    {"actiontype", SYS_NAMEOID},
    {"actparams", SYS_NAMEOID},
    {"actlabelname", SYS_NAMEOID},
    {"policyoid", SYS_OIDOID},
    {"actmodifydate", SYS_TIMESTAMPOID},
};
constexpr int GS_MASKING_POLICY_ACTIONS_COLS_CNT =
    sizeof(GS_MASKING_POLICY_ACTIONS_COLS) / sizeof(GS_MASKING_POLICY_ACTIONS_COLS[0]);
constexpr char GS_MASKING_POLICY_FILTERS_NAME[] = "gs_masking_policy_filters";
constexpr Oid GS_MASKING_POLICY_FILTERS_OID =  9640;
constexpr Oid GS_MASKING_POLICY_FILTERS_ROWTYPE_OID = 18255;
const TableColumn GS_MASKING_POLICY_FILTERS_COLS[] = {
    {"filtertype", SYS_NAMEOID},
    {"filterlabelname", SYS_NAMEOID},
    {"policyoid", SYS_OIDOID},
    {"modifydate", SYS_TIMESTAMPOID},
    {"logicaloperator", SYS_TEXTOID},
};
constexpr int GS_MASKING_POLICY_FILTERS_COLS_CNT =
    sizeof(GS_MASKING_POLICY_FILTERS_COLS) / sizeof(GS_MASKING_POLICY_FILTERS_COLS[0]);
constexpr char GS_ENCRYPTED_COLUMNS_NAME[] = "gs_encrypted_columns";
constexpr Oid GS_ENCRYPTED_COLUMNS_OID =  9700;
constexpr Oid GS_ENCRYPTED_COLUMNS_ROWTYPE_OID = 18256;
const TableColumn GS_ENCRYPTED_COLUMNS_COLS[] = {
    {"rel_id", SYS_OIDOID},
    {"column_name", SYS_NAMEOID},
    {"column_key_id", SYS_OIDOID},
    {"encryption_type", SYS_INT1OID},
    {"data_type_original_oid", SYS_OIDOID},
    {"data_type_original_mod", SYS_INT4OID},
    {"create_date", SYS_TIMESTAMPOID},
};
constexpr int GS_ENCRYPTED_COLUMNS_COLS_CNT = sizeof(GS_ENCRYPTED_COLUMNS_COLS) / sizeof(GS_ENCRYPTED_COLUMNS_COLS[0]);
constexpr char GS_COLUMN_KEYS_NAME[] = "gs_column_keys";
constexpr Oid GS_COLUMN_KEYS_OID =  9720;
constexpr Oid GS_COLUMN_KEYS_ROWTYPE_OID = 18257;
const TableColumn GS_COLUMN_KEYS_COLS[] = {
    {"column_key_name", SYS_NAMEOID},
    {"column_key_distributed_id", SYS_OIDOID},
    {"global_key_id", SYS_OIDOID},
    {"key_namespace", SYS_OIDOID},
    {"key_owner", SYS_OIDOID},
    {"create_date", SYS_TIMESTAMPOID},
    {"key_acl", SYS_ACLITEMARRAYOID},
};
constexpr int GS_COLUMN_KEYS_COLS_CNT = sizeof(GS_COLUMN_KEYS_COLS) / sizeof(GS_COLUMN_KEYS_COLS[0]);
constexpr char GS_COLUMN_KEYS_ARGS_NAME[] = "gs_column_keys_args";
constexpr Oid GS_COLUMN_KEYS_ARGS_OID =  9740;
constexpr Oid GS_COLUMN_KEYS_ARGS_ROWTYPE_OID = 18258;
const TableColumn GS_COLUMN_KEYS_ARGS_COLS[] = {
    {"column_key_id", SYS_OIDOID},
    {"function_name", SYS_NAMEOID},
    {"key", SYS_NAMEOID},
    {"value", SYS_BYTEAOID},
};
constexpr int GS_COLUMN_KEYS_ARGS_COLS_CNT =
    sizeof(GS_COLUMN_KEYS_ARGS_COLS) / sizeof(GS_COLUMN_KEYS_ARGS_COLS[0]);
constexpr char GS_CLIENT_GLOBAL_KEYS_NAME[] = "gs_client_global_keys";
constexpr Oid GS_CLIENT_GLOBAL_KEYS_OID =  9710;
constexpr Oid GS_CLIENT_GLOBAL_KEYS_ROWTYPE_OID = 18259;
const TableColumn GS_CLIENT_GLOBAL_KEYS_COLS[] = {
    {"global_key_name", SYS_NAMEOID},
    {"key_namespace", SYS_OIDOID},
    {"key_owner", SYS_OIDOID},
    {"key_acl", SYS_ACLITEMARRAYOID},
    {"create_date", SYS_TIMESTAMPOID},
};
constexpr int GS_CLIENT_GLOBAL_KEYS_COLS_CNT =
    sizeof(GS_CLIENT_GLOBAL_KEYS_COLS) / sizeof(GS_CLIENT_GLOBAL_KEYS_COLS[0]);
constexpr char GS_ENCRYPTED_PROC_NAME[] = "gs_encrypted_proc";
constexpr Oid GS_ENCRYPTED_PROC_OID =  9750;
constexpr Oid GS_ENCRYPTED_PROC_ROWTYPE_OID = 18260;
const TableColumn GS_ENCRYPTED_PROC_COLS[] = {
    {"func_id", SYS_OIDOID},
    {"prorettype_orig", SYS_INT4OID},
    {"last_change", SYS_TIMESTAMPOID},
    {"proargcachedcol", SYS_OIDVECTOROID},
    {"proallargtypes_orig", SYS_OIDARRAYOID},
};
constexpr int GS_ENCRYPTED_PROC_COLS_CNT = sizeof(GS_ENCRYPTED_PROC_COLS) / sizeof(GS_ENCRYPTED_PROC_COLS[0]);
constexpr char GS_CLIENT_GLOBAL_KEYS_ARGS_NAME[] = "gs_client_global_keys_args";
constexpr Oid GS_CLIENT_GLOBAL_KEYS_ARGS_OID =  9730;
constexpr Oid GS_CLIENT_GLOBAL_KEYS_ARGS_ROWTYPE_OID = 18261;
const TableColumn GS_CLIENT_GLOBAL_KEYS_ARGS_COLS[] = {
    {"global_key_id", SYS_OIDOID},
    {"function_name", SYS_NAMEOID},
    {"key", SYS_NAMEOID},
    {"value", SYS_BYTEAOID},
};
constexpr int GS_CLIENT_GLOBAL_KEYS_ARGS_COLS_CNT =
    sizeof(GS_CLIENT_GLOBAL_KEYS_ARGS_COLS) / sizeof(GS_CLIENT_GLOBAL_KEYS_ARGS_COLS[0]);
constexpr char PG_JOB_NAME[] = "pg_job";
constexpr Oid PG_JOB_OID =  9022;
constexpr Oid PG_JOB_ROWTYPE_OID = 18262;
const TableColumn PG_JOB_COLS[] = {
    {"job_id", SYS_INT8OID},
    {"current_postgres_pid", SYS_INT8OID},
    {"log_user", SYS_NAMEOID},
    {"priv_user", SYS_NAMEOID},
    {"dbname", SYS_NAMEOID},
    {"node_name", SYS_NAMEOID},
    {"job_status", SYS_CHAROID},
    {"start_date", SYS_TIMESTAMPOID},
    {"next_run_date", SYS_TIMESTAMPOID},
    {"failure_count", SYS_INT2OID},
    {"interval", SYS_TEXTOID},
    {"last_start_date", SYS_TIMESTAMPOID},
    {"last_end_date", SYS_TIMESTAMPOID},
    {"last_suc_date", SYS_TIMESTAMPOID},
    {"this_run_date", SYS_TIMESTAMPOID},
    {"nspname", SYS_NAMEOID},
    {"job_name", SYS_TEXTOID},
    {"end_date", SYS_TIMESTAMPOID},
    {"enable", SYS_BOOLOID},
    {"failure_msg", SYS_TEXTOID},
};
constexpr int PG_JOB_COLS_CNT = sizeof(PG_JOB_COLS) / sizeof(PG_JOB_COLS[0]);
constexpr char GS_ASP_NAME[] = "gs_asp";
constexpr Oid GS_ASP_OID =  9534;
constexpr Oid GS_ASP_ROWTYPE_OID = 3465;
const TableColumn GS_ASP_COLS[] = {
    {"sampleid", SYS_INT8OID},
    {"sample_time", SYS_TIMESTAMPTZOID},
    {"need_flush_sample", SYS_BOOLOID},
    {"databaseid", SYS_OIDOID},
    {"thread_id", SYS_INT8OID},
    {"sessionid", SYS_INT8OID},
    {"start_time", SYS_TIMESTAMPTZOID},
    {"event", SYS_TEXTOID},
    {"lwtid", SYS_INT4OID},
    {"psessionid", SYS_INT8OID},
    {"tlevel", SYS_INT4OID},
    {"smpid", SYS_INT4OID},
    {"userid", SYS_OIDOID},
    {"application_name", SYS_TEXTOID},
    {"client_addr", SYS_INETOID},
    {"client_hostname", SYS_TEXTOID},
    {"client_port", SYS_INT4OID},
    {"query_id", SYS_INT8OID},
    {"unique_query_id", SYS_INT8OID},
    {"user_id", SYS_OIDOID},
    {"cn_id", SYS_INT4OID},
    {"unique_query", SYS_TEXTOID},
    {"locktag", SYS_TEXTOID},
    {"lockmode", SYS_TEXTOID},
    {"block_sessionid", SYS_INT8OID},
    {"wait_status", SYS_TEXTOID},
    {"global_sessionid", SYS_TEXTOID},
    {"xact_start_time", SYS_TIMESTAMPTZOID},
    {"query_start_time", SYS_TIMESTAMPTZOID},
    {"state", SYS_TEXTOID},
};
constexpr int GS_ASP_COLS_CNT = sizeof(GS_ASP_COLS) / sizeof(GS_ASP_COLS[0]);
constexpr char PG_JOB_PROC_NAME[] = "pg_job_proc";
constexpr Oid PG_JOB_PROC_OID =  9023;
constexpr Oid PG_JOB_PROC_ROWTYPE_OID = 18263;
const TableColumn PG_JOB_PROC_COLS[] = {
    {"job_id", SYS_INT4OID},
    {"what", SYS_TEXTOID},
    {"job_name", SYS_TEXTOID},
};
constexpr int PG_JOB_PROC_COLS_CNT = sizeof(PG_JOB_PROC_COLS) / sizeof(PG_JOB_PROC_COLS[0]);
constexpr char PG_EXTENSION_DATA_SOURCE_NAME[] = "pg_extension_data_source";
constexpr Oid PG_EXTENSION_DATA_SOURCE_OID =  4211;
constexpr Oid PG_EXTENSION_DATA_SOURCE_ROWTYPE_OID = 7177;
const TableColumn PG_EXTENSION_DATA_SOURCE_COLS[] = {
    {"srcname", SYS_NAMEOID},
    {"srcowner", SYS_OIDOID},
    {"srctype", SYS_TEXTOID},
    {"srcversion", SYS_TEXTOID},
    {"srcacl", SYS_ACLITEMARRAYOID},
    {"srcoptions", SYS_TEXTARRAYOID},
};
constexpr int PG_EXTENSION_DATA_SOURCE_COLS_CNT =
    sizeof(PG_EXTENSION_DATA_SOURCE_COLS) / sizeof(PG_EXTENSION_DATA_SOURCE_COLS[0]);
constexpr char PG_STATISTIC_EXT_NAME[] = "pg_statistic_ext";
constexpr Oid PG_STATISTIC_EXT_OID =  3220;
constexpr Oid PG_STATISTIC_EXT_ROWTYPE_OID = 18264;
const TableColumn PG_STATISTIC_EXT_COLS[] = {
    {"starelid", SYS_OIDOID},
    {"starelkind", SYS_CHAROID},
    {"stainherit", SYS_BOOLOID},
    {"stanullfrac", SYS_FLOAT4OID},
    {"stawidth", SYS_INT4OID},
    {"stadistinct", SYS_FLOAT4OID},
    {"stadndistinct", SYS_FLOAT4OID},
    {"stakind1", SYS_INT2OID},
    {"stakind2", SYS_INT2OID},
    {"stakind3", SYS_INT2OID},
    {"stakind4", SYS_INT2OID},
    {"stakind5", SYS_INT2OID},
    {"staop1", SYS_OIDOID},
    {"staop2", SYS_OIDOID},
    {"staop3", SYS_OIDOID},
    {"staop4", SYS_OIDOID},
    {"staop5", SYS_OIDOID},
    {"stakey", SYS_INT2VECTOROID},
    {"stanumbers1", SYS_FLOAT4ARRAYOID},
    {"stanumbers2", SYS_FLOAT4ARRAYOID},
    {"stanumbers3", SYS_FLOAT4ARRAYOID},
    {"stanumbers4", SYS_FLOAT4ARRAYOID},
    {"stanumbers5", SYS_FLOAT4ARRAYOID},
    {"stavalues1", SYS_ANYARRAYOID},
    {"stavalues2", SYS_ANYARRAYOID},
    {"stavalues3", SYS_ANYARRAYOID},
    {"stavalues4", SYS_ANYARRAYOID},
    {"stavalues5", SYS_ANYARRAYOID},
    {"staexprs", SYS_PGNODETREEOID},
};
constexpr int PG_STATISTIC_EXT_COLS_CNT = sizeof(PG_STATISTIC_EXT_COLS) / sizeof(PG_STATISTIC_EXT_COLS[0]);
constexpr char PG_OBJECT_NAME[] = "pg_object";
constexpr Oid PG_OBJECT_OID =  9025;
constexpr Oid PG_OBJECT_ROWTYPE_OID = 18265;
const TableColumn PG_OBJECT_COLS[] = {
    {"object_oid", SYS_OIDOID},
    {"object_type", SYS_CHAROID},
    {"creator", SYS_OIDOID},
    {"ctime", SYS_TIMESTAMPTZOID},
    {"mtime", SYS_TIMESTAMPTZOID},
    {"createcsn", SYS_INT8OID},
    {"changecsn", SYS_INT8OID},
};
constexpr int PG_OBJECT_COLS_CNT = sizeof(PG_OBJECT_COLS) / sizeof(PG_OBJECT_COLS[0]);
constexpr char PG_SYNONYM_NAME[] = "pg_synonym";
constexpr Oid PG_SYNONYM_OID =  3546;
constexpr Oid PG_SYNONYM_ROWTYPE_OID = 18266;
const TableColumn PG_SYNONYM_COLS[] = {
    {"synname", SYS_NAMEOID},
    {"synnamespace", SYS_OIDOID},
    {"synowner", SYS_OIDOID},
    {"synobjschema", SYS_NAMEOID},
    {"synobjname", SYS_NAMEOID},
};
constexpr int PG_SYNONYM_COLS_CNT = sizeof(PG_SYNONYM_COLS) / sizeof(PG_SYNONYM_COLS[0]);
constexpr char GS_OBSSCANINFO_NAME[] = "gs_obsscaninfo";
constexpr Oid GS_OBSSCANINFO_OID =  5680;
constexpr Oid GS_OBSSCANINFO_ROWTYPE_OID = 18267;
const TableColumn GS_OBSSCANINFO_COLS[] = {
    {"query_id", SYS_INT8OID},
    {"user_id", SYS_TEXTOID},
    {"table_name", SYS_TEXTOID},
    {"file_type", SYS_TEXTOID},
    {"time_stamp", SYS_TIMESTAMPTZOID},
    {"actual_time", SYS_FLOAT8OID},
    {"file_scanned", SYS_INT8OID},
    {"data_size", SYS_FLOAT8OID},
    {"billing_info", SYS_TEXTOID},
};
constexpr int GS_OBSSCANINFO_COLS_CNT = sizeof(GS_OBSSCANINFO_COLS) / sizeof(GS_OBSSCANINFO_COLS[0]);
constexpr char PG_DIRECTORY_NAME[] = "pg_directory";
constexpr Oid PG_DIRECTORY_OID =  4347;
constexpr Oid PG_DIRECTORY_ROWTYPE_OID = 18268;
const TableColumn PG_DIRECTORY_COLS[] = {
    {"dirname", SYS_NAMEOID},
    {"owner", SYS_OIDOID},
    {"dirpath", SYS_TEXTOID},
    {"diracl", SYS_ACLITEMARRAYOID},
};
constexpr int PG_DIRECTORY_COLS_CNT = sizeof(PG_DIRECTORY_COLS) / sizeof(PG_DIRECTORY_COLS[0]);
constexpr char PG_HASHBUCKET_NAME[] = "pg_hashbucket";
constexpr Oid PG_HASHBUCKET_OID =  9027;
constexpr Oid PG_HASHBUCKET_ROWTYPE_OID = 9108;
const TableColumn PG_HASHBUCKET_COLS[] = {
    {"bucketid", SYS_OIDOID},
    {"bucketcnt", SYS_INT4OID},
    {"bucketmapsize", SYS_INT4OID},
    {"bucketref", SYS_INT4OID},
    {"bucketvector", SYS_OIDVECTOREXTENDOID},
};
constexpr int PG_HASHBUCKET_COLS_CNT = sizeof(PG_HASHBUCKET_COLS) / sizeof(PG_HASHBUCKET_COLS[0]);
constexpr char GS_GLOBAL_CHAIN_NAME[] = "gs_global_chain";
constexpr Oid GS_GLOBAL_CHAIN_OID =  5818;
constexpr Oid GS_GLOBAL_CHAIN_ROWTYPE_OID = 18269;
const TableColumn GS_GLOBAL_CHAIN_COLS[] = {
    {"blocknum", SYS_INT8OID},
    {"dbname", SYS_NAMEOID},
    {"username", SYS_NAMEOID},
    {"starttime", SYS_TIMESTAMPTZOID},
    {"relid", SYS_OIDOID},
    {"relnsp", SYS_NAMEOID},
    {"relname", SYS_NAMEOID},
    {"relhash", SYS_HASH16OID},
    {"globalhash", SYS_HASH32OID},
    {"txcommand", SYS_TEXTOID},
};
constexpr int GS_GLOBAL_CHAIN_COLS_CNT = sizeof(GS_GLOBAL_CHAIN_COLS) / sizeof(GS_GLOBAL_CHAIN_COLS[0]);
constexpr char GS_GLOBAL_CONFIG_NAME[] = "gs_global_config";
constexpr Oid GS_GLOBAL_CONFIG_OID =  9080;
constexpr Oid GS_GLOBAL_CONFIG_ROWTYPE_OID = 18270;
const TableColumn GS_GLOBAL_CONFIG_COLS[] = {
    {"name", SYS_NAMEOID},
    {"value", SYS_TEXTOID},
};
constexpr int GS_GLOBAL_CONFIG_COLS_CNT = sizeof(GS_GLOBAL_CONFIG_COLS) / sizeof(GS_GLOBAL_CONFIG_COLS[0]);
constexpr char STREAMING_STREAM_NAME[] = "streaming_stream";
constexpr Oid STREAMING_STREAM_OID =  9028;
constexpr Oid STREAMING_STREAM_ROWTYPE_OID = 18271;
const TableColumn STREAMING_STREAM_COLS[] = {
    {"relid", SYS_OIDOID},
    {"queries", SYS_BYTEAOID},
};
constexpr int STREAMING_STREAM_COLS_CNT = sizeof(STREAMING_STREAM_COLS) / sizeof(STREAMING_STREAM_COLS[0]);
constexpr char STREAMING_CONT_QUERY_NAME[] = "streaming_cont_query";
constexpr Oid STREAMING_CONT_QUERY_OID =  9029;
constexpr Oid STREAMING_CONT_QUERY_ROWTYPE_OID = 18272;
const TableColumn STREAMING_CONT_QUERY_COLS[] = {
    {"id", SYS_INT4OID},
    {"type", SYS_CHAROID},
    {"relid", SYS_OIDOID},
    {"defrelid", SYS_OIDOID},
    {"active", SYS_BOOLOID},
    {"streamrelid", SYS_OIDOID},
    {"matrelid", SYS_OIDOID},
    {"lookupidxid", SYS_OIDOID},
    {"step_factor", SYS_INT2OID},
    {"ttl", SYS_INT4OID},
    {"ttl_attno", SYS_INT2OID},
    {"dictrelid", SYS_OIDOID},
    {"grpnum", SYS_INT2OID},
    {"grpidx", SYS_INT2VECTOROID},
};
constexpr int STREAMING_CONT_QUERY_COLS_CNT = sizeof(STREAMING_CONT_QUERY_COLS) / sizeof(STREAMING_CONT_QUERY_COLS[0]);
constexpr char STREAMING_REAPER_STATUS_NAME[] = "streaming_reaper_status";
constexpr Oid STREAMING_REAPER_STATUS_OID =  9030;
constexpr Oid STREAMING_REAPER_STATUS_ROWTYPE_OID = 18273;
const TableColumn STREAMING_REAPER_STATUS_COLS[] = {
    {"id", SYS_INT4OID},
    {"contquery_name", SYS_NAMEOID},
    {"gather_interval", SYS_TEXTOID},
    {"gather_completion_time", SYS_TEXTOID},
};
constexpr int STREAMING_REAPER_STATUS_COLS_CNT =
    sizeof(STREAMING_REAPER_STATUS_COLS) / sizeof(STREAMING_REAPER_STATUS_COLS[0]);
constexpr char GS_MATVIEW_NAME[] = "gs_matview";
constexpr Oid GS_MATVIEW_OID =  9982;
constexpr Oid GS_MATVIEW_ROWTYPE_OID = 18274;
const TableColumn GS_MATVIEW_COLS[] = {
    {"matviewid", SYS_OIDOID},
    {"mapid", SYS_OIDOID},
    {"ivm", SYS_BOOLOID},
    {"needrefresh", SYS_BOOLOID},
    {"refreshtime", SYS_TIMESTAMPOID},
};
constexpr int GS_MATVIEW_COLS_CNT = sizeof(GS_MATVIEW_COLS) / sizeof(GS_MATVIEW_COLS[0]);
constexpr char GS_MATVIEW_DEPENDENCY_NAME[] = "gs_matview_dependency";
constexpr Oid GS_MATVIEW_DEPENDENCY_OID =  9985;
constexpr Oid GS_MATVIEW_DEPENDENCY_ROWTYPE_OID = 18275;
const TableColumn GS_MATVIEW_DEPENDENCY_COLS[] = {
    {"matviewid", SYS_OIDOID},
    {"relid", SYS_OIDOID},
    {"mlogid", SYS_OIDOID},
    {"mxmin", SYS_INT4OID},
};
constexpr int GS_MATVIEW_DEPENDENCY_COLS_CNT =
    sizeof(GS_MATVIEW_DEPENDENCY_COLS) / sizeof(GS_MATVIEW_DEPENDENCY_COLS[0]);
constexpr char PGXC_SLICE_NAME[] = "pgxc_slice";
constexpr Oid PGXC_SLICE_OID =  9035;
constexpr Oid PGXC_SLICE_ROWTYPE_OID = 18276;
const TableColumn PGXC_SLICE_COLS[] = {
    {"relname", SYS_NAMEOID},
    {"type", SYS_CHAROID},
    {"strategy", SYS_CHAROID},
    {"relid", SYS_OIDOID},
    {"referenceoid", SYS_OIDOID},
    {"sindex", SYS_INT4OID},
    {"interval", SYS_TEXTARRAYOID},
    {"transitboundary", SYS_TEXTARRAYOID},
    {"transitno", SYS_INT4OID},
    {"nodeoid", SYS_OIDOID},
    {"boundaries", SYS_TEXTARRAYOID},
    {"specified", SYS_BOOLOID},
    {"sliceorder", SYS_INT4OID},
};
constexpr int PGXC_SLICE_COLS_CNT = sizeof(PGXC_SLICE_COLS) / sizeof(PGXC_SLICE_COLS[0]);
constexpr char GS_OPT_MODEL_NAME[] = "gs_opt_model";
constexpr Oid GS_OPT_MODEL_OID =  9998;
constexpr Oid GS_OPT_MODEL_ROWTYPE_OID = 18277;
const TableColumn GS_OPT_MODEL_COLS[] = {
    {"template_name", SYS_NAMEOID},
    {"model_name", SYS_NAMEOID},
    {"datname", SYS_NAMEOID},
    {"ip", SYS_NAMEOID},
    {"port", SYS_INT4OID},
    {"max_epoch", SYS_INT4OID},
    {"learning_rate", SYS_FLOAT4OID},
    {"dim_red", SYS_FLOAT4OID},
    {"hidden_units", SYS_INT4OID},
    {"batch_size", SYS_INT4OID},
    {"feature_size", SYS_INT4OID},
    {"available", SYS_BOOLOID},
    {"is_training", SYS_BOOLOID},
    {"label", SYS_CHARARRAYOID},
    {"max", SYS_INT8ARRAYOID},
    {"acc", SYS_FLOAT4ARRAYOID},
    {"description", SYS_TEXTOID},
};
constexpr int GS_OPT_MODEL_COLS_CNT = sizeof(GS_OPT_MODEL_COLS) / sizeof(GS_OPT_MODEL_COLS[0]);
constexpr char GS_RECYCLEBIN_NAME[] = "gs_recyclebin";
constexpr Oid GS_RECYCLEBIN_OID =  8643;
constexpr Oid GS_RECYCLEBIN_ROWTYPE_OID = 18278;
const TableColumn GS_RECYCLEBIN_COLS[] = {
    {"rcybaseid", SYS_OIDOID},
    {"rcydbid", SYS_OIDOID},
    {"rcyrelid", SYS_OIDOID},
    {"rcyname", SYS_NAMEOID},
    {"rcyoriginname", SYS_NAMEOID},
    {"rcyoperation", SYS_CHAROID},
    {"rcytype", SYS_INT4OID},
    {"rcyrecyclecsn", SYS_INT8OID},
    {"rcyrecycletime", SYS_TIMESTAMPTZOID},
    {"rcycreatecsn", SYS_INT8OID},
    {"rcychangecsn", SYS_INT8OID},
    {"rcynamespace", SYS_OIDOID},
    {"rcyowner", SYS_OIDOID},
    {"rcytablespace", SYS_OIDOID},
    {"rcyrelfilenode", SYS_OIDOID},
    {"rcycanrestore", SYS_BOOLOID},
    {"rcycanpurge", SYS_BOOLOID},
    {"rcyfrozenxid", SYS_SHORTXIDOID},
    {"rcyfrozenxid64", SYS_XIDOID},
};
constexpr int GS_RECYCLEBIN_COLS_CNT = sizeof(GS_RECYCLEBIN_COLS) / sizeof(GS_RECYCLEBIN_COLS[0]);
constexpr char GS_TXN_SNAPSHOT_NAME[] = "gs_txn_snapshot";
constexpr Oid GS_TXN_SNAPSHOT_OID =  8645;
constexpr Oid GS_TXN_SNAPSHOT_ROWTYPE_OID = 18279;
const TableColumn GS_TXN_SNAPSHOT_COLS[] = {
    {"snptime", SYS_TIMESTAMPTZOID},
    {"snpxmin", SYS_INT8OID},
    {"snpcsn", SYS_INT8OID},
    {"snpsnapshot", SYS_TEXTOID},
};
constexpr int GS_TXN_SNAPSHOT_COLS_CNT = sizeof(GS_TXN_SNAPSHOT_COLS) / sizeof(GS_TXN_SNAPSHOT_COLS[0]);
constexpr char GS_MODEL_WAREHOUSE_NAME[] = "gs_model_warehouse";
constexpr Oid GS_MODEL_WAREHOUSE_OID =  3991;
constexpr Oid GS_MODEL_WAREHOUSE_ROWTYPE_OID = 3994;
const TableColumn GS_MODEL_WAREHOUSE_COLS[] = {
    {"modelname", SYS_NAMEOID},
    {"modelowner", SYS_OIDOID},
    {"createtime", SYS_TIMESTAMPOID},
    {"processedtuples", SYS_INT4OID},
    {"discardedtuples", SYS_INT4OID},
    {"preprocesstime", SYS_FLOAT4OID},
    {"exectime", SYS_FLOAT4OID},
    {"iterations", SYS_INT4OID},
    {"outputtype", SYS_OIDOID},
    {"modeltype", SYS_TEXTOID},
    {"query", SYS_TEXTOID},
    {"modeldata", SYS_BYTEAOID},
    {"weight", SYS_FLOAT4ARRAYOID},
    {"hyperparametersnames", SYS_TEXTARRAYOID},
    {"hyperparametersvalues", SYS_TEXTARRAYOID},
    {"hyperparametersoids", SYS_OIDARRAYOID},
    {"coefnames", SYS_TEXTARRAYOID},
    {"coefvalues", SYS_TEXTARRAYOID},
    {"coefoids", SYS_OIDARRAYOID},
    {"trainingscoresname", SYS_TEXTARRAYOID},
    {"trainingscoresvalue", SYS_FLOAT4ARRAYOID},
    {"modeldescribe", SYS_TEXTARRAYOID},
};
constexpr int GS_MODEL_WAREHOUSE_COLS_CNT = sizeof(GS_MODEL_WAREHOUSE_COLS) / sizeof(GS_MODEL_WAREHOUSE_COLS[0]);
constexpr char GS_PACKAGE_NAME[] = "gs_package";
constexpr Oid GS_PACKAGE_OID =  7815;
constexpr Oid GS_PACKAGE_ROWTYPE_OID = 9745;
const TableColumn GS_PACKAGE_COLS[] = {
    {"pkgnamespace", SYS_OIDOID},
    {"pkgowner", SYS_OIDOID},
    {"pkgname", SYS_NAMEOID},
    {"pkgspecsrc", SYS_TEXTOID},
    {"pkgbodydeclsrc", SYS_TEXTOID},
    {"pkgbodyinitsrc", SYS_TEXTOID},
    {"pkgacl", SYS_ACLITEMARRAYOID},
    {"pkgsecdef", SYS_BOOLOID},
};
constexpr int GS_PACKAGE_COLS_CNT = sizeof(GS_PACKAGE_COLS) / sizeof(GS_PACKAGE_COLS[0]);
constexpr char GS_JOB_ARGUMENT_NAME[] = "gs_job_argument";
constexpr Oid GS_JOB_ARGUMENT_OID =  9036;
constexpr Oid GS_JOB_ARGUMENT_ROWTYPE_OID = 18280;
const TableColumn GS_JOB_ARGUMENT_COLS[] = {
    {"argument_position", SYS_INT4OID},
    {"argument_type", SYS_NAMEOID},
    {"job_name", SYS_TEXTOID},
    {"argument_name", SYS_TEXTOID},
    {"argument_value", SYS_TEXTOID},
    {"default_value", SYS_TEXTOID},
};
constexpr int GS_JOB_ARGUMENT_COLS_CNT = sizeof(GS_JOB_ARGUMENT_COLS) / sizeof(GS_JOB_ARGUMENT_COLS[0]);
constexpr char GS_JOB_ATTRIBUTE_NAME[] = "gs_job_attribute";
constexpr Oid GS_JOB_ATTRIBUTE_OID =  9031;
constexpr Oid GS_JOB_ATTRIBUTE_ROWTYPE_OID = 18281;
const TableColumn GS_JOB_ATTRIBUTE_COLS[] = {
    {"job_name", SYS_TEXTOID},
    {"attribute_name", SYS_TEXTOID},
    {"attribute_value", SYS_TEXTOID},
};
constexpr int GS_JOB_ATTRIBUTE_COLS_CNT = sizeof(GS_JOB_ATTRIBUTE_COLS) / sizeof(GS_JOB_ATTRIBUTE_COLS[0]);
constexpr char GS_UID_NAME[] = "gs_uid";
constexpr Oid GS_UID_OID =  8666;
constexpr Oid GS_UID_ROWTYPE_OID = 18282;
const TableColumn GS_UID_COLS[] = {
    {"relid", SYS_OIDOID},
    {"uid_backup", SYS_INT8OID},
};
constexpr int GS_UID_COLS_CNT = sizeof(GS_UID_COLS) / sizeof(GS_UID_COLS[0]);
constexpr char GS_DB_PRIVILEGE_NAME[] = "gs_db_privilege";
constexpr Oid GS_DB_PRIVILEGE_OID =  5566;
constexpr Oid GS_DB_PRIVILEGE_ROWTYPE_OID = 18283;
const TableColumn GS_DB_PRIVILEGE_COLS[] = {
    {"roleid", SYS_OIDOID},
    {"privilege_type", SYS_TEXTOID},
    {"admin_option", SYS_BOOLOID},
};
constexpr int GS_DB_PRIVILEGE_COLS_CNT = sizeof(GS_DB_PRIVILEGE_COLS) / sizeof(GS_DB_PRIVILEGE_COLS[0]);
constexpr char PG_REPLICATION_ORIGIN_NAME[] = "pg_replication_origin";
constexpr Oid PG_REPLICATION_ORIGIN_OID =  6134;
constexpr Oid PG_REPLICATION_ORIGIN_ROWTYPE_OID = 6143;
const TableColumn PG_REPLICATION_ORIGIN_COLS[] = {
    {"roident", SYS_OIDOID},
    {"roname", SYS_TEXTOID},
};
constexpr int PG_REPLICATION_ORIGIN_COLS_CNT =
    sizeof(PG_REPLICATION_ORIGIN_COLS) / sizeof(PG_REPLICATION_ORIGIN_COLS[0]);
constexpr char PG_PUBLICATION_NAME[] = "pg_publication";
constexpr Oid PG_PUBLICATION_OID =  6130;
constexpr Oid PG_PUBLICATION_ROWTYPE_OID = 6141;
const TableColumn PG_PUBLICATION_COLS[] = {
    {"pubname", SYS_NAMEOID},
    {"pubowner", SYS_OIDOID},
    {"puballtables", SYS_BOOLOID},
    {"pubinsert", SYS_BOOLOID},
    {"pubupdate", SYS_BOOLOID},
    {"pubdelete", SYS_BOOLOID},
};
constexpr int PG_PUBLICATION_COLS_CNT = sizeof(PG_PUBLICATION_COLS) / sizeof(PG_PUBLICATION_COLS[0]);
constexpr char PG_PUBLICATION_REL_NAME[] = "pg_publication_rel";
constexpr Oid PG_PUBLICATION_REL_OID =  6132;
constexpr Oid PG_PUBLICATION_REL_ROWTYPE_OID = 6142;
const TableColumn PG_PUBLICATION_REL_COLS[] = {
    {"prpubid", SYS_OIDOID},
    {"prrelid", SYS_OIDOID},
};
constexpr int PG_PUBLICATION_REL_COLS_CNT = sizeof(PG_PUBLICATION_REL_COLS) / sizeof(PG_PUBLICATION_REL_COLS[0]);
constexpr char PG_SUBSCRIPTION_NAME[] = "pg_subscription";
constexpr Oid PG_SUBSCRIPTION_OID =  6126;
constexpr Oid PG_SUBSCRIPTION_ROWTYPE_OID = 6128;
const TableColumn PG_SUBSCRIPTION_COLS[] = {
    {"subdbid", SYS_OIDOID},
    {"subname", SYS_NAMEOID},
    {"subowner", SYS_OIDOID},
    {"subenabled", SYS_BOOLOID},
    {"subconninfo", SYS_TEXTOID},
    {"subslotname", SYS_NAMEOID},
    {"subsynccommit", SYS_TEXTOID},
    {"subpublications", SYS_TEXTARRAYOID},
};
constexpr int PG_SUBSCRIPTION_COLS_CNT = sizeof(PG_SUBSCRIPTION_COLS) / sizeof(PG_SUBSCRIPTION_COLS[0]);
const SysTableDef NORMAL_SYSTABLE[] = {
    {PG_PARTITION_NAME, PG_PARTITION_OID, PG_PARTITION_ROWTYPE_OID, PG_PARTITION_COLS, PG_PARTITION_COLS_CNT},
    {PG_ATTRDEF_NAME, PG_ATTRDEF_OID, PG_ATTRDEF_ROWTYPE_OID, PG_ATTRDEF_COLS, PG_ATTRDEF_COLS_CNT},
    {PG_CONSTRAINT_NAME, PG_CONSTRAINT_OID, PG_CONSTRAINT_ROWTYPE_OID, PG_CONSTRAINT_COLS, PG_CONSTRAINT_COLS_CNT},
    {PG_INHERITS_NAME, PG_INHERITS_OID, PG_INHERITS_ROWTYPE_OID, PG_INHERITS_COLS, PG_INHERITS_COLS_CNT},
    {PG_INDEX_NAME, PG_INDEX_OID, PG_INDEX_ROWTYPE_OID, PG_INDEX_COLS, PG_INDEX_COLS_CNT},
    {PG_OPERATOR_NAME, PG_OPERATOR_OID, PG_OPERATOR_ROWTYPE_OID, PG_OPERATOR_COLS, PG_OPERATOR_COLS_CNT},
    {PG_OPFAMILY_NAME, PG_OPFAMILY_OID, PG_OPFAMILY_ROWTYPE_OID, PG_OPFAMILY_COLS, PG_OPFAMILY_COLS_CNT},
    {PG_OPCLASS_NAME, PG_OPCLASS_OID, PG_OPCLASS_ROWTYPE_OID, PG_OPCLASS_COLS, PG_OPCLASS_COLS_CNT},
    {PG_AM_NAME, PG_AM_OID, PG_AM_ROWTYPE_OID, PG_AM_COLS, PG_AM_COLS_CNT},
    {PG_AMOP_NAME, PG_AMOP_OID, PG_AMOP_ROWTYPE_OID, PG_AMOP_COLS, PG_AMOP_COLS_CNT},
    {PG_AMPROC_NAME, PG_AMPROC_OID, PG_AMPROC_ROWTYPE_OID, PG_AMPROC_COLS, PG_AMPROC_COLS_CNT},
    {PG_LANGUAGE_NAME, PG_LANGUAGE_OID, PG_LANGUAGE_ROWTYPE_OID, PG_LANGUAGE_COLS, PG_LANGUAGE_COLS_CNT},
    {PG_LARGEOBJECT_METADATA_NAME, PG_LARGEOBJECT_METADATA_OID, PG_LARGEOBJECT_METADATA_ROWTYPE_OID,
     PG_LARGEOBJECT_METADATA_COLS, PG_LARGEOBJECT_METADATA_COLS_CNT},
    {PG_LARGEOBJECT_NAME, PG_LARGEOBJECT_OID, PG_LARGEOBJECT_ROWTYPE_OID, PG_LARGEOBJECT_COLS, PG_LARGEOBJECT_COLS_CNT},
    {PG_AGGREGATE_NAME, PG_AGGREGATE_OID, PG_AGGREGATE_ROWTYPE_OID, PG_AGGREGATE_COLS, PG_AGGREGATE_COLS_CNT},
    {PG_STATISTIC_NAME, PG_STATISTIC_OID, PG_STATISTIC_ROWTYPE_OID, PG_STATISTIC_COLS, PG_STATISTIC_COLS_CNT},
    {PG_REWRITE_NAME, PG_REWRITE_OID, PG_REWRITE_ROWTYPE_OID, PG_REWRITE_COLS, PG_REWRITE_COLS_CNT},
    {PG_TRIGGER_NAME, PG_TRIGGER_OID, PG_TRIGGER_ROWTYPE_OID, PG_TRIGGER_COLS, PG_TRIGGER_COLS_CNT},
    {PG_DESCRIPTION_NAME, PG_DESCRIPTION_OID, PG_DESCRIPTION_ROWTYPE_OID, PG_DESCRIPTION_COLS, PG_DESCRIPTION_COLS_CNT},
    {PG_CAST_NAME, PG_CAST_OID, PG_CAST_ROWTYPE_OID, PG_CAST_COLS, PG_CAST_COLS_CNT},
    {PG_ENUM_NAME, PG_ENUM_OID, PG_ENUM_ROWTYPE_OID, PG_ENUM_COLS, PG_ENUM_COLS_CNT},
    {PG_NAMESPACE_NAME, PG_NAMESPACE_OID, PG_NAMESPACE_ROWTYPE_OID, PG_NAMESPACE_COLS, PG_NAMESPACE_COLS_CNT},
    {PG_CONVERSION_NAME, PG_CONVERSION_OID, PG_CONVERSION_ROWTYPE_OID, PG_CONVERSION_COLS, PG_CONVERSION_COLS_CNT},
    {PG_DEPEND_NAME, PG_DEPEND_OID, PG_DEPEND_ROWTYPE_OID, PG_DEPEND_COLS, PG_DEPEND_COLS_CNT},
    {PG_DATABASE_NAME, PG_DATABASE_OID, PG_DATABASE_ROWTYPE_OID, PG_DATABASE_COLS, PG_DATABASE_COLS_CNT},
    {PG_DB_ROLE_SETTING_NAME, PG_DB_ROLE_SETTING_OID, PG_DB_ROLE_SETTING_ROWTYPE_OID, PG_DB_ROLE_SETTING_COLS,
     PG_DB_ROLE_SETTING_COLS_CNT},
    {PG_TABLESPACE_NAME, PG_TABLESPACE_OID, PG_TABLESPACE_ROWTYPE_OID, PG_TABLESPACE_COLS, PG_TABLESPACE_COLS_CNT},
    {PG_PLTEMPLATE_NAME, PG_PLTEMPLATE_OID, PG_PLTEMPLATE_ROWTYPE_OID, PG_PLTEMPLATE_COLS, PG_PLTEMPLATE_COLS_CNT},
    {PG_AUTHID_NAME, PG_AUTHID_OID, PG_AUTHID_ROWTYPE_OID, PG_AUTHID_COLS, PG_AUTHID_COLS_CNT},
    {PG_AUTH_MEMBERS_NAME, PG_AUTH_MEMBERS_OID, PG_AUTH_MEMBERS_ROWTYPE_OID, PG_AUTH_MEMBERS_COLS,
     PG_AUTH_MEMBERS_COLS_CNT},
    {PG_SHDEPEND_NAME, PG_SHDEPEND_OID, PG_SHDEPEND_ROWTYPE_OID, PG_SHDEPEND_COLS, PG_SHDEPEND_COLS_CNT},
    {PG_SHDESCRIPTION_NAME, PG_SHDESCRIPTION_OID, PG_SHDESCRIPTION_ROWTYPE_OID, PG_SHDESCRIPTION_COLS,
     PG_SHDESCRIPTION_COLS_CNT},
    {PG_TS_CONFIG_NAME, PG_TS_CONFIG_OID, PG_TS_CONFIG_ROWTYPE_OID, PG_TS_CONFIG_COLS, PG_TS_CONFIG_COLS_CNT},
    {PG_TS_CONFIG_MAP_NAME, PG_TS_CONFIG_MAP_OID, PG_TS_CONFIG_MAP_ROWTYPE_OID, PG_TS_CONFIG_MAP_COLS,
     PG_TS_CONFIG_MAP_COLS_CNT},
    {PG_TS_DICT_NAME, PG_TS_DICT_OID, PG_TS_DICT_ROWTYPE_OID, PG_TS_DICT_COLS, PG_TS_DICT_COLS_CNT},
    {PG_TS_PARSER_NAME, PG_TS_PARSER_OID, PG_TS_PARSER_ROWTYPE_OID, PG_TS_PARSER_COLS, PG_TS_PARSER_COLS_CNT},
    {PG_TS_TEMPLATE_NAME, PG_TS_TEMPLATE_OID, PG_TS_TEMPLATE_ROWTYPE_OID, PG_TS_TEMPLATE_COLS, PG_TS_TEMPLATE_COLS_CNT},
    {PG_AUTH_HISTORY_NAME, PG_AUTH_HISTORY_OID, PG_AUTH_HISTORY_ROWTYPE_OID, PG_AUTH_HISTORY_COLS,
     PG_AUTH_HISTORY_COLS_CNT},
    {PG_USER_STATUS_NAME, PG_USER_STATUS_OID, PG_USER_STATUS_ROWTYPE_OID, PG_USER_STATUS_COLS, PG_USER_STATUS_COLS_CNT},
    {PG_EXTENSION_NAME, PG_EXTENSION_OID, PG_EXTENSION_ROWTYPE_OID, PG_EXTENSION_COLS, PG_EXTENSION_COLS_CNT},
    {PG_OBSSCANINFO_NAME, PG_OBSSCANINFO_OID, PG_OBSSCANINFO_ROWTYPE_OID, PG_OBSSCANINFO_COLS, PG_OBSSCANINFO_COLS_CNT},
    {PG_FOREIGN_DATA_WRAPPER_NAME, PG_FOREIGN_DATA_WRAPPER_OID, PG_FOREIGN_DATA_WRAPPER_ROWTYPE_OID,
     PG_FOREIGN_DATA_WRAPPER_COLS, PG_FOREIGN_DATA_WRAPPER_COLS_CNT},
    {PG_FOREIGN_SERVER_NAME, PG_FOREIGN_SERVER_OID, PG_FOREIGN_SERVER_ROWTYPE_OID, PG_FOREIGN_SERVER_COLS,
     PG_FOREIGN_SERVER_COLS_CNT},
    {PG_USER_MAPPING_NAME, PG_USER_MAPPING_OID, PG_USER_MAPPING_ROWTYPE_OID, PG_USER_MAPPING_COLS,
     PG_USER_MAPPING_COLS_CNT},
    {PGXC_CLASS_NAME, PGXC_CLASS_OID, PGXC_CLASS_ROWTYPE_OID, PGXC_CLASS_COLS, PGXC_CLASS_COLS_CNT},
    {PGXC_NODE_NAME, PGXC_NODE_OID, PGXC_NODE_ROWTYPE_OID, PGXC_NODE_COLS, PGXC_NODE_COLS_CNT},
    {PGXC_GROUP_NAME, PGXC_GROUP_OID, PGXC_GROUP_ROWTYPE_OID, PGXC_GROUP_COLS, PGXC_GROUP_COLS_CNT},
    {PG_RESOURCE_POOL_NAME, PG_RESOURCE_POOL_OID, PG_RESOURCE_POOL_ROWTYPE_OID, PG_RESOURCE_POOL_COLS,
     PG_RESOURCE_POOL_COLS_CNT},
    {PG_WORKLOAD_GROUP_NAME, PG_WORKLOAD_GROUP_OID, PG_WORKLOAD_GROUP_ROWTYPE_OID, PG_WORKLOAD_GROUP_COLS,
     PG_WORKLOAD_GROUP_COLS_CNT},
    {PG_APP_WORKLOADGROUP_MAPPING_NAME, PG_APP_WORKLOADGROUP_MAPPING_OID, PG_APP_WORKLOADGROUP_MAPPING_ROWTYPE_OID,
     PG_APP_WORKLOADGROUP_MAPPING_COLS, PG_APP_WORKLOADGROUP_MAPPING_COLS_CNT},
    {PG_FOREIGN_TABLE_NAME, PG_FOREIGN_TABLE_OID, PG_FOREIGN_TABLE_ROWTYPE_OID, PG_FOREIGN_TABLE_COLS,
     PG_FOREIGN_TABLE_COLS_CNT},
    {PG_RLSPOLICY_NAME, PG_RLSPOLICY_OID, PG_RLSPOLICY_ROWTYPE_OID, PG_RLSPOLICY_COLS, PG_RLSPOLICY_COLS_CNT},
    {PG_DEFAULT_ACL_NAME, PG_DEFAULT_ACL_OID, PG_DEFAULT_ACL_ROWTYPE_OID, PG_DEFAULT_ACL_COLS, PG_DEFAULT_ACL_COLS_CNT},
    {PG_SECLABEL_NAME, PG_SECLABEL_OID, PG_SECLABEL_ROWTYPE_OID, PG_SECLABEL_COLS, PG_SECLABEL_COLS_CNT},
    {PG_SHSECLABEL_NAME, PG_SHSECLABEL_OID, PG_SHSECLABEL_ROWTYPE_OID, PG_SHSECLABEL_COLS, PG_SHSECLABEL_COLS_CNT},
    {PG_COLLATION_NAME, PG_COLLATION_OID, PG_COLLATION_ROWTYPE_OID, PG_COLLATION_COLS, PG_COLLATION_COLS_CNT},
    {PG_RANGE_NAME, PG_RANGE_OID, PG_RANGE_ROWTYPE_OID, PG_RANGE_COLS, PG_RANGE_COLS_CNT},
    {GS_POLICY_LABEL_NAME, GS_POLICY_LABEL_OID, GS_POLICY_LABEL_ROWTYPE_OID, GS_POLICY_LABEL_COLS,
     GS_POLICY_LABEL_COLS_CNT},
    {GS_AUDITING_POLICY_NAME, GS_AUDITING_POLICY_OID, GS_AUDITING_POLICY_ROWTYPE_OID, GS_AUDITING_POLICY_COLS,
     GS_AUDITING_POLICY_COLS_CNT},
    {GS_AUDITING_POLICY_ACCESS_NAME, GS_AUDITING_POLICY_ACCESS_OID, GS_AUDITING_POLICY_ACCESS_ROWTYPE_OID,
     GS_AUDITING_POLICY_ACCESS_COLS, GS_AUDITING_POLICY_ACCESS_COLS_CNT},
    {GS_AUDITING_POLICY_PRIVILEGES_NAME, GS_AUDITING_POLICY_PRIVILEGES_OID, GS_AUDITING_POLICY_PRIVILEGES_ROWTYPE_OID,
     GS_AUDITING_POLICY_PRIVILEGES_COLS, GS_AUDITING_POLICY_PRIVILEGES_COLS_CNT},
    {GS_AUDITING_POLICY_FILTERS_NAME, GS_AUDITING_POLICY_FILTERS_OID, GS_AUDITING_POLICY_FILTERS_ROWTYPE_OID,
     GS_AUDITING_POLICY_FILTERS_COLS, GS_AUDITING_POLICY_FILTERS_COLS_CNT},
    {GS_MASKING_POLICY_NAME, GS_MASKING_POLICY_OID, GS_MASKING_POLICY_ROWTYPE_OID, GS_MASKING_POLICY_COLS,
     GS_MASKING_POLICY_COLS_CNT},
    {GS_MASKING_POLICY_ACTIONS_NAME, GS_MASKING_POLICY_ACTIONS_OID, GS_MASKING_POLICY_ACTIONS_ROWTYPE_OID,
     GS_MASKING_POLICY_ACTIONS_COLS, GS_MASKING_POLICY_ACTIONS_COLS_CNT},
    {GS_MASKING_POLICY_FILTERS_NAME, GS_MASKING_POLICY_FILTERS_OID, GS_MASKING_POLICY_FILTERS_ROWTYPE_OID,
     GS_MASKING_POLICY_FILTERS_COLS, GS_MASKING_POLICY_FILTERS_COLS_CNT},
    {GS_ENCRYPTED_COLUMNS_NAME, GS_ENCRYPTED_COLUMNS_OID, GS_ENCRYPTED_COLUMNS_ROWTYPE_OID, GS_ENCRYPTED_COLUMNS_COLS,
     GS_ENCRYPTED_COLUMNS_COLS_CNT},
    {GS_COLUMN_KEYS_NAME, GS_COLUMN_KEYS_OID, GS_COLUMN_KEYS_ROWTYPE_OID, GS_COLUMN_KEYS_COLS, GS_COLUMN_KEYS_COLS_CNT},
    {GS_COLUMN_KEYS_ARGS_NAME, GS_COLUMN_KEYS_ARGS_OID, GS_COLUMN_KEYS_ARGS_ROWTYPE_OID, GS_COLUMN_KEYS_ARGS_COLS,
     GS_COLUMN_KEYS_ARGS_COLS_CNT},
    {GS_CLIENT_GLOBAL_KEYS_NAME, GS_CLIENT_GLOBAL_KEYS_OID, GS_CLIENT_GLOBAL_KEYS_ROWTYPE_OID,
     GS_CLIENT_GLOBAL_KEYS_COLS, GS_CLIENT_GLOBAL_KEYS_COLS_CNT},
    {GS_ENCRYPTED_PROC_NAME, GS_ENCRYPTED_PROC_OID, GS_ENCRYPTED_PROC_ROWTYPE_OID, GS_ENCRYPTED_PROC_COLS,
     GS_ENCRYPTED_PROC_COLS_CNT},
    {GS_CLIENT_GLOBAL_KEYS_ARGS_NAME, GS_CLIENT_GLOBAL_KEYS_ARGS_OID, GS_CLIENT_GLOBAL_KEYS_ARGS_ROWTYPE_OID,
     GS_CLIENT_GLOBAL_KEYS_ARGS_COLS, GS_CLIENT_GLOBAL_KEYS_ARGS_COLS_CNT},
    {PG_JOB_NAME, PG_JOB_OID, PG_JOB_ROWTYPE_OID, PG_JOB_COLS, PG_JOB_COLS_CNT},
    {GS_ASP_NAME, GS_ASP_OID, GS_ASP_ROWTYPE_OID, GS_ASP_COLS, GS_ASP_COLS_CNT},
    {PG_JOB_PROC_NAME, PG_JOB_PROC_OID, PG_JOB_PROC_ROWTYPE_OID, PG_JOB_PROC_COLS, PG_JOB_PROC_COLS_CNT},
    {PG_EXTENSION_DATA_SOURCE_NAME, PG_EXTENSION_DATA_SOURCE_OID, PG_EXTENSION_DATA_SOURCE_ROWTYPE_OID,
     PG_EXTENSION_DATA_SOURCE_COLS, PG_EXTENSION_DATA_SOURCE_COLS_CNT},
    {PG_STATISTIC_EXT_NAME, PG_STATISTIC_EXT_OID, PG_STATISTIC_EXT_ROWTYPE_OID, PG_STATISTIC_EXT_COLS,
     PG_STATISTIC_EXT_COLS_CNT},
    {PG_OBJECT_NAME, PG_OBJECT_OID, PG_OBJECT_ROWTYPE_OID, PG_OBJECT_COLS, PG_OBJECT_COLS_CNT},
    {PG_SYNONYM_NAME, PG_SYNONYM_OID, PG_SYNONYM_ROWTYPE_OID, PG_SYNONYM_COLS, PG_SYNONYM_COLS_CNT},
    {GS_OBSSCANINFO_NAME, GS_OBSSCANINFO_OID, GS_OBSSCANINFO_ROWTYPE_OID, GS_OBSSCANINFO_COLS, GS_OBSSCANINFO_COLS_CNT},
    {PG_DIRECTORY_NAME, PG_DIRECTORY_OID, PG_DIRECTORY_ROWTYPE_OID, PG_DIRECTORY_COLS, PG_DIRECTORY_COLS_CNT},
    {PG_HASHBUCKET_NAME, PG_HASHBUCKET_OID, PG_HASHBUCKET_ROWTYPE_OID, PG_HASHBUCKET_COLS, PG_HASHBUCKET_COLS_CNT},
    {GS_GLOBAL_CHAIN_NAME, GS_GLOBAL_CHAIN_OID, GS_GLOBAL_CHAIN_ROWTYPE_OID, GS_GLOBAL_CHAIN_COLS,
     GS_GLOBAL_CHAIN_COLS_CNT},
    {GS_GLOBAL_CONFIG_NAME, GS_GLOBAL_CONFIG_OID, GS_GLOBAL_CONFIG_ROWTYPE_OID, GS_GLOBAL_CONFIG_COLS,
     GS_GLOBAL_CONFIG_COLS_CNT},
    {STREAMING_STREAM_NAME, STREAMING_STREAM_OID, STREAMING_STREAM_ROWTYPE_OID, STREAMING_STREAM_COLS,
     STREAMING_STREAM_COLS_CNT},
    {STREAMING_CONT_QUERY_NAME, STREAMING_CONT_QUERY_OID, STREAMING_CONT_QUERY_ROWTYPE_OID, STREAMING_CONT_QUERY_COLS,
     STREAMING_CONT_QUERY_COLS_CNT},
    {STREAMING_REAPER_STATUS_NAME, STREAMING_REAPER_STATUS_OID, STREAMING_REAPER_STATUS_ROWTYPE_OID,
     STREAMING_REAPER_STATUS_COLS, STREAMING_REAPER_STATUS_COLS_CNT},
    {GS_MATVIEW_NAME, GS_MATVIEW_OID, GS_MATVIEW_ROWTYPE_OID, GS_MATVIEW_COLS, GS_MATVIEW_COLS_CNT},
    {GS_MATVIEW_DEPENDENCY_NAME, GS_MATVIEW_DEPENDENCY_OID, GS_MATVIEW_DEPENDENCY_ROWTYPE_OID,
     GS_MATVIEW_DEPENDENCY_COLS, GS_MATVIEW_DEPENDENCY_COLS_CNT},
    {PGXC_SLICE_NAME, PGXC_SLICE_OID, PGXC_SLICE_ROWTYPE_OID, PGXC_SLICE_COLS, PGXC_SLICE_COLS_CNT},
    {GS_OPT_MODEL_NAME, GS_OPT_MODEL_OID, GS_OPT_MODEL_ROWTYPE_OID, GS_OPT_MODEL_COLS, GS_OPT_MODEL_COLS_CNT},
    {GS_RECYCLEBIN_NAME, GS_RECYCLEBIN_OID, GS_RECYCLEBIN_ROWTYPE_OID, GS_RECYCLEBIN_COLS, GS_RECYCLEBIN_COLS_CNT},
    {GS_TXN_SNAPSHOT_NAME, GS_TXN_SNAPSHOT_OID, GS_TXN_SNAPSHOT_ROWTYPE_OID, GS_TXN_SNAPSHOT_COLS,
     GS_TXN_SNAPSHOT_COLS_CNT},
    {GS_MODEL_WAREHOUSE_NAME, GS_MODEL_WAREHOUSE_OID, GS_MODEL_WAREHOUSE_ROWTYPE_OID, GS_MODEL_WAREHOUSE_COLS,
     GS_MODEL_WAREHOUSE_COLS_CNT},
    {GS_PACKAGE_NAME, GS_PACKAGE_OID, GS_PACKAGE_ROWTYPE_OID, GS_PACKAGE_COLS, GS_PACKAGE_COLS_CNT},
    {GS_JOB_ARGUMENT_NAME, GS_JOB_ARGUMENT_OID, GS_JOB_ARGUMENT_ROWTYPE_OID, GS_JOB_ARGUMENT_COLS,
     GS_JOB_ARGUMENT_COLS_CNT},
    {GS_JOB_ATTRIBUTE_NAME, GS_JOB_ATTRIBUTE_OID, GS_JOB_ATTRIBUTE_ROWTYPE_OID, GS_JOB_ATTRIBUTE_COLS,
     GS_JOB_ATTRIBUTE_COLS_CNT},
    {GS_UID_NAME, GS_UID_OID, GS_UID_ROWTYPE_OID, GS_UID_COLS, GS_UID_COLS_CNT},
    {GS_DB_PRIVILEGE_NAME, GS_DB_PRIVILEGE_OID, GS_DB_PRIVILEGE_ROWTYPE_OID, GS_DB_PRIVILEGE_COLS,
     GS_DB_PRIVILEGE_COLS_CNT},
    {PG_REPLICATION_ORIGIN_NAME, PG_REPLICATION_ORIGIN_OID, PG_REPLICATION_ORIGIN_ROWTYPE_OID,
     PG_REPLICATION_ORIGIN_COLS, PG_REPLICATION_ORIGIN_COLS_CNT},
    {PG_PUBLICATION_NAME, PG_PUBLICATION_OID, PG_PUBLICATION_ROWTYPE_OID, PG_PUBLICATION_COLS, PG_PUBLICATION_COLS_CNT},
    {PG_PUBLICATION_REL_NAME, PG_PUBLICATION_REL_OID, PG_PUBLICATION_REL_ROWTYPE_OID, PG_PUBLICATION_REL_COLS,
     PG_PUBLICATION_REL_COLS_CNT},
    {PG_SUBSCRIPTION_NAME, PG_SUBSCRIPTION_OID, PG_SUBSCRIPTION_ROWTYPE_OID, PG_SUBSCRIPTION_COLS,
     PG_SUBSCRIPTION_COLS_CNT},
};

} // namespace DSTORE
#endif