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
 * dstore_fake_class.h
 *
 * IDENTIFICATION
 *        dstore/include/catalog/dstore_fake_namespace.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_GAUSSKERNEL_INCLUDE_COMMON_DSTORE_FAKE_NAMESPACE_H
#define SRC_GAUSSKERNEL_INCLUDE_COMMON_DSTORE_FAKE_NAMESPACE_H
#include "systable/systable_relation.h"
#include "common/dstore_datatype.h"
#include "catalog/dstore_catalog_struct.h"
namespace DSTORE {

#pragma pack (push, 1)
struct FormData_pg_namespace {
    DstoreNameData    nspname;
    Oid         nspowner;
    int8        nsptimeline;

#ifdef CATALOG_VARLEN            /* variable-length fields start here */
    aclitem     nspacl[1];
	char        inredistribution;
    bool        nspblockchain;
    Oid         nspcollation;
#endif
};
#pragma pack (pop)

using Form_pg_namespace = FormData_pg_namespace *;
}

#endif /* SRC_GAUSSKERNEL_INCLUDE_COMMON_STORAGE_FAKE_CLASS_H */
