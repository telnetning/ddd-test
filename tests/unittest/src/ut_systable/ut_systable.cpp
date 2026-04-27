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
#include "ut_systable/ut_systable.h"
#include "systable/dstore_systable_interface.h"
#include "systable/systable_bootstrap_buildin_data.h"
#include "systable/dstore_systable_utility.h"
#include "heap/dstore_heap_struct.h"

/* dstore_systable_utility.cpp */
TEST_F(UTSysTable, CreateCoreSystableTest_level0)
{
    DSTORE::PageId segment_id = DSTORE::INVALID_PAGE_ID;
    DSTORE::RetStatus ret = SystableInterface::CreateCoreSystable(DSTORE::INVALID_PDB_ID, DSTORE::DSTORE_INVALID_OID,
        DSTORE::DSTORE_INVALID_OID, segment_id);
    ASSERT_EQ(ret, DSTORE_FAIL);

    ret = SystableInterface::CreateCoreSystable(DSTORE::INVALID_PDB_ID, DSTORE::SYSTABLE_RELATION_OID,
        static_cast<DSTORE::TablespaceId>(DSTORE::TBS_ID::CATALOG_TABLE_SPACE_ID), segment_id);
    ASSERT_EQ(ret, DSTORE_FAIL);

    ret = SystableInterface::CreateCoreSystable(g_defaultPdbId, DSTORE::SYSTABLE_RELATION_OID,
        static_cast<DSTORE::TablespaceId>(DSTORE::TBS_ID::CATALOG_TABLE_SPACE_ID), segment_id);
    ASSERT_EQ(ret, DSTORE_SUCC);
    ASSERT_NE(segment_id, DSTORE::INVALID_PAGE_ID);
}

TEST_F(UTSysTable, CreateTupleDescTest_level0)
{
    TupleDesc tupleDesc = CreateTupleDesc(SYS_RELATION_COLS_CNT, true, SYS_RELATION_COLS);
    ASSERT_EQ(tupleDesc->natts, SYS_RELATION_COLS_CNT);
    DstorePfree(tupleDesc);
}

TEST_F(UTSysTable, CreateLobTupleDescTest_level0)
{
    TupleDesc lobTupleDesc = CreateLobTupleDesc();
    ASSERT_EQ(lobTupleDesc->natts, 1);
    DstorePfreeExt(lobTupleDesc);
}

TEST_F(UTSysTable, GetCoreSystableSegmentIdTest_level0)
{
    DSTORE::PageId pageId = DSTORE::INVALID_PAGE_ID;
    DSTORE::RetStatus ret = SystableInterface::GetCoreSystableSegmentId(DSTORE::INVALID_PDB_ID,
        DSTORE::DSTORE_INVALID_OID, pageId);
    ASSERT_EQ(ret, DSTORE_FAIL);

    ret = SystableInterface::GetCoreSystableSegmentId(g_defaultPdbId, DSTORE::SYSTABLE_RELATION_OID, pageId);
    /* in ut, iterator.NextItem() and iterator_local.NextItem() is false */
    ASSERT_EQ(ret, DSTORE_FAIL);
    ASSERT_EQ(pageId, DSTORE::INVALID_PAGE_ID);
}

TEST_F(UTSysTable, AddRelationMapTest_level0)
{
    DSTORE::PageId pageId = DSTORE::INVALID_PAGE_ID;
    DSTORE::RetStatus ret = SystableInterface::AddRelationMap(DSTORE::INVALID_PDB_ID,
        DSTORE::DSTORE_INVALID_OID, pageId);
    ASSERT_EQ(ret, DSTORE_FAIL);

    ret = SystableInterface::AddRelationMap(g_defaultPdbId, DSTORE::SYSTABLE_RELATION_OID, pageId);
    ASSERT_EQ(ret, DSTORE_SUCC);
}

/* dstore_systable_interface.cpp */
TEST_F(UTSysTable, GetNewObjectIdTest_level0)
{
    DSTORE::Oid relOid = SystableInterface::GetNewObjectId(g_defaultPdbId, false, true);
    ASSERT_NE(relOid, DSTORE::DSTORE_INVALID_OID);
}
