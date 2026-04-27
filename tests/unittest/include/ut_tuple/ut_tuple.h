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
#ifndef DSTORE_UT_TUPLE_H
#define DSTORE_UT_TUPLE_H

#include "ut_tablehandler/ut_table_handler.h"

using namespace DSTORE;

class TupleTest : public DSTORETEST {
protected:
    void SetUp() override;
    void TearDown() override;

    UTTableHandler *m_tableHandler;
    const char table1Name[15] = "test_table_1";
    TableStorageMgr *m_table;
};

#endif /* DSTORE_UT_TUPLE_H */
