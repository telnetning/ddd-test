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

#ifndef DSTORE_DATASEGMENTSCANCONTEXTTEST_H
#define DSTORE_DATASEGMENTSCANCONTEXTTEST_H

#include "ut_utilities/ut_dstore_framework.h"
#include "ut_data_segment.h"
#include "heap/dstore_heap_struct.h"
#include "tablespace/dstore_data_segment_context.h"

using namespace DSTORE;

class DataSegmentScanContextTest : public DataSegmentTest {
protected:
    void SetUp() override
    {
        int buffer = DSTORETEST::m_guc.buffer;
        DSTORETEST::m_guc.buffer = 40960;
        DataSegmentTest::SetUp();
        DSTORETEST::m_guc.buffer = buffer;
    }

    void TearDown() override
    {
        DataSegmentTest::TearDown();
    }

};

#endif //DSTORE_DATASEGMENTSCANCONTEXTTEST_H

