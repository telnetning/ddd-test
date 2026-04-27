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
 * ut_wal_redo_manager.cpp
 *
 * Description:
 *
 * ---------------------------------------------------------------------------------------
 *
 */

#include "ut_wal/ut_wal_basic.h"
#include "ut_utilities/ut_dstore_framework.h"
#include "ut_mock/ut_mock.h"
#include "ut_buffer/ut_buffer_fake_instance.h"
#include "common/algorithm/dstore_scalable_array.h"

namespace DSTORE {

class WalRedoManagerTest : public WALBASICTEST {
protected:
    void SetUp() override
    {
        SetDefaultPdbId(PDB_TEMPLATE1_ID);
        WALBASICTEST::SetUp();
        m_maxTerm = 512;
    }

    void TearDown() override
    {
        WALBASICTEST::TearDown();
    }
    
    uint64 m_maxTerm;
};
}