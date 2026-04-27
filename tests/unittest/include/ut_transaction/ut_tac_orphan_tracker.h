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
#ifndef UT_TAC_ORPHAN_TRX_TRACKER_H
#define UT_TAC_ORPHAN_TRX_TRACKER_H

#include <gtest/gtest.h>

#include "ut_utilities/ut_dstore_framework.h"
#include "ut_buffer/ut_buffer_fake_instance.h"
#include "framework/dstore_instance.h"
#include "transaction/dstore_csn_mgr.h"

class TacOrphanTrxTrackerTest : public DSTORETEST {
protected:
    void SetUp() override
    {
        using namespace DSTORE;
        DSTORETEST::SetUp();
        UtInstance *instance = DstoreNew(m_ut_memory_context)UtInstance();
        /* The grace period is set to be one second for testing purpose */
        DSTORETEST::m_guc.tacGracePeriod = 1;
        instance->Install(&DSTORETEST::m_guc, m_ut_memory_context);
        instance->Startup(&DSTORETEST::m_guc);
    }

    void TearDown() override
    {
        using namespace DSTORE;
        UtInstance *instance = (UtInstance *)g_storageInstance;
        instance->Shutdown();
        delete instance;
        DSTORETEST::TearDown();
    }
};

#endif