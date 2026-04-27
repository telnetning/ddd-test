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
 * ut_wal_stream.cpp
 *
 * Description:
 *
 * ---------------------------------------------------------------------------------------
 *
 */
#include "diagnose/dstore_wal_diagnose.h"
#include "ut_wal/ut_wal_basic.h"
#include "ut_utilities/ut_dstore_framework.h"
#include "ut_mock/ut_mock.h"
#include "ut_buffer/ut_buffer_fake_instance.h"

namespace DSTORE {

class WalDiagnoseTest : public WALBASICTEST {
protected:
    void SetUp() override;

    void TearDown() override;

private:
};
}

void WalDiagnoseTest::SetUp()
{
    WALBASICTEST::SetUp();
    PrepareControlFileContent();
}

void WalDiagnoseTest::TearDown()
{
    WALBASICTEST::TearDown();
}

TEST_F(WalDiagnoseTest, GetWalStreamInfoLocallyTest)
{
    WalStreamStateInfo *walStreamInfo = nullptr;
    uint32 walStreamCount = 0;
    char *errMsg = nullptr;
    RetStatus ret = WalDiagnose::GetWalStreamInfoLocally(g_defaultPdbId, &walStreamInfo, &walStreamCount, &errMsg);
    ASSERT_EQ(ret, DSTORE_SUCC);
    ASSERT_EQ(errMsg, nullptr);
    ASSERT_EQ(walStreamCount, 1);
    ASSERT_EQ(walStreamInfo[0].walId, 1);
    ASSERT_EQ(walStreamInfo[0].pdbId, g_defaultPdbId);
    ASSERT_EQ(strcmp(walStreamInfo[0].usage, "WRITE_WAL"), 0);
    ASSERT_EQ(strcmp(walStreamInfo[0].state, "USING"), 0);
    DstorePfreeExt(walStreamInfo);
}