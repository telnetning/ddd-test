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
 * Description: CloudNativeDatabase UTTableOperationTest(ut table Operate test class)
 */

#ifndef UT_TABLE_OPERATION_TEST_H
#define UT_TABLE_OPERATION_TEST_H

#include "ut_utilities/ut_dstore_framework.h"

namespace DSTORE {
class UTTableOperation;
class UTTableOperationTest : public DSTORETEST {
protected:
    void SetUp() override;
    void TearDown() override;

    void RestartInstance();
    void StopInstance();

    void CheckAllTable();
    void CheckTable(uint8_t tableNameType);
    void CheckIndex(uint8_t tableNameType);

    UTTableOperation          *m_utTableOperate = nullptr;
};

} /* namespace DSTORE */

#endif /* UT_TABLE_OPERATION_TEST_H */
