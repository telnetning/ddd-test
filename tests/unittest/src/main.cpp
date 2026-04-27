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
#include "ut_utilities/ut_dstore_framework.h"
#include "common/dstore_datatype.h"
#include "common/log/dstore_log.h"

char g_utTopDir[MAXPGPATH] = {'0'};
char g_utDir[MAXPGPATH] = {'0'};
char g_utLogDir[MAXPGPATH] = {'0'};
char g_utDataDir[MAXPGPATH] = {'0'};

extern int RemoveDir(const char *dirPath);

void SetUpGlobalPath()
{
    getcwd(g_utTopDir, MAXPGPATH);
    int rc = sprintf_s(g_utDir, MAXPGPATH, "%s/utdir", g_utTopDir);
    storage_securec_check_ss(rc);

    rc = sprintf_s(g_utDataDir, MAXPGPATH, "%s/data", g_utDir);
    storage_securec_check_ss(rc);

    rc = sprintf_s(g_utLogDir, MAXPGPATH, "%s/utlog", g_utDir);
    storage_securec_check_ss(rc);
}

void PrepareEnv()
{
    int rc = mkdir(g_utDir, 0777);
    StorageAssert(rc == 0);

    rc = mkdir(g_utLogDir, 0777);
    StorageAssert(rc == 0);
}

void ClearEnv()
{
    int rc = RemoveDir(g_utDir);
    StorageAssert(rc == 0);
}

GTEST_API_ int main(int argc, char **argv) {
    printf("Running main() from %s\n", __FILE__);
    SetUpGlobalPath();
    ClearEnv();
    PrepareEnv();

    DSTORETEST::ProcEnter();

    testing::InitGoogleTest(&argc, argv);
    int exitCode = RUN_ALL_TESTS();

    DSTORETEST::ProcExit(exitCode);
}