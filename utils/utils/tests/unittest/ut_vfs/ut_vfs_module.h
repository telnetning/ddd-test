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

#ifndef UTILS_COMMON_UT_VFS_MODULE_H
#define UTILS_COMMON_UT_VFS_MODULE_H

#include "ut_vfs_common.h"

class VfsModuleTest : public ::testing::Test {
protected:
    void SetUp() override
    {}

    void TearDown() override
    {}

    VfsLibParameter testLibPara = {SIZE_8K, 1};
    const char *testClusterName = "ClusterName";
    const char *testVfsName = "VfsName";
    const char *testSnapshotName = "SnapshotName";
    FileParameter testFilePara = {
        "storeSpaceName1",
        VFS_DEFAULT_FILE_STREAM_ID,
        IN_PLACE_WRITE_FILE,
        DATA_FILE_TYPE,
        DEFAULT_RANGE_SIZE,
        0xFFFFFFFFU,
        0,
        FILE_READ_AND_WRITE_MODE,
        false
    };
    FileOpenParam testFileOpenPara = {
        .flags = IN_PLACE_WRITE_FILE,
        .filePath = "file",
        .snapshotName = "SnapshotName"
    };
};

#endif /* UTILS_COMMON_UT_VFS_MODULE_H */
