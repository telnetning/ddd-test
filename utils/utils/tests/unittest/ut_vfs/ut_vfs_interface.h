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

#ifndef DSTORE_UT_VFS_INTERFACE_H
#define DSTORE_UT_VFS_INTERFACE_H

#include <fcntl.h>
#include <sys/stat.h>

#include "gtest/gtest.h"

#include "vfs/vfs_interface.h"

#include "vfs/vfs_linux_common.h"
#include <dlfcn.h>
#include "vfs/vfs_linux_static.h"
#include "vfs/vfs_utils.h"

struct FlushContext {
    off_t flushOffset = 0;
    ErrorCode errCode = 0;
};

class VfsInterfaceTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        unlink(UT_FILENAME_1);
        unlink(UT_FILENAME_2);
        ASSERT_EQ(InitVfsModule(NULL), 0);
        ASSERT_EQ(GetStaticLocalVfsInstance(&m_testStaticVfs), 0);
    }

    void TearDown() override
    {
        ASSERT_EQ(ExitVfsModule(), 0);
        unlink(UT_FILENAME_1);
        unlink(UT_FILENAME_2);
    }

    VirtualFileSystem *m_testStaticVfs;

    static const char UT_CLUSTER_NAME[];
    static const char UT_VFS_NAME[];
    static const uint16_t UT_FILEID_1;
    static const uint16_t UT_FILEID_2;
    static const char UT_FILENAME_1[];
    static const char UT_FILENAME_2[];
};

const char VfsInterfaceTest::UT_CLUSTER_NAME[] = "UtClusterName";
const char VfsInterfaceTest::UT_VFS_NAME[] = "UtVfsName";
const uint16_t VfsInterfaceTest::UT_FILEID_1 = 800;
const uint16_t VfsInterfaceTest::UT_FILEID_2 = 900;
const char VfsInterfaceTest::UT_FILENAME_1[] = "VfsUtFile800.data";
const char VfsInterfaceTest::UT_FILENAME_2[] = "VfsUtFile900.data";

#endif //DSTORE_UT_VFS_INTERFACE_H
