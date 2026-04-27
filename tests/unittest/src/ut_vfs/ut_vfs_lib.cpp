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

#include "config/dstore_vfs_config.h"

#include "vfs/vfs_interface.h"
#include "vfs/vfs_error_code.h"

/* Uncomment me when you're ready. */
// #define DYNAMIC_LINK_VFS_LIB

extern char g_utDataDir[MAXPGPATH];

class VfsLibTest : public DSTORETEST {
protected:
#ifdef DYNAMIC_LINK_VFS_LIB
    void ConfigVFS()
    {
        /* Get the config file. */
        memset(m_configFilePath, 0, MAXPGPATH);
        char *execPath = m_configFilePath;
        int ret = readlink("/proc/self/exe", execPath, MAXPGPATH);
        ASSERT_GT(ret, 0);
        char *lastSlashPtr = strrchr(execPath, '/');
        ASSERT_NE(lastSlashPtr, nullptr);

        snprintf(lastSlashPtr + 1, MAXPGPATH / 2, "dynamic_vfs.conf.init");
        ASSERT_EQ(access(m_configFilePath, F_OK), 0); /* It must exist. */
        DSTORE::DynamicLinkVFS(StorageInstanceType::SINGLE, 0);
        DSTORE::CreateVFS(&m_vfsConfig, &m_testStaticVfs);
    }
#endif

    void SetUp() override
    {
        /* When StartLogger, will call InitVfsModule */
        DSTORETEST::SetUp();
        errno_t rc = memset_s(m_filePathName, MAXPGPATH, 0, MAXPGPATH);
        storage_securec_check(rc, "\0", "\0");
        snprintf(m_filePathName, MAXPGPATH, "%s/%s", g_utDataDir, UT_VFS_FILENAME);
        unlink(m_filePathName); /* Delete the file if it exists. */

        m_testStaticVfs = nullptr;


#ifdef DYNAMIC_LINK_VFS_LIB
        ConfigVFS();
#else
        ASSERT_EQ(::GetStaticLocalVfsInstance(&m_testStaticVfs), 0);
#endif
    }

    void TearDown() override
    {
#ifdef DYNAMIC_LINK_VFS_LIB
        DSTORE::DynamicUnlinkVFS(m_testStaticVfs, &m_vfsConfig, true);
#endif

        unlink(m_filePathName);
        /* when StopLogger, will call ExitVfsModule */
        DSTORETEST::TearDown();
    }

    static constexpr char UT_VFS_FILENAME[] = "UtVfsLibFile.data";
    static constexpr uint16 UT_STORE_SPACE_ID = 10;

    VFSPageStoreConfig m_vfsConfig;
    ::VirtualFileSystem *m_testStaticVfs;

    char m_configFilePath[MAXPGPATH];
    char m_filePathName[MAXPGPATH];

    FileId m_fileId{100};
    ::FileDescriptor *m_fd;
    ::FileParameter m_filePara = {
        "ut_default_storespace",
        VFS_DEFAULT_FILE_STREAM_ID,
        IN_PLACE_WRITE_FILE,
        DATA_FILE_TYPE,
        (64 << 10),           /* 64KB */
        (1 << 30),              /* 1GB */
        0,
        FILE_READ_AND_WRITE_MODE,
        false,
    };
};

const char VfsLibTest::UT_VFS_FILENAME[];

/* As we're testing external vfs library, we explicitly use root namespace to differentiate the outer interfaces with
 * the inner vfs_outdated ones. For another thing, we are not using fileId here since vfs interfaces lib have been 
 * changed. */
TEST_F(VfsLibTest, BasicCreationRemovalTest)
{
    ASSERT_EQ(::Create(m_testStaticVfs, m_filePathName, m_filePara, &m_fd), 0);

    bool fileIsExist;
    ASSERT_EQ(::FileIsExist(m_testStaticVfs, m_filePathName, &fileIsExist), 0);
    ASSERT_TRUE(fileIsExist);

    ASSERT_EQ(::Close(m_fd), 0);

    /* Cannot create a file that is existed */
    ASSERT_NE(::Create(m_testStaticVfs, m_filePathName, m_filePara, &m_fd), 0);

    ASSERT_EQ(::Remove(m_testStaticVfs, m_filePathName), 0);
    ASSERT_EQ(::FileIsExist(m_testStaticVfs, m_filePathName, &fileIsExist), 0);
    ASSERT_FALSE(fileIsExist);
}

TEST_F(VfsLibTest, BasicReadWritelTest)
{
    const char writeText[BLCKSZ] = "HelloWorld";
    char readText[BLCKSZ];
    const int writeTextLen = BLCKSZ;
    const int extendSize = (1 << 20); /* 1MB */
    int64 writeSize = 0;
    int64 readSize = 0;

    ASSERT_EQ(::Create(m_testStaticVfs, m_filePathName, m_filePara, &m_fd), 0);

    ASSERT_EQ(::Extend(m_fd, extendSize), 0);
    int64 currentFileSize;
    ASSERT_EQ(::GetSize(m_fd, &currentFileSize), 0);
    ASSERT_EQ(currentFileSize, extendSize);

    const int writeTime = 2;
    int offset = 0;
    for (int i = 0; i < writeTime; ++i) {
        ::PwriteSync(m_fd, writeText, writeTextLen, offset, &writeSize);
        ASSERT_EQ(writeSize, writeTextLen);
        offset += writeSize;
    }

    offset = 0;
    ASSERT_EQ(::Pread(m_fd, readText, writeTextLen, offset, &readSize), 0);
    ASSERT_EQ(readSize, writeTextLen);
    ASSERT_EQ(strncmp(writeText, readText, writeTextLen), 0);

    offset += writeTextLen;
    ASSERT_EQ(::Pread(m_fd, readText, writeTextLen, offset, &readSize), 0);
    ASSERT_EQ(readSize, writeTextLen);
    ASSERT_EQ(strncmp(writeText, readText, writeTextLen), 0);

    ASSERT_EQ(::Close(m_fd), 0);
    ASSERT_EQ(::Remove(m_testStaticVfs, m_filePathName), 0);
}