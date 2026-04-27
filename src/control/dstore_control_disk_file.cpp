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
 * dstore_control_disk_file.cpp
 *
 *
 *
 * IDENTIFICATION
 *        dstore/src/control/dstore_control_disk_file.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "common/memory/dstore_mctx.h"
#include "common/dstore_datatype.h"
#include "framework/dstore_instance.h"
#include "control/dstore_control_disk_file.h"

namespace DSTORE {

ControlDiskFile::ControlDiskFile(VFSAdapter *vfs, char *storeSpaceName)
    : m_vfs(vfs), m_filePath{nullptr}, m_fileName(nullptr), m_fd{nullptr}, m_storeSpaceName(storeSpaceName)
{}

ControlDiskFile::~ControlDiskFile()
{
    m_vfs = nullptr;
    m_fd = nullptr;
    DstorePfreeExt(m_filePath);
    m_storeSpaceName = nullptr;
}

RetStatus ControlDiskFile::Init(const char *dataDir, const char *name)
{
    m_filePath = static_cast<char *>(DstorePalloc0(MAXPGPATH));
    if (unlikely(m_filePath == nullptr)) {
        return DSTORE_FAIL;
    }
    m_fileName = name;
    if (dataDir != nullptr) {
        errno_t rc = sprintf_s(m_filePath, MAXPGPATH, "%s/%s", dataDir, name);
        storage_securec_check_ss(rc);
    } else {
        errno_t rc = sprintf_s(m_filePath, MAXPGPATH, "%s", name);
        storage_securec_check_ss(rc)
    }
    return DSTORE_SUCC;
}

bool ControlDiskFile::IsExist()
{
    StorageAssert(m_fileName != nullptr);
    return m_vfs->FileExists(m_fileName);
}

RetStatus ControlDiskFile::Create()
{
    /* Step 1. set filepara. */
    FileParameter filePara;
    filePara.streamId = VFS_DEFAULT_FILE_STREAM_ID;
    filePara.flag = IN_PLACE_WRITE_FILE;
    filePara.fileSubType = DATA_FILE_TYPE;
    filePara.rangeSize = CONTROL_FILE_RANGE_SIZE;
    filePara.maxSize = (uint64)DSTORE_MAX_BLOCK_NUMBER * BLCKSZ;
    filePara.recycleTtl = 0;
    filePara.mode = FILE_READ_AND_WRITE_MODE;
    filePara.isReplayWrite = false;
    if (unlikely(m_storeSpaceName == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Control file store space name is null."));
        return DSTORE_FAIL;
    }
    errno_t rc = strcpy_s(filePara.storeSpaceName, STORESPACE_NAME_MAX_LEN, m_storeSpaceName);
    storage_securec_check(rc, "\0", "\0");
    if (unlikely(IsExist())) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Control file already exist, filePath(%s).", m_filePath));
        return DSTORE_FAIL;
    }
    /* Step 2. create file */
    FileDescriptor *fileDesc;
    RetStatus ret = m_vfs->CreateFile(m_fileName, filePara, &fileDesc);
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Create control file failed, filePath(%s).", m_filePath));
        return DSTORE_FAIL;
    }
    m_fd = fileDesc;
    ErrLog(DSTORE_LOG, MODULE_CONTROL, ErrMsg("Create control file success, filePath(%s).", m_filePath));
    return DSTORE_SUCC;
}

RetStatus ControlDiskFile::Extend(int64 length)
{
    StorageAssert(m_fd != nullptr);
    return m_vfs->Extend(m_fd, length);
}

RetStatus ControlDiskFile::Open()
{
    return m_vfs->OpenFile(m_fileName, FILE_READ_AND_WRITE_FLAG, &m_fd);
}

RetStatus ControlDiskFile::Close()
{
    if (m_fd == nullptr) {
        ErrLog(DSTORE_LOG, MODULE_CONTROL, ErrMsg("fd is nullptr when close control file."));
        return DSTORE_SUCC;
    }
    RetStatus ret = m_vfs->CloseFile(m_fd);
    if (ret != DSTORE_SUCC) {
        return DSTORE_FAIL;
    }
    m_fd = nullptr;
    return DSTORE_SUCC;
}

RetStatus ControlDiskFile::ReadPage(BlockNumber blockNum, void *outBuffer)
{
    StorageAssert(m_fd != nullptr);
    g_storageInstance->GetStat()->m_reportWaitEvent(
        static_cast<uint32_t>(GsStatWaitEvent::WAIT_EVENT_CONTROLFILE_READ_META_PAGE));
    RetStatus rc = m_vfs->ReadPageSync(m_fd, blockNum, outBuffer);
    g_storageInstance->GetStat()->m_reportWaitEvent(static_cast<uint32_t>(OPTUTIL_GSSTAT_WAIT_EVENT_END));
    return rc;
}

RetStatus ControlDiskFile::WritePage(BlockNumber blockNum, void *inBuffer)
{
    StorageAssert(m_fd != nullptr);
    g_storageInstance->GetStat()->m_reportWaitEvent(
        static_cast<uint32_t>(GsStatWaitEvent::WAIT_EVENT_CONTROLFILE_WRITE_PAGE_SYNC));
    RetStatus rc = m_vfs->WritePageSync(m_fd, blockNum, inBuffer);
    g_storageInstance->GetStat()->m_reportWaitEvent(static_cast<uint32_t>(OPTUTIL_GSSTAT_WAIT_EVENT_END));
    return rc;
}

RetStatus ControlDiskFile::Fsync()
{
    StorageAssert(m_fd != nullptr);
    return m_vfs->Fsync(m_fd);
}

RetStatus ControlDiskFile::PwriteSync(const void *outBuffer, uint64 writeSize, int64 offset)
{
    if (unlikely(m_fd == nullptr || m_vfs == nullptr)) {
        return DSTORE_FAIL;
    }
    return m_vfs->PwriteSync(m_fd, outBuffer, writeSize, offset);
}

RetStatus ControlDiskFile::PreadSync(void *buf, uint64 bufSize, int64 offset, int64 *readSize)
{
    if (unlikely(m_fd == nullptr || m_vfs == nullptr)) {
        return DSTORE_FAIL;
    }
    return m_vfs->Pread(m_fd, buf, bufSize, offset, readSize);
}

}  // namespace DSTORE