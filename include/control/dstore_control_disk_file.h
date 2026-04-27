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
 * dstore_control_disk_file.h
 *  control disk file for dstore
 *
 *
 * IDENTIFICATION
 *        dstore/include/control/dstore_control_disk_file.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_CONTROL_DISK_FILE_H
#define DSTORE_CONTROL_DISK_FILE_H

#include "framework/dstore_vfs_adapter.h"
#include "common/log/dstore_log.h"
#include "common/algorithm/dstore_checksum_impl.h"
#include "common/datatype/dstore_uuid_utils.h"

namespace DSTORE {

class ControlDiskFile : public BaseObject {
public:
    explicit ControlDiskFile(VFSAdapter *vfs, char *storeSpaceName);
    ~ControlDiskFile();

    RetStatus Init(const char *dataDir, const char *name);
    bool IsExist();
    RetStatus Create();
    RetStatus Extend(int64 length);
    RetStatus Open();
    RetStatus Close();
    RetStatus ReadPage(BlockNumber blockNum, void *outBuffer);
    RetStatus WritePage(BlockNumber blockNum, void *inBuffer);
    RetStatus Fsync();
    RetStatus PwriteSync(const void *outBuffer, uint64 writeSize, int64 offset);
    RetStatus PreadSync(void *buf, uint64 bufSize, int64 offset, int64 *readSize);
    inline const char* GetFileName() { return m_fileName; }
#ifdef UT
    char *UTGetFilePath()
    {
        return m_filePath;
    }
#endif
private:
    VFSAdapter *m_vfs;
    char *m_filePath;
    const char *m_fileName;
    FileDescriptor *m_fd;
    char *m_storeSpaceName;
};
}  // namespace DSTORE

#endif  // DSTORE_CONTROL_FILE_MGR_H