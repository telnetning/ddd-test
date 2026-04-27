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
 * dstore_page_diagnose.h
 *
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/dstore/include/page/dstore_page_diagnose.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_PAGE_DIAGNOSE_H
#define DSTORE_PAGE_DIAGNOSE_H

#include "config/dstore_vfs_config.h"
#include "page/dstore_page_struct.h"
#include "framework/dstore_vfs_adapter.h"
#include "framework/dstore_instance_interface.h"
#include "control/dstore_control_file_page.h"
namespace DSTORE {
struct Page;
struct ControlFileMetaPage;
struct ControlDataPage;
struct DecodeDictMetaPage;
struct DecodeDictPage;
}  // namespace DSTORE

namespace PageDiagnose {

constexpr uint32 INVALID_COMM_AUTH_TYPE = UINT32_MAX;
constexpr int INVALID_COMM_THRD_MIN = -1;
constexpr int INVALID_COMM_THRD_MAX = -1;
constexpr int INVALID_COMM_PRO_TYPE = -1;
constexpr uint32 DEFAULT_COMM_AUTH_TYPE = 0;
constexpr int DEFAULT_COMM_THRD_MIN = 120;
constexpr int DEFAULT_COMM_THRD_MAX = 1500;

constexpr int HOST_NAME_LEN = 128;

struct DumpCommConfig {
    uint32 authType;
    int threadMin;
    int threadMax;
    char *localIp;
};

struct DumpToolHelperInitParam {
    bool reuseVfs;
    DSTORE::VFSAdapter *vfs;
    const char *pdbVfsName;
    const DumpCommConfig *commConfig;
};

#pragma GCC visibility push(default)
class DumpToolHelper {
public:
    static void SetPrintTarget(FILE *printTarget);

    explicit DumpToolHelper(DSTORE::StorageType vfsType, char *configPath = nullptr);

    ~DumpToolHelper();

    DSTORE::RetStatus Init(DumpToolHelperInitParam *param);

    void Destroy() noexcept;

    DSTORE::RetStatus Open(const char *fileName, int flags, FileDescriptor **fileDesc);

    void Close(FileDescriptor *fileDesc) noexcept;

    DSTORE::RetStatus ReadOffset(FileDescriptor *fileDesc, uint64_t count, int64_t offset, void *outBuffer,
        int64 *readBytes) const;

    DSTORE::RetStatus ReadPage(FileDescriptor *fileDesc, const uint32_t pageId, void *outBuffer,
        uint32 bufferSize) const;

    int64 Size(FileDescriptor *fileDesc) const;

    DSTORE::RetStatus FileIsExist(const char *fileName, bool *fileExist) const;

    static FILE *dumpPrint;

private:

    DSTORE::StorageType m_vfsType;
    char *m_pageStoreConfigPath;
    DSTORE::TenantConfig m_tenantConfig;
    bool m_reuseVfs;
    VirtualFileSystem *m_staticVfs;
    DSTORE::VFSAdapter *m_vfsAdapter;
};

char *PageDump(DSTORE::Page *page, bool showTupleData = false, DSTORE::Page *metaPage = nullptr);
char *PageDump(DSTORE::ControlFileMetaPage *page);
char *PageDump(DSTORE::ControlPage *page);
char *PageDump(DSTORE::DecodeDictMetaPage *page);
char *PageDump(DSTORE::DecodeDictPage *page);

DSTORE::PageType GetPageType(DSTORE::Page* page);

DSTORE::BlockNumber GetIndexMetaPageBlockId(DSTORE::Page* page);

DSTORE::FileId GetIndexMetaFileId(DSTORE::Page* page);

DSTORE::RetStatus ParseCommThreadNum(const char *procName, char *arg, DumpCommConfig *config);

void InitCommConfig(DumpCommConfig *config);

void GetFileName(uint16_t fileId, char *fileName, uint32 fileNameLen);

#pragma GCC visibility pop
}  // namespace PageDiagnose
#endif
