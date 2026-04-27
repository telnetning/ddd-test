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
 * dstore_tbs_file_meta_page.h
 *
 * IDENTIFICATION
 *        include/page/dstore_tbs_file_meta_page.h
 *
 * ---------------------------------------------------------------------------------------
 */

#include <netdb.h>
#include <arpa/inet.h>

#include "page/dstore_index_page.h"
#include "page/dstore_heap_page.h"
#include "page/dstore_undo_page.h"
#include "page/dstore_undo_segment_meta_page.h"
#include "page/dstore_data_segment_meta_page.h"
#include "page/dstore_heap_segment_meta_page.h"
#include "page/dstore_bitmap_page.h"
#include "page/dstore_bitmap_meta_page.h"
#include "page/dstore_tbs_file_meta_page.h"
#include "page/dstore_btr_recycle_partition_meta_page.h"
#include "page/dstore_btr_recycle_root_meta_page.h"
#include "page/dstore_btr_queue_page.h"
#include "logical_replication/dstore_decode_dict_file.h"
#include "framework/dstore_vfs_interface.h"
#include "framework/dstore_config_interface.h"
#include "page/dstore_page_diagnose.h"

namespace PageDiagnose {
using namespace DSTORE;

FILE *DumpToolHelper::dumpPrint = nullptr;

void DumpToolHelper::SetPrintTarget(FILE *printTarget)
{
    if (printTarget != nullptr) {
        dumpPrint = printTarget;
    }
}

PageType GetPageType(Page *page)
{
    return page->GetType();
}

BlockNumber GetIndexMetaPageBlockId(DSTORE::Page *page)
{
    StorageAssert(page->GetType() == PageType::INDEX_PAGE_TYPE);
    return (static_cast<BtrPage *>(page))->GetBtrMetaPageId().m_blockId;
}

DSTORE::FileId GetIndexMetaFileId(DSTORE::Page* page)
{
    StorageAssert(page->GetType() == PageType::INDEX_PAGE_TYPE);
    return (static_cast<BtrPage *>(page))->GetBtrMetaPageId().m_fileId;
}

char* PageDump(Page *page, bool showTupleData, Page *metaPage)
{
    char* data = nullptr;
    if (STORAGE_VAR_NULL(page)) {
        ErrLog(DSTORE_PANIC, MODULE_PAGE, ErrMsg("Page is nullptr."));
    }
    switch (page->GetType()) {
        case PageType::HEAP_PAGE_TYPE: {
            data = (static_cast<HeapPage *>(page))->Dump(showTupleData);
            break;
        }
        case PageType::INDEX_PAGE_TYPE: {
            StorageAssert(metaPage != nullptr);
            data = (static_cast<BtrPage *>(page))->Dump(metaPage);
            break;
        }
        case PageType::TRANSACTION_SLOT_PAGE: {
            data = (static_cast<TransactionSlotPage *>(page))->Dump();
            break;
        }
        case PageType::UNDO_PAGE_TYPE: {
            data = (static_cast<UndoRecordPage *>(page))->Dump();
            break;
        }
        case PageType::FSM_PAGE_TYPE: {
            data = (static_cast<FsmPage *>(page))->Dump();
            break;
        }
        case PageType::FSM_META_PAGE_TYPE: {
            data = (static_cast<FreeSpaceMapMetaPage *>(page))->Dump();
            break;
        }
        case PageType::DATA_SEGMENT_META_PAGE_TYPE: {
            data = (static_cast<DataSegmentMetaPage *>(page))->DumpDataSegmentMetaPage();
            break;
        }
        case PageType::HEAP_SEGMENT_META_PAGE_TYPE: {
            data = (static_cast<HeapSegmentMetaPage *>(page))->DumpHeapSegmentMetaPage();
            break;
        }
        case PageType::UNDO_SEGMENT_META_PAGE_TYPE: {
            data = (static_cast<UndoSegmentMetaPage *>(page))->DumpUndoSegmentMetaPage();
            break;
        }
        case PageType::TBS_EXTENT_META_PAGE_TYPE: {
            data = (static_cast<SegExtentMetaPage *>(page))->Dump();
            break;
        }
        case PageType::TBS_BITMAP_PAGE_TYPE: {
            data = (static_cast<TbsBitmapPage *>(page))->Dump();
            break;
        }
        case PageType::TBS_BITMAP_META_PAGE_TYPE: {
            data = (static_cast<TbsBitmapMetaPage *>(page))->Dump();
            break;
        }
        case PageType::TBS_FILE_META_PAGE_TYPE: {
            data = (static_cast<TbsFileMetaPage *>(page))->Dump();
            break;
        }
        case PageType::BTR_QUEUE_PAGE_TYPE: {
            data = (static_cast<BtrQueuePage *>(page))->Dump();
            break;
        }
        case PageType::BTR_RECYCLE_PARTITION_META_PAGE_TYPE: {
            data = (static_cast<BtrRecyclePartitionMetaPage *>(page))->Dump();
            break;
        }
        case PageType::BTR_RECYCLE_ROOT_META_PAGE_TYPE: {
            data = (static_cast<BtrRecycleRootMetaPage *>(page))->Dump();
            break;
        }
        case PageType::TBS_SPACE_META_PAGE_TYPE: {
            data = (static_cast<TbsSpaceMetaPage *>(page))->Dump();
            break;
        }
        default:
            if (page->GetType() == PageType::INVALID_PAGE_TYPE || page->GetType() >= PageType::MAX_PAGE_TYPE) {
                StringInfoData str;
                if (unlikely(!str.init())) {
                    storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
                    ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("DstorePalloc fail."));
                    return nullptr;
                }
                str.append("Unkown data type %hu", static_cast<uint16>(page->GetType()));
                data = str.data;
            }
    }
    return data;
}

char *PageDump(DSTORE::ControlFileMetaPage *page)
{ return page->Dump(); }

char *PageDump(DSTORE::ControlPage *page)
{ return page->Dump(); }

char *PageDump(DSTORE::DecodeDictMetaPage *page)
{
    StringInfoData dumpInfo;
    if (unlikely(!dumpInfo.init())) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("DstorePalloc fail."));
        return nullptr;
    }
    page->Dump(&dumpInfo);
    return dumpInfo.data;
}

char *PageDump(DSTORE::DecodeDictPage *page)
{
    StringInfoData dumpInfo;
    if (unlikely(!dumpInfo.init())) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("DstorePalloc fail."));
        return nullptr;
    }
    page->Dump(&dumpInfo);
    return dumpInfo.data;
}

RetStatus ParseCommThreadNum(const char *procName, char *arg, DumpCommConfig *config)
{
    char *saveStr;
    char *threadMinStr = strtok_r(arg, ":", &saveStr);
    char *threadMaxStr = strtok_r(nullptr, ":", &saveStr);
    if (threadMinStr == nullptr || threadMaxStr == nullptr) {
        (void)fprintf(DumpToolHelper::dumpPrint, "%s: \"%s\" invalid option format.\n", procName, arg);
        return DSTORE_FAIL;
    }
    if (sscanf_s(threadMinStr, "%d", &config->threadMin) != 1) {
        (void)fprintf(DumpToolHelper::dumpPrint, "%s: could not parse communication thread min num str \"%s\"\n",
            procName, threadMinStr);
        return DSTORE_FAIL;
    }
    if (sscanf_s(threadMaxStr, "%d", &config->threadMax) != 1) {
        (void)fprintf(DumpToolHelper::dumpPrint, "%s: could not parse communication thread max num str \"%s\"\n",
            procName, threadMaxStr);
        return DSTORE_FAIL;
    }
    if (config->threadMin > config->threadMax) {
        (void)fprintf(DumpToolHelper::dumpPrint, "%s: communication thread num max less than min \"%s\"\n", procName,
            threadMaxStr);
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

void InitCommConfig(DumpCommConfig *config)
{
    config->authType = INVALID_COMM_AUTH_TYPE;
    config->threadMin = INVALID_COMM_THRD_MIN;
    config->threadMax = INVALID_COMM_THRD_MAX;
    config->localIp = nullptr;
}

void GetFileName(uint16_t fileId, char *fileName, uint32 fileNameLen)
{
    int rc = snprintf_s(fileName, fileNameLen, MAXPGPATH - 1, "%u", fileId);
    storage_securec_check_ss(rc);
}

DumpToolHelper::DumpToolHelper(DSTORE::StorageType vfsType, char *configPath)
    : m_vfsType(vfsType),
    m_pageStoreConfigPath(nullptr),
    m_tenantConfig{},
    m_reuseVfs(false),
    m_staticVfs(nullptr)
{
    m_tenantConfig = {};
    if (m_vfsType == DSTORE::StorageType::PAGESTORE) {
        m_pageStoreConfigPath = configPath;
    }
}

DumpToolHelper::~DumpToolHelper()
{
    m_staticVfs = nullptr;
    m_pageStoreConfigPath = nullptr;
}

static void GetNetworkAdapterLocalIp(char *ip, Size ipLen)
{
    int rc = memset_s(ip, ipLen, 0, ipLen);
    storage_securec_check(rc, "\0", "\0");

    char hname[HOST_NAME_LEN];
    gethostname(hname, sizeof(hname));
    struct hostent *hent = gethostbyname(hname);
    if (hent == nullptr) {
        (void)fprintf(DumpToolHelper::dumpPrint, "Failed to get host by name\n");
        return;
    }
    char *localIp = inet_ntoa(*(static_cast<struct in_addr*>(static_cast<void *>(hent->h_addr_list[0]))));
    if (localIp == nullptr) {
        (void)fprintf(DumpToolHelper::dumpPrint, "localIp is null.\n");
        return;
    }
    rc = memcpy_s(ip, ipLen, localIp, strlen(localIp));
    storage_securec_check(rc, "\0", "\0");
}

static void InitDefaultCommConfig(DSTORE::TenantConfig *tenantConfig)
{
    /* Init communication config to default value */
    CommunicationConfig *cfg = &tenantConfig->communicationConfig;
    /* auth type */
    cfg->authType = DEFAULT_COMM_AUTH_TYPE;
    /* local ip */
    GetNetworkAdapterLocalIp(cfg->localConfig.localIp, sizeof(cfg->localConfig.localIp));
    /* thread min */
    cfg->dstoreCommThreadMin = DEFAULT_COMM_THRD_MIN;
    /* thread max */
    cfg->dstoreCommThreadMax = DEFAULT_COMM_THRD_MAX;
}

static void UpdateCommConfig(const PageDiagnose::DumpCommConfig *commConfig, DSTORE::TenantConfig *tenantConfig)
{
    /* Update communication config according user's input */
    CommunicationConfig *cfg = &tenantConfig->communicationConfig;
    /* auth type */
    if (commConfig->authType != INVALID_COMM_AUTH_TYPE) {
        cfg->authType = commConfig->authType;
    }
    /* local ip */
    if (commConfig->localIp != nullptr) {
        Size ipLen = sizeof(cfg->localConfig.localIp);
        int rc = memset_s(cfg->localConfig.localIp, ipLen, 0, ipLen);
        storage_securec_check(rc, "\0", "\0");
        rc = memcpy_s(cfg->localConfig.localIp, ipLen, commConfig->localIp, strlen(commConfig->localIp));
        storage_securec_check(rc, "\0", "\0");
    }
    /* thread min */
    if (commConfig->threadMin != INVALID_COMM_THRD_MIN) {
        cfg->dstoreCommThreadMin = commConfig->threadMin;
    }
    /* thread max */
    if (commConfig->threadMax != INVALID_COMM_THRD_MAX) {
        cfg->dstoreCommThreadMax = commConfig->threadMax;
    }
}

static void PrintTenantConfig(DSTORE::TenantConfig *tenantConfig)
{
    (void)fprintf(DumpToolHelper::dumpPrint, "Tenant config information:\n");
    (void)fprintf(DumpToolHelper::dumpPrint, "clusterName: %s, tenantName: %s, tenantId: %d, nodeId: %u.\n",
        tenantConfig->clusterName, tenantConfig->tenantName, tenantConfig->tenantId, tenantConfig->nodeId);

    StorageConfig *storageCfg = &tenantConfig->storageConfig;
    (void)fprintf(DumpToolHelper::dumpPrint, "Storage config information:\n");
    (void)fprintf(DumpToolHelper::dumpPrint,
        "Type: %d, clientLibPath: %s, serverAddresses: %s, serverProtocolType: %s, rootpdbVfsName: %s, "
        "template0VfsName: %s, template1VfsName: %s, votingVfsName: %s, runlogVfsName: %s.\n",
        static_cast<int>(storageCfg->type), storageCfg->clientLibPath, storageCfg->serverAddresses,
        storageCfg->serverProtocolType, storageCfg->rootpdbVfsName, storageCfg->template0VfsName,
        storageCfg->template1VfsName, storageCfg->votingVfsName, storageCfg->runlogVfsName);

    CommunicationConfig *commCfg = &tenantConfig->communicationConfig;
    (void)fprintf(DumpToolHelper::dumpPrint, "Communication config information:\n");
    (void)fprintf(DumpToolHelper::dumpPrint,
        "clusterId: %d, localIp: %s, localPort: %d, commConfigStr: %s, dstoreCommThreadMin: %d, dstoreCommThreadMax: "
        "%d, authType: %u\n",
        commCfg->clusterId, commCfg->localConfig.localIp, commCfg->localConfig.localPort, commCfg->commConfigStr,
        commCfg->dstoreCommThreadMin, commCfg->dstoreCommThreadMax, commCfg->authType);
}

RetStatus DumpToolHelper::Init(DumpToolHelperInitParam *param)
{
    if (param->reuseVfs) {
        m_reuseVfs = true;
        if (param->vfs == nullptr) {
            (void)fprintf(dumpPrint, "Failed to init vfs module, reuse vfs is nullptr.\n");
            return DSTORE_FAIL;
        }
        m_vfsAdapter = param->vfs;
        return DSTORE_SUCC;
    }
    ErrorCode ret = InitVfsModule(nullptr);
    if (unlikely(ret != 0 && ret != VFS_ERROR_VFS_MODULE_ALREADY_INIT)) {
        (void)fprintf(dumpPrint, "Failed to init vfs module. VFS_ERROR = %lld\n", ret);
        return DSTORE_FAIL;
    }
    if (m_vfsType == DSTORE::StorageType::PAGESTORE) {
        /* Init communication config to defaule value */
        InitDefaultCommConfig(&m_tenantConfig);
        /* Get tenant config and update communication config according to json file */
        RetStatus retStatus = TenantConfigInterface::GetTenantConfig(m_pageStoreConfigPath, &m_tenantConfig);
        if (STORAGE_FUNC_FAIL(retStatus)) {
            (void)fprintf(dumpPrint, "Failed to parse tenant config.\n");
            return DSTORE_FAIL;
        }
        /* Update communication config if user specified it */
        UpdateCommConfig(param->commConfig, &m_tenantConfig);

        PrintTenantConfig(&m_tenantConfig);

        /* Here VFS_IO_FENCE_NOT_FILTER_CLIENTID is mainly used for tools like pagedump to link to pagestore,
         * without impact of the gaussdb process */
        m_tenantConfig.nodeId = VFS_IO_FENCE_NOT_FILTER_CLIENTID;
        retStatus = DynamicLinkVFS(&m_tenantConfig, StorageInstanceType::SINGLE, 0, nullptr, false);
        if (STORAGE_FUNC_FAIL(retStatus)) {
            (void)fprintf(dumpPrint, "Failed to get dynamic vfs library.\n");
            return DSTORE_FAIL;
        }

        if (strlen(param->pdbVfsName) == 0) {
            param->pdbVfsName = m_tenantConfig.storageConfig.rootpdbVfsName;
        }
        ret = MountVfs(GetDefaultVfsClientHandle(), m_tenantConfig.tenantName, param->pdbVfsName, &m_staticVfs);
        if (unlikely(ret != 0)) {
            (void)fprintf(dumpPrint, "Failed to mount VFS. VFS_ERROR = %lld\n", ret);
            return DSTORE_FAIL;
        }
    } else if ((m_vfsType == DSTORE::StorageType::LOCAL) || (m_vfsType == DSTORE::StorageType::TENANT_ISOLATION)) {
        PrintTenantConfig(&m_tenantConfig);
        ret = GetStaticLocalVfsInstance(&m_staticVfs);
        if (ret != 0) {
            (void)fprintf(dumpPrint, "Failed to get static linux vfs library. VFS_ERROR = %lld\n", ret);
            return DSTORE_FAIL;
        }
    } else {
        (void)fprintf(dumpPrint, "-t vfsType should be 0 or 1, now is %hhu\n", static_cast<uint8>(m_vfsType));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

void DumpToolHelper::Destroy() noexcept
{
    if (m_reuseVfs) {
        m_staticVfs = nullptr;
        m_pageStoreConfigPath = nullptr;
        return;
    }
    if (m_vfsType == DSTORE::StorageType::PAGESTORE && m_staticVfs != nullptr) {
        ErrorCode errorCode = UnmountVfs(m_staticVfs);
        if (errorCode != 0) {
            (void)fprintf(dumpPrint, "Failed to unmount vfs. VFS_ERROR = %lld\n", errorCode);
            return;
        }

        errorCode = OffloadVfsLib(GetVfsLibHandle());
        if (errorCode != 0) {
            (void)fprintf(dumpPrint, "Failed to OffloadVfsLib. VFS_ERROR = %lld\n", errorCode);
            return;
        }
    }
    m_staticVfs = nullptr;
    ErrorCode ret = ExitVfsModule();
    if (ret != 0) {
        (void)fprintf(dumpPrint, "Failed to exit vfs module. VFS_ERROR = %lld\n", ret);
        return;
    }
    m_pageStoreConfigPath = nullptr;
}

RetStatus DumpToolHelper::Open(const char *fileName, int flags, FileDescriptor **fileDesc)
{
    if (unlikely(fileDesc == nullptr)) {
        return DSTORE_FAIL;
    }

    if (unlikely(*fileDesc != nullptr)) {
        return DSTORE_SUCC;
    }

    if (m_reuseVfs) {
        RetStatus result = m_vfsAdapter->OpenFile(fileName, flags, fileDesc);
        if (unlikely(result != DSTORE_SUCC)) {
            *fileDesc = nullptr;
            (void)fprintf(dumpPrint, "Failed to open file. file name = %s.\n", fileName);
            return DSTORE_FAIL;
        }
        return DSTORE_SUCC;
    }
    ErrorCode vfsRet = ::Open(m_staticVfs, fileName, flags, fileDesc);
    if (unlikely(vfsRet != 0)) {
        *fileDesc = nullptr;
        (void)fprintf(dumpPrint, "Failed to open file. file name = %s. VFS_ERROR = %lld.\n",
                      fileName, vfsRet);
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

void DumpToolHelper::Close(FileDescriptor *fileDesc) noexcept
{
    if (fileDesc == nullptr) {
        return;
    }
    if (m_reuseVfs) {
        RetStatus result = m_vfsAdapter->CloseFile(fileDesc);
        if (unlikely(result != DSTORE_SUCC)) {
            (void)fprintf(dumpPrint, "Failed to close file\n");
        }
        return;
    }
    ErrorCode vfsRet = ::Close(fileDesc);
    if (unlikely(vfsRet != 0)) {
        (void)fprintf(dumpPrint, "Failed to close file, error code is %lld.", vfsRet);
    }
}

RetStatus DumpToolHelper::ReadOffset(FileDescriptor *fileDesc, uint64_t count, int64_t offset, void *outBuffer,
    int64 *readBytes) const
{
    ErrorCode vfsRet = ::Pread(fileDesc, outBuffer, count, offset, readBytes);
    if (unlikely(vfsRet != 0)) {
        (void)fprintf(dumpPrint, "Failed to read file, offset %ld, count %lu, error code is %lld.", offset, count,
            vfsRet);
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus DumpToolHelper::ReadPage(FileDescriptor *fileDesc, const uint32_t pageId, void *outBuffer,
    uint32 bufferSize) const
{
    if (bufferSize != BLCKSZ) {
        (void)fprintf(stderr, "Out buffer size not enough\n");
        return DSTORE_FAIL;
    }
    int64 offset = static_cast<int64>(pageId) * BLCKSZ;
    int64 readBytes = 0;
    if (STORAGE_FUNC_FAIL(ReadOffset(fileDesc, BLCKSZ, offset, outBuffer, &readBytes)) || readBytes != BLCKSZ) {
        (void)fprintf(dumpPrint, "Read page %u failed\n", pageId);
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

int64 DumpToolHelper::Size(FileDescriptor *fileDesc) const
{
    int64 fileSize;
    if (fileDesc == nullptr) {
        (void)fprintf(dumpPrint, "Get file size failed\n");
        return -1;
    }
    ErrorCode vfsRet = ::GetSize(fileDesc, &fileSize);
    if (unlikely(vfsRet != 0)) {
        (void)fprintf(dumpPrint, "Get file size failed, error code is %lld.\n", vfsRet);
        return -1;
    }
    return fileSize;
}

RetStatus DumpToolHelper::FileIsExist(const char *fileName, bool *fileExist) const
{
    if (m_reuseVfs) {
        *fileExist = m_vfsAdapter->FileExists(fileName);
        return DSTORE_SUCC;
    }
    ErrorCode vfsRet = ::FileIsExist(m_staticVfs, fileName, fileExist);
    if (unlikely(vfsRet != 0)) {
        (void)fprintf(dumpPrint, "Failed to judge if file exist or not, error code is %lld.", vfsRet);
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

}  // namespace PageDiagnose
