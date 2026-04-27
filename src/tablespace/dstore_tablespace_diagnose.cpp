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
 * dstore_tablespace_diagnose.cpp
 *
 * IDENTIFICATION
 *        src/tablespace/dstore_tablespace_diagnose.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "buffer/dstore_buf_mgr.h"
#include "tablespace/dstore_tablespace.h"
#include "tablespace/dstore_heap_normal_segment.h"
#include "tablespace/dstore_index_normal_segment.h"
#include "tablespace/dstore_index_temp_segment.h"
#include "tablespace/dstore_heap_temp_segment.h"
#include "lock/dstore_lock_interface.h"
#include "diagnose/dstore_tablespace_diagnose.h"

namespace DSTORE {

uint64 TableSpaceDiagnose::GetSegmentSize(PdbId pdbId, TablespaceId tablespaceId, const PageId& segmentId)
{
    BufMgrInterface *bufMgr = (tablespaceId == static_cast<TablespaceId>(TBS_ID::TEMP_TABLE_SPACE_ID)) ?
        thrd->GetTmpLocalBufMgr() : g_storageInstance->GetBufferMgr();
    uint64 segmentSize = 0;
    if (tablespaceId == INVALID_TABLESPACE_ID) {
        return segmentSize;
    } else {
        BufferDesc *segMetaBufferDesc = bufMgr->Read(pdbId, segmentId, LW_SHARED);
        if (unlikely(segMetaBufferDesc == INVALID_BUFFER_DESC)) {
            return segmentSize;
        }
        SegmentMetaPage *segmentMetaPage = static_cast<SegmentMetaPage *>(segMetaBufferDesc->GetPage());
        SegmentType type = segmentMetaPage->segmentHeader.segmentType;
        bufMgr->UnlockAndRelease(segMetaBufferDesc);
        uint64 blockCount = 0;
        switch (type) {
            case SegmentType::INDEX_SEGMENT_TYPE: {
                IndexNormalSegment inSegment(pdbId, segmentId, tablespaceId, bufMgr);
                blockCount = inSegment.GetDataBlockCount();
                break;
            }
            case SegmentType::HEAP_SEGMENT_TYPE: {
                HeapNormalSegment hnSegment(pdbId, segmentId, tablespaceId, bufMgr);
                blockCount = hnSegment.GetDataBlockCount();
                break;
            }
            case SegmentType::INDEX_TEMP_SEGMENT_TYPE: {
                IndexTempSegment itSegment(pdbId, segmentId, tablespaceId, bufMgr);
                blockCount = itSegment.GetDataBlockCount();
                break;
            }
            case SegmentType::HEAP_TEMP_SEGMENT_TYPE: {
                HeapTempSegment htSegment(pdbId, segmentId, tablespaceId, bufMgr);
                blockCount = htSegment.GetDataBlockCount();
                break;
            }
            default :
                ErrLog(DSTORE_PANIC, MODULE_SEGMENT, ErrMsg("Invalid segmentType %u", static_cast<uint32_t>(type)));
                break;
        }
        segmentSize = blockCount * BLCKSZ;
    }
    return segmentSize;
}

ExtentsScanner *TableSpaceDiagnose::ScanExtentsBegin(PdbId pdbId, const PageId &segmentId)
{
    if (!StoragePdb::IsValidPdbId(pdbId) || segmentId.IsInvalid()) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
               ErrMsg("Invalid input, pdbid:%u, segmenId(%hu, %u)!", pdbId, segmentId.m_fileId, segmentId.m_blockId));
        return nullptr;
    }

    StoragePdb *pdb = g_storageInstance->GetPdb(pdbId);
    if (STORAGE_VAR_NULL(pdb)) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("Failed to get pdb %u.", pdbId));
        return nullptr;
    }
    char fileName[MAXPGPATH] = {0};
    VFSAdapter *vfs = pdb->m_vfs;
    if (STORAGE_VAR_NULL(vfs)) {
        return nullptr;
    }
    if (STORAGE_FUNC_FAIL(vfs->GetFileNameFromFileId(segmentId.m_fileId, fileName))) {
        return nullptr;
    }
    if (!vfs->FileExists(segmentId.m_fileId, fileName)) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("File(%hu) not exist.", segmentId.m_fileId));
        return nullptr;
    }

    TablespaceMgr *tblsMgr = pdb->GetTablespaceMgr();
    if (STORAGE_VAR_NULL(tblsMgr)) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("Failed to get tablespace mgr on pdb %d.", pdbId));
        return nullptr;
    }

    TbsDataFile *datafile = tblsMgr->AcquireDatafile(segmentId.m_fileId, LW_SHARED);
    if (STORAGE_VAR_NULL(datafile)) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("Load datafile failed, fileid:%hu.", segmentId.m_fileId));
        return nullptr;
    }
    TablespaceId tblsId = datafile->GetDataFilePageItemData().tablespaceId;
    tblsMgr->ReleaseDatafileLock(datafile);
    if (tblsId == INVALID_TABLESPACE_ID) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("Invalid tablespace id, fileid:%hu", segmentId.m_fileId));
        return nullptr;
    }
    BufMgrInterface *bufMgr = (tblsId == static_cast<TablespaceId>(TBS_ID::TEMP_TABLE_SPACE_ID)) ?
        thrd->GetTmpLocalBufMgr() : g_storageInstance->GetBufferMgr();

    ExtentsScanner *scanner = DstoreNew(g_dstoreCurrentMemoryContext) ExtentsScanner(pdbId, segmentId, bufMgr);
    if (STORAGE_VAR_NULL(scanner)) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("Failed to new scanner!"));
        return scanner;
    }
    if (!scanner->CheckSegmentMeta()) {
        delete scanner;
        scanner = nullptr;
    }
    return scanner;
}

bool TableSpaceDiagnose::ScanExtentsNext(ExtentsScanner *scanner)
{
    if (STORAGE_VAR_NULL(scanner)) {
        return false;
    }
    return scanner->Next();
}

PageId TableSpaceDiagnose::GetExtentMeta(ExtentsScanner *scanner)
{
    if (STORAGE_VAR_NULL(scanner)) {
        return INVALID_PAGE_ID;
    }
    return scanner->GetExtMetaPageId();
}

uint16_t TableSpaceDiagnose::GetExtentSize(ExtentsScanner *scanner)
{
    if (STORAGE_VAR_NULL(scanner)) {
        return 0;
    }
    return scanner->GetExtSize();
}

void TableSpaceDiagnose::ScanExtentsEnd(ExtentsScanner *scanner)
{
    delete scanner;
}

void TableSpaceDiagnose::GetSegmentMetaInfo(PdbId pdbId, const PageId& segmentId,
    bool* isValid, DSTORE::SegmentInfo* segmentInfo)
{
    TablespaceId tablespaceId = 0;
    if (!StoragePdb::IsValidPdbId(pdbId) || segmentId.IsInvalid()) {
        *isValid = false;
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
            ErrMsg("Invalid para on GetSegmentMetaInfo. pdb(%u) segmentId(%hu, %u)", pdbId,
            segmentId.m_fileId, segmentId.m_blockId));
        return;
    }

    TablespaceMgr *tablespaceMgr = g_storageInstance->GetTablespaceMgrByPdbId(pdbId);
    if (STORAGE_VAR_NULL(tablespaceMgr)) {
        *isValid = false;
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
            ErrMsg("Failed to get tablespace mgr on pdb %d.", pdbId));
        return;
    }

    TbsDataFile *datafile = tablespaceMgr->AcquireDatafile(segmentId.m_fileId, LW_SHARED);
    if (STORAGE_VAR_NULL(datafile)) {
        *isValid = false;
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Load datafile failed, fileId is %hu.", segmentId.m_fileId));
        return;
    }
    tablespaceId = datafile->GetDataFilePageItemData().tablespaceId;
    tablespaceMgr->ReleaseDatafileLock(datafile);

    BufMgrInterface *bufMgr = (tablespaceId == static_cast<TablespaceId>(TBS_ID::TEMP_TABLE_SPACE_ID)) ?
        thrd->GetTmpLocalBufMgr() : g_storageInstance->GetBufferMgr();

    TableSpace *tablespace = tablespaceMgr->OpenTablespace(tablespaceId, DSTORE::DSTORE_ACCESS_SHARE_LOCK);
    if (tablespace == nullptr) {
        *isValid = false;
        tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_ACCESS_SHARE_LOCK);
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("Get segment info for Pdb(%u) tablespace(%hu) pageId(%hu, %u):"
            " no such tablespace.", pdbId, tablespaceId, segmentId.m_fileId, segmentId.m_blockId));
        return;
    } else {
        BufferDesc *segMetaBufferDesc = bufMgr->Read(pdbId, segmentId, LW_SHARED);
        if (unlikely(segMetaBufferDesc == INVALID_BUFFER_DESC)) {
            *isValid = false;
            tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_ACCESS_SHARE_LOCK);
            ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("Get segment info for Pdb(%u) tablespace(%hu) pageId(%hu, %u):"
                " no such segmentId.", pdbId, tablespaceId, segmentId.m_fileId, segmentId.m_blockId));
            return;
        }

        SegmentMetaPage *segmentMetaPage = static_cast<SegmentMetaPage *>(segMetaBufferDesc->GetPage());
        SegmentType type = segmentMetaPage->segmentHeader.segmentType;
        uint64 segMetaMagic = segmentMetaPage->extentMeta.magic;
        PageId pageId = segmentMetaPage->GetSelfPageId();
        if (!((segMetaMagic == SEGMENT_META_MAGIC) && (type >= SegmentType::HEAP_SEGMENT_TYPE) &&
            (type <= SegmentType::INDEX_TEMP_SEGMENT_TYPE) && (pageId == segmentId))) {
            *isValid = false;
            bufMgr->UnlockAndRelease(segMetaBufferDesc);
            tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_ACCESS_SHARE_LOCK);
            ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("Get segment info for Pdb(%u) tablespace(%hu) pageId(%hu, %u):"
                " invalid segmentId, page id(%hu, %u), page type(%hhu).",
                pdbId, tablespaceId, segmentId.m_fileId, segmentId.m_blockId,
                pageId.m_fileId, pageId.m_blockId, static_cast<uint8>(type)));
            return;
        }

        *isValid = true;
        segmentInfo->segmentType = static_cast<uint64_t>(segmentMetaPage->segmentHeader.segmentType);
        segmentInfo->totalBlocks = static_cast<uint64_t>(segmentMetaPage->segmentHeader.totalBlockCount);
        segmentInfo->totalExtents = static_cast<uint64_t>(segmentMetaPage->segmentHeader.extents.count);
        segmentInfo->plsn = static_cast<uint64_t>(segmentMetaPage->segmentHeader.plsn);
        segmentInfo->glsn = static_cast<uint64_t>(segmentMetaPage->segmentHeader.glsn);
        bufMgr->UnlockAndRelease(segMetaBufferDesc);
        tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_ACCESS_SHARE_LOCK);
    }
    return;
}
}
