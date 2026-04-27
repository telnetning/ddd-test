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

#include "diagnose/dstore_bg_page_writer_diagnose.h"
#include "framework/dstore_instance.h"
#include "common/algorithm/dstore_string_info.h"
#include "buffer/dstore_bg_page_writer_mgr.h"
#include "buffer/dstore_bg_disk_page_writer.h"

namespace DSTORE {

BgPageWriterDiagnose::BgPageWriterDiagnose(uint32_t pdbId)
{
    StoragePdb *storagePdb = g_storageInstance->GetPdb(pdbId);
    StorageReleasePanic(storagePdb == nullptr, MODULE_BUFFER, ErrMsg("pdb %u is nullptr", pdbId));
    if (!storagePdb->IsInit()) {
        ErrLog(DSTORE_PANIC, MODULE_BUFFER, ErrMsg("StoragePdb is not initialized."));
    }
    m_bgPageWriterMgr = storagePdb->GetBgPageWriterMgr();
}
char *BgPageWriterDiagnose::GetBgPageWriterSummaryInfo()
{
    return m_bgPageWriterMgr->DumpSummaryInfo();
}
bool BgPageWriterDiagnose::Init(void *bgPageWriter)
{
    m_bgPageWriter = (BgDiskPageMasterWriter *)bgPageWriter;

    return m_bgPageWriterMgr->IsContains(m_bgPageWriter);
}

bool BgPageWriterDiagnose::Init(const uint32 slotId)
{
    m_bgPageWriter = m_bgPageWriterMgr->GetBgPageWriterBySlot(slotId);
    return m_bgPageWriter != nullptr;
}

char *BgPageWriterDiagnose::GetDirtyPageInfo()
{
    return m_bgPageWriter->Dump();
}

Size BgPageWriterDiagnose::GetBgPageWriterArraySize() const
{
    return m_bgPageWriterMgr->GetBgPageWriterArraySize();
}

uint32 BgPageWriterDiagnose::GetAioCompleterInfo(AioCompleterInfo **aioCompleterInfo)
{
    return m_bgPageWriterMgr->GetFlushInfo(aioCompleterInfo);
}

uint32 BgPageWriterDiagnose::GetAioSlotUsageInfo(AioSlotUsageInfo **aioSlotUsageInfo)
{
    return m_bgPageWriterMgr->GetSlotUsageInfo(aioSlotUsageInfo);
}

uint32 BgPageWriterDiagnose::GetPagewriterInfo(PagewriterInfo **pagewriterInfo)
{
    return m_bgPageWriterMgr->GetPagewriterInfo(pagewriterInfo);
}

void BgPageWriterDiagnose::FreeAioCompleterInfoArr(AioCompleterInfo *infos)
{
    m_bgPageWriterMgr->FreeAioCompleterInfoArr(infos);
}

void BgPageWriterDiagnose::FreeAioSlotUsageInfoArr(AioSlotUsageInfo *infos)
{
    m_bgPageWriterMgr->FreeAioSlotUsageInfoArr(infos);
}

void BgPageWriterDiagnose::FreePagewriterInfoArr(PagewriterInfo *infos)
{
    m_bgPageWriterMgr->FreePagewriterInfoArr(infos);
}

}  // namespace DSTORE
