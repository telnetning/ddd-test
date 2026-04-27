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

#ifndef DSTORE_BG_PAGE_WRITER_DIAGNOSE
#define DSTORE_BG_PAGE_WRITER_DIAGNOSE

#include <cstddef>
#include <cstdint>
#include "common/dstore_common_utils.h"
namespace DSTORE {

#pragma GCC visibility push(default)
class BgPageWriterMgr;
class BgDiskPageMasterWriter;
struct AioCompleterInfo;
struct AioSlotUsageInfo;
struct PagewriterInfo;
class BgPageWriterDiagnose {
public:
    explicit BgPageWriterDiagnose(uint32_t pdbId);
    ~BgPageWriterDiagnose() = default;
    /**
     *  Get the array info of the bg page writer mgr.
     */
    char *GetBgPageWriterSummaryInfo();

    bool Init(void *bgPageWriter);
    bool Init(uint32_t slotId);

    /**
     *  Get the bg page writer info.
     */
    char* GetDirtyPageInfo();

    /*
     * Get the bg page writer array size.
     */
    size_t GetBgPageWriterArraySize() const;

    uint32_t GetAioCompleterInfo(AioCompleterInfo **aioCompleterInfo);

    uint32_t GetAioSlotUsageInfo(AioSlotUsageInfo **aioSlotUsageInfo);

    uint32_t GetPagewriterInfo(PagewriterInfo **pagewriterInfo);

    uint32_t GetRemoteAioSlotUsageInfo(AioSlotUsageInfo **aioSlotUsageInfo, uint32_t pdbId);

    void FreeAioCompleterInfoArr(AioCompleterInfo *infos);

    void FreeAioSlotUsageInfoArr(AioSlotUsageInfo *infos);

    void FreePagewriterInfoArr(PagewriterInfo *infos);

private:
    BgPageWriterMgr *m_bgPageWriterMgr = nullptr;
    BgDiskPageMasterWriter *m_bgPageWriter = nullptr;
};
#pragma GCC visibility pop
}  // namespace DSTORE

#endif
