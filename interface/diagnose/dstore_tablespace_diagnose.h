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
 * dstore_tablespace_diagnose.h
 *
 * IDENTIFICATION
 *        interface/diagnose/dstore_tablespace_diagnose.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_TABLESPACE_DIAGNOSE
#define DSTORE_TABLESPACE_DIAGNOSE

#include "common/dstore_common_utils.h"
#include "systable/dstore_relation.h"
#include "page/dstore_page_struct.h"

namespace DSTORE {
class ExtentsScanner;
struct SegmentInfo {
    uint64_t segmentType;
    uint64_t totalBlocks;
    uint64_t totalExtents;
    uint64_t plsn;
    uint64_t glsn;
};
#pragma GCC visibility push(default)
class TableSpaceDiagnose {
public:

    static uint64_t GetSegmentSize(PdbId pdbId, TablespaceId tablespaceId, const PageId &segmentId);

    static ExtentsScanner *ScanExtentsBegin(PdbId pdbId, const PageId &segmentId);
    static bool ScanExtentsNext(ExtentsScanner *scanner);
    static PageId GetExtentMeta(ExtentsScanner *scanner);
    static uint16_t GetExtentSize(ExtentsScanner *scanner);
    static void ScanExtentsEnd(ExtentsScanner *scanner);

    static void GetSegmentMetaInfo(PdbId pdbId, const PageId& segmentId,
        bool* isValid, DSTORE::SegmentInfo* segmentInfo);
};

#pragma GCC visibility pop
}  // namespace DSTORE

#endif
