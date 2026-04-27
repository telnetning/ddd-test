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
 * dstore_systable_wal.h
 *
 * IDENTIFICATION
 *        include/systable/dstore_systable_wal.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_SYSTABLE_WAL_H
#define DSTORE_SYSTABLE_WAL_H

#include "wal/dstore_wal_struct.h"
#include "pdb/dstore_pdb_interface.h"

namespace DSTORE {

struct WalRecordSystable : public WalRecord {
    static void RedoSystableRecord(const WalRecordSystable *systableRecord, PdbId pdbId);
    static void DumpSystableRecord(const WalRecordSystable *systableRecord, FILE *fp);
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordSystable);

struct WalRecordSystableWriteBuiltinRelMap : public WalRecordSystable {
    RelMapType type;
    int count;
    char builtinRelMap[];

    inline void SetHeader(WalType walType, Size walTotalSize, RelMapType typeInput, int countInput)
    {
        SetType(walType);
        SetSize(walTotalSize);
        type = typeInput;
        count = countInput;
    }
    void SetData(char *data, uint32 size);
    void Redo(PdbId pdbId) const;
    void DumpSystable(FILE *fp) const
    {
        (void)fprintf(fp, "type(%s), count(%d).",
            type == RelMapType::RELMAP_SHARED ? "RELMAP_SHARED" : "RELMAP_LOCAL", count);
        RelMapNode *nodes = static_cast<RelMapNode *>(static_cast<void *>(const_cast<char *>(builtinRelMap)));
        for (int i = 0; i < count; i++) {
            (void)fprintf(fp, "relid(%u), segid(%hu, %u).",
                nodes[i].relid, nodes[i].segid.m_fileId, nodes[i].segid.m_blockId);
        }
    }
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordSystableWriteBuiltinRelMap);

}  // namespace DSTORE

#endif
