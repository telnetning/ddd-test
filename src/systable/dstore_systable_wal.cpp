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
 * dstore_systable_wal.cpp
 *
 * IDENTIFICATION
 *        storage/src/systable/dstore_systable_wal.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include <functional>
#include <utility>
#include "framework/dstore_instance.h"
#include "framework/dstore_pdb.h"
#include "systable/dstore_systable_wal.h"

namespace DSTORE {

struct SystableWalRedoItem {
    WalType type;
    std::function<void(const WalRecordSystable*, PdbId)> redo;

    SystableWalRedoItem(WalType walType, std::function<void(const WalRecordSystable *, PdbId)> redoFunc) noexcept
        : type(walType), redo(std::move(redoFunc))
    {}
};

static const SystableWalRedoItem SYSTABLE_WAL_REDO_TABLE[] {
    {WAL_SYSTABLE_WRITE_BUILTIN_RELMAP, [](const WalRecordSystable *self, PdbId pdbId)
        { (static_cast<const WalRecordSystableWriteBuiltinRelMap *>(self))->Redo(pdbId); }}
};

void WalRecordSystable::RedoSystableRecord(const WalRecordSystable *systableRecord, PdbId pdbId)
{
    StorageAssert(systableRecord != nullptr);

    WalType recordType = systableRecord->m_type;
    for (uint32 i = 0; i < sizeof(SYSTABLE_WAL_REDO_TABLE) / sizeof(SYSTABLE_WAL_REDO_TABLE[0]); ++i) {
        if (SYSTABLE_WAL_REDO_TABLE[i].type == recordType) {
            SYSTABLE_WAL_REDO_TABLE[i].redo(systableRecord, pdbId);
        }
    }
}

void WalRecordSystableWriteBuiltinRelMap::SetData(char *data, uint32 size)
{
    errno_t rc =
        memcpy_s(builtinRelMap, static_cast<uint32>(m_size - sizeof(WalRecordSystableWriteBuiltinRelMap)), data, size);
    storage_securec_check(rc, "\0", "\0");
}

void WalRecordSystableWriteBuiltinRelMap::Redo(PdbId pdbId) const
{
    StoragePdb *pdb = g_storageInstance->GetPdb(pdbId);
    StorageReleasePanic(pdb == nullptr, MODULE_FRAMEWORK, ErrMsg("WAL_SYSTABLE_WRITE_BUILTIN_RELMAP: pdb is null."));

    RelMapNode *nodes = static_cast<RelMapNode *>(static_cast<void *>(const_cast<char *>(builtinRelMap)));
    if (STORAGE_FUNC_FAIL(pdb->WriteBuiltinRelMapRedo(type, nodes, count))) {
        ErrLog(DSTORE_PANIC, MODULE_FRAMEWORK, ErrMsg("WAL_SYSTABLE_WRITE_BUILTIN_RELMAP: "
            "write builtin relmap failed. PdbId is:%d.", pdbId));
    }
}

struct SystableWalDumpItem {
    WalType type;
    std::function<void(const WalRecordSystable*, FILE *fp)> dump;

    SystableWalDumpItem(WalType walType, std::function<void(const WalRecordSystable *, FILE *fp)> dumpFunc) noexcept
        : type(walType), dump(std::move(dumpFunc))
    {}
};

static const SystableWalDumpItem SYSTABLE_WAL_DUMP_TABLE[] {
    {WAL_SYSTABLE_WRITE_BUILTIN_RELMAP, [](const WalRecordSystable *self, FILE *fp)
        { (static_cast<const WalRecordSystableWriteBuiltinRelMap *>(self))->DumpSystable(fp); }}
};

void WalRecordSystable::DumpSystableRecord(const WalRecordSystable *systableRecord, FILE *fp)
{
    StorageAssert(systableRecord != nullptr);
    WalType recordType = systableRecord->m_type;
    for (uint32 i = 0; i < sizeof(SYSTABLE_WAL_DUMP_TABLE) / sizeof(SYSTABLE_WAL_DUMP_TABLE[0]); ++i) {
        if (SYSTABLE_WAL_DUMP_TABLE[i].type == recordType) {
            SYSTABLE_WAL_DUMP_TABLE[i].dump(systableRecord, fp);
        }
    }
}

} /* namespace DSTORE */
