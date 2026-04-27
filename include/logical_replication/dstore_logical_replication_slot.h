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
 * dstore_logical_replication_slot.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_LOGICAL_REPLICATION_SLOT_H
#define DSTORE_LOGICAL_REPLICATION_SLOT_H

#include "wal/dstore_wal.h"
#include "common/dstore_datatype.h"
#include "common/dstore_common_utils.h"

#ifdef UT
#ifndef ENABLE_LOGICAL_REPL
/* LOGICAL REPL is not supported for now. */
#define ENABLE_LOGICAL_REPL
#endif
#endif

namespace DSTORE {

#ifdef ENABLE_LOGICAL_REPL
constexpr int INVALID_SLOT_ID = -1; /* slotId is InValid */

/*
 * Status of replication slot
 * consistent means OK to start logical replication
 */
enum class StartPointState : int {
    DEFAULT,
    WAIT_ACTIVE_TRX_FINISH,
    WAIT_FULL_SNAPSHOT,
    CONSISTENT,
    WAITING_SYNC_CATALOG
};

class LogicalReplicationSlot : public BaseObject {
public:
    LogicalReplicationSlot();
    ~LogicalReplicationSlot() = default;
    void Init(PdbId pdbId, char *slotName, char *pluginName, WalId walId, int slotId);
    void Destroy();
    void Reset();
    void SerializeToDisk(bool isCreate);
    void RestoreFromDisk(const ControlLogicalReplicationSlotPageItemData item);
    RetStatus FindStartPoint(const std::function<RetStatus(const CommitSeqNo)> syncCatalogCallBack);

    inline bool IsInUse() const
    {
        return m_inUse;
    }

    inline void SetUse(bool inUse)
    {
        /* lock outside to change this flag */
        m_inUse = inUse;
    }

    inline bool IsActive() const
    {
        return m_isActive;
    }

    inline void SetActive(bool isActive)
    {
        m_isActive = isActive;
    }

    inline char *GetName()
    {
        return m_name.data;
    }

    inline void SetName(const char *slotName)
    {
        errno_t rc = strncpy_s(m_name.data, NAME_DATA_LEN, slotName, NAME_DATA_LEN);
        storage_securec_check(rc, "\0", "\0");
    }

    inline bool IsNameEqual(char *slotName) const
    {
        return strcmp(slotName, m_name.data) == 0;
    }

    inline char *GetPluginName()
    {
        return m_plugin.data;
    }

    inline void SetPluginName(const char *pluginName)
    {
        errno_t rc = strncpy_s(m_plugin.data, NAME_DATA_LEN, pluginName, NAME_DATA_LEN);
        storage_securec_check(rc, "\0", "\0");
    }

    inline WalId GetWalId() const
    {
        return m_walId;
    }

    inline void SetWalId(WalId walId)
    {
        m_walId = walId;
    }

    inline int GetSlotId() const
    {
        return m_slotId;
    }

    inline void SetSlotId(int slotId)
    {
        m_slotId = slotId;
    }

    inline CommitSeqNo GetCatalogCsnMin() const
    {
        return m_catalogCsnMin;
    }

    inline void SetCatalogCsnMin(CommitSeqNo catalogCsnMin)
    {
        m_catalogCsnMin = catalogCsnMin;
    }

    inline StartPointState GetStartPointStatus()
    {
        return m_state;
    }

    inline void SetStartPointStatus(StartPointState state)
    {
        m_state = state;
    }

    inline CommitSeqNo GetConfirmCsn()
    {
        m_slotLock.Acquire();
        CommitSeqNo confirmCsn = m_confirmedCsn;
        m_slotLock.Release();
        return confirmCsn;
    }

    inline void AdvanceConfirmCsn(CommitSeqNo toCsn)
    {
        m_slotLock.Acquire();
        m_confirmedCsn = toCsn;
        m_slotLock.Release();
    }

    inline WalPlsn GetRestartPlsn()
    {
        m_slotLock.Acquire();
        WalPlsn restartPlsn = m_restartPlsn;
        m_slotLock.Release();
        return restartPlsn;
    }

    inline void AdvanceRestartPlsn(WalPlsn toPlsn)
    {
        m_slotLock.Acquire();
        m_restartPlsn = toPlsn;
        m_slotLock.Release();
    }

    inline CommitSeqNo GetDecodeDictCsnMin()
    {
        m_slotLock.Acquire();
        CommitSeqNo decodeDictCsnMin = m_decodeDictCsnMin;
        m_slotLock.Release();
        return decodeDictCsnMin;
    }

    inline void AdvanceDecodeDictCsnMin(CommitSeqNo toCsn)
    {
        m_slotLock.Acquire();
        m_decodeDictCsnMin = toCsn;
        m_slotLock.Release();
    }

    inline void AdvanceAndSerialize(CommitSeqNo toCsn, WalPlsn toPlsn)
    {
        m_slotLock.Acquire();
        m_confirmedCsn = toCsn;
        m_restartPlsn = toPlsn;
        SerializeToDisk(false);
        m_slotLock.Release();
    }

    inline PdbId GetPdbId()
    {
        StorageReleasePanic(m_pdbId == INVALID_PDB_ID, MODULE_LOGICAL_REPLICATION,
            ErrMsg("logical replication slot can not be inavlid."));
        return m_pdbId;
    }
private:
    RetStatus SyncCatalog(const std::function<RetStatus(const CommitSeqNo)> syncCatalogCallBack);
    static void PutAllActiveTrxXidToHTAB(Xid xid, void *arg);

    /* slot name */
    DstoreNameData m_name;
    /* slot works to */
    WalId m_walId;
    /* plugin name */
    DstoreNameData m_plugin;
    /* oldest PLSN that might be required by this logical replication slot */
    WalPlsn m_restartPlsn;
    /* minimal catalog version that might be required by this logical replication slot */
    CommitSeqNo m_catalogCsnMin;
    /* decode dict version that might be required by this logical replication slot */
    CommitSeqNo m_decodeDictCsnMin;
    /* oldest CSN that has been acked receipt for */
    CommitSeqNo m_confirmedCsn;
    /* logical replication slot state, only CONSISTENT can be started logical decode */
    StartPointState m_state;
    /* is this slot defined and serialized */
    bool m_inUse;
    /* is somebody streaming out changes for this slot */
    bool m_isActive;
    /* used internal, as the index of this slot in slotsArray */
    int m_slotId;
    DstoreSpinLock m_slotLock;
    PdbId m_pdbId;
};

#endif
}  // namespace DSTORE
#endif
