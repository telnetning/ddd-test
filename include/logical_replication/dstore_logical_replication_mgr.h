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
 * dstore_logical_replication_mgr.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_LOGICAL_REP_MGR_H
#define DSTORE_LOGICAL_REP_MGR_H

#include "framework/dstore_thread.h"
#include "common/dstore_datatype.h"
#include "dstore_logical_replication_slot.h"
#include "dstore_decode_handler.h"
#include "dstore_decode_dict.h"


namespace DSTORE {

#ifdef ENABLE_LOGICAL_REPL
/* tmp, this param will be moved to GUC later. */
constexpr int32 MAX_LOGICAL_SLOT_NUM = 6;

/*
 * 1. Manage logical replication resources;
 * 2. Interacting with recycle modules to ensure all logical-decoding tasks run correctly;
 */
class LogicalReplicaMgr : public BaseObject {
public:
    explicit LogicalReplicaMgr(PdbId pdbId, DstoreMemoryContext memoryContext);
    virtual ~LogicalReplicaMgr();
    DISALLOW_COPY_AND_MOVE(LogicalReplicaMgr);

    virtual RetStatus Init();
    virtual void Destroy();

    /**
     * Create logical replication slot.
     * @param slotName slot name, identity.
     * @param plugin plugin name, identity.
     * @return
     */
    virtual RetStatus CreateLogicalReplicationSlot(char *slotName, char *plugin,
        const std::function<RetStatus(const CommitSeqNo)> syncCatalogCallBack);

    /**
     * Drop logical replication slot.
     * @param slotName slot name, identity.
     * @return
     */
    virtual RetStatus DropLogicalReplicationSlot(char *slotName);

    virtual LogicalReplicationSlot *AcquireLogicalReplicationSlot(char *slotName);

    virtual void ReleaseLogicalReplicationSlot(LogicalReplicationSlot *slot);

    virtual RetStatus AdvanceLogicalReplicationSlot(LogicalReplicationSlot* slot,
        CommitSeqNo uptoCSN, bool needCheck = true);

    virtual void ReportLogicalReplicationSlot(char *name, StringInfo slotInfo);

    /**
     * Create Decoding Context.
     * @param logicalSlot decode for who.
     * @param DecodeOptions decode options.
     * @return create status.
     */
    virtual LogicalDecodeHandler* CreateLogicalDecodeHandler(LogicalReplicationSlot *logicalSlot,
        DecodeOptions *decodeOptions);

    /**
     * Drop Decoding Context.
     * @param logicalSlot decode for who.
     * @return
     */
    virtual void DeleteLogicalDecodeHandler(LogicalDecodeHandler *decodeContext);

    /**
     * Start up logical decode.
     * @param LogicalDecodeHandler start up logical deocde task.
     * @return
     */
    virtual void StartUpLogicalDecode(LogicalDecodeHandler *decodeContext);

    /**
     * Stop logical decode.
     * @param LogicalDecodeHandler stop logical deocde task.
     * @return
     */
    virtual void StopLogicalDecode(LogicalDecodeHandler *decodeContext);

    /**
     * Sync catalogInfo.
     * @param rawCatalog formed by scan systable, to be cached and persistent to decode dict.
     * @return
     */
    RetStatus SyncCatalogToDecodeDict(CatalogInfo *rawCatalog);
    inline PdbId GetPdbId() { return m_pdbId; }
#ifndef UT
protected:
#endif
    RetStatus LoadLogicalReplicationSlotsFromDisk();
    RetStatus DoCreateLogicalReplicationSlot(char *slotName, char *plugin, const WalId walId,
        const std::function<RetStatus(const CommitSeqNo)> syncCatalogCallBack);
    void DoDropLogicalReplicationSlot(LogicalReplicationSlot *slot);
    void FlushDependentMinPlsn();
    void FlushDependentMinCatalogCsn();
    void FlushDependentMinDecodeDictCsn();
    void FlushSlotsRelatedLimit();

    PdbId m_pdbId;
    DstoreMemoryContext m_memoryContext;
    DecodeDict *m_decodeDict;

    /* local variables used for block corresponding recycle */
    WalPlsn m_slotsPlsnMin;
    CommitSeqNo m_slotsCatalogCsnMin;
    CommitSeqNo m_slotsDecodeDictCsnMin;

    /* logical replication resource manage */
    LogicalReplicationSlot m_logicalSlotArray[MAX_LOGICAL_SLOT_NUM];
    LogicalDecodeHandler *m_logicalDecodedCtx[MAX_LOGICAL_SLOT_NUM];
    LWLock m_slotCtlLock;

private:
    /* logical replication slots creation/drop lock on single node */
    LWLock m_slotAllocLock;
};
#endif

}

#endif