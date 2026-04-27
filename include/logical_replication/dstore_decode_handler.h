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
 * dstore_logical_decode_handler.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_LOGICAL_DECODE_HANDLER_H
#define DSTORE_LOGICAL_DECODE_HANDLER_H

#include "wal/dstore_wal.h"
#include "common/dstore_datatype.h"
#include "common/dstore_common_utils.h"
#include "dstore_logical_replication_slot.h"
#include "dstore_wal_dispatcher.h"
#include "logical_replication/dstore_logical_replication_struct.h"

namespace DSTORE {

#ifdef ENABLE_LOGICAL_REPL
#ifndef UT
constexpr int STREAM_DECODE_ADDVANCE_TIME_INTERVAL = 3; /* s */
#else
constexpr int STREAM_DECODE_ADDVANCE_TIME_INTERVAL = 1; /* s */
#endif

class LogicalDecodeHandler : public BaseObject {
public:
    LogicalDecodeHandler(LogicalReplicationSlot *logicalSlot, DecodeOptions *decodeOptions, DecodeDict *decodeDict,
                         DstoreMemoryContext mctx, PdbId pdbId);

    virtual ~LogicalDecodeHandler();
    DISALLOW_COPY_AND_MOVE(LogicalDecodeHandler);

    virtual RetStatus Init();
    virtual void Destroy();

    /* startup logical decode */
    virtual void StartUp();
    virtual void Stop();

    int GetDecodeSlotId() const;

    /* interface of get all logical log by this decode task. */
    virtual TrxLogicalLog *GetNextTrxLogicalLog();
    /* interface of pop logical log by this decoder */
    virtual void ConfirmTrxLogicalLog(TrxLogicalLog *trxLog);

    virtual PdbId GetPdbId() { return m_pdbId; }
#ifndef UT
protected:
#endif
    virtual void DoLogicalSlotAdvance();
    bool DecodeToLimit();
    bool IsStreamDecode();
    void ClearParallelWorkers(int clearNums);

    LogicalReplicationSlot *m_logicalSlot;
    /* decode options */
    DecodeOptions m_decodeOptions;
    DecodePlugin *m_decodePlugin;
    DecodeDict *m_decodeDict;
    DstoreMemoryContext m_memoryContext;

    DstoreSpinLock m_statusLock;
    /* 1 wal dispatcher */
    WalDispatcher *m_walDispatcher;
    /* n decoders */
    ParallelDecodeWorker *m_parallelDecodeWorker[MAX_DECODE_WORKER_NUM];

    /* used for logical log sender to fetch logical log through polling */
    int m_curFetchLogCursor;
    int m_fetchedChanges;
    CommitSeqNo m_fetchedCsn;

    /* client return */
    CommitSeqNo m_lastConfirmedCsn;
    WalPlsn m_lastConfirmedPlsn;

    /* last advance time */
    TimestampTz m_lastAdvanceTime;
    /* which pdb do i belong to */
    PdbId m_pdbId;
};

#endif
}  // namespace DSTORE
#endif
