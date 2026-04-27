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
 * logical_replication_struct.h
 *
 *
 *
 * IDENTIFICATION
 *       logical_replication_struct.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef LOGICAL_REPLICATION_STRUCT
#define LOGICAL_REPLICATION_STRUCT

#include "tuple/dstore_tuple_struct.h"
#include "systable/systable_relation.h"
#include "systable/systable_attribute.h"
#include "errorcode/dstore_logical_replication_error_code.h"
#include "common/dstore_common_utils.h"

namespace DSTORE {

constexpr int INIT_ROW_PER_TRX = 30;
constexpr int32_t INVALID_WORKER_ID = -1;
constexpr int32_t INVALID_NCHANGES = 0;
struct StringInfoData;
struct DecodeTableInfo;

/* catalog */
struct CatalogInfo {
    CommitSeqNo csn;
    Oid tableOid;
    int natts;
    Form_pg_class sysRel;
    Form_pg_attribute *sysAttr;
    Oid nspOid;
    char *nspName;
    PdbId pdbId;
    DecodeTableInfo *ConvertToDecodeTableInfo();
};

#pragma GCC visibility push(default)
/* the type of logical log per row/sql */
enum class RowLogicalLogType : uint8_t {
    DML = 1,
    COMMIT = 2,
    ABORT = 3,
    EMPTY = 4,
    /* unused now */
    DDL = 5,
    WAL_NEXT_CSN = 6,
    WAL_BARRIER_CSN = 7,
};
/* the format of logical log, per row */
struct RowLogicalLog {
    WalPlsn rowStartPlsn;
    RowLogicalLogType type;
    StringInfoData *out;

    void Dump(StringInfoData *dumpInfo);
};

/* Output logcial log, per trx */
struct TrxLogicalLog {
    int maxRows;
    NodeId nodeId;
    int workerId;
    uint64_t xid;
    WalPlsn startPlsn;
    WalPlsn endPlsn;
    WalPlsn commitPlsn;
    WalPlsn restartDecodingPlsn; /* this is used for plsn advance */
    CommitSeqNo commitCsn;
    int nRows;
    RowLogicalLog **rowArray;

    void Init();
    void FreeLogicalLog();
    RetStatus AddRowLogicalLog(RowLogicalLog *rowLog);
    void Send(StringInfoData *sendBuf);
    void Dump(StringInfoData *dumpInfo);
};
#pragma GCC visibility pop
using LogicalLogOutputWriteCallback = void(*)(struct TrxLogicalLog *);

struct DecodeOptions {
    bool includeXidsFlag;
    bool includeTimeStampFlag;  /* not surpport now */
    bool skipEmptyXactsFlag;
    bool skipAttrNullsFlag;
    bool advanceSlotFlag;
    int parallelDecodeWorkerNum;
    CommitSeqNo uptoCSN;
    int32_t uptoNchanges;
    LogicalLogOutputWriteCallback outputWriteCb;
};


} /* namespace DSTORE */
#endif
