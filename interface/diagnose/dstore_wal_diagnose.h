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
 * dstore_wal_diagnose.h
 *
 * Description:
 * Provide diagnose function for Wal perf and status
 * ---------------------------------------------------------------------------------------
 *
 */
#ifndef DSTORE_DSTORE_WAL_DIAGNOSE_H
#define DSTORE_DSTORE_WAL_DIAGNOSE_H
#include <cstdint>
#include "common/dstore_common_utils.h"

namespace DSTORE {
#pragma GCC visibility push(default)
constexpr uint16_t MAX_STATE_INFO_LEN = 32;
constexpr uint16_t MAX_WAL_FILE_NAME_LEN = 64;
struct WalRecoveryInfo {
    PdbId pdbId;
    WalId walId;
    uint64_t term;
    uint64_t recoveryStartPlsn;
    uint64_t recoveryEndPlsn;
    uint64_t lastGroupEndPlsn;
    const char *stage;
    long dirtyPageEntryArraySize;
    const char *redoMode;
    bool isDiskCkpt;
};

struct WalRedoInfo {
    uint64_t progress;
    uint64_t recovery_start_plsn;
    uint64_t recovery_end_plsn;
    uint64_t curr_redo_plsn;
    uint64_t redo_speed;

    void Init();
};

struct WalStreamStateInfo {
    WalId walId;
    PdbId pdbId;
    uint32_t nodeId;
    char usage[MAX_STATE_INFO_LEN];
    char state[MAX_STATE_INFO_LEN];
    uint16_t walFileCount;
    char headFileName[MAX_WAL_FILE_NAME_LEN];
    char tailFileName[MAX_WAL_FILE_NAME_LEN];
    uint64_t maxAppendedPlsn;
    uint64_t maxWrittenToFilePlsn;
    uint64_t maxFlushFinishPlsn;
    uint64_t term;
    char redoMode[MAX_STATE_INFO_LEN];
    uint64_t redoStartPlsn;
    uint64_t redoFinishedPlsn;
    char redoStage[MAX_STATE_INFO_LEN];
    uint64_t redoDonePlsn;

    void Init();
    void Copy(const WalStreamStateInfo &info);
};

struct WalReadIoStat {
    volatile uint64_t readCount;
    volatile uint64_t readLen;
    volatile uint64_t actualReadLen;
    volatile uint64_t readTime;
    volatile uint64_t waitCount;
    volatile uint64_t waitTime;

    WalReadIoStat(): readCount(0), readLen(0), actualReadLen(0), readTime(0), waitCount(0), waitTime(0) {}
    void Copy(const WalReadIoStat &walReadIoStat);
};

struct WalWriteIoStat {
    uint64_t writeCount;
    uint64_t writeLen;
    uint64_t writeTime;
    uint64_t syncCount;
    uint64_t syncLen;
    uint64_t syncTime;
    uint64_t waitCount;
    uint64_t waitTime;

    WalWriteIoStat(): writeCount(0), writeLen(0), writeTime(0), syncCount(0), syncLen(0), syncTime(0), waitCount(0),
        waitTime(0) {}
    void Copy(const WalWriteIoStat &walWriteIoStat);
};

class WalDiagnose {
public:
    /* Print xlog by parameters */
    static char *WalDump(PdbId pdbId, uint32_t dumpType, uint64_t id1, uint32_t id2);
    static void FreeWalStreamStateInfo(WalStreamStateInfo *walStreamInfo);
    static RetStatus GetWalStreamInfoLocally(PdbId pdbId, WalStreamStateInfo **walStreamInfo, uint32_t *walStreamCount,
        char **errorMsg);
    static RetStatus GetAllWalStreamInfo(PdbId pdbId, WalStreamStateInfo **walStreamInfo, uint32_t *walStreamCount,
        char **errorMsg);
    static RetStatus GetWalRedoInfoLocally(PdbId pdbId, WalId walId, WalRedoInfo *walRedoInfo, char **errorMsg,
                                           bool *walstreamExist);
    static RetStatus GetWalRedoInfo(PdbId pdbId, WalId walId, WalRedoInfo *walRedoInfo, char **errorMsg);
    static char *GetWalRedoStat(uint64_t timeVal);
    static RetStatus StartCollectWalReadIoStat(PdbId pdbId, WalId walId, char **errorMsg, NodeId *nodeId);
    static RetStatus StopCollectWalReadIoStat(PdbId pdbId, WalId walId, NodeId nodeId, WalReadIoStat *walReadIoStat,
        char **errorMsg);
    static RetStatus StartCollectWalWriteIoStat(PdbId pdbId, WalId walId, char **errorMsg, NodeId *nodeId);
    static RetStatus StopCollectWalWriteIoStat(PdbId pdbId, WalId walId, NodeId nodeId, WalWriteIoStat *walWriteIoStat,
        char **errorMsg);
};
#pragma GCC visibility pop
}  // namespace DSTORE

#endif /* DSTORE_STORAGE_WAL_DIAGNOSE_H */
