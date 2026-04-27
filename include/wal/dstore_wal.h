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
 * dstore_wal.h
 *
 * Description:
 * Wal public header file, including interfaces about basic type definition, recovery, threads management and utils.
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_WAL_H
#define DSTORE_WAL_H

#include "common/dstore_datatype.h"
#include "wal/dstore_wal_logstream.h"
#include "wal/dstore_wal_reader.h"
#include "wal/dstore_wal_redo_manager.h"

namespace DSTORE {

enum class WalLevel {
    WAL_LEVEL_MINIMAL = 0,
    WAL_LEVEL_ARCHIVE,
    WAL_LEVEL_HOT_STANDBY,
    WAL_LEVEL_LOGICAL = 3
};

enum class WalInitState;

extern bool WalLogicalActive();

/*
 * Entry class for all wal main function, like wal init, recovery or backup function.
 */
class WalManager : public BaseObject {
public:
    explicit WalManager(DstoreMemoryContext memoryContext);
    ~WalManager();
    DISALLOW_COPY_AND_MOVE(WalManager);

    /*
     * Call once after process start, including init mem struct, creating default wal stream, clog, csnlog and so on.
     * and construct all WalStream for AtomicWalWriterContext
     *
     * @return: init result
     */
    RetStatus Init(ControlFile *controlFile);

    /*
     * Clear all resources.
     */
    void Destroy();

    /*
     * return WalManager init state
     *
     * @return: true if WalManager init success, false otherwise
     */
    bool IsInited() const;
    bool isUninit() const;

    /*
     * Get Wal StreamManager instance pointer if init success.
     *
     * @return: StreamManager pointer.
     */
    WalStreamManager *GetWalStreamManager();

    /*
     * Get the reference of WalRedoManager.
     */
    WalRedoManager &GetWalRedoManager();

    /*
     * Start recovery a wal stream.
     *
     * @param: walId: the id of target wal stream
     * @param: pdbId is PDB id
     * @param: point is backup-restore condition's consistencyCsn and backup plsn
     * @param: term is valid in memberview change's fault handle condition
     * @param: tryRecoveryFromDisk is true when redo should cover wal between [diskcheckpoint, memorycheckpoint]
     *
     * @return: OK if success, detail error info otherwise.
     */
    RetStatus Recovery(WalId walId, PdbId pdbId,
        uint64 term = 0, bool tryRecoveryFromDisk = false);

    /*
     * Wait until the specific task replay finish.
     */
    void WaitRecoveryFinish(RedoTask *task);

    /*
     * Get all transactions which csn is bigger than backConsistencyCsn.
     * @param[out] rollbackArray is the csn and xid of all transactions that need rollback
     * @param[out] totalRollbackNum is the num of transactions that need rollback
     *
     * @return: OK if success, detail error info otherwise.
     */
    RetStatus GetAllTransactionsNeedRollback(
        TransactionNeedRollbackInfo **rollbackArray, uint64 &totalRollbackNum);

    /**
     * This function gets called when the current PDB knows which WAL streams need to be recovered.
     *
     * @param walIdArray the array of WAL stream ID
     * @param length the length of the walIdArray
     * @return success or fail
     */
    RetStatus TakeOverStreams(WalId *walIdArray, const uint32 length);
    
    /**
     * This function gets target page's wal record info lists in taken oven streams, and lock the wal records list in
     * share mode if walRecordInfoList is not null.
     *
     * @param pageId the target page id
     * @param pageLsnInfo is target page's Lsn and get wal after it
     * @param fromDiskWalRecovery is true when should collect from WalStream's m_diskWalRecovery target
     * @param recordIter iterator of target fetched WalRecord set
     * @param isRecoveryDone is true if the specified wal stream of walId is RECOVERY_REDO_DONE
     * @return success or fail
     */
    RetStatus GetPageWalRecordInfoListAndLock(const PageId pageId, const WalRecordLsnInfo pageLsnInfo,
                                              bool fromDiskWalRecovery, BigArray::Iter *recordIter,
                                              bool *isRecoveryDone);

    /**
     * Release the obtained wal record info list lock if get the lock successfully.
     *
     * @param walId get the wal records of specified walId
     * @param fromDiskWalRecovery is true when should collect from WalStream's m_diskWalRecovery target
     * @return void
     */
    void ReleasePageWalRecordInfoListLock(WalId walId, bool fromDiskWalRecovery);
private:
    WalPlsn m_minLogicalRepRequired;
    bool ModifyInitState(WalInitState oldState, WalInitState newState);

    DstoreMemoryContext m_memoryContext;
    WalStreamManager *m_streamManager;
    WalRedoManager m_redoManager;
    WalInitState m_initState;
    ControlFile *m_controlFile;
    PdbId m_pdbId;
};

}
#endif // STORAGE_WAL_H
