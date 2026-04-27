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
 * dstore_wal_logstream.h
 *
 * Description:
 * Wal public header file, including interfaces about WalStream management.
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_WAL_LOGSTREAM_H
#define DSTORE_WAL_LOGSTREAM_H

#include "lock/dstore_lwlock.h"
#include "control/dstore_control_file.h"
#include "dstore_wal_buffer.h"
#include "dstore_wal_file_manager.h"
#include "dstore_wal_bgwriter.h"
#include "wal/dstore_wal_recovery.h"
#include "diagnose/dstore_wal_diagnose.h"

namespace DSTORE {

using WalStreamFilter = bool (*)(WalStream *walStream);
constexpr size_t WAL_DIO_BLOCK_SIZE = 4096;
const uint64 TEMPLATE_WAL_FILE_SIZE = (128UL << 20);  // 128m
const uint16 TEMPLATE_WAL_FILE_COUNT = 1;
inline bool WalReadWriteWithDio()
{
#ifdef UT
    return false;
#else
    StorageType type = g_storageInstance->GetGuc()->tenantConfig->storageConfig.type;
    return type == StorageType::LOCAL || type == StorageType::TENANT_ISOLATION;
#endif
}

/*
 * WalStreamState defines all state of log stream.
 */
enum class WalStreamState : uint8_t {
    CREATING = 0,   /* log stream is being creating, after that it changes to USING */
    USING,      /* log stream is in normal use and appending log */
    SYNC_DONE,     /* log stream has been all synced to standby */
    CLOSE_DROPPING,   /* log stream need to be dropped because close pdb, all files will be deleted */
    RECOVERY_DROPPING,   /* log stream need to be dropped because recovery finish, all files will be deleted */
};

/* String representations for WalStreamState enum values */
constexpr const char* g_walStreamStateStr[] = {
    "CREATING",             /* [0] */
    "USING",                /* [1] */
    "SYNC_DONE",            /* [2] */
    "CLOSE_DROPPING",       /* [3] */
    "RECOVERY_DROPPING"     /* [4] */
};

/*
 * WalStreamState defines all state of log stream.
 */
enum class WalStreamUsage {
    WAL_STREAM_USAGE_INVALID,   /* invalid wal stream state */
    WAL_STREAM_USAGE_WRITE_WAL, /* these wal stream can write and read wal */
    WAL_STREAM_USAGE_ONLY_READ, /* these wal stream usually only is used to read wal */
};

enum class WalInitState {
    UNINITIALIZED = 0,
    INITIALIZING = 1,
    INITIALIZED = 2,
};

struct WalBatchInsertStatus {
    bool isInProgressing;
    gs_atomic_uint32 walGroupNextMember;
    WalGroupLsnInfo *recordPos;
    uint8 *data;
    uint32 length;
    inline void Init()
    {
        isInProgressing = false;
        walGroupNextMember = INVALID_THREAD_CORE_ID;
        recordPos = nullptr;
        data = nullptr;
        length = 0;
    }
    inline void AddWalStatus(WalGroupLsnInfo *walRecordPos, uint8 *walData, uint32 walLength)
    {
        isInProgressing = true;
        recordPos = walRecordPos;
        data = walData;
        length = walLength;
    }
};

/* To reduce the cost of notify threads, we divide them into small groups, in which only one thread will be a leader
   thread. BgWalFlush thread only notify leader in each group, and then each leader will notify all other follower
   threads. */
class PlsnWaitSlot : virtual public BaseObject {
public:
    PlsnWaitSlot() : m_waitLock(), m_waitMtx(), m_waitCv(), m_waiterCount()
    {
        LWLockInitialize(&m_waitLock, LWLOCK_GROUP_PLSN_WAIT);
    }
    ~PlsnWaitSlot() {}

    uint64 GetPlsnWaiterCount()
    {
        return GsAtomicReadU64(&m_waiterCount);
    }

    void IncreaseWaitCount()
    {
        GsAtomicFetchAddU64(&m_waiterCount, 1);
    }

    void DecreaseWaitCount()
    {
        GsAtomicFetchSubU64(&m_waiterCount, 1);
    }

    LWLock m_waitLock;
    std::mutex m_waitMtx;
    std::condition_variable m_waitCv;
private:
    gs_atomic_uint64 m_waiterCount;
};

struct WalPlsnRecord {
    PdbId pdbId;
    WalId walId;
    uint64 plsn;
};

class PdbsPlsnRecords {
public:
    PdbsPlsnRecords():m_count(0), m_cursor(0), m_records(nullptr) {}
    ~PdbsPlsnRecords()
    {
        Destroy();
    }

    void Init()
    {
        m_count = 0;
        m_cursor = 0;
        m_records = (WalPlsnRecord *)DstorePalloc0(sizeof(WalPlsnRecord) * MAX_WAL_STREAM_COUNT);
        StorageReleasePanic(m_records == nullptr, MODULE_WAL, ErrMsg("alloc memory for store plsn failed!"));
    }

    bool GetNext(WalPlsnRecord &plsnRecord) const
    {
        if (unlikely(m_cursor == m_count)) {
            return false;
        }
        plsnRecord = m_records[m_cursor++];
        return true;
    }

    void ResetCursor() const
    {
        m_cursor = 0;
    }

    void Add(const WalPlsnRecord &record)
    {
        if (unlikely(m_count >= MAX_WAL_STREAM_COUNT)) {
            /* if wal stream count exceeds 1024, just panic temporarily */
            StorageReleasePanic(true, MODULE_WAL,
                ErrMsg("the count of wal stream exceeds the upper limit(%u).", MAX_WAL_STREAM_COUNT));
            return;
        }
        m_records[m_count++] = record;
    }

    void Destroy()
    {
        if (m_records != nullptr) {
            DstorePfree(m_records);
            m_records = nullptr;
        }
    }

private:
    uint16 m_count;
    mutable uint16 m_cursor; /* const object can also be read by GetNext() */
    WalPlsnRecord *m_records;
};

/*
 * Wal stream object, notice that write process not use it, because Wal automatically choose one in available
 * WalStream.
 */
class WalStream : virtual public BaseObject, virtual public WalStreamBytesReader {
public:
    WalStream(DstoreMemoryContext memoryContext, WalId walId, WalFileManager *walFileManager, uint64 walFileSize,
              PdbId pdbId);
    ~WalStream() override;

    void Init(WalStreamBuffer *walStreamBuffer);

    /*
     * Init walStream with allocate memory for walStreamBuffer and init it.
     */
    RetStatus Init(WalStreamBuffer **walStreamBuffer);
    DISALLOW_COPY_AND_MOVE(WalStream)
    /*
     * Init flush parameters
     */
    virtual void InitFlushParams(uint64 lastEndPlsn);

    PlsnWaitSlot *InitWaitPlsnSlots();

    void FreeWaitPlsnSlots();

    void NotifySlotLeaderIfNecessary(uint64 slot);

    void UpdateNowFlushedPlsn(uint64 &nowFlushedPlsn);

    void WaitPlsnSlots(uint64 slot, uint64 targetPlsn, uint64 nowFlushedPlsn);

    /*
     * Getter for Log Stream Usage.
     */
    virtual WalStreamUsage GetStreamUsage() const;

    /*
     * Convert WalStreamUsage to string
     */
    static const char *StreamUsageToStr(WalStreamUsage usage);

    /*
     * Setter for Log Stream Usage
     */
    virtual void SetStreamUsage(WalStreamUsage usage);

    /*
     * Getter for Log Stream WalStreamState.
     */
    WalStreamState GetWalStreamState() const;

    /*
     * Wal stream taked over by primary, can do ckpt itself.
     */
    void SetStreamSelfCkpt(bool selfCkpt);
    bool IsStreamSelfCkpt() const;

    /*
     * During this node lifecycle, the wal stream initially served as the primary one.
     */
    void SetStreamDemoting(bool isDemoting);
    bool IsStreamDemoting() const;

    /*
     * During this node lifecycle, the wal stream is redo done.
     */
    void SetStreamPromoting(bool isPromoted);
    bool IsStreamPromoting() const;

    /*
     * Convert WalStreamState to string
     */
    static const char *StreamStateToStr(WalStreamState state);

    /*
     * Setter for Log Stream WalStreamState
     */
    void SetWalStreamState(WalStreamState state);

    /*
     * Getter for Wal stream id.
     */
    virtual WalId GetWalId() const;

    /*
     * Getter for segment size
     */
    uint64 GetWalFileSize() const;

    /*
     * Getter for isDio flag
     */
    bool IsDioReadWrite();

    /*
     * append data to wal stream tail, and return record position.
     *
     * @param: data is to insert data
     * @param: len is to insert data len
     *
     * @return: inserted position of data
     */
    virtual WalGroupLsnInfo Append(uint8 *data, uint32 len);

    /**
     * standby copy wal received from the primary to wal buffer.
     *
     * @param: data is to insert data.
     * @param: len is insert data len.
     * @param: startPlsn is start plsn of data.
     * @param: endPlsn is end plsn of data.
     */
    void StandbyPdbAppend(const uint8 *data, uint32 len, uint64 startPlsn, uint64 endPlsn);

    /*
     * write data in WalFile target plsn.
     *
     * @param: plsn is write start offset.
     * @param: data is content to write.
     * @param: dataLen to content len to write.
     * @param: isAsync.
     *
     * @return: OK if success, detail error info otherwise
     */
    RetStatus Write(uint64 plsn, uint8 *data, uint64 dataLen, bool isAsync = true);

    /*
     * Read data in WalFile target plsn.
     *
     * @param: plsn is read start offset.
     * @param: data:read buffer, used to save read data result
     * @param: readLen is called target len to read.
     * @param: resultLen:output parameter, is actual read data len, equal to read_len if read success.
     *
     * @return: OK if success, detail error info otherwise
     */
    RetStatus Read(uint64 plsn, uint8 *data, uint64 readLen, uint64 *resultLen) override;

    /*
     * Get a potentially WalRecord read start plsn before input plsn
     *
     * @param: plsn is end bound of target plsn
     * @return: target plsn if exist, otherwise return INVALID_PLSN
     */
    uint64 GetPrevReadStartPoint(uint64 plsn) override;

    /*
     * Get FileDescriptor of WalFile thata contains plsn
     *
     * @param: plsn is target plsn to contains
     * @return: target WalFile * if exist, nullptr otherwise
     */
    FileDescriptor *GetWalFileDescriptor(uint64 plsn);

    WalFileManager *GetWalFileManager();

    /*
     * Sync all data to disk from startPlsn to endPlsn.
     *
     * @param: startPlsn is sync start offset
     * @param: endPlsn is sync tail offset
     *
     * @return: OK if success, detail error info otherwise
     */
    RetStatus Flush(uint64 startPlsn, uint64 endPlsn);

    /*
     * Truncate all data after but not include target plsn.
     *
     * @param: plsn is truncate start offset.
     *
     * @return: OK if success, detail error info otherwise
     */
    RetStatus Truncate(uint64 plsn);

    /*
     * Make sure all data before target_plsn has been recorded on disk.
     *
     * @param:target_plsn, before and including target_plsn byte in stream should be on disk if return OK
     *
     * @return: OK if success, detail error info otherwise
     */
    virtual void WaitTargetPlsnPersist(uint64 targetPlsn);

    virtual void HandleFileFlushResult(FileDescriptor *fd, off_t offset);

    /*
     * Bind WalStream with target bgWriter
     */
    virtual void BindBgWalWriter(BgWalWriter *bgWriter);

    /*
     * Stop BgWalWriter.
     */
    void DestroyBgWalWriter() noexcept;

    /*
     * Wait all writen wal flushed;
     */
    void WaitAllWritenWalFlushed();

    /*
     * Main function for backgroud wal writer to flush data to disk periodicity.
     *
     * @return: flush date byte len this time
     */
    virtual uint64 Flush();

    /*
     * Get last byte of data that has been appended to stream, all data should is continuous.
     *
     * @return: last byte of all appended data.
     */
    virtual uint64 GetMaxAppendedPlsn() const;

    /*
     * Get last byte of data that has been flushed to disk, all data should is continuous.
     *
     * @return: last byte of all appended data.
     */
    uint64 GetMaxFlushedPlsn() const;

    uint64 GetStandbyPdbRecoveryPlsn() const;

    uint64 GetStandbyPdbRedoFinishedPlsn() const;

    /*
     * Wait until the wal stream replay finish.
     */
    void WaitRecoveryFinish();

    /*
     * Do Wal redo work.
     *
     * @return: OK if success, detail error info otherwise.
     */
    RetStatus Recovery(PdbId pdbId, RedoMode redoMode = RedoMode::RECOVERY_REDO,
        uint64 term = 0, bool tryRecoveryFromDisk = false);

    /*
     * Do flush and create checkpoint after redo done.
     */
    void CheckpointAfterRedo(PdbId pdbId, uint64 term);

    /*
     * Fetch Last Wal Group Info to get lastGroupEndPlsn.
     *
     * @param: redoPlsn is start plsn to read
     * @param: lastGroupEndPlsn is fetched last wal group plsn
     * @return: OK if success, detail error info otherwise.
     */
    RetStatus FetchLastWalGroupInfo(uint64 redoPlsn, uint64 &lastGroupEndPlsn);

    /*
     * Get WalRecovery object.
     */
    WalRecovery *GetWalRecovery();

    /*
     * Get WalRecovery object.
     */
    WalRecovery *GetDiskWalRecovery();

    /*
     * @param startPlsn is start position and get PageEntryCopy after all wal after
     * @param retArraySize is entry number is result
     *
     * @return built WalDirtyPageEntry array
     */
    WalDirtyPageEntry *GetDirtyPageEntryArrayCopy(uint64 startPlsn, long &retArraySize);

    /**
     * For get PdbId.
     */
    PdbId GetPdbId() const
    {
        return m_pdbId;
    }
    /**
     * For standby pdb redo.
     */
    void StartStandbyPdbRecovery();

    /**
     * Get redo finished plsn.
     */
    uint64 GetRedoFinishedPlsn();

    /**
     * Get m_maxWrittenToFilePlsn.
     */
    uint64 GetMaxWrittenToFilePlsn();

    void WaitUntilCorrespondBufferCanReuse(uint64 endPlsn);

    /**
     * Get m_isInRecovery.
     */
    bool IsInRecovering() const;

    /**
     * Set m_isInRecovery.
     */
    void SetInRecovering(bool isInRecovery);

    /**
     * Get m_hasFinishedReceiving.
     */
    bool IsHasStopWalReceiving() const
    {
        return m_hasFinishedReceiving.load(std::memory_order_acquire);
    }

    /**
     * Get m_walStreamBuffer insertCtl end pos.
     */
    uint64 GetStreamBufferInsertEndPos() const
    {
        return m_walStreamBuffer->GetInsertCtlEndBytePos();
    }

    /*
     * Flush given walBlock directly to disk.
     */
    RetStatus FlushWalBlock(uint64 startPlsn, uint8 *data, uint64 dataLen, bool isAsync = true);

    /*
     * Register WalStreamBuffer as Zero copy memory to comm.
     */
    RetStatus SetBufferZeroCopy();

    /*
     * Pause wal recycle for snapshort backup.
     */
    void PauseRecycle() const;

    /*
     * Rerun wal recycle after snapshort backup finished.
     */
    void RerunRecycle() const;

    /*
     * Stop and delete BgWriter
     */
    RetStatus StopBgWriter();

    RetStatus RemoveAllWalFiles();

    bool IsRecoveryRecycleFinish(bool *isStopRecycle = nullptr);

    uint64 GetStandbyMaxFlushedPlsn() const;
    RetStatus GetRedoInfo(WalRedoInfo *walRedoInfo);

    inline bool IsLoadWorkerStarted() const
    {
        return m_walRecovery.IsLoadWorkerStarted();
    }

    inline bool NeedRecoverWithDiskWalRecovery() const
    {
        return m_needRecoverWithDiskWalRecovery;
    }

    static inline bool IsWalStreamUsing(WalStream *walStream)
    {
        return walStream->GetWalStreamState() == WalStreamState::USING;
    }

    static inline bool IsWalStreamUsingAndUsageValid(WalStream *walStream)
    {
        return walStream->GetWalStreamState() == WalStreamState::USING &&
               walStream->GetStreamUsage() != WalStreamUsage::WAL_STREAM_USAGE_INVALID;
    }

    static inline bool IsWalStreamStillNeedRecovery(WalStream *walStream)
    {
        WalStreamState streamState = walStream->GetWalStreamState();
        return streamState != WalStreamState::CREATING &&
            streamState != WalStreamState::CLOSE_DROPPING &&
            streamState != WalStreamState::RECOVERY_DROPPING;
    }

    static inline bool IsWalStreamNeedCkpt(WalStream *walStream)
    {
        return (!walStream->IsStreamSelfCkpt()) && IsWalStreamStillNeedRecovery(walStream) &&
            walStream->GetStreamUsage() != WalStreamUsage::WAL_STREAM_USAGE_INVALID;
    }

    static inline bool IsWalStreamRecoveryDropping(WalStream *walStream)
    {
        return walStream->GetWalStreamState() == WalStreamState::RECOVERY_DROPPING;
    }

    static inline bool IsWalStreamForWrite(WalStream *walStream)
    {
        return walStream->GetWalStreamState() == WalStreamState::USING &&
            walStream->GetStreamUsage() == WalStreamUsage::WAL_STREAM_USAGE_WRITE_WAL;
    }

    static inline bool IsWalStreamForReadOnly(WalStream *walStream)
    {
        return IsWalStreamStillNeedRecovery(walStream) &&
            walStream->GetStreamUsage() == WalStreamUsage::WAL_STREAM_USAGE_ONLY_READ;
    }

    static inline bool IsWalStreamForReadOnlyIgnoreState(WalStream *walStream)
    {
        return walStream->GetStreamUsage() == WalStreamUsage::WAL_STREAM_USAGE_ONLY_READ;
    }

    static inline bool IsWalStreamRedoIncomplete(WalStream *walStream)
    {
        return IsWalStreamStillNeedRecovery(walStream) &&
            walStream->GetStreamUsage() == WalStreamUsage::WAL_STREAM_USAGE_ONLY_READ &&
            walStream->GetWalRecovery() != nullptr &&
            walStream->GetWalRecovery()->GetWalRecoveryStage() < WalRecoveryStage::RECOVERY_REDO_DONE;
    }

    static inline bool IsWalStreamBeforeDirtyPageFlush(WalStream *walStream)
    {
        return IsWalStreamStillNeedRecovery(walStream) &&
            walStream->GetStreamUsage() == WalStreamUsage::WAL_STREAM_USAGE_ONLY_READ &&
            walStream->GetWalRecovery() != nullptr &&
            walStream->GetWalRecovery()->GetWalRecoveryStage() < WalRecoveryStage::RECOVERY_DIRTY_PAGE_FLUSHED;
    }

    static inline bool IsWalStreamRedoDone(WalStream *walStream)
    {
        return IsWalStreamStillNeedRecovery(walStream) &&
            walStream->GetStreamUsage() == WalStreamUsage::WAL_STREAM_USAGE_ONLY_READ &&
            walStream->GetWalRecovery() != nullptr &&
            walStream->GetWalRecovery()->GetWalRecoveryStage() == WalRecoveryStage::RECOVERY_REDO_DONE;
    }

#ifdef UT
    inline bool GetPauseRecycleStart() const
    {
        return m_walFileManager->GetPauseRecycleStart();
    }

    inline bool GetPauseRecycleFinish() const
    {
        return m_walFileManager->GetPauseRecycleFinish();
    }
#endif

    inline bool HasValidConsensusPlsn() const
    {
        return m_consensusPlsn != INVALID_END_PLSN;
    }

    inline uint64 GetWalStreamBufferSize()
    {
        return m_walStreamBuffer->GetBufferSize();
    }

    inline void StartCollectWalReadIoStat()
    {
        m_collectWalReadIoStat = true;
    }

    inline void StartCollectWalWriteIoStat()
    {
        m_collectWalWriteIoStat = true;
    }

    inline void StopCollectWalReadIoStat()
    {
        m_collectWalReadIoStat = false;
    }

    inline void StopCollectWalWriteIoStat()
    {
        m_collectWalWriteIoStat = false;
    }

    inline bool IsCollectWalReadIoStat() const
    {
        return m_collectWalReadIoStat;
    }

    inline bool IsCollectWalWriteIoStat() const
    {
        return m_collectWalWriteIoStat;
    }

    inline WalReadIoStat& GetWalReadIoStat()
    {
        return m_readIoStat;
    }

    inline WalWriteIoStat& GetWalWriteIoStat()
    {
        return m_writeIoStat;
    }

    void SetStandbyRedoFinishPlsn(uint64 redoFinishPlsn)
    {
        GsAtomicWriteU64(&m_standbyRedoFinishPlsn, redoFinishPlsn);
        while (m_startRecoveryPlsnCnt.load(std::memory_order_acquire) != 0) {
            GaussUsleep(STORAGE_USECS_PER_MSEC);
            ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("pdb standby redo wait get syncerplsn finish, pdb %u, walId:%lu.",
                m_pdbId, m_walId));
        }
    }

    uint64 GetStandbyRedoFinishPlsn() const
    {
        const uint64 redoFinishPlsn = GsAtomicReadU64(const_cast<volatile uint64_t*>(&m_standbyRedoFinishPlsn));
        return redoFinishPlsn;
    }

    inline void ThrottleIfNeed()
    {
        m_bgWalWriter->ThrottleIfNeed();
    }

    std::thread *m_asyncStopAndDropThread{nullptr};
    friend class WalReadBuffer;
    mutable std::atomic<uint64> m_startRecoveryPlsnCnt{0};

#ifndef UT
private:
#endif
    static inline uint32 CalculateFreeSpaceInBlock(uint64 plsn);

    WalGroupLsnInfo SingleAppend(uint8 *data, uint32 len);
    WalGroupLsnInfo BatchAppend(uint8 *data, uint32 len);
    void InsertRecordGroupLeader(ThreadCore *leader);
    void InsertRecordGroupFollowers(ThreadCore *leader, uint32 head);

    void SanityCheckLSN(uint64 startPlsn, uint64 endPlsn) const;

    template<bool isDcf = false>
    void CopyToBuffer(const uint8 *data, uint32 walLen, uint64 startPlsn, uint64 endPlsn);

    void StandbyRedoWalWorkerMain(RedoMode redoMode);
    void DiskWalRecoveryWorkerMain(uint64 startPlsn, uint64 endPlsn);

    void UnsetBufferZeroCopy() noexcept;

    void SetWalFileHeader(uint8 *data, uint64 startPlsn) const;

    void UpdateMaxFlushedPlsn(uint64 plsn);

    void PushEndFlushPlsn(WalFile *walFile);

    inline bool NeedStartDiskWalRecovery(bool tryRecoveryFromDisk, WalRecovery *walRecovery)
    {
        if (!tryRecoveryFromDisk) {
            return false;
        }
        return walRecovery->GetRecoveryStartPlsn() > walRecovery->GetDiskRecoveryStartPlsn();
    }
    void InitAndStartDiskWalRecovery(WalRecovery *walRecovery);

    void AdaptDioWrite(uint64 plsn, uint8 **buf, uint64 *count, uint64 *offset, bool *needWriteTail);

    void WriteAdaptedData(bool isAsync, uint8 *adaptedData, uint64 adaptedDataLen,
                          uint64 adaptedOffset, bool *needWriteTail);

    DstoreMemoryContext m_memoryContext;
    WalStreamBuffer *m_walStreamBuffer;
    WalId m_walId;
    WalStreamUsage m_usage;
    WalStreamState m_state;
    std::atomic<bool> m_selfCkpt;
    std::atomic<bool> m_isDemoting;
    std::atomic<bool> m_isPromoting;
    BgWalWriter *m_bgWalWriter;
    uint64 m_walFileSize;
    PdbId m_pdbId;
    bool m_diorw;
    uint8 *m_dioReadBuffer;
    uint32 m_dioReadBufferSize;
    uint8 *m_dioWriteTailBuffer;

    uint64 m_maxWrittenToFilePlsn;
    uint64 m_maxFlushFinishPlsn;
    uint64 m_standbyRedoFinishPlsn;
    WalRecovery m_walRecovery;
    /**
     * m_diskWalRecovery is supplement WalRecovery in some duplicate fault condition.
     * like compute node and memmory node fail in sequence: compute node fail then we try to redo wal from
     * memory-checkpoint which m_walRecovery will implement, however wal between disk-checkpoint and memory-checkpoint
     * corresponding dirty pages need to be flushed to push disk-checkpoint to end and mark wal-stream free, this part
     * is implemented by m_diskWalRecovery.
     */
    WalRecovery m_diskWalRecovery;
    uint64 m_redoFinishedPlsn;
    std::atomic<bool> m_isInRecovery;

    WalFileManager *m_walFileManager;
    WalFile *m_writingWalFile;
    WalFile *m_flushingWalFile;
    uint64 m_eachWriteLenLimit;
    VFSAdapter *m_vfs;

    uint64 m_consensusPlsn;
    std::atomic_bool m_hasFinishedReceiving;

    std::thread *m_redoThread;
    std::thread *m_standbyRedoThread;
    PlsnWaitSlot *m_plsnWaitSlot;
    gs_atomic_uint64 m_lastSlotNo;
    std::thread *m_diskWalRecoveryThread;
    bool m_diskWalRecoveryNeedStop;
    bool m_needRecoverWithDiskWalRecovery;

    bool m_zeroCopyRegistered;
    bool m_collectWalReadIoStat;
    bool m_collectWalWriteIoStat;
    WalReadIoStat m_readIoStat;
    WalWriteIoStat m_writeIoStat;
};

enum class WalStreamAllocatePolicy {
    /*
     * allocated WalStream by transaction, relying on transaction's Xid
     */
    WAL_ALLOCATE_WRITER_BY_XID,
    /*
     * allocated WalStream by transaction, relying on transaction's first modify-page's id if existed;
     * and equal to WAL_ALLOCATE_WRITER_BY_XID if no page modified.
     */
    WAL_ALLOCATE_WRITER_BY_FIRST_PAGE,
};

struct WalStreamNode {
    dlist_node node;
    WalStream* walStream{nullptr};
    gs_atomic_uint32 refCnt{0};
};

struct StandbyWalStreamTransInfo {
    WalId walId;
    uint64 maxFlushedPlsn;      /* max plsn flushed by standby walStream */
    uint64 maxReplayedPlsn;     /* max plsn replayed by standby walStream */
};

/*
 * Manager for all Wal stream objects.
 */
class WalStreamManager : public BaseObject {
public:
    explicit WalStreamManager(DstoreMemoryContext memoryContext);
    ~WalStreamManager();
    DISALLOW_COPY_AND_MOVE(WalStreamManager)

    /*
     * Initialize all wal stream resource. All wal stream resource has been created during tenant resource initialize,
     * now we should load wal stream by loading stream name, timeline, state and other info, and truncate stream
     * tail's damaged record.
     *
     * @return: OK if success, detail error info otherwise.
     */
    RetStatus Init(ControlFile *controlFile);

    /*
     * Create new writing wal stream when pdb promoting, must called after Init;
     * @return: OK if success, detail error info otherwise.
     */
    RetStatus CreateWritingWalStreamWhenPromoting();

    /*
     * When wal stream resource changes, reload them, and notice that don't allow to delete wal stream resource.
     *
     * @return: OK if success, detail error info otherwise
     */
    RetStatus Reload() const;

    /*
     * Get Wal stream based on Wal id.
     *
     * @param: walId is target log stream's WalId
     *
     * @return: target wal stream, is nullptr if find target log stream failed
     */
    WalStream *GetWalStream(WalId walId);

    bool isWalIdOwnedRedoDone(WalId walId);

    /**
     * Get Wal stream whose usage is writing wal, normally there only one writing Wal stream in a Pdb.
     *
     * @return: target wal stream, is nullptr if find target log stream failed
     */
    WalStream *GetWritingWalStream();

    void LockWalStreamsList(LWLockMode mode, bool needWait = true, bool *isLocked = nullptr)
    {
        if (needWait) {
            DstoreLWLockAcquireByMode(&m_lwlock, mode);
        } else {
            bool locked = DstoreLWLockConditionalAcquire(&m_lwlock, mode);
            if (isLocked != nullptr) {
                *isLocked = locked;
            }
        }
    }

    void UnLockWalStreamsList()
    {
        LWLockRelease(&m_lwlock);
    }

    /*
     * Wal streams iterator, get next Wal stream satisfies condition
     *
     * @param iter dlist_mutable_iter contains cur return node and it's prev node and next node
     * @param filter filter conditions for Wal stream
     * @param shouldRefNode true if caller wants to add refCnt of this node to avoid it to be deleted
     * @return target wal stream, is nullptr if find target log stream failed
     */
    WalStreamNode *GetNextWalStream(dlist_mutable_iter *iter, WalStreamFilter filter = nullptr,
                                    bool shouldRefNode = false);

    /* If caller use GetNextWalStream with souldRefNode equals true, they should call this deref function after using.
     */
    void DerefWalStreamNode(WalStreamNode *refNode);

    WalStreamNode *GetWalStreamNode(WalId walId);

    /*
     * Get all Wal stream that are created or loaded
     *
     * @param[out]: totalWalStreams is number of stream object this wal manager created or loaded
     */
    uint32 GetTotalWalStreamsCount() const
    {
        return m_totalWalStreamsCount;
    }

    ControlFile *GetControlFile()
    {
        return m_controlFile;
    }

    /*
     * Check if wal stream is current writing wal stream
     *
     * @param: walId is target log stream's WalId to be verified.
     *
     * @return: DSTORE_SUCC if walId equal to current writing wal stream's id, DSTORE_FAIL otherwise.
     */
    bool IsSelfWritingWalStream(WalId walId);

    /**
     * Get all the WAL stream IDs that are under recovery. Note: 1). the caller has to ensure the memory of ID array be
     * released when it's finished using it. 2). the function is not thread safe
     *
     * @param[out] walIdArray the WAL stream ID array to be returned
     * @param WalStreamFilter filter condition for Wal stream
     * @return the length of the walIdArray
     */
    uint32 GetOwnedStreamIds(WalId **walIdArray, WalStreamFilter filter = nullptr);

    /**
     * This function gets called when the current PDB knows which WAL streams need to be recovered.
     *
     * @param walIdArray the array of WAL stream ID
     * @param length the length of the walIdArray
     * @return success or fail
     */
    RetStatus TakeOverStreams(WalId *walIdArray, const uint32 walIdArrayLen);

    /*
     * WalStream should not write wal any more, and it will try wait all writen wal flushed and return.
     */
    RetStatus StopWalStreamWriteWal(WalId walId);

    /*
     * Init BgWriter for Walstream
    */
    RetStatus InitWalStreamBgWriter(WalStream *stream);

    /*
     * Stop BgWriter of all the Walstreams
     */
    RetStatus StopWalStreamBgWriters();

    /*
     * Remove walStreams from walstream manager
     */
    void DestroyWalStreams();
    void SetAllWalStreamsUsage(WalStreamUsage usage, WalRecoveryStage stage);

    /* Delete target walstream */
    void DeleteWalStream(WalId walId);

    /* Delete walstreams that marked dropping and recovery recycle finish */
    void DeleteDroppedWalStream();

    void PrintAllWalStreams(ErrLevel errLevel);

    RetStatus UpdateWalStreamStateInControlFile(WalId walId, const WalStreamState &state);
    RetStatus LoadWalStreamForPdbStandbyRedo(const ControlWalStreamPageItemData &walStreamConfig);
    void GetStandbyWalStreamTransInfo(StandbyWalStreamTransInfo *&walStreamTransInfos, uint32 &walStreamCount);
    RetStatus GetAllWalStreamInfo(WalStreamStateInfo **walStreamInfo, uint32 *walStreamCount);
    RetStatus StartCollectWalReadIoStat(WalId walId, bool *found);
    RetStatus StartCollectWalWriteIoStat(WalId walId, bool *found);
    RetStatus StopCollectWalReadIoStat(WalId walId, WalReadIoStat *walReadIoStat, bool *found);
    RetStatus StopCollectWalWriteIoStat(WalId walId, WalWriteIoStat *walWriteIoStat, bool *found);
    void DecreaseOneStream()
    {
        m_totalWalStreamsCount--;
        GS_WRITE_BARRIER();
    }

    void PauseWalFileRecycle();
    void ResumeWalFileRecycle();

#ifdef UT
    void SetInitializeState(WalInitState initialized)
    {
        m_initialized = initialized;
    }
#endif
#ifndef UT
private:
#endif
    void GetWalStreamFromCtlFile(VnodeControlWalStreamPageItemDatas *walStreamPageItemDatas, uint16 *streamCount);
    RetStatus CreateStream(const ControlWalStreamPageItemData &walStreamConfig, WalFileManager **walFileManager,
                           WalStreamBuffer **buffer, WalStream **stream, bool forRead);
    RetStatus LoadWalStreamFromConfig(const ControlWalStreamPageItemData &walStreamConfig,
        bool loadWhenPromoting = false);
    RetStatus LoadWalStreamForRead(const ControlWalStreamPageItemData &walStreamConfig);
    void UpdateWritingWalStream();

    WalStreamNode* DoDestroyWalStreams();

    DstoreMemoryContext m_memoryContext;
    ControlFile *m_controlFile;
    PdbId m_pdbId;
    WalStream *m_writingWalStream; /* To avoid repeatedly search walStreamsList when write wal */

    WalStreamAllocatePolicy m_streamAllocatePolicy;
    LWLock m_lwlock; /* lock for walStreamsList */
    uint32 m_totalWalStreamsCount;
    dlist_head m_walStreamsListHead;

    WalInitState m_initialized;
};
}  // namespace DSTORE
#endif  // STORAGE_WAL_LOGSTREAM_H
