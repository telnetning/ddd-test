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
 * dstore_wal_buffer.h
 *
 * Description:
 * Wal private header file, including interfaces about wal buffer.
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_WAL_BUFFER_H
#define DSTORE_WAL_BUFFER_H

#include "common/dstore_datatype.h"

namespace DSTORE {

constexpr uint16 GROUP_INSERT_LOCKS_COUNT = 9;

struct WalInsertLock {
    gs_atomic_uint32 walGroupFirst;
} ALIGNED(DSTORE_CACHELINE_SIZE);

/*
 * control Insert Structure, guaranteed reservation sequence.
 *
 * endBytePos: is a space only contains WalRecordGroup,
 * wal file header and other struct is not included, then it doesn't represent byte offset in WalFile, aka plsn.
 *
 * padding: in order to use 128B CAS, we need make sure address is 16 Byte align
 */
struct WalInsertControl {
    std::atomic<uint64> endBytePos;
    char padding[DSTORE_CACHELINE_SIZE - sizeof(uint64)];
    WalInsertLock** walInsertLocks;
    void Init()
    {
        endBytePos = 0;
        walInsertLocks = nullptr;
    }
} ALIGNED(DSTORE_CACHELINE_SIZE);

/*
 * controller for Wal buffer, which is constructed with multi block. controller record meta info for each block.
 * continuous block will be returned for flush, and will be reused after flush to disk success
 */
struct WalBufferCtlData {
    uint64 maxFlushedPlsn;
    char padding[64];
    uint32 blockCount;
    uint8 *blockPtrArr;
};

struct WalBufferInsertStateEntry {
    std::atomic<uint64> endPlsn;
};

/*
 * controller for all reserved buffer space, which is identified by WalBufferInsertStateEntry.
 * Note: Wal buffer's 'flush' action means return continuous data and next flush start from this time's end
 */
struct alignas(DSTORE_CACHELINE_SIZE) WalBufferInsertState {
    uint32 flushStartEntryIndex;
    char padding[DSTORE_CACHELINE_SIZE - sizeof(uint32)];
    WalBufferInsertStateEntry *insertStateEntrys;
};

struct WalBufferInsertPos {
    uint64 startBytePos;
    uint64 endBytePos;
};

class WalStream;
/*
 * Each log stream usually related to one buffer, used to accelerate log write and flush efficiency.
 */
class alignas(DSTORE_CACHELINE_SIZE) WalStreamBuffer : public BaseObject {
public:
    WalStreamBuffer(DstoreMemoryContext memoryContext, uint32 blockCount);
    ~WalStreamBuffer();
    DISALLOW_COPY_AND_MOVE(WalStreamBuffer)

    /*
     * Init wal buffer and inner data structure.
     *
     * @param: lastEndPlsn: stream max plsn
     * @param: walFileSize: wal file size
     *
     * @return: OK if success, detail error info otherwise
     */
    RetStatus Init(uint64 lastEndPlsn, uint64 walFileSize);

    /*
     * Init wal buffer and inner data structure.
     *
     * @param: lastEndPlsn: is start plsn of next record, get lastEndPlsn from startup recovery.
     * @param: walStream is used to init walbuffer if use dio read and write
     *
     * @return: OK if success, detail error info otherwise
     */
    RetStatus InitPlsn(uint64 lastEndPlsn, WalStream *walStream);

    /*
     * Reserves the right amount of space for a record of given size from the wal buffer.
     *
     * @param: reserveSize: the total size this time need reserve.
     * @param: startBytePos: the start position of the wal
     * @param: endBytePos: the end position of the wal.
     */
    void ReserveInsertByteLocation(uint32 reserveSize, WalBufferInsertPos &insertPos);

    /*
     * Reserves the right amount of space for a record of given size from the wal buffer.
     *
     * @param: size: WalRecordGroup size
     * @param: startPlsn: position where this wal start
     * @param: endPlsn: position where this wal end
     */
    void ReserveInsertLocation(uint32 size, uint64 &startPlsn, uint64 &endPlsn);

    /*
     * Atomic set m_insertCtl if endPlsn is larger than current endPlsn. Used to set maxAppendedPlsn in standby pdb.
     * Note: LastInsertSize in standby != lastWalGroupSize. Primary sends walBlocks of one or more groups to standby.
     *
     * @param: endPlsn: position where this wal end
     */
    void SetInsertCtl(uint64 endPlsn);

    /*
     * Get a pointer to the right location in the Wal buffer containing the given WalPtr.
     *
     * @param: plsn: is byte position that target buffer block contains
     *
     * @return: target buffer block start pointer
     */
    uint8* GetBufferBlock(uint64 plsn);

    /*
     * mark wal buffer's data of target startPlsn copy finished
     *
     * @param: startPlsn: wal start plsn
     * @param: endPlsn: wal end plsn
     */
    void MarkInsertFinish(uint64 startPlsn, uint64 endPlsn);

    /*
     * Return data which is continuous in WalFile Space, then will flush them to WalFile.
     * This method does not support concurrent invocation.
     *
     * @param: requestPlsn: requestPlsn may be less than startBound
     * @param: startPlsn: output parameter, flush data start plsn
     * @param: endPlsn: output parameter, flush data end plsn
     * @param: data: output parameter, flush data
     * @param: reachBufferTail: output parameter, if the data get from buffer reach the buffer tail.
     */
    void GetNextFlushData(uint64 &startPlsn, uint64 &endPlsn, uint8 *&data, bool &reachBufferTail);

    /*
     * get buffer size.
     *
     * @return buffer size
     */
    uint64 GetBufferSize() const;

    /*
     * Get buffer's end insert location.
     *
     * @param: endPlsn: output parameter, end insert wal data plsn.
     */
    void GetFinalInsertLocation(uint64 &endPlsn);

    /*
     * Transfor Wal bytes position to plsn, WalBytes space only have wal datas, plsn have segment header and so on.
     *
     * @param: bytepos: is Wal bytes position.
     * @param: IsEnd: if should jumper next segment file header if wal bytes is at this segment files end.
     *
     * @return: plsn
     */
    uint64 WalBytePosToPlsn(uint64 bytepos, bool isEnd) const;

    /*
     * Get Wal Insert Lock.
     *
     * @param: numaId: is this thrd's numa id, user should make sure this is smaller than numaId when WalBuffer init.
     * @param: groupId: is this thrd's numa id, user should make sure this is smaller than groupId when WalBuffer init.
     *
     * @return: Wal Insert Lock ptr.
     */
    inline WalInsertLock* GetWalInsertLock(int numaId, int groupId)
    {
        return &m_insertCtl.walInsertLocks[numaId][groupId];
    }

    inline uint64 GetLastScannedPlsn()
    {
        return m_lastScannedPlsn;
    }

    /*
     * Get m_insertCtl endBytePos.
     */
    uint64 GetInsertCtlEndBytePos() const
    {
        return m_insertCtl.endBytePos.load(std::memory_order_acquire);
    }

    /*
     * Get buffer's start ptr.
     *
     * @return buffer's start ptr.
     */
    void *GetBufferPtr() const;

#ifndef UT
private:
#endif
    inline static uint32 GetNextEntryIndex(uint32 entryIndex);

    inline uint32 PlsnToBufferBlockIndex(uint64 plsn) const;

    RetStatus InitInsertLocks();

    void InitWalBufferCtrl(uint64 lastEndPlsn);
    RetStatus InitBufferCtlData(uint64 lastEndPlsn);

    uint64 WalPlsnToBytePos(uint64 plsn) const;
    uint64 GetMaxContinuesPlsn();
    RetStatus InitLastBlock(WalStream *walStream, uint64 lastEndPlsn);
    void Clear() noexcept;

    alignas(DSTORE_CACHELINE_SIZE) WalInsertControl m_insertCtl;
    alignas(DSTORE_CACHELINE_SIZE) WalBufferCtlData m_walBufferCtl;
    alignas(DSTORE_CACHELINE_SIZE) WalBufferInsertState m_walBufferInsertState;
    alignas(DSTORE_CACHELINE_SIZE)
        std::atomic<uint64> m_maxContinuousPlsn; /* max Continuous Plsn which has been copied to wal buffer */
    alignas(DSTORE_CACHELINE_SIZE) uint64 m_lastScannedPlsn;
    alignas(DSTORE_CACHELINE_SIZE) uint64 m_bufferSize;
    DstoreMemoryContext m_memoryContext;
    uint64 m_alignedBufferSize;
    uint64 m_maxReserveSize; /* need make sure this and m_insertCtl in the same cache line */
    uint64 m_walFileSize;
};

/* only use for internal */
void FlushAllDirtyPages(PdbId pdbId);
}
#endif // STORAGE_WAL_BUFFER_H
