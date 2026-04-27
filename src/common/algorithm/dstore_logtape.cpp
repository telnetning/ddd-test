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
 * IDENTIFICATION
 *        src/common/algorithm/dstore_logtape.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "common/algorithm/dstore_logtape.h"
#include "common/error/dstore_error.h"
#include "common/log/dstore_log.h"
#include "errorcode/dstore_common_error_code.h"
#include "errorcode/dstore_index_error_code.h"
#include "securec.h"

namespace DSTORE {

LogicalTapeBlockMgr::LogicalTapeBlockMgr(const char *tapeBaseName)
    : m_pfile(nullptr),
      m_nBlocksAllocated(0L),
      m_nBlocksWritten(0L),
      m_forgetFreeSpace(false),
      m_freeBlocks(nullptr),
      m_nFreeBlocks(0L),
      m_freeBlocksLen(32U),
      m_tapeBaseName(tapeBaseName)
{}

RetStatus LogicalTapeBlockMgr::Init()
{
    m_freeBlocks = static_cast<long *>(DstorePalloc(m_freeBlocksLen * sizeof(long)));
    m_pfile = BufFileCreateTemp(false, m_tapeBaseName);
    if (unlikely(m_freeBlocks == nullptr || m_pfile == nullptr)) {
        Destroy();
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

void LogicalTapeBlockMgr::Destroy()
{
    if (m_freeBlocks) {
        DstorePfree(m_freeBlocks);
        m_freeBlocks = nullptr;
    }
    BufFileClose(m_pfile);
    m_pfile = nullptr;
}

/*
 * Write a block-sized buffer to the specified block of the underlying file.
 */
RetStatus LogicalTapeBlockMgr::WriteBlock(long blocknum, void *buffer)
{
    /*
     * BufFile does not support "holes", so if we're about to write a block
     * that's past the current end of file, fill the space between the current
     * end of file and the target block with zeros.
     *
     * This can happen either when tapes preallocate blocks; or for the last
     * block of a tape which might not have been flushed.
     *
     * Note that BufFile concatenation can leave "holes" in BufFile between
     * worker-owned block ranges.  These are tracked for reporting purposes
     * only.  We never read from nor write to these hole blocks, and so they
     * are not considered here.
     */
    while (blocknum > m_nBlocksWritten) {
        char data[BLCKSZ];
        errno_t rc = memset_sp(data, BLCKSZ, 0, BLCKSZ);
        storage_securec_check(rc, "\0", "\0");
        if (STORAGE_FUNC_FAIL(WriteBlock(m_nBlocksWritten, data))) {
            return DSTORE_FAIL;
        }
    }

    /* Write the requested block */
    if (BufFileSeekBlock(m_pfile, blocknum) != 0) {
        char tmpFileName[MAXPGPATH] = {0};
        GetFileName(m_pfile, tmpFileName);
        storage_set_error(LOGTAPE_ERROR_COULD_NOT_SEEK_BLOCK, blocknum, tmpFileName);
        ErrLog(DSTORE_ERROR, MODULE_COMMON,
            ErrMsg("Failed to read block %ld of file \"%s\"", blocknum, tmpFileName));
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(BufFileWrite(m_pfile, buffer, BLCKSZ))) {
        return DSTORE_FAIL;
    }
    /* Update m_nBlocksWritten, if we extended the file */
    if (blocknum == m_nBlocksWritten) {
        m_nBlocksWritten++;
    }
    return DSTORE_SUCC;
}

/*
 * Read a block-sized buffer from the specified block of the underlying file.
 */
RetStatus LogicalTapeBlockMgr::ReadBlock(long blocknum, void *buffer)
{
    if (BufFileSeekBlock(m_pfile, blocknum) != 0) {
        char tmpFileName[MAXPGPATH] = {0};
        GetFileName(m_pfile, tmpFileName);
        storage_set_error(LOGTAPE_ERROR_COULD_NOT_SEEK_BLOCK, blocknum, tmpFileName);
        ErrLog(DSTORE_ERROR, MODULE_COMMON,
            ErrMsg("Failed to read block %ld of file \"%s\"", blocknum, tmpFileName));
        return DSTORE_FAIL;
    }
    size_t nread = BufFileRead(m_pfile, buffer, BLCKSZ);
    if (nread != BLCKSZ) {
        char tmpFileName[MAXPGPATH] = {0};
        GetFileName(m_pfile, tmpFileName);
        storage_set_error(LOGTAPE_ERROR_COULD_NOT_SEEK_BLOCK_READONLY, blocknum, tmpFileName, nread, BLCKSZ);
        ErrLog(DSTORE_ERROR, MODULE_COMMON,
            ErrMsg("Size(%lu) of block %ld in file \"%s\" is not 8k.", nread, blocknum, tmpFileName));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

static inline unsigned long LeftOffset(unsigned long i)
{
    return (i << 1) + 1;
}

static inline unsigned long RightOffset(unsigned long i)
{
    return (i + 1) << 1;
}

static inline unsigned long ParentOffset(unsigned long i)
{
    return (i - 1) >> 1;
}

long LogicalTapeBlockMgr::GetFreeBlock()
{
    long       *heap = m_freeBlocks;
    long        blocknum;
    unsigned long         heapsize;
    long        holeval;
    unsigned long holepos;

    /* freelist empty; allocate a new block */
    if (m_nFreeBlocks == 0) {
        return m_nBlocksAllocated++;
    }

    /* easy if heap contains one element */
    if (m_nFreeBlocks == 1) {
        m_nFreeBlocks--;
        return m_freeBlocks[0];
    }

    /* remove top of minheap */
    blocknum = heap[0];

    /* we'll replace it with end of minheap array */
    holeval = heap[--m_nFreeBlocks];

    /* sift down */
    holepos = 0;                /* holepos is where the "hole" is */
    heapsize = static_cast<unsigned long>(m_nFreeBlocks);
    for (;;) {
        unsigned long left = LeftOffset(holepos);
        unsigned long right = RightOffset(holepos);
        unsigned long minChild;

        if (left < heapsize && (right) < heapsize) {
            minChild = (heap[left] < heap[right]) ? left : right;
        } else if (left < heapsize) {
            minChild = left;
        } else if (right < heapsize) {
            minChild = right;
        } else {
            break;
        }

        if (heap[minChild] >= holeval) {
            break;
        }

        heap[holepos] = heap[minChild];
        holepos = minChild;
    }
    heap[holepos] = holeval;

    return blocknum;
}

RetStatus LogicalTapeBlockMgr::ReleaseBlock(long blocknum)
{
    long       *heap;
    unsigned long holepos;

    if (m_forgetFreeSpace) {
        return DSTORE_SUCC;
    }
    /* Enlarge m_freeBlocks array if full. */
    if (static_cast<Size>(m_nFreeBlocks) >= m_freeBlocksLen) {
        /* If the freelist becomes very large, just return and leak this free block. */
        if ((m_freeBlocksLen << 1) * sizeof(long) > MaxAllocSize) {
            return DSTORE_SUCC;
        }
        m_freeBlocksLen = m_freeBlocksLen << 1;
        long *tmp = static_cast<long *>(DstoreRepalloc(m_freeBlocks, m_freeBlocksLen * sizeof(long)));
        if (STORAGE_VAR_NULL(tmp)) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstoreRepalloc fail when ReleaseBlock."));
            storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
            return DSTORE_FAIL;
        }
        m_freeBlocks = tmp;
    }

    /* create a "hole" at end of minheap array */
    heap = m_freeBlocks;
    holepos = static_cast<unsigned long>(m_nFreeBlocks);
    m_nFreeBlocks++;

    /* sift up to insert blocknum */
    while (holepos != 0) {
        unsigned long parent = ParentOffset(holepos);
        if (heap[parent] < blocknum) {
            break;
        }

        heap[holepos] = heap[parent];
        holepos = parent;
    }
    heap[holepos] = blocknum;
    return DSTORE_SUCC;
}

void LogicalTapeBlockMgr::ForgetFreeSpace()
{
    m_forgetFreeSpace = true;
}

LogicalTape::LogicalTape() : m_tapeBlockMgr(nullptr), m_writingStatus(true), m_frozenStatus(false),
                             m_dataBufferDirty(false), m_firstBlockNumber(-1L), m_curBlockNumber(-1L),
                             m_nextBlockNumber(-1L), m_dataBuffer(nullptr), m_dataBufferSize(0),
                             m_nextReadWritePos(0), m_dataBufferCurValidSize(0)
{}

void LogicalTape::Destroy()
{
    m_tapeBlockMgr = nullptr;
    if (m_dataBuffer) {
        DstorePfree(m_dataBuffer);
        m_dataBuffer = nullptr;
    }
}

/*
 * Read as many blocks as we can into the per-tape buffer.
 *
 * Returns true if anything was read, 'false' on EOF.
 */
RetStatus LogicalTape::ReadFillBuffer(bool *readMore)
{
    m_nextReadWritePos = 0;
    m_dataBufferCurValidSize = 0;

    do {
        char       *thisbuf = m_dataBuffer + m_dataBufferCurValidSize;
        long        datablocknum = m_nextBlockNumber;

        /* Fetch next block number */
        if (datablocknum == -1L) {
            break; /* EOF */
        }

        /* Read the block */
        if (STORAGE_FUNC_FAIL(m_tapeBlockMgr->ReadBlock(datablocknum, static_cast<void *>(thisbuf)))) {
            return DSTORE_FAIL;
        }
        if (!m_frozenStatus) {
            RetStatus rc = m_tapeBlockMgr->ReleaseBlock(datablocknum);
            if (rc == DSTORE_FAIL) {
                return rc;
            }
        }
        m_curBlockNumber = m_nextBlockNumber;

        m_dataBufferCurValidSize += TapeBlockGetNBytes(thisbuf);
        if (TapeBlockIsLast(thisbuf)) {
            m_nextBlockNumber = -1L;
            /* EOF */
            break;
        } else {
            m_nextBlockNumber = TapeBlockGetLinkData(thisbuf)->next;
        }

        /* Advance to next block, if we have m_dataBuffer space left */
    } while (m_dataBufferSize - m_dataBufferCurValidSize > BLCKSZ);

    if (readMore != nullptr) {
        *readMore = (m_dataBufferCurValidSize > 0);
    }
    return DSTORE_SUCC;
}

RetStatus LogicalTape::InitReadBuffer()
{
    StorageAssert(m_dataBufferSize > 0);
    m_dataBuffer = static_cast<char*>(DstorePalloc(static_cast<uint32>(m_dataBufferSize)));
    if (unlikely(m_dataBuffer == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstorePalloc fail when InitReadBuffer."));
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        return DSTORE_FAIL;
    }
    /* Read the first block, or reset if tape is empty */
    m_nextBlockNumber = m_firstBlockNumber;
    m_nextReadWritePos = 0;
    m_dataBufferCurValidSize = 0;
    return ReadFillBuffer();
}

RetStatus LogicalTape::DumpCurBlock()
{
    if (!m_dataBufferDirty) {
        storage_set_error(LOGTAPE_ERROR_INVALID_LOGTAPE_STATE);
        ErrLog(DSTORE_ERROR, MODULE_COMMON,
               ErrMsg("[%s]invalid block status, bufferDirty is false", __FUNCTION__));
        return DSTORE_FAIL;
    }

    /* First allocate the next block, so that we can store it in the 'next' pointer of this block. */
    long nextBlockNum = m_tapeBlockMgr->GetFreeBlock();

    /* set the next-pointer and dump the current block. */
    TapeBlockGetLinkData(m_dataBuffer)->next = nextBlockNum;
    if (STORAGE_FUNC_FAIL(m_tapeBlockMgr->WriteBlock(m_curBlockNumber, static_cast<void *>(m_dataBuffer)))) {
        return DSTORE_FAIL;
    }

    /* initialize the prev-pointer of the next block */
    TapeBlockGetLinkData(m_dataBuffer)->prev = m_curBlockNumber;
    m_curBlockNumber = nextBlockNum;
    m_nextReadWritePos = 0;
    m_dataBufferCurValidSize = 0;
    return DSTORE_SUCC;
}

RetStatus LogicalTape::Write(void *ptr, size_t size)
{
    size_t      sizeThisTime;

    StorageAssert(m_writingStatus);

    /* Allocate data m_dataBuffer and first block on first write */
    if (m_dataBuffer == nullptr) {
        m_dataBuffer = static_cast<char *>(DstorePalloc(BLCKSZ));
        if (unlikely(m_dataBuffer == nullptr)) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstorePalloc fail when White."));
            storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
            return DSTORE_FAIL;
        }
        m_dataBufferSize = BLCKSZ;
    }
    if (m_curBlockNumber == -1) {
        StorageAssert(m_firstBlockNumber == -1);
        StorageAssert(m_nextReadWritePos == 0);

        m_curBlockNumber = m_tapeBlockMgr->GetFreeBlock();
        m_firstBlockNumber = m_curBlockNumber;

        TapeBlockGetLinkData(m_dataBuffer)->prev = -1L;
    }

    StorageAssert(m_dataBufferSize == BLCKSZ);
    while (size > 0) {
        if (m_nextReadWritePos >= TAPE_BLOCK_PAYLOAD_SIZE) {
            /* Buffer full, dump it out */
            if (STORAGE_FUNC_FAIL(DumpCurBlock())) {
                return DSTORE_FAIL;
            }
        }

        size_t remainBufferSize = sizeThisTime =
            static_cast<size_t>(static_cast<long>(TAPE_BLOCK_PAYLOAD_SIZE - m_nextReadWritePos));
        if (sizeThisTime > size) {
            sizeThisTime = size;
        }
        StorageAssert(sizeThisTime > 0);

        errno_t rc = memcpy_s(m_dataBuffer + m_nextReadWritePos, remainBufferSize, ptr, sizeThisTime);
        storage_securec_check(rc, "", "");

        m_dataBufferDirty = true;
        m_nextReadWritePos += static_cast<int>(sizeThisTime);
        if (m_dataBufferCurValidSize < m_nextReadWritePos) {
            m_dataBufferCurValidSize = m_nextReadWritePos;
        }
        ptr = static_cast<void *>(static_cast<char *>(ptr) + sizeThisTime);
        size -= sizeThisTime;
    }
    return DSTORE_SUCC;
}

/*
 * Rewind logical tape and switch from writing to reading.
 *
 * The tape must currently be in writing state, or "frozen" in read state.
 *
 * 'buffer_size' specifies how much memory to use for the read buffer.
 * Regardless of the argument, the actual amount of memory used is between BLCKSZ and MaxAllocSize,
 * and is a multiple of BLCKSZ.  The given value is rounded down and truncated to fit those constraints, if necessary.
 * If the tape is frozen, the 'buffer_size' argument is ignored, and a small BLCKSZ byte buffer is used.
 */
RetStatus LogicalTape::RewindForRead(size_t bufSize)
{
    /* need at least one block */
    if (bufSize < BLCKSZ) {
        bufSize = BLCKSZ;
    }

    if (bufSize > MaxAllocSize) {
        bufSize = MaxAllocSize;
    }

    /* round down to BLCKSZ boundary */
    bufSize -= bufSize % BLCKSZ;
    /* Completion of a write phase.  Flush last partial data block, and rewind for normal (destructive) read. */
    RetStatus ret = DSTORE_SUCC;
    if (m_dataBufferDirty) {
        TapeBlockSetNBytes(m_dataBuffer, m_dataBufferCurValidSize);
        ret = m_tapeBlockMgr->WriteBlock(m_curBlockNumber, static_cast<void *>(m_dataBuffer));
    }
    m_writingStatus = false;

    if (m_dataBuffer) {
        DstorePfree(m_dataBuffer);
    }

    /* the m_dataBuffer is lazily allocated, but set the size here */
    m_dataBuffer = nullptr;
    m_dataBufferSize = static_cast<int>(bufSize);
    return ret;
}

RetStatus LogicalTape::Read(void *ptr, size_t size, size_t &resultSize)
{
    resultSize = 0;
    StorageAssert(!m_writingStatus);

    if ((m_dataBuffer == nullptr) && STORAGE_FUNC_FAIL(InitReadBuffer())) {
        return DSTORE_FAIL;
    }

    RetStatus ret = DSTORE_SUCC;
    bool readMore = false;
    while (size > 0) {
        if (m_nextReadWritePos >= m_dataBufferCurValidSize) {
            /* Try to load more data into m_dataBuffer. */
            ret = ReadFillBuffer(&readMore);
            if (STORAGE_FUNC_FAIL(ret) || !readMore) {
                break; /* EOF */
            }
        }

        size_t sizeThisTime = static_cast<size_t>(static_cast<long>(m_dataBufferCurValidSize - m_nextReadWritePos));
        if (sizeThisTime > size) {
            sizeThisTime = size;
        }
        StorageAssert(sizeThisTime > 0);

        errno_t rc = memcpy_s(ptr, size, m_dataBuffer + m_nextReadWritePos, sizeThisTime);
        storage_securec_check(rc, "", "");

        m_nextReadWritePos += static_cast<int>(sizeThisTime);
        ptr = static_cast<void *>(static_cast<char *>(ptr) + sizeThisTime);
        size -= sizeThisTime;
        resultSize += sizeThisTime;
    }

    return ret;
}

/*
 * Once a tape is frozen, its contents will not be released until the LogicalTapeSet is destroyed.
 */
RetStatus LogicalTape::Freeze()
{
    StorageAssert(m_writingStatus);

    if (m_dataBufferDirty) {
        TapeBlockSetNBytes(m_dataBuffer, m_dataBufferCurValidSize);
        if (STORAGE_FUNC_FAIL(m_tapeBlockMgr->WriteBlock(m_curBlockNumber, static_cast<void *>(m_dataBuffer)))) {
            return DSTORE_FAIL;
        }
    }
    m_writingStatus = false;
    m_frozenStatus = true;

    if (!m_dataBuffer || m_dataBufferSize != BLCKSZ) {
        if (m_dataBuffer) {
            DstorePfree(m_dataBuffer);
        }
        m_dataBuffer = static_cast<char *>(DstorePalloc(BLCKSZ));
        if (unlikely(m_dataBuffer == nullptr)) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstorePalloc fail when Freeze."));
            storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
            return DSTORE_FAIL;
        }
        m_dataBufferSize = BLCKSZ;
    }

    /* Read the first block, or reset if tape is empty */
    m_curBlockNumber = m_firstBlockNumber;
    m_nextReadWritePos = 0;
    m_dataBufferCurValidSize = 0;

    if (m_firstBlockNumber == -1L) {
        m_nextBlockNumber = -1L;
    }
    if (STORAGE_FUNC_FAIL(m_tapeBlockMgr->ReadBlock(m_curBlockNumber, static_cast<void *>(m_dataBuffer)))) {
        return DSTORE_FAIL;
    }
    if (TapeBlockIsLast(m_dataBuffer)) {
        m_nextBlockNumber = -1L;
    } else {
        m_nextBlockNumber = TapeBlockGetLinkData(m_dataBuffer)->next;
    }
    m_dataBufferCurValidSize = TapeBlockGetNBytes(m_dataBuffer);
    return DSTORE_SUCC;
}

TapeBlockLinkData* LogicalTape::TapeBlockGetLinkData(char *buf) const
{
    return (static_cast<TapeBlockLinkData *>(static_cast<void*>(buf + TAPE_BLOCK_PAYLOAD_SIZE)));
}

bool LogicalTape::TapeBlockIsLast(char *buf) const
{
    return (TapeBlockGetLinkData(buf)->next < 0);
}

int LogicalTape::TapeBlockGetNBytes(char *buf) const
{
    if (TapeBlockIsLast(buf)) {
        return static_cast<int>(-TapeBlockGetLinkData(buf)->next);
    } else {
        return TAPE_BLOCK_PAYLOAD_SIZE;
    }
}

void LogicalTape::TapeBlockSetNBytes(char *buf, int nbytes) const
{
    TapeBlockGetLinkData(buf)->next = -(nbytes);
}

RetStatus LogicalTape::GetNextTupleLen(unsigned int &nextTupleLen)
{
    size_t resultSize = 0;
    if (STORAGE_FUNC_FAIL(Read(&nextTupleLen, sizeof(nextTupleLen), resultSize))) {
        return DSTORE_FAIL;
    }
    if (resultSize != sizeof(nextTupleLen)) {
        storage_set_error(TUPLESORT_ERROR_UNEXPECTED_END_OF_TAPE);
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

void LogicalTape::SetLogicalTapeBlockMgr(LogicalTapeBlockMgr *tapeBlockMgr)
{
    m_tapeBlockMgr = tapeBlockMgr;
}
}
