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
 * IDENTIFICATION
 *        include/common/algorithm/dstore_logtape.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_LOGTAPE_H
#define DSTORE_LOGTAPE_H

#include "common/dstore_datatype.h"
#include "common/memory/dstore_mctx.h"
#include "common/algorithm/fakefile/dstore_buffile.h"

namespace DSTORE {

class LogicalTapeBlockMgr : public BaseObject {
public:
    LogicalTapeBlockMgr(const char *tapeBaseName);
    virtual ~LogicalTapeBlockMgr() = default;
    RetStatus Init();
    void Destroy();

    void ForgetFreeSpace();
    RetStatus WriteBlock(long blocknum, void *buffer);
    RetStatus ReadBlock(long blocknum, void *buffer);
    long GetFreeBlock();
    RetStatus ReleaseBlock(long blocknum);
private:
    BufFile    *m_pfile;
    long        m_nBlocksAllocated;
    long        m_nBlocksWritten; /* num of blocks used in underlying file */

    bool        m_forgetFreeSpace; /* true for not writing anymore */
    long       *m_freeBlocks;
    long        m_nFreeBlocks;
    Size        m_freeBlocksLen;
    /* base name for temp file of tapemgr, used by BufFile,
     * and should not be freed of the lifetime of LogicalTapeBlockMgr
     */
    const char *m_tapeBaseName;
};

/*
 * A TapeBlockLinkData is stored at the end of each BLCKSZ block.
 */
struct TapeBlockLinkData {
    long prev;           /* previous block on this tape, or -1 on first block */
    long next;           /* next block on this tape, or # of valid bytes on last block (if < 0) */
};

constexpr int TAPE_BLOCK_PAYLOAD_SIZE = BLCKSZ - sizeof(TapeBlockLinkData);

class LogicalTape : public BaseObject {
public:
    LogicalTape();
    virtual ~LogicalTape() = default;
    void Destroy();

    RetStatus Write(void *ptr, size_t size);
    RetStatus Read(void *ptr, size_t size, size_t &resultSize);
    RetStatus Freeze();
    RetStatus RewindForRead(size_t bufSize);

    RetStatus InitReadBuffer();
    RetStatus ReadFillBuffer(bool *readMore = nullptr);

    RetStatus GetNextTupleLen(unsigned int &nextTupleLen);

    void SetLogicalTapeBlockMgr(LogicalTapeBlockMgr *tapeBlockMgr);
private:
    LogicalTapeBlockMgr *m_tapeBlockMgr; /* palloc or free by caller */
    bool        m_writingStatus;
    bool        m_frozenStatus;
    bool        m_dataBufferDirty;

    long        m_firstBlockNumber;
    long        m_curBlockNumber;
    long        m_nextBlockNumber;

    char       *m_dataBuffer;
    int         m_dataBufferSize;
    int         m_nextReadWritePos;
    int         m_dataBufferCurValidSize;

    RetStatus DumpCurBlock();
    TapeBlockLinkData *TapeBlockGetLinkData(char *buf) const;
    bool TapeBlockIsLast(char *buf) const;
    int  TapeBlockGetNBytes(char *buf) const;
    void TapeBlockSetNBytes(char *buf, int nbytes) const;
};
}
#endif
