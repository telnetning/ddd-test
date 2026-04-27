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
 * dstore_wal_reader.cpp
 *
 * IDENTIFICATION
 * src/wal/dstore_wal_reader.cpp
 *
 * ---------------------------------------------------------------------------------------
 *
 */

#include "wal/dstore_wal_file_manager.h"
#include "common/log/dstore_log.h"
#include "wal/dstore_wal_utils.h"
#include "wal/dstore_wal_perf_unit.h"
#include "wal/dstore_wal_reader.h"

namespace DSTORE {
constexpr uint32 WAL_GROUP_HEADER_SIZE = offsetof(WalRecordAtomicGroup, walRecords);
constexpr uint32 WAL_GROUP_LEN_SIZE = sizeof((static_cast<WalRecordAtomicGroup*>(nullptr))->groupLen);

WalDioReadAdaptor::WalDioReadAdaptor(DstoreMemoryContext memoryContext, uint32 alignSize)
    : m_memoryContext(memoryContext),
    m_alignLen(alignSize),
    m_memory(nullptr),
    m_memorySize(0)
{
}

WalDioReadAdaptor::~WalDioReadAdaptor()
{
    if (m_memory != nullptr) {
        DstorePfreeAligned(m_memory);
        m_memory = nullptr;
    }
    m_memorySize = 0;
    m_memoryContext = nullptr;
}

RetStatus WalDioReadAdaptor::Read(uint8 *data, uint64 readLen, WalFile *walFile, uint64 offset, int64 *resultLen)
{
    if (unlikely(readLen == 0)) {
        *resultLen = 0;
        ErrLog(DSTORE_WARNING, MODULE_WAL, ErrMsg("AdaptDioRead len is 0."));
        return DSTORE_SUCC;
    }
    if (unlikely(DstoreIsAlignedAddr(data, m_alignLen) && (readLen % m_alignLen == 0) && (offset % m_alignLen == 0))) {
        if (STORAGE_FUNC_FAIL(walFile->Read(data, readLen, static_cast<off_t>(offset), resultLen))) {
            ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("WalDioReadAdaptor direct read offset:%lu len:%lu failed",
                offset, readLen));
            return DSTORE_FAIL;
        }
        return DSTORE_SUCC;
    }

    uint64 adaptedOffset = offset - offset % m_alignLen;
    uint64 adaptedLen = ((offset + readLen - 1) / m_alignLen + 1) * m_alignLen - adaptedOffset;

    if (STORAGE_FUNC_FAIL(AdaptDioBufferSize(adaptedLen))) {
        return DSTORE_FAIL;
    }
    int64 readSize;
    if (STORAGE_FUNC_FAIL(walFile->Read(m_memory, adaptedLen, static_cast<off_t>(adaptedOffset), &readSize))) {
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("WalDioReadAdaptor read offset:%lu len:%lu failed.",
            adaptedOffset, adaptedLen));
        return DSTORE_FAIL;
    }
    if (unlikely(readSize < 0 || static_cast<uint64>(readSize) > adaptedLen)) {
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("AdaptDioRead read invalid len:%ld expected adapted len:%lu.",
            readSize, adaptedLen));
        return DSTORE_FAIL;
    }
    uint32 targetDataOffset = offset % m_alignLen;
    int rc = memcpy_s(data, readLen, m_memory + targetDataOffset, readLen);
    storage_securec_check(rc, "\0", "\0");
    *resultLen = readSize <= static_cast<int64>(targetDataOffset) ? 0
                 : static_cast<uint64>(readSize) >= static_cast<uint64>(targetDataOffset) + readLen
                     ? readLen
                     : static_cast<uint32>(readSize) - targetDataOffset;
    return DSTORE_SUCC;
}

RetStatus WalDioReadAdaptor::AdaptDioBufferSize(uint32 targetLen)
{
    if (targetLen <= m_memorySize) {
        return DSTORE_SUCC;
    }

    if (m_memory != nullptr) {
        DstorePfreeAligned(m_memory);
    }
    uint32 newLen = (targetLen / m_alignLen + 1) * m_alignLen;
    m_memory = static_cast<uint8 *>(DstorePallocAlignedHugeMemory(newLen, m_alignLen, m_memoryContext));
    if (m_memory == nullptr) {
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("AdaptDioBufferSize to size %u failed.", newLen));
        return DSTORE_FAIL;
    }
    m_memorySize = newLen;
    return DSTORE_SUCC;
}

WalGroupParser::WalGroupParser(DstoreMemoryContext memoryContext, const WalGroupParserInitParam &param)
    : m_memoryContext(memoryContext),
    m_readSource(param.readSource),
    m_fileReader(param.fileReader),
    m_walReadBuffer(param.walReadBuffer),
    m_readPreBuf(nullptr),
    m_readBuf(nullptr),
    m_readRecordBuf(nullptr),
    m_lastReadBlockLen(0),
    m_lastReadBlockPlsn(INVALID_END_PLSN),
    m_readBlockSize(param.readBlockSize),
    m_walFileSize(param.walFileSize),
    m_curReadPlsn(param.curReadPlsn),
    m_readNextStartPlsn(param.curReadPlsn),
    m_needCheckGroupHeader(true),
    m_groupHeaderChecked(false),
    m_segHeadOff(0),
    m_blockStartPlsn(0)
{
}

WalGroupParser::~WalGroupParser()
{
    m_memoryContext = nullptr;
    m_fileReader = nullptr;
    m_walReadBuffer = nullptr;
    if (m_readSource == WalReadSource::WAL_READ_FROM_DISK) {
        DstorePfreeExt(m_readPreBuf);
        DstorePfreeExt(m_readRecordBuf);
    }
    m_readPreBuf = nullptr;
    m_readBuf = nullptr;
    m_readRecordBuf = nullptr;
}

RetStatus WalGroupParser::Init()
{
    if (m_readBlockSize <= WAL_GROUP_HEADER_SIZE) {
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("Reader ReadBlockSize is too small."));
        return DSTORE_FAIL;
    }
    if (m_readSource == WalReadSource::WAL_READ_FROM_DISK) {
        if (m_fileReader == nullptr) {
            ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("file reader is null when read from disk."));
        }
        if (m_readPreBuf == nullptr) {
            m_readPreBuf = (uint8 *) DstoreMemoryContextAllocZero(m_memoryContext, m_readBlockSize + m_readBlockSize);
            if (unlikely(m_readPreBuf == nullptr)) {
                storage_set_error(WAL_ERROR_INIT_ALLOC_OOM);
                ErrLog(DSTORE_PANIC, MODULE_WAL,
                       ErrMsg("Allocate space OOM, allocate m_readPreBuf fail when WalRecordReader Init."));
                return DSTORE_FAIL;
            }
        }
        m_readBuf = m_readPreBuf + m_readBlockSize;
    } else if (m_readSource == WalReadSource::WAL_READ_FROM_BUFFER) {
        if (m_walReadBuffer == nullptr) {
            ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("wal read buffer is null when read from buffer."));
        }
    }
    return DSTORE_SUCC;
}

/*
 * Read Wal group from the header of the block(not exist actually) where the target Wal is located.
 */
RetStatus WalGroupParser::ReadBlock(uint64 targetBlockPlsn, uint64 *resultLen)
{
    /*
     * check whether we have all the requested data already.
     * for standby redo, need m_lastReadBlockLen == m_readBlockSize.
     */
    if (m_lastReadBlockPlsn == targetBlockPlsn && m_lastReadBlockLen == m_readBlockSize) {
        *resultLen = m_lastReadBlockLen;
        return DSTORE_SUCC;
    }

    /*
     * Note that the requested length may not be the same as the actual read length(resultLen). For example,
     * if the segment file contains only 2 KB data, only 2 KB data may be returned when 8 KB data is requested.
     */
    RetStatus result;
    if (m_readSource == WalReadSource::WAL_READ_FROM_BUFFER) {
        result = m_walReadBuffer->ReadFromBuffer(targetBlockPlsn, m_readBlockSize, resultLen, &m_readBuf);
    } else {
        result = m_fileReader->Read(targetBlockPlsn, m_readBuf, m_readBlockSize, resultLen);
    }
    if (STORAGE_FUNC_FAIL(result)) {
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("ReadBlock call WalStreamBytesReader Read() return fail."));
        return DSTORE_FAIL;
    }

    /* Updates the current read status variable. */
    m_lastReadBlockPlsn = targetBlockPlsn;
    m_lastReadBlockLen = *resultLen;

    return DSTORE_SUCC;
}

/* Verify the basic correctness of the Wal group. */
RetStatus WalGroupParser::ValidWalGroupLen(const WalRecordAtomicGroup *&curGroup)
{
    if (unlikely(!m_needCheckGroupHeader)) {
        return DSTORE_SUCC;
    }
    RetStatus retStatus = DSTORE_SUCC;
    StorageReleasePanic(curGroup == nullptr, MODULE_WAL, ErrMsg("ValidWalGroupLen get invalid walgroup."));
    if (unlikely(curGroup->groupLen < WAL_GROUP_HEADER_SIZE || curGroup->groupLen >= WAL_GROUP_MAX_SIZE)) {
        ErrLog(DSTORE_WARNING, MODULE_WAL,
            ErrMsg("Valid group len failed, wrong group length: %u, current read plsn: %lu, reach to the end of wal.",
            curGroup->groupLen, m_curReadPlsn));
        retStatus = DSTORE_FAIL;
    }
    m_groupHeaderChecked = true;
    return retStatus;
}

void WalGroupParser::SetReadStartPlsn(uint64 startPlsn)
{
    m_curReadPlsn = startPlsn;
    m_readNextStartPlsn = startPlsn;
}

/* Handling errors during reading */
void WalGroupParser::ClearReadBufState()
{
    m_lastReadBlockLen = 0;
    m_lastReadBlockPlsn = INVALID_END_PLSN;
}

uint64 WalGroupParser::GetCurRecordEndPlsn(uint64 offset) const
{
    return WalUtils::GetRecordPlsn<true>(m_curReadPlsn, sizeof(WalRecordAtomicGroup) + offset, m_walFileSize);
}

void WalGroupParser::SetNeedCheckGroupHeader(bool check)
{
    m_needCheckGroupHeader = check;
}

uint64 WalGroupParser::GetCurGroupStartPlsn() const
{
    return m_curReadPlsn;
}

uint64 WalGroupParser::GetCurGroupEndPlsn() const
{
    return m_readNextStartPlsn;
}

void WalGroupParser::SetNewReadConf(const WalGroupParserInitParam &param)
{
    m_fileReader = param.fileReader;
    m_walReadBuffer = param.walReadBuffer;
    m_walFileSize = param.walFileSize;
    m_readSource = param.readSource;
}

bool WalGroupParser::NeedFreeCurGroup(const WalRecordAtomicGroup *group) const
{
    if (m_readSource == WalReadSource::WAL_READ_FROM_DISK) {
        return false;
    }
    if (m_readRecordBuf != nullptr && m_readRecordBuf == static_cast<const uint8*>(static_cast<const void*>(group))) {
        return true;
    }
    return false;
}

void WalGroupParser::ResetReadState()
{
    if (m_readSource == WalReadSource::WAL_READ_FROM_DISK) {
        DstorePfreeExt(m_readRecordBuf);
    }
    m_readRecordBuf = nullptr;
    m_groupHeaderChecked = false;
    m_segHeadOff = 0;
    m_blockStartPlsn = 0;
}

RetStatus WalGroupParser::ValidWalGroup(const WalRecordAtomicGroup *&curGroup) const
{
    uint32 offset = offsetof(WalRecordAtomicGroup, crc) + sizeof((static_cast<WalRecordAtomicGroup*>(nullptr))->crc);
    const uint8 *recordGroupData = reinterpret_cast<const uint8*>(curGroup);
    uint32 checkSum = CompChecksum(recordGroupData + offset, curGroup->groupLen - offset, CHECKSUM_CRC);
    if (curGroup->crc != checkSum) {
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus WalGroupParser::ReadWholeGroup(uint64 groupAlreadyReadLen, uint64 groupLen,
    const WalRecordAtomicGroup *&curGroup, uint64 &nextGroupStartPlsn)
{
    uint32 hdrLenIncluded = 0;
    uint64 curDataLen = 0;
    uint64 totalReadLenFromBuf = 0;
    do {
        /* During the last read, the data of a block size does not completely contains the data to be read.
           Therefore, the data continues to be read from the next block. */
        m_blockStartPlsn += m_readBlockSize;
        hdrLenIncluded = m_blockStartPlsn % m_walFileSize == 0 ? WAL_FILE_HDR_SIZE : 0;
        /* A maximum of one block can be read at a time. If not all data is read, read next time. */
        if (STORAGE_FUNC_FAIL(ReadBlock(m_blockStartPlsn, &totalReadLenFromBuf))) {
            ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("Read block at plsn %lu fail.", m_blockStartPlsn));
            return DSTORE_FAIL;
        }
        if (totalReadLenFromBuf < DstoreMin((groupLen - groupAlreadyReadLen) + hdrLenIncluded, m_readBlockSize)) {
            ErrLog(DSTORE_LOG, MODULE_WAL,
                ErrMsg("Read whole group fail, read len %lu from buffer at block start plsn %lu not enough, group "
                       "len is %lu, group already read len %lu, reach to the end of wal.",
                totalReadLenFromBuf, m_blockStartPlsn, groupLen, groupAlreadyReadLen));
            return DSTORE_FAIL;
        }

        /* In the case of across segment, the data after the header is the subsequent data of the recordGroup. */
        curDataLen = totalReadLenFromBuf - hdrLenIncluded;
        if (curDataLen > groupLen - groupAlreadyReadLen) {
            curDataLen = groupLen - groupAlreadyReadLen;
        }
        if (m_readRecordBuf != nullptr) {
            errno_t rc = memcpy_s(m_readRecordBuf + groupAlreadyReadLen, groupLen - groupAlreadyReadLen,
                m_readBuf + hdrLenIncluded, curDataLen);
            storage_securec_check(rc, "\0", "\0");
        }
        groupAlreadyReadLen += curDataLen;
        if (!m_groupHeaderChecked && groupAlreadyReadLen >= WAL_GROUP_LEN_SIZE) {
            /* This block contains at least the header of the recordGroup. */
            if (STORAGE_FUNC_FAIL(ValidWalGroupLen(curGroup))) {
                return DSTORE_FAIL;
            }
        }
    } while (groupAlreadyReadLen < groupLen);
    nextGroupStartPlsn = m_blockStartPlsn + curDataLen + hdrLenIncluded;
    return DSTORE_SUCC;
}

/* Read the required Wal group from memory(read from disk). */
RetStatus WalGroupParser::FetchWalRecordGroupToBuffer(uint64 groupAlreadyReadLen, uint64 groupLen,
    const WalRecordAtomicGroup *&curGroup)
{
    /* The data read from the block does not cover the target Wal group. */
    if (m_readRecordBuf == nullptr && (m_readSource == WalReadSource::WAL_READ_FROM_DISK ||
        (m_curReadPlsn % m_walFileSize) + groupLen > m_walFileSize ||
        m_walReadBuffer->ReadRangeReachBufferEnd(m_curReadPlsn, groupLen))) {
        /* Construct the space for storing the currently read group. */
        m_readRecordBuf = static_cast<uint8 *>(DstoreMemoryContextAllocZero(m_memoryContext, groupLen));
        if (unlikely(m_readRecordBuf == nullptr)) {
            storage_set_error(WAL_ERROR_INIT_ALLOC_OOM);
            ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("Allocate space OOM, allocate m_readRecordBuf fail."));
            return DSTORE_FAIL;
        }
        /* Copy the available data read last time to the m_readRecordBuf. */
        errno_t rc = memcpy_s(m_readRecordBuf, groupAlreadyReadLen, curGroup, groupAlreadyReadLen);
        storage_securec_check(rc, "\0", "\0");
        curGroup = STATIC_CAST_PTR_TYPE(m_readRecordBuf, WalRecordAtomicGroup *);
    }
    uint64 nextGroupStartPlsn = 0;
    if (ReadWholeGroup(groupAlreadyReadLen, groupLen, curGroup, nextGroupStartPlsn) == DSTORE_FAIL) {
        ErrLog(DSTORE_WARNING, MODULE_WAL, ErrMsg("Fetch wal record group to buffer fail, reach to the end of wal."));
        return DSTORE_FAIL;
    }

    /* Verify the CRC of the read recordGroup. */
    if (ValidWalGroup(curGroup) == DSTORE_FAIL) {
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("Check wal group crc fail, reach to the end of wal."));
        return DSTORE_FAIL;
    }
    m_readNextStartPlsn = nextGroupStartPlsn;
    return DSTORE_SUCC;
}

void WalGroupParser::GetGroupLenToRecordBuf(uint64 groupAlreadyReadLen, uint64 &groupLen, uint64 startPosOffInBlk,
    uint64 leftLen)
{
    static_assert(sizeof(uint32) == WAL_GROUP_LEN_SIZE, "Wal group type is not uint32");
    uint8 *groupLenBuf = STATIC_CAST_PTR_TYPE(&groupLen, uint8*);
    errno_t rc = memcpy_s(groupLenBuf, leftLen, m_readPreBuf + startPosOffInBlk, leftLen);
    storage_securec_check(rc, "\0", "\0");
    rc = memcpy_s(groupLenBuf + leftLen, WAL_GROUP_LEN_SIZE - leftLen, m_readBuf + m_segHeadOff,
        WAL_GROUP_LEN_SIZE - leftLen);
    storage_securec_check(rc, "\0", "\0");

    m_readRecordBuf = static_cast<uint8 *>(DstoreMemoryContextAllocZero(m_memoryContext, groupLen));
    if (unlikely(m_readRecordBuf == nullptr)) {
        storage_set_error(WAL_ERROR_INIT_ALLOC_OOM);
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("Allocate space OOM, allocate m_readRecordBuf fail."));
        return;
    }
    rc = memcpy_s(m_readRecordBuf, WAL_GROUP_LEN_SIZE, groupLenBuf, WAL_GROUP_LEN_SIZE);
    storage_securec_check(rc, "\0", "\0");

    uint64 groupRemainDataLen = DstoreMin(groupAlreadyReadLen, groupLen) - WAL_GROUP_LEN_SIZE;
    rc = memcpy_s(m_readRecordBuf + WAL_GROUP_LEN_SIZE, groupRemainDataLen,
        m_readBuf + m_segHeadOff + (WAL_GROUP_LEN_SIZE - leftLen), groupRemainDataLen);
    storage_securec_check(rc, "\0", "\0");
}

RetStatus WalGroupParser::GetGroupLenCrossTwoBlock(uint64 &groupAlreadyReadLen, uint64 &totalReadLenFromBuf,
    uint64 &groupLen, const WalRecordAtomicGroup *&curGroup)
{
    uint64 startPosOffInBlk = m_curReadPlsn % m_readBlockSize;
    uint64 leftLen = m_readBlockSize - startPosOffInBlk;

    /* Restore the left length of group length to pre buf. */
    if (m_readSource == WalReadSource::WAL_READ_FROM_DISK) {
        errno_t rc = memcpy_s(m_readPreBuf + startPosOffInBlk, leftLen, m_readBuf + startPosOffInBlk, leftLen);
        storage_securec_check(rc, "\0", "\0");
    } else {
        m_readPreBuf = m_readBuf;
    }

    m_blockStartPlsn += m_readBlockSize;
    if (STORAGE_FUNC_FAIL(ReadBlock(m_blockStartPlsn, &totalReadLenFromBuf))) {
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("Read block at plsn %lu fail.", m_blockStartPlsn));
        return DSTORE_FAIL;
    }

    m_segHeadOff = m_blockStartPlsn % m_walFileSize == 0 ? WAL_FILE_HDR_SIZE : 0;
    /* GotLen is the available part read from the WAL file. */
    groupAlreadyReadLen = (totalReadLenFromBuf - m_segHeadOff) + leftLen;
    if (totalReadLenFromBuf <= m_segHeadOff || groupAlreadyReadLen < WAL_GROUP_LEN_SIZE) {
        ErrLog(DSTORE_LOG, MODULE_WAL,
            ErrMsg("Get group len across two block fail, group already read len is %lu, read from buffer size %lu is "
                   "not enough at block start plsn %lu , reach to the end of wal.",
            groupAlreadyReadLen, totalReadLenFromBuf, m_blockStartPlsn));
        curGroup = nullptr;
        return DSTORE_SUCC;
    }
    if (m_segHeadOff == WAL_FILE_HDR_SIZE || (m_readSource == WalReadSource::WAL_READ_FROM_BUFFER &&
        m_walReadBuffer->ReadRangeReachBufferEnd(m_curReadPlsn, WAL_GROUP_LEN_SIZE))) {
        GetGroupLenToRecordBuf(groupAlreadyReadLen, groupLen, startPosOffInBlk, leftLen);
        curGroup = STATIC_CAST_PTR_TYPE(m_readRecordBuf, WalRecordAtomicGroup *);
    } else {
        curGroup = STATIC_CAST_PTR_TYPE(m_readPreBuf + startPosOffInBlk, WalRecordAtomicGroup*);
        groupLen = curGroup->groupLen;
    }

    if (STORAGE_FUNC_FAIL(ValidWalGroupLen(curGroup))) {
        curGroup = nullptr;
        return DSTORE_SUCC;
    }

    return DSTORE_SUCC;
}

RetStatus WalGroupParser::ForwardToGroupStartPos(uint64 totalReadLenFromBuf, uint64 &startPosOffInBlk)
{
    /* If start plsn is at the beginning of wal file, move it to first group start plsn. */
    uint64 startPosOffInFile = m_curReadPlsn % m_walFileSize;
    /* Start plsn is not at a segment header. */
    if (startPosOffInFile >= WAL_FILE_HDR_SIZE) {
        return DSTORE_SUCC;
    }
    /* Below, the start plsn is at a segment header. */
    if (totalReadLenFromBuf <= WAL_FILE_HDR_SIZE) {
        ErrLog(DSTORE_LOG, MODULE_WAL,
            ErrMsg("Read len %lu is not enough for wal file header size %u, reach to the end of wal.",
            totalReadLenFromBuf, static_cast<uint32>(WAL_FILE_HDR_SIZE)));
        return DSTORE_FAIL;
    }
    WalFileHeaderData *header = STATIC_CAST_PTR_TYPE(m_readBuf, WalFileHeaderData *);
    m_curReadPlsn += (WAL_FILE_HDR_SIZE + header->lastRecordRemainLen - startPosOffInFile);
    startPosOffInBlk = WAL_FILE_HDR_SIZE + header->lastRecordRemainLen;
    return DSTORE_SUCC;
}

RetStatus WalGroupParser::ReadGroupLen(uint64 &groupAlreadyReadLen, uint64 &totalReadLenFromBuf,
    uint64 &startPosOffInBlk)
{
    if (STORAGE_FUNC_FAIL(ReadBlock(m_blockStartPlsn, &totalReadLenFromBuf))) {
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("Read block at start plsn %lu failed.", m_blockStartPlsn));
        return DSTORE_FAIL;
    }

    /* Read length can not reach start plsn position. */
    if (totalReadLenFromBuf <= startPosOffInBlk) {
        ErrLog(DSTORE_DEBUG1, MODULE_WAL,
            ErrMsg("Read group len fail, read len %lu is not enough for offset %lu in block which start plsn %lu, "
                   "reach to the end of wal.",
            totalReadLenFromBuf, startPosOffInBlk, m_blockStartPlsn));
        return DSTORE_FAIL;
    }

    if (STORAGE_FUNC_FAIL(ForwardToGroupStartPos(totalReadLenFromBuf, startPosOffInBlk))) {
        ErrLog(DSTORE_WARNING, MODULE_WAL,
            ErrMsg("Forward to group start position fail, offset %lu in block which start plsn is %lu, reach to the "
                   "end of wal.",
            startPosOffInBlk, m_blockStartPlsn));
        return DSTORE_FAIL;
    }

    groupAlreadyReadLen = totalReadLenFromBuf - startPosOffInBlk;

    return DSTORE_SUCC;
}

RetStatus WalGroupParser::GetGroupLen(uint64 &groupAlreadyReadLen, uint64 &totalReadLenFromBuf, uint64 &groupLen,
    const WalRecordAtomicGroup *&curGroup)
{
    /* Obtain the the block start plsn where the Wal group to be read is located. */
    m_blockStartPlsn = m_curReadPlsn - m_curReadPlsn % m_readBlockSize;
    /* Offset of the Wal group start plsn on the read block. */
    uint64 startPosOffInBlk = m_curReadPlsn % m_readBlockSize;

    if (STORAGE_FUNC_FAIL(ReadGroupLen(groupAlreadyReadLen, totalReadLenFromBuf, startPosOffInBlk))) {
        curGroup = nullptr;
        return DSTORE_SUCC;
    }

    /* If read size less than size of group len, that is reach the end of wal or group len cross two block. */
    if (groupAlreadyReadLen < WAL_GROUP_LEN_SIZE) {
        if (totalReadLenFromBuf < m_readBlockSize) {
            ErrLog(DSTORE_LOG, MODULE_WAL,
                ErrMsg("Read len %lu is not enough for %u when get group len, reach to the end of wal.",
                groupAlreadyReadLen, static_cast<uint32>(WAL_GROUP_LEN_SIZE)));
            curGroup = nullptr;
            return DSTORE_SUCC;
        }
        StorageAssert(totalReadLenFromBuf == m_readBlockSize);
        return GetGroupLenCrossTwoBlock(groupAlreadyReadLen, totalReadLenFromBuf, groupLen, curGroup);
    }

    /* Read size is enough to parse group len, set group pointer to it. */
    curGroup = STATIC_CAST_PTR_TYPE(m_readBuf + startPosOffInBlk, WalRecordAtomicGroup *);
    if (STORAGE_FUNC_FAIL(ValidWalGroupLen(curGroup))) {
        curGroup = nullptr;
        return DSTORE_SUCC;
    }

    groupLen = curGroup->groupLen;
    /* Read block does not contain file header, set head offset to 0. */
    m_segHeadOff = 0;
    return DSTORE_SUCC;
}

RetStatus WalGroupParser::GetGroupData(uint64 groupAlreadyReadLen, uint64 totalReadLenFromBuf, uint64 groupLen,
    const WalRecordAtomicGroup *&curGroup)
{
    /* If the content read from the WAL file contains the whole group content. */
    if (groupAlreadyReadLen >= groupLen) {
        /* Verify the CRC of the read recordGroup. */
        if (ValidWalGroup(curGroup) == DSTORE_FAIL) {
            ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("Check wal group crc fail, reach to the end of wal."));
            return DSTORE_FAIL;
        }
        /* After current read process complete, record the endPlsn for the next reading.
         * Essential, bond to WAL buffer insert logic.
         */
        m_readNextStartPlsn = m_curReadPlsn + groupLen + m_segHeadOff;
    } else {
        /* If read size less than block size, reach the end of wal, can not fetch whole group. */
        if (totalReadLenFromBuf < m_readBlockSize) {
            curGroup = nullptr;
            return DSTORE_SUCC;
        }

        if (FetchWalRecordGroupToBuffer(groupAlreadyReadLen, groupLen, curGroup) == DSTORE_FAIL) {
            curGroup = nullptr;
            return DSTORE_SUCC;
        }
    }
    return DSTORE_SUCC;
}

RetStatus WalGroupParser::ReadCurrent(const WalRecordAtomicGroup *&curGroup)
{
    /* The totalReadLenFromBuf is the total data length read from buffer now */
    uint64 totalReadLenFromBuf = 0;
    /* The groupAlreadyReadLen is the record group data length read from buffer now,
     * that is totalReadLenFromBuf - file header size(if included) - page header size(if included).
     */
    uint64 groupAlreadyReadLen = 0;
    uint64 groupLen = 0;
    /* Update cur read plsn to next start plsn */
    m_curReadPlsn = m_readNextStartPlsn;

    if (GetGroupLen(groupAlreadyReadLen, totalReadLenFromBuf, groupLen, curGroup) == DSTORE_FAIL) {
        ErrLog(DSTORE_WARNING, MODULE_WAL,
            ErrMsg("Get group header fail, lastReadEndPlsn %lu, curReadPlsn %lu.", m_readNextStartPlsn, m_curReadPlsn));
        return DSTORE_FAIL;
    }

    if (curGroup == nullptr) {
        return DSTORE_SUCC;
    }

    if (GetGroupData(groupAlreadyReadLen, totalReadLenFromBuf, groupLen, curGroup) == DSTORE_FAIL) {
        ErrLog(DSTORE_WARNING, MODULE_WAL, ErrMsg("Get group data fail."));
        return DSTORE_FAIL;
    }

    return DSTORE_SUCC;
}

WalRecordReader::WalRecordReader(DstoreMemoryContext memoryContext, const WalReaderConf &conf)
    : m_memoryContext(memoryContext),
    m_walId(conf.walId),
    m_fileReader(conf.fileReader),
    m_walReadBuffer(conf.walReadBuffer),
    m_groupParser(nullptr),
    parserParam(WalGroupParserInitParam{
        .fileReader = conf.fileReader,
        .walReadBuffer = conf.walReadBuffer,
        .curReadPlsn = conf.startPlsn,
        .walFileSize = conf.walFileSize,
        .readSource = conf.walReadSource,
        .readBlockSize = DstoreMin(WAL_READ_BUFFER_BLOCK_SIZE, conf.walFileSize)
    }),
    m_lastReadRecordGroupStartPlsn(0),
    m_curGroup(nullptr),
    m_curRecordIndex(0),
    m_offset(0)
{
}

WalRecordReader::~WalRecordReader()
{
    if (m_groupParser != nullptr) {
        delete m_groupParser;
    }
    m_curGroup = nullptr;
    m_memoryContext = nullptr;
    m_fileReader = nullptr;
    m_walReadBuffer = nullptr;
    m_walId = INVALID_WAL_ID;
}

RetStatus WalRecordReader::Init()
{
    /* Initializes the memory for caching data from Wal files. */
    m_groupParser = DstoreNew(m_memoryContext)WalGroupParser(m_memoryContext, parserParam);
    if (unlikely(m_groupParser == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("Allocate Wal group parser failed"));
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(m_groupParser->Init())) {
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("Allocate Wal group parser failed"));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

/* Handling errors during reading */
void WalRecordReader::ClearReadBufState()
{
    m_groupParser->ClearReadBufState();
}

void WalRecordReader::SetReadStartPlsn(uint64 startPlsn)
{
    m_groupParser->SetReadStartPlsn(startPlsn);

    m_lastReadRecordGroupStartPlsn = 0;
    ResetReadState();
    ClearReadBufState();
}

void WalRecordReader::SetNewReadConf(const WalReaderConf &newConf)
{
    m_walId = newConf.walId;
    m_fileReader = newConf.fileReader;
    m_walReadBuffer = newConf.walReadBuffer;
    WalGroupParserInitParam param = {
        .fileReader = newConf.fileReader,
        .walReadBuffer = newConf.walReadBuffer,
        .curReadPlsn = newConf.startPlsn,
        .walFileSize = newConf.walFileSize,
        .readSource = newConf.walReadSource,
        /* readBlockSize can not be set */
        .readBlockSize = 0
    };
    m_groupParser->SetNewReadConf(param);
    SetReadStartPlsn(newConf.startPlsn);
}

RetStatus WalRecordReader::AllocateWalReader(const WalReaderConf &conf, WalRecordReader **reader,
                                             DstoreMemoryContext memoryContext)
{
    /* Step1: Check the WalReaderConf */
    if ((conf.fileReader == nullptr && conf.walReadBuffer == nullptr) || conf.walFileSize <= WAL_FILE_HDR_SIZE) {
        *reader = nullptr;
        storage_set_error(WAL_ERROR_UNREACHABLE_CODE);
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("The given conf is invalid."));
        return DSTORE_FAIL;
    }

    /* Step2: Allocate a WalRecordReader */
    (*reader) = DstoreNew(memoryContext) WalRecordReader(memoryContext, conf);
    if (unlikely(*reader == nullptr)) {
        storage_set_error(WAL_ERROR_UNREACHABLE_CODE);
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("Allocate Wal reader memory failed"));
        return DSTORE_FAIL;
    }
    RetStatus walReaderInitResult = (*reader)->Init();
    if (STORAGE_FUNC_FAIL(walReaderInitResult)) {
        delete *reader;
        *reader = nullptr;
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("Wal record reader init failed"));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

void WalRecordReader::ResetReadState()
{
    m_curGroup = nullptr;
    m_groupParser->ResetReadState();
}

RetStatus WalRecordReader::ReadNext(const WalRecordAtomicGroup **recordGroup)
{
    LatencyStat::Timer timer(&WalPerfUnit::GetInstance().m_walRedoReadNextWalGroup);

    ResetReadState();

    RetStatus retStatus = m_groupParser->ReadCurrent(m_curGroup);
    *recordGroup = m_curGroup;

    if (STORAGE_FUNC_FAIL(retStatus) || *recordGroup == nullptr) {
        ClearReadBufState();
    }
    return retStatus;
}

const WalRecord *WalRecordReader::GetNextWalRecord()
{
    if (m_curGroup == nullptr) {
        return nullptr;
    }

    const WalRecord *logRecord = nullptr;
    if (m_lastReadRecordGroupStartPlsn != m_groupParser->GetCurGroupStartPlsn()) { /* Get a new Wal group */
        m_curRecordIndex = 0;
        m_offset = 0;
    }

    if (m_curRecordIndex < m_curGroup->recordNum) {
        logRecord = reinterpret_cast<const WalRecord *>(reinterpret_cast<const uint8 *>(m_curGroup) +
                WAL_GROUP_HEADER_SIZE + m_offset);
        StorageAssert(logRecord->m_size < m_curGroup->groupLen);
        m_offset += logRecord->m_size;
        m_curRecordIndex++;
    } else {
        logRecord = nullptr;
    }
    m_lastReadRecordGroupStartPlsn = m_groupParser->GetCurGroupStartPlsn();
    return logRecord;
}

void WalRecordReader::ResetCurrRecordIndex()
{
    m_curRecordIndex = 0;
    m_offset = 0;
}

uint64 WalRecordReader::GetCurGroupStartPlsn() const
{
    return m_groupParser->GetCurGroupStartPlsn();
}

uint64 WalRecordReader::GetCurGroupEndPlsn() const
{
    return m_groupParser->GetCurGroupEndPlsn();
}

bool WalRecordReader::NeedFreeCurGroup(const WalRecordAtomicGroup *group) const
{
    return m_groupParser->NeedFreeCurGroup(group);
}

uint64 WalRecordReader::GetCurRecordEndPlsn() const
{
    return m_groupParser->GetCurRecordEndPlsn(m_offset);
}

void WalRecordReader::SetNeedCheckGroupHeader(bool check)
{
    m_groupParser->SetNeedCheckGroupHeader(check);
}

WalRecordForPageReader::WalRecordForPageReader(DstoreMemoryContext memoryContext)
    : m_initialized(false),
    m_memoryContext(memoryContext),
    m_conf(nullptr),
    m_readerConf(nullptr),
    m_reader(nullptr),
    m_nowRecordReadFinish(false),
    m_nowReadRecordGroup(nullptr),
    m_prevRecordWalId(INVALID_WAL_ID),
    m_prevRecordEndPlsn(INVALID_PLSN)
{
}

WalRecordForPageReader::~WalRecordForPageReader()
{
    delete m_reader;
    m_reader = nullptr;
    DstorePfreeExt(m_conf);
    m_readerConf = nullptr;
    m_nowReadRecordGroup = nullptr;
    m_memoryContext = nullptr;
}

RetStatus WalRecordForPageReader::Init(const WalReaderForPageConf &conf)
{
    if (m_initialized) {
        return DSTORE_SUCC;
    }
    if (m_memoryContext == nullptr || conf.pageId == INVALID_PAGE_ID || conf.walStreamNum == 0) {
        ErrLog(DSTORE_WARNING, MODULE_WAL,
               ErrMsg("Init fail for invalid para mmemory_context is null:%d pageId invalid:%d walStreamNum is:%d",
                      m_memoryContext == nullptr, conf.pageId == INVALID_PAGE_ID, conf.walStreamNum));
        return DSTORE_FAIL;
    }

    /* copy config */
    uint32 confLen = sizeof(WalReaderForPageConf) + conf.walStreamNum * sizeof(WalReaderConf);
    WalReaderForPageConf *copyConf =
        static_cast<WalReaderForPageConf *>(DstoreMemoryContextAlloc(m_memoryContext, confLen));
    if (copyConf == nullptr) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("Init fail for malloc copyConf memory failed"));
        return DSTORE_FAIL;
    }
    errno_t ret = memcpy_s(copyConf, confLen, &conf, confLen);
    storage_securec_check(ret, "", "");

    /* this reader point to target record's end rather than head */
    const WalReaderConf *readerConf =
        GetWalReaderConf(copyConf, copyConf->prevRecord.walId, copyConf->prevRecord.endPlsn);
    if (readerConf == nullptr) {
        ErrLog(DSTORE_WARNING,
            MODULE_WAL, ErrMsg("init failed for target stream %lu not find", m_prevRecordWalId));
        goto ERR_EXIT;
    }
    m_reader = DstoreNew(m_memoryContext) WalRecordReader(m_memoryContext, *readerConf);
    if (m_reader == nullptr) {
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("Init fail for malloc reader memory failed"));
        goto ERR_EXIT;
    }
    if (STORAGE_FUNC_FAIL(m_reader->Init())) {
        ErrLog(DSTORE_WARNING, MODULE_WAL, ErrMsg("int WalReacordReader failed"));
        goto ERR_EXIT;
    }
    m_reader->SetNeedCheckGroupHeader(false);

    m_conf = copyConf;
    m_prevRecordWalId = m_conf->prevRecord.walId;
    m_prevRecordEndPlsn = m_conf->prevRecord.endPlsn;

    m_initialized = true;
    return DSTORE_SUCC;

ERR_EXIT:
    delete m_reader;
    m_reader = nullptr;
    m_readerConf = nullptr;
    DstorePfreeExt(copyConf);
    return DSTORE_FAIL;
}

RetStatus WalRecordForPageReader::SetPrevRecordInfo(const WalRecordLsnInfo &newLsn)
{
    const WalReaderConf *conf = GetWalReaderConf(m_conf, newLsn.walId, newLsn.endPlsn);
    if (conf == nullptr) {
        return DSTORE_FAIL;
    }
    m_reader->SetNewReadConf(*conf);
    m_nowRecordReadFinish = false;
    return DSTORE_SUCC;
}

RetStatus WalRecordForPageReader::ReadPrev(const WalRecordAtomicGroup **recordGroup)
{
    if (!m_initialized) {
        ErrLog(DSTORE_WARNING, MODULE_WAL, ErrMsg("read prev failed for not initialized"));
        return DSTORE_FAIL;
    }

    if (m_nowRecordReadFinish) {
        ErrLog(DSTORE_WARNING, MODULE_WAL, ErrMsg("not allow repeat read and need reset new plsn"));
        return DSTORE_FAIL;
    }
    RetStatus result = ReadRecordByEndPlsn(&m_nowReadRecordGroup);
    if (STORAGE_FUNC_FAIL(result)) {
        return DSTORE_FAIL;
    }
    *recordGroup = m_nowReadRecordGroup;
    m_nowRecordReadFinish = true;
    return DSTORE_SUCC;
}

const WalRecord *WalRecordForPageReader::GetNextWalRecord()
{
    return m_reader->GetNextWalRecord();
}

RetStatus WalRecordForPageReader::ReadRecordByEndPlsn(const WalRecordAtomicGroup **recordGroup)
{
    return SequentialReadRecord(recordGroup);
}

RetStatus WalRecordForPageReader::SequentialReadRecord(const WalRecordAtomicGroup **recordGroup)
{
    uint16 tryReadCount = 0;
    uint64 prevReadStartPlsn = m_readerConf->fileReader->GetPrevReadStartPoint(m_readerConf->startPlsn);
    do {
        if (prevReadStartPlsn == INVALID_PLSN) {
            ErrLog(DSTORE_WARNING, MODULE_WAL, ErrMsg("find valid prev read start plsn failed"));
            /* make sure ReadRecordByEndPlsn behave the same as not do SequentialReadRecord */
            *recordGroup = nullptr;
            return DSTORE_SUCC;
        }
        m_reader->SetReadStartPlsn(prevReadStartPlsn);
        if (STORAGE_FUNC_FAIL(m_reader->ReadNext(recordGroup))) {
            ErrLog(DSTORE_WARNING, MODULE_WAL, ErrMsg("read prev record failed"));
            return DSTORE_FAIL;
        }
        /* we got a valid start plsn */
        if (*recordGroup != nullptr) {
            break;
        }
        if (++tryReadCount > ((WAL_GROUP_MAX_SIZE / m_readerConf->walFileSize) + 1)) {
            ErrLog(DSTORE_WARNING, MODULE_WAL, ErrMsg("try find prev read start plsn more than %d time",
                tryReadCount));
            return DSTORE_FAIL;
        }
        prevReadStartPlsn = m_readerConf->fileReader->GetPrevReadStartPoint(prevReadStartPlsn);
    } while (true);
    while (m_reader->GetCurGroupEndPlsn() != m_readerConf->startPlsn) {
        if (STORAGE_FUNC_FAIL(m_reader->ReadNext(recordGroup))) {
            ErrLog(DSTORE_WARNING, MODULE_WAL, ErrMsg("sequential read record failed"));
            return DSTORE_FAIL;
        }
        if (*recordGroup == nullptr) {
            ErrLog(DSTORE_WARNING, MODULE_WAL, ErrMsg("cant't read next record during sequential read"));
            return DSTORE_FAIL;
        }
    }
    return DSTORE_SUCC;
}

const WalReaderConf *WalRecordForPageReader::GetWalReaderConf(
    WalReaderForPageConf *conf, WalId walId, uint64 setStartPlsn)
{
    if (m_readerConf != nullptr && m_readerConf->walId == walId) {
        m_readerConf->startPlsn = setStartPlsn;
        return m_readerConf;
    }

    for (uint16 index = 0; index < conf->walStreamNum; index++) {
        if (conf->walStreams[index].walId == walId) {
            conf->walStreams[index].startPlsn = setStartPlsn;
            m_readerConf = &conf->walStreams[index];
            return m_readerConf;
        }
    }
    ErrLog(DSTORE_WARNING, MODULE_WAL, ErrMsg("not find target stream of id %lu in conf", walId));
    return nullptr;
}
}
