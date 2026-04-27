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
 * dstore_wal_reader.h
 *
 * Description:
 * Wal public header file, including interfaces for reading target WalGroup.
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_WAL_READER_H
#define DSTORE_WAL_READER_H

#include "dstore_wal_struct.h"
#include "dstore_wal_file_reader.h"
#include "dstore_wal_read_buffer.h"

namespace DSTORE {

enum class WalReadSource : uint8 {
    WAL_READ_FROM_BUFFER,
    WAL_READ_FROM_DISK,
};

struct WalReaderConf {
    WalId walId; /* target stream's id */
    uint64 startPlsn; /* start position when m_type is WAL_STREAM_READER */
    WalStreamBytesReader *fileReader;
    WalReadBuffer *walReadBuffer;
    uint64 walFileSize;
    WalReadSource walReadSource;
};

/*
 * Adaptor class for wal dio read
 */
class WalDioReadAdaptor {
public:
    WalDioReadAdaptor(DstoreMemoryContext memoryContext, uint32 alignSize);
    ~WalDioReadAdaptor();

    RetStatus Read(uint8 *data, uint64 readLen, WalFile *walFile, uint64 offset, int64 *resultLen);
private:
    RetStatus AdaptDioBufferSize(uint32 targetSize);
    DstoreMemoryContext m_memoryContext;
    uint32 m_alignLen;
    uint8 *m_memory;
    uint32 m_memorySize;
};

struct WalGroupParserInitParam {
    WalStreamBytesReader *fileReader;
    WalReadBuffer *walReadBuffer;
    uint64 curReadPlsn;
    uint64 walFileSize;
    WalReadSource readSource;
    uint64 readBlockSize;
};

class WalGroupParser : public BaseObject  {
public:
    WalGroupParser(DstoreMemoryContext memoryContext, const WalGroupParserInitParam &param);
    ~WalGroupParser();

    RetStatus Init();

    void SetReadStartPlsn(uint64 startPlsn);

    uint64 GetCurRecordEndPlsn(uint64 offset) const;

    void SetNeedCheckGroupHeader(bool check);

    void ClearReadBufState();

    uint64 GetCurGroupStartPlsn() const;

    uint64 GetCurGroupEndPlsn() const;

    void SetNewReadConf(const WalGroupParserInitParam &param);

    bool NeedFreeCurGroup(const WalRecordAtomicGroup *group) const;

    void ResetReadState();

    RetStatus ReadCurrent(const WalRecordAtomicGroup *&curGroup);

private:

    RetStatus ReadBlock(uint64 targetBlockPlsn, uint64 *resultLen);

    RetStatus ValidWalGroupLen(const WalRecordAtomicGroup *&curGroup);

    RetStatus ValidWalGroup(const WalRecordAtomicGroup *&curGroup) const;

    RetStatus ReadWholeGroup(uint64 groupAlreadyReadLen, uint64 groupLen, const WalRecordAtomicGroup *&curGroup,
        uint64 &nextGroupStartPlsn);

    RetStatus FetchWalRecordGroupToBuffer(uint64 groupAlreadyReadLen, uint64 groupLen,
        const WalRecordAtomicGroup *&curGroup);

    void GetGroupLenToRecordBuf(uint64 groupAlreadyReadLen, uint64 &groupLen, uint64 startPosOffInBlk, uint64 leftLen);

    RetStatus GetGroupLenCrossTwoBlock(uint64 &groupAlreadyReadLen, uint64 &totalReadLenFromBuf, uint64 &groupLen,
        const WalRecordAtomicGroup *&curGroup);

    RetStatus ForwardToGroupStartPos(uint64 totalReadLenFromBuf, uint64 &startPosOffInBlk);

    RetStatus ReadGroupLen(uint64 &groupAlreadyReadLen, uint64 &totalReadLenFromBuf, uint64 &startPosOffInBlk);

    RetStatus GetGroupLen(uint64 &groupAlreadyReadLen, uint64 &totalReadLenFromBuf, uint64 &groupLen,
        const WalRecordAtomicGroup *&curGroup);

    RetStatus GetGroupData(uint64 groupAlreadyReadLen, uint64 totalReadLenFromBuf, uint64 groupLen,
        const WalRecordAtomicGroup *&curGroup);

    DstoreMemoryContext m_memoryContext;

    WalReadSource m_readSource;
    /*
     * Data scratches loaded from disk into cache (Each time, a page (8 KB) content is read from
     * the segment file to the memory (reducing I/O).)
     */
    WalStreamBytesReader *m_fileReader; /* read source from wal file */
    WalReadBuffer *m_walReadBuffer; /* read source from wal read buffer */
    uint8* m_readPreBuf;
    uint8* m_readBuf;
    uint8* m_readRecordBuf; /* The target Wal group to be read is stored here. */
    uint64 m_lastReadBlockLen;
    uint64 m_lastReadBlockPlsn;

    uint64 m_readBlockSize;
    uint64 m_walFileSize;

    /* beginning of the WAL recordGroup being read. */
    uint64 m_curReadPlsn; /* The start plsn of the Wal group currently being read */
    uint64 m_readNextStartPlsn;

    bool m_needCheckGroupHeader;
    bool m_groupHeaderChecked;

    uint64 m_segHeadOff;
    uint64 m_blockStartPlsn;
};

/*
 * Wal Reader is allocated by WalManager, and Wal module don't own its lifecycle and user need to release it.
 */
class WalRecordReader : public BaseObject {
public:
    WalRecordReader(DstoreMemoryContext memoryContext, const WalReaderConf &conf);
    virtual ~WalRecordReader();
    DISALLOW_COPY_AND_MOVE(WalRecordReader)

    /*
     * Init inner data structure.
     * Including init target page info and load WalRecordPtr on it when XReaderType is WAL_STREAM_PAGE.
     *
     * @return: OK if success, detail error info otherwise
     */
    RetStatus Init();

    /*
     * Allocate WalRecordReader of specified type. and wal module not manage WalRecordReader resource, they should be
     * released by caller if not useful anymore.
     *
     * @param: conf is configure for WalRecordReader
     * @param: reader:output parameter, not nullptr if allocate success
     * @param: memoryContext: mem ctx used by reader
     *
     * @return: OK if success, detail error info otherwise
     */
    static RetStatus AllocateWalReader(const WalReaderConf &conf, WalRecordReader **reader,
                                       DstoreMemoryContext memoryContext);

    /*
     * Read logic-next recordGroup, recordGroup is nullptr and return DSTORE_SUCC if reach end.
     *
     * @param: recordGroup:output parameter
     *
     * @return: OK if success, detail error info otherwise
     */
    RetStatus ReadNext(const WalRecordAtomicGroup **recordGroup);

    /*
     * Parses the current read WalRecordAtomicGroup and obtains WalRecord inside.
     *
     * @return: return a WalRecord pointer, and return nullptr if no WalRecord remains.
     */
    const WalRecord *GetNextWalRecord();

    /*
     * Reset current read wal record in current wal group. After calling this, GetNextWalRecord can get first wal record
     * of current wal group.
     *
     */
    void ResetCurrRecordIndex();

    /*
     * Set read start plsn for ReadNext and clear current read state.
     *
     * @param: startPlsn:read start plsn to be set.
     */
    void SetReadStartPlsn(uint64 startPlsn);

    /*
     * Set new config for this reader, usually need to read other WalStream
     *
     * @param: newConf is new set conf
     */
    void SetNewReadConf(const WalReaderConf &newConf);

    /*
     * Set need check group header flag;
     */
    void SetNeedCheckGroupHeader(bool check);

    /*
     * Get the start plsn of current read WalRecordAtomicGroup.
     *
     * @return: return start plsn of current read WalRecordAtomicGroup.
     */
    uint64 GetCurGroupStartPlsn() const;

    /*
     * Get the end plsn of current read WalRecordAtomicGroup.
     *
     * @return: return end plsn of current read WalRecordAtomicGroup.
     */
    uint64 GetCurGroupEndPlsn() const;

    /*
     * Judge if the WalRecordGroup exist in the m_readRecordBuf (i.e. dynamically allocated).
     *
     * @param: group: current WalRecordGroup ptr.
     * @return: true or false.
     */
    bool NeedFreeCurGroup(const WalRecordAtomicGroup *group) const;

    /*
     * Get the end plsn of current read WalRecord.
     *
     * @return: return end plsn of current read WalRecord.
     */
    uint64 GetCurRecordEndPlsn() const;

private:
    void ClearReadBufState(); /* Error Handle During Read process */
    void ResetReadState(); /* read prepare for ReadNext and ReadPrev */

    DstoreMemoryContext m_memoryContext;
    WalId m_walId;
    WalStreamBytesReader *m_fileReader; /* read source from wal file */
    WalReadBuffer *m_walReadBuffer; /* read source from wal read buffer */
    WalGroupParser *m_groupParser;
    WalGroupParserInitParam parserParam;

    uint64 m_lastReadRecordGroupStartPlsn;

    /* Parses the current read WalRecordAtomicGroup */
    const WalRecordAtomicGroup *m_curGroup;
    uint16 m_curRecordIndex;
    size_t m_offset;
};

struct WalReaderForPageConf {
    PageId pageId; /* target page */
    WalRecordLsnInfo prevRecord; /* this is prev record's end plsn */
    uint16 walStreamNum; /* all pdb's wal stream config */
    WalReaderConf walStreams[]; /* each stream's reader conf */
};

/*
 * Compared with WalRecordReader, WalRecordForPageReader focus on read all record of target page.
 */
class WalRecordForPageReader : public BaseObject {
public:
    explicit WalRecordForPageReader(DstoreMemoryContext memoryContext);
    ~WalRecordForPageReader();
    DISALLOW_COPY_AND_MOVE(WalRecordForPageReader)

    /*
     * Init inner data structure.
     *
     * @return: OK if success, detail error info otherwise
     */
    RetStatus Init(const WalReaderForPageConf &conf);

    /*
     * Destroy inner resources.
     */
    void Destroy();

    /*
     * set read position to next read record's end plsn.
     *
     * @return: OK if set success, detail error info otherwise
     */
    RetStatus SetPrevRecordInfo(const WalRecordLsnInfo &newLsn);

    /*
     * Read prev WalRecordGroup and its end plsn is conf.startRecord.
     * prev WalRecordGroup's lsn in this record's WalRecordForPage struct; or set by init conf.startRecord.
     * we implement this by first read and check following WalRecordGroup whose start plsn is conf.startRecord,
     * then we read target WalRecordGroup by following WalRecordGroup's m_prevGroupPlsn.
     * Node that we must read all following WalRecordGroup to check it's correctness.
     *
     * @param[out]: recordGroup is read result
     *
     * @return: STROAGE_SUCC if read WalRecordGroup and contains WalRecord for target page, or fail otherwise
     */
    RetStatus ReadPrev(const WalRecordAtomicGroup **recordGroup);

    /*
     * It will search all WalRecordGroup if ReadAllAfter success.
     *
     * @return: target WalRecord pointer, or return null if reach end or ReadPrev/ReadAllAfter failed
     */
    const WalRecord *GetNextWalRecord();

private:
    RetStatus ReadRecordByEndPlsn(const WalRecordAtomicGroup **recordGroup);
    /*
     * Because cant't get target WalRecordGroup's start plsn from following one's m_prevGroupPlsn
     * then we must read from start
     */
    RetStatus SequentialReadRecord(const WalRecordAtomicGroup **recordGroup);
    const WalReaderConf *GetWalReaderConf(WalReaderForPageConf *conf, WalId walId, uint64 setStartPlsn);
    bool m_initialized;
    DstoreMemoryContext m_memoryContext;
    WalReaderForPageConf *m_conf;
    WalReaderConf *m_readerConf;
    WalRecordReader *m_reader;

    bool m_nowRecordReadFinish;
    const WalRecordAtomicGroup *m_nowReadRecordGroup;
    WalId m_prevRecordWalId;
    uint64 m_prevRecordEndPlsn;
};
}
#endif // STORAGE_WAL_READER_H
