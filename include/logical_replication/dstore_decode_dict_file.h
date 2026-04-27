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
 * dstore_decode_dict_file.h
 *  This file define page info of decode dict.
 *
 *
 * IDENTIFICATION
 *        storage/include/logical_replication/dstore_decode_dict_file.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_LOGICAL_DECODE_DICT_FILE_H
#define DSTORE_LOGICAL_DECODE_DICT_FILE_H

#include "common/dstore_datatype.h"
#include "common/memory/dstore_mctx.h"
#include "common/algorithm/dstore_string_info.h"
#include "page/dstore_itemptr.h"
#include "tuple/dstore_tuple_struct.h"
#include "systable/dstore_normal_systable.h"
#include "catalog/dstore_fake_type.h"
#include "systable/systable_relation.h"
#include "framework/dstore_vfs_adapter.h"

#ifdef UT
#ifndef ENABLE_LOGICAL_REPL
/* LOGICAL REPL is not supported for now. */
#define ENABLE_LOGICAL_REPL
#endif
#endif

namespace DSTORE {

constexpr const char* DECODEDICT_FILE_1_NAME = "decodedict_file_1";
constexpr const char* DECODEDICT_FILE_2_NAME = "decodedict_file_2";
const uint64 DECODE_DICT_FILE_MAGIC_NUMBER = 0x436F6E74726F6C66;   /* hex of string "decode dict file" */
constexpr uint16 DECODE_DICT_PAGE_INVALID_OFFSET = USHRT_MAX;

constexpr const uint32 DECODE_DICT_META_BLOCK = 0;
constexpr uint16 DECODE_DICT_INIT_PAGE_COUNT = 1;
constexpr uint16 DECODE_DICT_EXTEND_PAGE_COUNT = 8;

constexpr uint32 DECODEDICT_BUF_VALID = (1UL << 1);
constexpr uint32 DECODEDICT_BUF_DIRTY = (1UL << 2);  /* data is dirty */
constexpr uint32 DECODEDICT_BUF_NEED_CRC = (1UL << 3);  /* data is need CRC check */

constexpr int ATTRIBUTE_FIXED_SIZE = (offsetof(FormData_pg_attribute, attcollation) + sizeof(Oid));


/* Three page types of decode dict file */
enum class DecodeDictPageType : uint8 {
    INVALID = 0,
    META_PAGE = 1,
    DECODE_DICT_PAGE = 2,
    MAX_PAGE_TYPE
};

enum class DecodeTableInfoStatus : uint8 {
    INVALID = 0,               /* unused */
    EXIST_BUT_UNCOLLECTED = 1, /* existed in catalog but not yet synchronize to our decode dict */
    COLLECTED = 2,             /* full synchronized, and can be used to decode */
    DELETE = 3                 /* correspond item deleted */
};

/* table info persistent data */
struct DecodeTableInfoDiskData {
    uint16 size;
    Oid tableOid;
    char relName[NAME_DATA_LEN];
    Oid nspId;
    char nspName[NAME_DATA_LEN];
    CommitSeqNo csn;
    DecodeTableInfoStatus status;
    bool relHasOids;
    int natts;
    char attrsData[];

    void Dump(StringInfo str)
    {
        str->append("tableOid = %u | relName = %s | csn = %lu | nspId = %u | nspName = %s | natts = %d | status = %s\n",
            tableOid, relName, csn, nspId, nspName, natts,
            status == DecodeTableInfoStatus::EXIST_BUT_UNCOLLECTED ? "EXIST_BUT_UNCOLLECTED" : "COLLECTED");
        char *attr = attrsData;
        for (int i = 0; i < natts; i++) {
            Form_pg_attribute cur = static_cast<Form_pg_attribute>(static_cast<void *>(attr));
            StorageAssert(cur->attrelid == tableOid);
            UNUSED_VARIABLE(cur);
            str->append("attrIdx = %hd attrName = %s \n", cur->attnum, cur->attname.data);
            attr += ATTRIBUTE_FIXED_SIZE;
        }
    }
} PACKED;


/* Note: The Decode Dict related page has its' own buffer management without intrude to buffer pool */
/* LOGICAL_META_PAGE_TYPE page definition */
struct DecodeDictMetaPage {
    uint32 checksum;
    uint64 magic;
    uint16 version;
    DecodeDictPageType pageType;
    /* This parameter is used to check whether the file is successfully written. 0: not written, 1: written */
    uint8 flag;
    uint16 pageSize;            /* Block size of the decode dict file, The current size is 8 KB. */
    BlockNumber totalPageCount; /* The actual page count of decode dict file */
    BlockNumber usedPageCount;  /* used count in the decode dict file */

    BlockNumber firstDecodeInfoPage;
    BlockNumber lastDecodeInfoPage;

    void Init()
    {
        version = 0;
        checksum = 1;
        pageType = DecodeDictPageType::META_PAGE;
        totalPageCount = DECODE_DICT_EXTEND_PAGE_COUNT; /* entend EXTEND_PAGE_COUNT block when create file */
        flag = 1;
        usedPageCount = DECODE_DICT_INIT_PAGE_COUNT; /* just meta page */
        magic = DECODE_DICT_FILE_MAGIC_NUMBER;
        pageSize = BLCKSZ;
        firstDecodeInfoPage = DSTORE_INVALID_BLOCK_NUMBER;
        lastDecodeInfoPage = DSTORE_INVALID_BLOCK_NUMBER;
    }

    void Dump(StringInfo str)
    {
        str->AppendString("DecodeTableInfoMeta: \n");
        str->append("checksum = %u \n", checksum);
        str->append("magic = %lu \n", magic);
        str->append("version = %hu \n", version);
        str->append("pageSize = %hu \n", pageSize);
        str->append("totalPageCount = %u \n", totalPageCount);
        str->append("usedPageCount = %u \n", usedPageCount);
        str->append("firstDecodeInfoPage = %u \n", firstDecodeInfoPage);
        str->append("lastDecodeInfoPage = %u \n", lastDecodeInfoPage);
    }
};

struct DecodeDictPageHeader {
    uint32 checksum;
    DecodeDictPageType pageType;
    BlockNumber nextPage;
    uint16 writeOffset;
} PACKED;

struct DecodeDictPage {
    DecodeDictPageHeader dictPageHeader;
    char data[BLCKSZ - sizeof(DecodeDictPageHeader)];

    void Init()
    {
        dictPageHeader.checksum = 0;
        dictPageHeader.pageType = DecodeDictPageType::DECODE_DICT_PAGE;
        dictPageHeader.nextPage = DSTORE_INVALID_BLOCK_NUMBER;
        dictPageHeader.writeOffset = 0;
        errno_t rc = memset_s(data, BLCKSZ - sizeof(DecodeDictPageHeader), 0, BLCKSZ - sizeof(DecodeDictPageHeader));
        storage_securec_check(rc, "\0", "\0");
    }

    inline void AddItem(const DecodeTableInfoDiskData *item)
    {
        if (unlikely(item == nullptr)) {
            return;
        }
        StorageAssert(BLCKSZ - (sizeof(DecodeDictPageHeader) + dictPageHeader.writeOffset) >= item->size);
        errno_t rc = memcpy_s(data + dictPageHeader.writeOffset, item->size,
                              static_cast<const void *>(item), item->size);
        storage_securec_check(rc, "\0", "\0");
        dictPageHeader.writeOffset += item->size;
    }

    inline void RemoveItem(uint16 offset, uint16 len)
    {
        if ((offset + len) != dictPageHeader.writeOffset) {
            uint16 size = BLCKSZ - (offset + static_cast<uint16>(sizeof(DecodeDictPageHeader)));
            /* the item will be removed is not the last one, move the memory forward */
            errno_t rc = memmove_s(data + offset, size, data + offset + len, size - len);
            storage_securec_check(rc, "\0", "\0");
        }
        /* NOTE: if the item is the last one, just move the write offset directly */
        dictPageHeader.writeOffset = dictPageHeader.writeOffset - len;
    }

    inline DecodeTableInfoDiskData* GetDecodeTableInfoItem(uint16 offset)
    {
        StorageAssert(offset <= dictPageHeader.writeOffset);
        return static_cast<DecodeTableInfoDiskData *>(static_cast<void *>(data + offset));
    }

    inline BlockNumber GetNextPage() const
    {
        return dictPageHeader.nextPage;
    }

    inline uint32 GetWriteOffset() const
    {
        return dictPageHeader.writeOffset;
    }

    inline uint32 GetAvailableSize() const
    {
        return ((BLCKSZ - sizeof(DecodeDictPageHeader)) - dictPageHeader.writeOffset);
    }

    inline void SetNextPage(BlockNumber blockNumber)
    {
        dictPageHeader.nextPage = blockNumber;
    }

    void Dump(StringInfo str)
    {
        str->AppendString("---HEADER---\n");
        str->append("checksum = %u \n", dictPageHeader.checksum);
        str->append("pageType = %u \n", static_cast<uint32>(dictPageHeader.pageType));
        str->append("nextPage = %u \n", dictPageHeader.nextPage);
        str->append("writeOffset = %hu \n", dictPageHeader.writeOffset);
        str->AppendString("---Item List---\n");
        char* start = data;
        char* end = data + dictPageHeader.writeOffset;
        int i = 1;
        while (start < end) {
            str->append("---Item%d---\n", i);
            DecodeTableInfoDiskData *curItem = static_cast<DecodeTableInfoDiskData *>(static_cast<void *>(start));
            curItem->Dump(str);
            start += curItem->size;
            i++;
        }
    }
};
static_assert(sizeof(DecodeDictPage) == BLCKSZ);

class DecodeDictFile : public BaseObject {
public:
    DecodeDictFile(PdbId pdbId);
    ~DecodeDictFile();

    /**
     * Create DecodeDictFile, the function is called when create pdb.
     * @param decodedict_file1_name Double-Write, decode dict file1 name.
     * @param decodedict_file2_name Double-Write, decode dict file2 name.
     * @param dataDir Specify where DecodeDictFile are stored.
     * @return
     */
    static RetStatus CreateFile(PdbId pdbId, const char *decodedict_file1_name,
                                const char *decodedict_file2_name, const char *dataDir);
#ifdef ENABLE_LOGICAL_REPL
    RetStatus Init();
    void Destroy();
    RetStatus Reset();

    void LoadToHTAB(HTAB *tableInfo);
    RetStatus AddDecodeTableInfoItem(const DecodeTableInfoDiskData *newItem, BlockNumber &outAddBlock);
    RetStatus RemoveDecodeTableInfoItem(const Oid oldTableOid, const CommitSeqNo oldTableCsn, const BlockNumber block);
    RetStatus UpdateDecodeTableInfoItem(const BlockNumber oldBlock, const Oid oldTableOid,
        const CommitSeqNo oldTableCsn, const DecodeTableInfoDiskData *newItem, BlockNumber &newBlock);

    class DecodeDictPageIterator {
    public:
        explicit DecodeDictPageIterator(DecodeDictFile *dictFile, BlockNumber iterFrom)
            : m_decodeDictFile(dictFile),
              m_currentBlock(iterFrom),
              m_currentOffset(DECODE_DICT_PAGE_INVALID_OFFSET),
              m_currentItem(nullptr)
        {}
        explicit DecodeDictPageIterator(DecodeDictFile *dictFile)
            : m_decodeDictFile(dictFile),
              m_currentBlock(dictFile->GetFirstPage()),
              m_currentOffset(DECODE_DICT_PAGE_INVALID_OFFSET),
              m_currentItem(nullptr)
        {}
        ~DecodeDictPageIterator()
        {
            m_decodeDictFile = nullptr;
            m_currentBlock = DSTORE_INVALID_BLOCK_NUMBER;
            m_currentOffset = DECODE_DICT_PAGE_INVALID_OFFSET;
            m_currentItem = nullptr;
        }
        DISALLOW_COPY_AND_MOVE(DecodeDictPageIterator);

        bool NextItem()
        {
            while (m_currentBlock != DSTORE_INVALID_BLOCK_NUMBER) {
                if (m_currentOffset == DECODE_DICT_PAGE_INVALID_OFFSET) {
                    m_currentOffset = 0;
                } else {
                    m_currentOffset += m_currentItem->size;
                }
                DecodeDictPage *page = m_decodeDictFile->GetPage(m_currentBlock);
                if (unlikely(page == nullptr)) {
                    return false;
                }
                if (m_currentOffset < page->GetWriteOffset()) {
                    m_currentItem = page->GetDecodeTableInfoItem(m_currentOffset);
                    StorageAssert((m_currentOffset + m_currentItem->size) <= page->GetWriteOffset());
                    return true;
                }
                StorageAssert(m_currentOffset == page->GetWriteOffset());
                m_currentBlock = page->GetNextPage();
                m_currentOffset = DECODE_DICT_PAGE_INVALID_OFFSET;
            }
            return false;
        }

        DecodeTableInfoDiskData *GetItem()
        {
            return m_currentItem;
        }

        uint16 GetCurrentOffset() const
        {
            return m_currentOffset;
        }

        BlockNumber GetCurrentBlock() const
        {
            return m_currentBlock;
        }

    private:
        DecodeDictFile *m_decodeDictFile;
        BlockNumber m_currentBlock;
        uint16 m_currentOffset;
        DecodeTableInfoDiskData *m_currentItem;
    };

protected:
    BlockNumber GetFirstPage() const;
    BlockNumber GetLastPage() const;
    RetStatus InsertIntoAvailablePage(const DecodeTableInfoDiskData *newItem, BlockNumber &outAddBlock);
    RetStatus InitDecodeFilePath();
    RetStatus CheckAndLoadMetaPage(FileDescriptor *fd);

    void AllocPageStateAndPageBufTag(uint32 blockCount);
    bool IsPageValid(BlockNumber blockNumber) const;
    bool IsPageDirty(BlockNumber blockNumber) const;
    bool IsPageNeedCrcCheck(BlockNumber blockNumber) const;
    void MarkPageNeedCrcCheck(BlockNumber blockNumber);
    void MarkPageValid(BlockNumber blockNumber, char *blockBuf);
    void MarkPageDirty(BlockNumber blockNumber);
    void CleanAllDirtyTagAndValidBuf();
    void CleanPageBuf(BlockNumber block);
    void CheckDirtyPageBufEmpty() const;
    DecodeDictPage* GetPage(BlockNumber blockNumber);
    DecodeDictPage* ReadFromFile(BlockNumber blockNumber);
    RetStatus WriteAllDirtyPage(FileDescriptor *fd);
    RetStatus FlushFileBuffer(FileDescriptor *fd);
    void UpdatePageCrc(uint32 *checksum, const void *page) const;
    RetStatus WritePage(FileDescriptor *fd, BlockNumber blockNumber);
    void SetLastPageBlockNumber(BlockNumber lastBlock);
    void SetFirstPageBlockNumber(BlockNumber firstBlock);
    RetStatus ExtendDecodeDict();
    RetStatus ExtendFile();
    void ExtendStatusBuf();
    RetStatus PostWriteFile();
#endif
    static RetStatus CreateOneFile(PdbId pdbId, const char *const fileName, const char *dataDir, FileDescriptor** fd);
    static RetStatus InitMetaPage(PdbId pdbId, FileDescriptor *fd1, FileDescriptor *fd2);
    static bool CheckPageCrcMatch(uint32 *checksum, const void *page);
    static RetStatus WritePage(PdbId pdbId, FileDescriptor *fd, uint32 *checksum, BlockNumber block, const char *page);

    PdbId m_pdbId;
    FileDescriptor *m_fd1;
    FileDescriptor *m_fd2;
    char* m_file1Path;
    char* m_file2Path;
    DecodeDictMetaPage *m_metaPage; /* cached meta page */
    char **m_pageBuf;               /* cache the data in memory to be updated */
    uint32 *m_state;                /* Identifies the state of pages */
    uint32 m_pageCount;             /* pageCount in memory */
    VFSAdapter *m_vfs;
};

}
#endif

