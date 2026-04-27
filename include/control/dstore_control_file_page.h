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
 * dstore_control_file_page.h
 *  Record control file page.
 *
 *
 * IDENTIFICATION
 *        dstore/include/control/dstore_control_file_page.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_CONTROL_FILE_PAGE_H
#define DSTORE_CONTROL_FILE_PAGE_H

#include "control/dstore_control_struct.h"
#include "common/algorithm/dstore_string_info.h"

namespace DSTORE {

const uint32 CONTROL_META_MAGIC_NUMBER = 0xFEAABBEF;
const uint32 CONTROL_DATA_MAGIC_NUMBER = 0xDABBCCAD;

class ControlBasePage {
public:
    ControlPageHeader m_pageHeader;
    char m_data[BLCKSZ - sizeof(ControlPageHeader)];

    void InitPage(ControlPageType pageType, uint16 metaHeaderSize, uint16 metaDataSize, uint32 magicNum)
    {
        m_pageHeader.m_checksum = 0;
        m_pageHeader.m_magic = magicNum;
        m_pageHeader.m_pageType = static_cast<uint16>(pageType);
        m_pageHeader.m_dataOffset = metaHeaderSize + metaDataSize;
        m_pageHeader.m_nextPage = DSTORE_INVALID_BLOCK_NUMBER;
        m_pageHeader.m_version = 0;
        m_pageHeader.m_reserved = 0;
        errno_t rc = memset_s(m_data, BLCKSZ - sizeof(ControlPageHeader), 0,
                              BLCKSZ - sizeof(ControlPageHeader));
        storage_securec_check(rc, "\0", "\0");
        m_pageHeader.m_writeOffset = m_pageHeader.m_dataOffset;
    }

    inline uint32 GetCheckSum() const
    {
        return m_pageHeader.m_checksum;
    }

    inline void SetCheckSum(uint32 checkSum)
    {
        m_pageHeader.m_checksum = checkSum;
    }

    inline ControlPageType GetPageType() const
    {
        if (m_pageHeader.m_pageType > static_cast<uint16>(CONTROL_MAX_PAGE_TYPE)) {
            return CONTROL_PAGE_TYPE_INVALID;
        }
        return static_cast<ControlPageType>(m_pageHeader.m_pageType);
    }

    inline void SetPageType(ControlPageType pageType)
    {
        m_pageHeader.m_pageType = static_cast<uint16>(pageType);
    }

    inline BlockNumber GetNextPage() const
    {
        return m_pageHeader.m_nextPage;
    }

    inline uint32 GetVersion() const
    {
        return m_pageHeader.m_version;
    }

    inline uint16 GetWriteOffset() const
    {
        return m_pageHeader.m_writeOffset;
    }

    inline uint32 GetAvailableSize() const
    {
        return ((BLCKSZ - sizeof(ControlPageHeader)) - m_pageHeader.m_writeOffset);
    }

    inline void SetNextPage(BlockNumber blockNumber)
    {
        m_pageHeader.m_nextPage = blockNumber;
    }

    inline RetStatus AddItem(const void *data, uint32 len)
    {
        errno_t rc = memcpy_s(
            m_data + m_pageHeader.m_writeOffset,
            static_cast<uint64>(static_cast<uint32>((static_cast<uint16>(BLCKSZ) - m_pageHeader.m_writeOffset) -
                                                    static_cast<uint16>(sizeof(ControlPageHeader)))),
            data, len);
        storage_securec_check(rc, "\0", "\0");

        m_pageHeader.m_writeOffset += static_cast<uint16>(len);
        return DSTORE_SUCC;
    }

    inline ControlPageType GetControlPageType()
    {
        return (ControlPageType)m_pageHeader.m_pageType;
    }

    inline RetStatus RemoveItem(size_t offset, size_t len)
    {
        if ((offset + len) != m_pageHeader.m_writeOffset) {
            size_t size = (BLCKSZ - offset) - sizeof(ControlPageHeader);
            /* the item will be removed is not the last one, move the memory forward */
            errno_t rc = memmove_s(m_data + offset, size, m_data + offset + len, size - len);
            storage_securec_check(rc, "\0", "\0");
        }
        /* NOTE: if the item is the last one, just move the write offset directly */
        m_pageHeader.m_writeOffset = static_cast<uint16>(m_pageHeader.m_writeOffset - len);
        return DSTORE_SUCC;
    }

    inline void *GetItem(uint16 offset, UNUSE_PARAM uint16 len)
    {
        StorageAssert((offset + len) <= m_pageHeader.m_writeOffset);
        return static_cast<void *>((m_data + offset));
    }

    char *Dump()
    {
        StringInfoData str;
        str.init();
        str.append("m_checksum = 0x%x \n", m_pageHeader.m_checksum);
        str.append("m_magic = 0x%x \n", m_pageHeader.m_magic);
        str.append("m_pageType = %hu \n", m_pageHeader.m_pageType);
        str.append("m_dataOffset = %hu \n", m_pageHeader.m_dataOffset);
        str.append("m_nextPage = %u \n", m_pageHeader.m_nextPage);
        str.append("m_version = %u \n", m_pageHeader.m_version);
        str.append("m_writeOffset = %hu \n", m_pageHeader.m_writeOffset);
        str.append("m_reserved = %hu \n", m_pageHeader.m_reserved);
        /* dump data */
        const ControlPageTypeInfo *pageInfo = nullptr;
        /* get pageinfo */
        for (uint32_t i = 0; i < sizeof(CONTROL_PAGE_TYPE_INFOS) / sizeof(CONTROL_PAGE_TYPE_INFOS[0]); ++i) {
            if (CONTROL_PAGE_TYPE_INFOS[i].type == static_cast<ControlPageType>(m_pageHeader.m_pageType)) {
                pageInfo = &CONTROL_PAGE_TYPE_INFOS[i];
                break;
            }
        }
        if (unlikely(pageInfo == nullptr)) {
            return str.data;
        }
        /* dump metaheader */
        if (unlikely(m_pageHeader.m_dataOffset > 0)) {
            ControlMetaHeader::Dump(static_cast<void *>(&m_data[0]), str);
            /* dump metadata */
            if (pageInfo->metadump != nullptr && m_pageHeader.m_dataOffset > sizeof(ControlMetaHeader)) {
                pageInfo->metadump(&m_data[sizeof(ControlMetaHeader)], str);
            }
        }
        if (pageInfo->dump != nullptr && pageInfo->size != 0) {
            char *start = m_data + m_pageHeader.m_dataOffset;
            char *end = m_data + m_pageHeader.m_writeOffset;
            while (start < end) {
                pageInfo->dump(static_cast<void *>(start), str);
                start += pageInfo->size;
            }
        }
        return str.data;
    }
};

class ControlMetaPage : public ControlBasePage {
public:
    void InitMetaPage(ControlPageType pageType, uint32 metaDataSize)
    {
        InitPage(pageType, sizeof(ControlMetaHeader), metaDataSize, CONTROL_META_MAGIC_NUMBER);
    }

    void *GetMetaData()
    {
        return m_data + sizeof(ControlMetaHeader);
    }

    ControlMetaHeader *GetMetaHeader()
    {
        return STATIC_CAST_PTR_TYPE(m_data, ControlMetaHeader*);
    }

    RetStatus SetMetaData(const void *data, uint32 size)
    {
        if (m_pageHeader.m_dataOffset - sizeof(ControlMetaHeader) != size) {
            return DSTORE_FAIL;
        }
        errno_t rc = memcpy_s(m_data + sizeof(ControlMetaHeader), m_pageHeader.m_dataOffset - sizeof(ControlMetaHeader),
                              data, m_pageHeader.m_dataOffset - sizeof(ControlMetaHeader));
        storage_securec_check(rc, "\0", "\0");
        return DSTORE_SUCC;
    }

    uint64 GetTerm()
    {
        return (static_cast<ControlMetaHeader *>(static_cast<void *>(m_data)))->m_term;
    }

    ControlMetaHeader *GetControlMetaHeader()
    {
        return static_cast<ControlMetaHeader *>(static_cast<void *>(m_data));
    }

    void SetTerm(uint64 term)
    {
        (static_cast<ControlMetaHeader *>(static_cast<void *>(m_data)))->m_term = term;
    }

    uint32 GetMaxPageId()
    {
        return (static_cast<ControlMetaHeader *>(static_cast<void *>(m_data)))->m_lastPageId;
    }

    void SetMaxPageId(uint32 pageId)
    {
        (static_cast<ControlMetaHeader *>(static_cast<void *>(m_data)))->m_lastPageId = pageId;
    }

    uint8 GetFlag()
    {
        return (static_cast<ControlMetaHeader *>(static_cast<void *>(m_data)))->m_flag;
    }

    void MarkWriting()
    {
        (static_cast<ControlMetaHeader *>(static_cast<void *>(m_data)))->m_flag = 1;
    }

    void MarkWriteFinished()
    {
        (static_cast<ControlMetaHeader *>(static_cast<void *>(m_data)))->m_flag = 0;
    }

    bool CheckIfWriting()
    {
        return (static_cast<ControlMetaHeader *>(static_cast<void *>(m_data)))->m_flag == 1;
    }
};

class ControlDataPage : public ControlBasePage {
public:
    void InitDataPage(ControlPageType pageType)
    {
        InitPage(pageType, 0, 0, CONTROL_DATA_MAGIC_NUMBER);
    }
};
using ControlPage = ControlDataPage;
class ControlFileMetaPage : public ControlMetaPage {
public:
    void InitFileMetaPage()
    {
        InitMetaPage(ControlPageType::CONTROL_FILE_MATAPAGE_TYPE, sizeof(ControlFileMetaData));
        SetCheckSum(1);
        ControlFileMetaData *m_fileMetaHeader = static_cast<ControlFileMetaData *>(GetMetaData());
        m_fileMetaHeader->maxPageType = CONTROL_MAX_PAGE_TYPE;
        m_fileMetaHeader->usedGroupType = CONTROL_GROUP_TYPE_MAX;
        m_fileMetaHeader->totalPageCount = CONTROLFILE_PAGEMAP_MAX;
        m_fileMetaHeader->usedPageCount = CONTROLFILE_PAGEMAP_LOGICALREP_MAX;
        m_fileMetaHeader->magic = MAGIC_NUMBER;
    }

    BlockNumber GetTotalPageCount()
    {
        return (static_cast<ControlFileMetaData *>(GetMetaData()))->totalPageCount;
    }

    BlockNumber GetUsedPageCount()
    {
        return (static_cast<ControlFileMetaData *>(GetMetaData()))->usedPageCount;
    }

    BlockNumber GetMaxPageType()
    {
        return (static_cast<ControlFileMetaData *>(GetMetaData()))->maxPageType;
    }

    BlockNumber GetUsedGroupType()
    {
        return (static_cast<ControlFileMetaData *>(GetMetaData()))->usedGroupType;
    }

    uint64 GetMagic()
    {
        return (static_cast<ControlFileMetaData *>(GetMetaData()))->magic;
    }
};
}  // namespace DSTORE
#endif  // DSTORE_CONTROL_FILE_PAGE_H