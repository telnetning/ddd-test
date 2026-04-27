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
 * dstore_transaction_mgr.cpp
 *
 *
 * IDENTIFICATION
 *        include/undo/dstore_undo_record.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_UNDO_RECORD_H
#define DSTORE_UNDO_RECORD_H

#include "common/algorithm/dstore_type_compress.h"
#include "undo/dstore_undo_types.h"
#include "common/algorithm/dstore_string_info.h"
#include "common/dstore_datatype.h"
#include "common/memory/dstore_mctx.h"
#include "page/dstore_td.h"
#include "buffer/dstore_buf.h"
#include "transaction/dstore_transaction_types.h"
#include "errorcode/dstore_undo_error_code.h"
#include "tuple/dstore_data_tuple.h"

namespace DSTORE {

struct UndoTdInfo {
    uint64 m_undoRecPtr;
    uint64 m_xid;
    CommitSeqNo m_csn;
    uint8 m_tdId;
    TdCsnStatus m_csnStatus;

    inline UndoRecPtr GetUndoPtr() const
    {
        return UndoRecPtr(m_undoRecPtr);
    }
    inline Xid GetXid() const
    {
        return Xid(m_xid);
    }
    inline CommitSeqNo GetCsn() const
    {
        return m_csn;
    }
    inline uint8 GetTdId() const
    {
        return m_tdId;
    }
    inline TdCsnStatus GetCsnStatus() const
    {
        return m_csnStatus;
    }
    inline void SetUndoRecPtr(UndoRecPtr undoRecPtr)
    {
        m_undoRecPtr = undoRecPtr.m_placeHolder;
    }
    inline void SetXid(Xid xid)
    {
        m_xid = xid.m_placeHolder;
    }
    inline void SetCsn(CommitSeqNo csn)
    {
        m_csn = csn;
    }
    inline void SetTdId(uint8 tdId)
    {
        m_tdId = tdId;
    }
    inline void SetCsnStatus(TdCsnStatus status)
    {
        m_csnStatus = status;
    }
} PACKED;

struct UndoRecordHeader {
    UndoType m_undoType;
    CommandId m_cid;
    /* record the td info when generate this undo, you can think it as placeholder for store previous undo record */
    UndoTdInfo m_tdPreInfo;
    uint64 m_txnPreUndoPtr;
    uint64 m_ctid;
    /* Reserved for file version verification. */
    uint64 m_fileVersion;

    void Init()
    {
        m_undoType = UNDO_UNKNOWN;
        m_cid = INVALID_CID;
        m_ctid = INVALID_ITEM_POINTER.m_placeHolder;
        m_txnPreUndoPtr = INVALID_UNDO_RECORD_PTR.m_placeHolder;
        m_tdPreInfo.m_undoRecPtr = INVALID_UNDO_RECORD_PTR.m_placeHolder;
        m_tdPreInfo.m_xid = INVALID_XID.m_placeHolder;
        m_tdPreInfo.m_csn = INVALID_CSN;
        m_tdPreInfo.m_tdId = INVALID_TD_SLOT;
        m_tdPreInfo.m_csnStatus = IS_INVALID;
        m_fileVersion = INVALID_FILE_VERSION;
    }

    inline UndoType GetUndoType() const
    {
        return m_undoType;
    }
    inline CommandId GetCid() const
    {
        return m_cid;
    }
    inline UndoTdInfo GetPreTdInfo() const
    {
        return m_tdPreInfo;
    }
    inline ItemPointerData GetCtid() const
    {
        return ItemPointerData(m_ctid);
    }
    inline uint64 GetFileVersion() const
    {
        return m_fileVersion;
    }
    inline void SetUndoType(const UndoType type)
    {
        m_undoType = type;
    }
    inline void SetCid(const CommandId cid)
    {
        m_cid = cid;
    }
    inline void SetCtid(const ItemPointerData ctid)
    {
        m_ctid = ctid.m_placeHolder;
    }
    inline void SetTxnPreUndoPtr(const UndoRecPtr &ptr)
    {
        m_txnPreUndoPtr = ptr.m_placeHolder;
    }
    inline UndoRecPtr GetTxnPreUndoPtr() const
    {
        return UndoRecPtr(m_txnPreUndoPtr);
    }
} PACKED;

class UndoRecord : public BaseObject {
public:
    UndoRecord();
    UndoRecord(UndoType type, uint8 tdId, TD *td, ItemPointerData ctid, CommandId cid);
    ~UndoRecord() noexcept;

    RetStatus CheckValidity() const;
    bool IsMatchedCtid(ItemPointerData ctid) const;

    inline PageId GetPageId() const
    {
        return m_header.GetCtid().GetPageId();
    }
    inline CommandId GetCid() const
    {
        return m_header.GetCid();
    }
    inline ItemPointerData GetCtid() const
    {
        return m_header.GetCtid();
    }
    inline uint64 GetFileVersion() const
    {
        return m_header.GetFileVersion();
    }
    inline UndoType GetUndoType() const
    {
        return m_header.GetUndoType();
    }
    inline void SetUndoType(const UndoType &type)
    {
        m_header.SetUndoType(type);
    }
    inline bool IsUndoDataValid() const
    {
        /* Check type */
        if (m_header.m_undoType >= UNDO_UNKNOWN) {
            return false;
        }
        /* The size of 0 is only allowed for insert/UpdateSmallTupleNewPage of Heap/Heap_tmp */
        if (m_header.m_undoType == UNDO_HEAP_INSERT ||
            m_header.m_undoType == UNDO_HEAP_INSERT_TMP ||
            m_header.m_undoType == UNDO_HEAP_ANOTHER_PAGE_APPEND_UPDATE_NEW_PAGE ||
            m_header.m_undoType == UNDO_HEAP_ANOTHER_PAGE_APPEND_UPDATE_NEW_PAGE_TMP) {
            return GetUndoDataSize() == 0;
        }
        return (GetUndoDataSize() > 0 && GetUndoData() != nullptr);
    }
    inline void SetCtid(const ItemPointerData &ctid)
    {
        m_header.SetCtid(ctid);
    }
    inline UndoRecordHeader GetUndoRecordHeader() const
    {
        return m_header;
    }
    inline Xid GetTdPreXid() const
    {
        return m_header.GetPreTdInfo().GetXid();
    }
    inline CommitSeqNo GetTdPreCsn() const
    {
        return m_header.GetPreTdInfo().GetCsn();
    }
    inline TdCsnStatus GetTdPreCsnStatus() const
    {
        return m_header.GetPreTdInfo().GetCsnStatus();
    }
    inline UndoRecPtr GetTdPreUndoPtr() const
    {
        return m_header.GetPreTdInfo().GetUndoPtr();
    }
    inline RetStatus Append(const char *data, int32 size)
    {
        if (unlikely(m_dataInfo.init(256) == false)) {
            ErrLog(DSTORE_ERROR, MODULE_UNDO, ErrMsg("Alloc space of undo record data info failed."));
            return DSTORE_FAIL;
        }
        RetStatus ret = m_dataInfo.append_binary(data, size);
        if (STORAGE_FUNC_FAIL(ret)) {
            DstorePfreeExt(m_dataInfo.data);
            m_dataInfo.len = 0;
        }
        return ret;
    }
    inline void *GetUndoData() const
    {
        return static_cast<void *>(m_dataInfo.data);
    }
    inline uint16 GetUndoDataSize() const
    {
        StorageAssert((m_dataInfo.len >= 0 && m_dataInfo.len < UINT16_MAX));
        return static_cast<uint16>(m_dataInfo.len);
    }

    inline uint8 GetTdId() const
    {
        return m_header.GetPreTdInfo().GetTdId();
    }
    inline void SetTdId(uint8 tdId)
    {
        m_header.m_tdPreInfo.SetTdId(tdId);
    }

    inline uint32 GetRecordSize() const
    {
        return GetCompressedSize() + sizeof(m_dataInfo.len) + static_cast<uint32>(m_dataInfo.len);
    }

    inline void SetPreTdInfo(uint8 tdId, TD *td)
    {
        m_header.m_tdPreInfo.SetTdId(tdId);
        m_header.m_tdPreInfo.SetUndoRecPtr(td->GetUndoRecPtr());
        m_header.m_tdPreInfo.SetXid(td->GetXid());
        m_header.m_tdPreInfo.SetCsn(td->GetCsn());
        m_header.m_tdPreInfo.SetCsnStatus(td->GetCsnStatus());
    }

    inline void SetPreTdInfo(uint8 tdId, UndoRecPtr ptr, Xid xid, CommitSeqNo csn, TdCsnStatus csnStatus)
    {
        m_header.m_tdPreInfo.SetTdId(tdId);
        m_header.m_tdPreInfo.SetUndoRecPtr(ptr);
        m_header.m_tdPreInfo.SetXid(xid);
        m_header.m_tdPreInfo.SetCsn(csn);
        m_header.m_tdPreInfo.SetCsnStatus(csnStatus);
    }

    inline void SetTxnPreUndoPtr(const UndoRecPtr &ptr)
    {
        m_header.SetTxnPreUndoPtr(ptr);
    }

    inline UndoRecPtr GetTxnPreUndoPtr() const
    {
        return m_header.GetTxnPreUndoPtr();
    }

    char *GetSerializeData()
    {
        return m_serializeData;
    }

    uint8 GetSerializeSize() const
    {
        return m_serializeSize;
    }

    void SetSerializeSize(uint8 serializeSize)
    {
        m_serializeSize = serializeSize;
    }

    uint8 GetCompressedSize() const
    {
        if (m_serializeSize != 0) {
            return m_serializeSize;
        }
        return sizeof(GetUndoType()) + VarintCompress::GetUnsigned32CompressedSize(GetCid()) +
               GetTdPreUndoPtr().GetCompressedSize() +
               VarintCompress::GetUnsigned64CompressedSize(GetTdPreXid().m_zoneId) +
               VarintCompress::GetUnsigned64CompressedSize(GetTdPreXid().m_logicSlotId) +
               VarintCompress::GetUnsigned64CompressedSize(GetTdPreCsn()) + sizeof(GetTdId()) +
               sizeof(GetTdPreCsnStatus()) + GetTxnPreUndoPtr().GetCompressedSize() + GetCtid().GetCompressedSize() +
               sizeof(GetFileVersion()) +
               sizeof(m_serializeSize);
    }

    void Serialize()
    {
        m_serializeSize = GetCompressedSize();
        StorageAssert(thrd->GetUndoContext() != nullptr);
        char *tempdiskData = thrd->GetUndoContext();
        m_serializeData = tempdiskData;
        errno_t rc = memcpy_s(tempdiskData, sizeof(m_serializeSize), &m_serializeSize, sizeof(m_serializeSize));
        storage_securec_check(rc, "\0", "\0");
        tempdiskData += sizeof(m_serializeSize);
        rc = memcpy_s(tempdiskData, sizeof(GetUndoType()), (char *)(&(m_header.m_undoType)), sizeof(GetUndoType()));
        storage_securec_check(rc, "\0", "\0");
        tempdiskData += sizeof(GetUndoType());
        uint8 thisSize = VarintCompress::CompressUnsigned32(GetCid(), tempdiskData);
        tempdiskData += thisSize;
        GetTdPreUndoPtr().Serialize(tempdiskData);
        thisSize = VarintCompress::CompressUnsigned64(GetTdPreXid().m_zoneId, tempdiskData);
        tempdiskData += thisSize;
        thisSize = VarintCompress::CompressUnsigned64(GetTdPreXid().m_logicSlotId, tempdiskData);
        tempdiskData += thisSize;
        thisSize = VarintCompress::CompressUnsigned64(GetTdPreCsn(), tempdiskData);
        tempdiskData += thisSize;
        rc = memcpy_s(tempdiskData, sizeof(GetTdId()), (char *)(&(m_header.m_tdPreInfo.m_tdId)), sizeof(GetTdId()));
        storage_securec_check(rc, "\0", "\0");
        tempdiskData += sizeof(GetTdId());
        rc = memcpy_s(tempdiskData, sizeof(GetTdPreCsnStatus()), (char *)(&(m_header.m_tdPreInfo.m_csnStatus)),
                      sizeof(GetTdPreCsnStatus()));
        storage_securec_check(rc, "\0", "\0");
        tempdiskData += sizeof(GetTdPreCsnStatus());
        GetTxnPreUndoPtr().Serialize(tempdiskData);
        GetCtid().Serialize(tempdiskData);
        (void)VarintCompress::CompressUnsigned64(m_header.m_fileVersion, tempdiskData);
#ifdef DSTORE_USE_ASSERT_CHECKING
        Check();
#endif
    }

    void Check()
    {
        UndoRecordHeader *tempHeader = (UndoRecordHeader *)DstorePalloc0(sizeof(m_header));
        if (STORAGE_VAR_NULL(tempHeader)) {
            ErrLog(DSTORE_ERROR, MODULE_UNDO, ErrMsg("Alloc space of undo record header for check fail."));
            return;
        }
        errno_t rc = memcpy_s(tempHeader, sizeof(m_header), &m_header, sizeof(m_header));
        storage_securec_check(rc, "\0", "\0");
        Deserialize();
        int result = memcmp(tempHeader, &m_header, sizeof(UndoRecordHeader));
        StorageReleasePanic(result != 0, MODULE_UNDO, ErrMsg("undo header deserialize check failed."));
        DstorePfree(tempHeader);
    }

    void PrepareDiskData()
    {
        m_serializeData = thrd->GetUndoContext();
        StorageReleasePanic(m_serializeData == nullptr, MODULE_UNDO, ErrMsg("undo context is nullptr."));
        errno_t rc = memcpy_s(m_serializeData, sizeof(m_serializeSize),
                              static_cast<char *>(static_cast<void *>(&m_serializeSize)), sizeof(m_serializeSize));
        storage_securec_check(rc, "\0", "\0");
    }

    void DestroyDiskData()
    {
        m_serializeData = nullptr;
    }

    RetStatus PrepareDiskDataForDump()
    {
        m_serializeData = (char *)DstorePalloc(m_serializeSize);
        if (STORAGE_VAR_NULL(m_serializeData)) {
            ErrLog(DSTORE_ERROR, MODULE_UNDO,
                   ErrMsg("Alloc space of preapre disk data size(%u) failed.", m_serializeSize));
            return DSTORE_FAIL;
        }
        errno_t rc = memcpy_s(m_serializeData, sizeof(m_serializeSize),
                              static_cast<char *>(static_cast<void *>(&m_serializeSize)), sizeof(m_serializeSize));
        storage_securec_check(rc, "\0", "\0");
        return DSTORE_SUCC;
    }

    void DestroyDiskDataForDump()
    {
        DstorePfree(m_serializeData);
    }

    void Deserialize()
    {
        const char *tempdiskData = m_serializeData;
        tempdiskData += sizeof(m_serializeSize);
        errno_t rc = memcpy_s((char *)(&(m_header.m_undoType)), sizeof(m_header.GetUndoType()), tempdiskData,
                              sizeof(m_header.GetUndoType()));
        storage_securec_check(rc, "\0", "\0");
        StorageAssert(GetUndoType() < UNDO_UNKNOWN);
        tempdiskData += sizeof(GetUndoType());
        uint8 thisSize;
        m_header.m_cid = VarintCompress::DecompressUnsigned32(tempdiskData, thisSize);
        tempdiskData += thisSize;

        ItemPointerData ctid;
        ctid.Deserialize(tempdiskData);
        m_header.m_tdPreInfo.SetUndoRecPtr(ctid);

        uint64 zoneId = VarintCompress::DecompressUnsigned64(tempdiskData, thisSize);
        tempdiskData += thisSize;
        uint64 slotId = VarintCompress::DecompressUnsigned64(tempdiskData, thisSize);
        tempdiskData += thisSize;
        m_header.m_tdPreInfo.m_xid = Xid(zoneId, slotId).m_placeHolder;

        m_header.m_tdPreInfo.m_csn = VarintCompress::DecompressUnsigned64(tempdiskData, thisSize);
        tempdiskData += thisSize;

        rc = memcpy_s((char *)(&(m_header.m_tdPreInfo.m_tdId)), sizeof(m_header.m_tdPreInfo.m_tdId), tempdiskData,
                      sizeof(m_header.m_tdPreInfo.m_tdId));
        storage_securec_check(rc, "\0", "\0");
        tempdiskData += sizeof(m_header.m_tdPreInfo.m_tdId);

        rc = memcpy_s((char *)(&(m_header.m_tdPreInfo.m_csnStatus)), sizeof(m_header.m_tdPreInfo.m_csnStatus),
                      tempdiskData, sizeof(m_header.m_tdPreInfo.m_csnStatus));
        storage_securec_check(rc, "\0", "\0");
        tempdiskData += sizeof(m_header.m_tdPreInfo.m_csnStatus);

        ctid.Deserialize(tempdiskData);
        m_header.SetTxnPreUndoPtr(ctid);
        ctid.Deserialize(tempdiskData);
        m_header.SetCtid(ctid);
        m_header.m_fileVersion = VarintCompress::DecompressUnsigned64(tempdiskData, thisSize);
    }

    bool IsBtreeUndoRecord() const
    {
        return (GetUndoType() > UNDO_HEAP_BOUND && GetUndoType() < UNDO_BTREE_BOUND) ||
               (GetUndoType() > UNDO_HEAP_BOUND_TMP && GetUndoType() < UNDO_BTREE_BOUND_TMP);
    }

    bool IsHeapUndoRecord() const
    {
        return (GetUndoType() < UNDO_HEAP_BOUND) ||
               (GetUndoType() >= UNDO_HEAP_INSERT_TMP && GetUndoType() < UNDO_HEAP_BOUND_TMP);
    }

    bool IsGlobalTempTableUndoRec() const
    {
        return GetUndoType() >= UNDO_HEAP_INSERT_TMP && GetUndoType() < UNDO_BTREE_BOUND_TMP;
    }

    char *Dump() const
    {
        StringInfoData str;
        bool res = str.init();
        if (unlikely(!res)) {
            return nullptr;
        }
        Xid xid(m_header.m_tdPreInfo.m_xid);
        ItemPointerData ctid(m_header.m_ctid);
        ItemPointerData undoRecptr(m_header.m_tdPreInfo.m_undoRecPtr);
        ItemPointerData txnPreUndoPtr(m_header.m_txnPreUndoPtr);
        str.append(
            "UndoRecord: [type: %d, cid: %u, tdPreInfo: [undoRecPtr: (%hu, %u, %hu), xid: [zoneId: %lu, slotId: %lu], "
            "csn: %lu, tdId: %d, csnStatus: %d], txnPreUndoPtr: (%hu, %u, %hu), ctid: (%hu, %u, %hu), datalen: %d]",
            static_cast<int>(m_header.m_undoType), m_header.m_cid, undoRecptr.GetFileId(), undoRecptr.GetBlockNum(),
            undoRecptr.GetOffset(), static_cast<uint64>(xid.m_zoneId), xid.m_logicSlotId, m_header.m_tdPreInfo.m_csn,
            m_header.m_tdPreInfo.m_tdId, static_cast<int>(m_header.m_tdPreInfo.m_csnStatus), txnPreUndoPtr.GetFileId(),
            txnPreUndoPtr.GetBlockNum(), txnPreUndoPtr.GetOffset(), ctid.GetFileId(), ctid.GetBlockNum(),
            ctid.GetOffset(), m_dataInfo.len);
        return str.data;
    }

    inline BufferDesc* GetCurrentFetchBuf()
    {
        return m_currentFetchUndoPageBuf;
    }

    inline void SetCurrentFetchBuf(BufferDesc* bufDesc)
    {
        m_currentFetchUndoPageBuf = bufDesc;
    }

    UndoRecordHeader m_header;

    char *m_serializeData;
    uint8 m_serializeSize;
    BufferDesc* m_currentFetchUndoPageBuf; /* Just reduce buffer read */
    StringInfoData m_dataInfo; /* Just len and data need to be written into disk. */

    int32 m_pallocDataSize; /* Don't write into disk, only used in ReadUndoRecord() */
};

const uint32 UNDO_VECTOR_EXPANSION_COEFFICIENT = 2;

const uint32 UNDO_RECORD_VECTOR_DEFAULT_CAPACITY = 8;

class UndoRecordVector : public BaseObject {
public:
    UndoRecordVector() : m_size(0), m_capacity(UNDO_RECORD_VECTOR_DEFAULT_CAPACITY), m_undoRecords(nullptr)
    {}

    ~UndoRecordVector()
    {
        for (uint32 i = 0; i < m_size; i++) {
            DstorePfree(m_undoRecords[i]);
            m_undoRecords[i] = nullptr;
        }
        DstorePfree(m_undoRecords);
        m_undoRecords = nullptr;
    }

    RetStatus Init()
    {
        AutoMemCxtSwitch autoMemCxtSwitch(thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_QUERY));
        m_undoRecords = static_cast<UndoRecord **>(DstorePalloc(sizeof(UndoRecord *) * m_capacity));
        if (STORAGE_VAR_NULL(m_undoRecords)) {
            ErrLog(DSTORE_ERROR, MODULE_UNDO, ErrMsg("Alloc space of undo record vector fail."));
            return DSTORE_FAIL;
        }
        return DSTORE_SUCC;
    }

    inline uint32 GetSize() const
    {
        return m_size;
    }

    RetStatus PushUndoRecord(const UndoRecord &undoRecord)
    {
        AutoMemCxtSwitch autoMemCxtSwitch(thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_QUERY));
        if (m_size == m_capacity) {
            m_capacity = m_capacity << 1;
            UndoRecord **records = static_cast<UndoRecord **>(DstorePalloc(sizeof(UndoRecord *) * m_capacity));
            if (unlikely(records == nullptr)) {
                storage_set_error(UNDO_ERROR_OUT_OF_MEMORY);
                ErrLog(DSTORE_ERROR, MODULE_UNDO,
                    ErrMsg("dstore palloc fail, size(%lu).", sizeof(UndoRecord *) * m_capacity));
                return DSTORE_FAIL;
            }
            errno_t rc = memcpy_s(records, sizeof(UndoRecord *) * m_size, m_undoRecords, sizeof(UndoRecord *) * m_size);
            storage_securec_check(rc, "\0", "\0");
            DstorePfree(m_undoRecords);
            m_undoRecords = records;
        }

        UndoRecord *record = static_cast<UndoRecord *>(DstorePalloc(sizeof(UndoRecord) + undoRecord.GetUndoDataSize()));
        if (unlikely(record == nullptr)) {
            storage_set_error(UNDO_ERROR_OUT_OF_MEMORY);
            ErrLog(DSTORE_ERROR, MODULE_UNDO,
                ErrMsg("dstore palloc fail, size(%lu).", sizeof(UndoRecord) + undoRecord.GetUndoDataSize()));
            return DSTORE_FAIL;
        }
        errno_t rc =
            memcpy_s(&record->m_header, sizeof(UndoRecordHeader), &undoRecord.m_header, sizeof(UndoRecordHeader));
        storage_securec_check(rc, "\0", "\0");
        record->m_dataInfo.len = undoRecord.GetUndoDataSize();
        if (record->m_dataInfo.len == 0) {
            record->m_dataInfo.data = nullptr;
        } else {
            record->m_dataInfo.data = static_cast<char *>(static_cast<void *>(record)) + sizeof(UndoRecord);
            rc = memcpy_s(record->m_dataInfo.data, static_cast<uint32>(record->m_dataInfo.len),
                undoRecord.GetUndoData(), undoRecord.GetUndoDataSize());
            storage_securec_check(rc, "\0", "\0");
        }

        m_undoRecords[m_size++] = record;
        return DSTORE_SUCC;
    }

    UndoRecord &GetUndoRecord(uint32 index)
    {
        StorageAssert(index < m_size);
        return *m_undoRecords[index];
    }

private:
    uint32 m_size;
    uint32 m_capacity;
    UndoRecord **m_undoRecords;
};

}  // namespace DSTORE
#endif  // STORAGE_UNDO_RECORD_H
