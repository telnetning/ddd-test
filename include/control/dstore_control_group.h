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
 * dstore_control_group.h
 *  control group for dstore
 *
 *
 * IDENTIFICATION
 *        dstore/include/control/dstore_control_group.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_CONTROL_GROUP_H
#define DSTORE_CONTROL_GROUP_H
#include "control/dstore_control_file_lock.h"
#include "control/dstore_control_struct.h"
#include "control/dstore_control_file_page.h"
#include "control/dstore_control_file_mgr.h"

namespace DSTORE {
class ControlGroup : public BaseObject {
public:
    ControlGroup(ControlFileMgr *controlFileMgr, DstoreMemoryContext memCtx, ControlGroupType groupType,
                 BlockNumber metaBlock, PdbId pdbId);
    virtual ~ControlGroup() {}
    virtual void Reload() = 0;

    RetStatus WriteMetaData(void *metadata, uint32 size);
    void *GetMetaData();

    RetStatus LoadGroup();
    RetStatus PostGroup();
    inline ControlGroupType GetGroupType()
    {
        return m_groupType;
    }
    RetStatus LockGroup(CFLockMode mode)
    {
        return m_lock->Lock(mode);
    }
    void UnLockGroup(CFLockMode mode)
    {
        return m_lock->Unlock(mode);
    }
    RetStatus MarkPageDirty(BlockNumber blockNum);
    RetStatus SetLastPageBlockNumber(BlockNumber blockNum);
    BlockNumber GetLastPageBlockNumber();
    RetStatus CheckCrcAndRecovery();
    ControlMetaPage *GetMetaPage();
    ControlDataPage *GetPage(BlockNumber blockNum);
    BlockNumber GetMaxPageNum();
    DstoreMemoryContext GetMemCtx()
    {
        return m_ctx;
    }
    template<typename T>
    class ControlPageIterator;

    void Destroy();

#ifdef UT
    RetStatus UTWriteFileForFaultInjection(BlockNumber block, bool isFile1Fault, bool isUpdateCheckSum)
    {
        return m_controlFileMgr->UTWriteFileForFaultInjection(block, isFile1Fault, isUpdateCheckSum);
    }
#endif

protected:
    RetStatus Init(DeployType deployType);
    RetStatus InsertIntoAvailablePage(const char *dataItem, uint32 dataLen, ControlPageType controlPageType);
    RetStatus AddOneItem(const char *dataItem, uint32 dataLen, ControlPageType controlPageType);
    RetStatus AddItemToControlFile(const char *dataItem, uint32 dataLen, ControlPageType controlPageType);
    RetStatus ExtendNewPage(ControlPageType controlPageType);
    BlockNumber m_metaBlock;
    BlockNumber m_maxBlock;
    ControlGroupType m_groupType;
    ControlFileGlobalLock *m_lock;
    ControlMetaPage *m_metaPage;
    ControlFileMgr *m_controlFileMgr;
    DstoreMemoryContext m_ctx;
    PdbId m_pdbId;
    PageHandle *m_pageHandle;
    bool m_isInitialized;
};

template<typename T>
class ControlGroup::ControlPageIterator {
public:
    ControlPageIterator(ControlGroup *controlGroup, BlockNumber firstBlock)
        : m_controlGroup(controlGroup),
          m_controlFileMgr{controlGroup->m_controlFileMgr},
          m_pageHandle(controlGroup->m_pageHandle),
          m_currentBlock{firstBlock},
          m_currOffset{INVALID_OFFSET},
          m_itemSize{sizeof(T)},
          m_isItemDelete{false}
    {}

    ControlPageIterator(ControlGroup *controlGroup, BlockNumber firstBlock, uint16 itemSize)
        : m_controlGroup(controlGroup),
          m_controlFileMgr{controlGroup->m_controlFileMgr},
          m_pageHandle(controlGroup->m_pageHandle),
          m_currentBlock{firstBlock},
          m_currOffset{INVALID_OFFSET},
          m_itemSize{itemSize},
          m_isItemDelete{false}
    {}

    bool NextItem()
    {
        if (m_isItemDelete) {
            m_isItemDelete = false;
            while (m_currentBlock != DSTORE_INVALID_BLOCK_NUMBER) {
                ControlDataPage *page = static_cast<ControlDataPage *>(
                    static_cast<void *>(m_controlFileMgr->ReadOnePage(m_pageHandle, m_currentBlock)));
                if (unlikely(page == nullptr)) {
                    return false;
                }
                if (m_currOffset == page->GetWriteOffset()) {
                    m_currentBlock = page->GetNextPage();
                    m_currOffset = 0;
                } else {
                    return true;
                }
            }
            return false;
        } else {
            while (m_currentBlock != DSTORE_INVALID_BLOCK_NUMBER) {
                if (m_currOffset == INVALID_OFFSET) {
                    m_currOffset = 0;
                } else {
                    m_currOffset += m_itemSize;
                }

                ControlDataPage *page = static_cast<ControlDataPage *>(
                    static_cast<void *>(m_controlFileMgr->ReadOnePage(m_pageHandle, m_currentBlock)));
                if (unlikely(page == nullptr)) {
                    return false;
                }
                if ((m_currOffset) < page->GetWriteOffset()) {
                    StorageAssert((m_currOffset + m_itemSize) <= page->GetWriteOffset());
                    return true;
                }

                StorageAssert(m_currOffset == page->GetWriteOffset());

                m_currentBlock = page->GetNextPage();
                m_currOffset = INVALID_OFFSET;
            }
            return false;
        }
    }

    T *GetItem()
    {
        ControlDataPage *page = static_cast<ControlDataPage *>(
            static_cast<void *>(m_controlFileMgr->ReadOnePage(m_pageHandle, m_currentBlock)));
        if (unlikely(page == nullptr)) {
            return nullptr;
        }
        return static_cast<T*>(page->GetItem(m_currOffset, m_itemSize));
    }

    void MarkItemDelete()
    {
        m_isItemDelete = true;
    }

    uint16 GetCurrentOffset()
    {
        return m_currOffset;
    }

    BlockNumber GetCurrentBlock()
    {
        return m_currentBlock;
    }

private:
    ControlGroup *m_controlGroup;
    ControlFileMgr *m_controlFileMgr;
    PageHandle *m_pageHandle;
    BlockNumber m_currentBlock;
    uint16 m_currOffset;
    uint16 m_itemSize;
    bool m_isItemDelete;
};

} // namespace DSTORE
#endif // DSTORE_CONTROL_GROUP_H