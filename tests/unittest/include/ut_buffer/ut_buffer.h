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
 */
#ifndef UT_BUFFER_H
#define UT_BUFFER_H

#include <fcntl.h>
#include <thread>
#include "ut_buffer/ut_buffer_fake_instance.h"
#include "buffer/dstore_buf.h"
#include "buffer/dstore_buf_mgr.h"
#include "framework/dstore_vfs_adapter.h"
#include "vfs/vfs_interface.h"

using namespace DSTORE;

struct FakeFile {
    FileId file_id;
    const char *path;
    BlockNumber max_block;
};

static const FakeFile FAKE_FILES[] = {
    {6100U, "6100", 1000U},
    {6101U, "6101", 1000U},
    {6102U, "6102", 1000U},
    {6103U, "6103", 1000U}
};

static const Size FAKE_SIZE = sizeof(FAKE_FILES) / sizeof(FakeFile);

class BufferTest : public DSTORETEST {
public:
    static const uint64 UNSET_FLAGS = (
                    Buffer::BUF_LOCKED |
                    Buffer::BUF_IO_IN_PROGRESS |
                    Buffer::BUF_IO_ERROR);

    const double BUFLRU_DEFAULT_HOT_RATIO = 0.8;

    using CRRange = std::pair<CommitSeqNo, CommitSeqNo>;
    CRRange range_0 = {INVALID_CSN, INVALID_CSN};
    CRRange range_1 = {10, 20};
    CRRange range_2 = {20, 30};
    CRRange range_3 = {30, 40};
    CRRange range_4 = {40, 50};
    CRRange range_5 = {50, 60};
    CRRange range_6 = {60, 70};
    CRRange range_7 = {70, 80};
    CRRange range_8 = {80, 90};
    CRRange range_9 = {90, 100};

    using Csn2RangeVector = std::vector<std::pair<CommitSeqNo, CRRange>>;

    std::map<CRRange, std::string> page_contents = {
            {range_0, "Page"},
            {range_1, "CR Page V1"},
            {range_2, "CR Page V2"},
            {range_3, "CR Page V3"},
            {range_4, "CR Page V4"},
            {range_5, "CR Page V5"},
            {range_6, "CR Page V6"},
            {range_7, "CR Page V7"},
            {range_8, "CR Page V8"},
            {range_9, "CR Page V9"}
    };

    void SetUp() override
    {
        DSTORETEST::SetUp();
        InstallDatabase(&DSTORETEST::m_guc, m_ut_memory_context);
        BufferMgrFakeStorageInstance *bufMgrInstance = DstoreNew(m_ut_memory_context)BufferMgrFakeStorageInstance();
        /* this test suite open local ADIO default */
        g_enableAsyncIoFlush = true;

        bufMgrInstance->Startup(&DSTORETEST::m_guc);
        prepare_fake_file();
    }
    void TearDown() override
    {
        remove_fake_file();
        BufferMgrFakeStorageInstance *bufMgrInstance = (BufferMgrFakeStorageInstance *)g_storageInstance;
        bufMgrInstance->Shutdown();
        delete bufMgrInstance;
        DSTORETEST::TearDown();
        g_enableAsyncIoFlush = false;
    }

    void prepare_fake_file()
    {
        VFSAdapter *vfs = g_storageInstance->GetPdb(g_defaultPdbId)->GetVFS();
        Size file_num = sizeof(FAKE_FILES) / sizeof(FakeFile);
        int ret = 0;
        FileParameter filePara;
        filePara.streamId = VFS_DEFAULT_FILE_STREAM_ID;
        filePara.flag = IN_PLACE_WRITE_FILE;
        filePara.fileSubType = DATA_FILE_TYPE;
        filePara.rangeSize = (64 << 10);
        filePara.maxSize = (uint64) DSTORE_MAX_BLOCK_NUMBER * BLCKSZ;
        filePara.recycleTtl = 0;
        filePara.mode = FILE_READ_AND_WRITE_MODE;
        filePara.isReplayWrite = false;
        ret = strcpy_s(filePara.storeSpaceName, STORESPACE_NAME_MAX_LEN, UT_DEFAULT_STORESPACE_NAME);
        storage_securec_check(ret, "\0", "\0");
        for (Size i = 0; i < file_num; i++) {
            ret = vfs->CreateFile(FAKE_FILES[i].file_id, FAKE_FILES[i].path, filePara);
            StorageAssert(ret == 0);
            ret = vfs->Extend(FAKE_FILES[i].file_id, GetOffsetByBlockNo(FAKE_FILES[i].max_block));
            StorageAssert(ret == 0);
        }
    }

    void reopen_fake_file_with_ADIO()
    {
        Size file_num = sizeof(FAKE_FILES) / sizeof(FakeFile);
        VFSAdapter *vfs = g_storageInstance->GetPdb(g_defaultPdbId)->GetVFS();
        for (Size i = 0; i < file_num; i++) {
            /* temp ways to avoid CreateFile cannot add ADIO flags but only By OpenFile */
            vfs->Close(FAKE_FILES[i].file_id);
            /*
            * default call EnableADIO to open local ADIO when local disk mount VFS,
            * and then open with DSTORE_FILE_ADIO_FLAG.
            */
            int ret = vfs->OpenFile(FAKE_FILES[i].file_id, FAKE_FILES[i].path, DSTORE_FILE_ADIO_FLAG);
            StorageAssert(ret == 0);
        }
    }

    void remove_fake_file()
    {
        VFSAdapter *vfs = g_storageInstance->GetPdb(g_defaultPdbId)->GetVFS();
        Size file_num = sizeof(FAKE_FILES) / sizeof(FakeFile);
        int ret = 0;
        for (Size i = 0; i < file_num; i++) {
            vfs->Close(FAKE_FILES[i].file_id);
            vfs->RemoveFile(FAKE_FILES[i].file_id, FAKE_FILES[i].path);
        }
    }

    testing::AssertionResult check_buffer(BufferDesc* buffer, PageId page_id)
    {
        BufferTag bufTag = {g_defaultPdbId, page_id};
        if (buffer == INVALID_BUFFER_DESC) {
            return testing::AssertionFailure() << "buffer is INVALID_BUFFER_DESC";
        }

        if (buffer->bufTag != bufTag) {
            return testing::AssertionFailure() << "buffer tag is wrong."
                                               << " expect:[" << bufTag.pdbId << "," << bufTag.pageId.m_fileId << "," << bufTag.pageId.m_blockId << "]"
                                               << " actual:[" << buffer->bufTag.pdbId << "," << buffer->bufTag.pageId.m_fileId << "," << buffer->bufTag.pageId.m_blockId << "]";
        }

        return testing::AssertionSuccess();
    }

    testing::AssertionResult check_buffer_state(BufferDesc* buffer, uint32 refcount, uint64 set_flags, uint64 unset_flags)
    {
        uint64 state = GsAtomicReadU64(&buffer->state);
        if (buffer->GetRefcount() != refcount) {
            return testing::AssertionFailure() << "buffer refcount is wrong." << " expect:" << refcount << " actual:" << buffer->GetRefcount();
        }

        if ((state & set_flags) != set_flags) {
            return testing::AssertionFailure() << "buffer don't have all set flag." << " expect:" << set_flags << " actual:" << (state & set_flags);
        }

        if ((state & unset_flags) != 0) {
            return testing::AssertionFailure() << "buffer has unset_flag." << "actual:" << (state & unset_flags);
        }

        return testing::AssertionSuccess();
    }

    testing::AssertionResult check_buffer_lru(BufferDesc* buffer, LruNodeType type, uint32 usage)
    {
        if (type == DSTORE::LN_PENDING) {
            if (!buffer->lruNode.IsInPendingState()) {
                return testing::AssertionFailure() << "buffer[" << buffer->bufTag.pdbId << "," << buffer->bufTag.pageId.m_fileId
                                                   << "," << buffer->bufTag.pageId.m_blockId << "] is not in pending state";
            }
        } else if (type == DSTORE::LN_CANDIDATE) {
            if (!buffer->lruNode.IsInCandidateList()) {
                return testing::AssertionFailure() << "buffer[" << buffer->bufTag.pdbId << "," << buffer->bufTag.pageId.m_fileId
                                                   << "," << buffer->bufTag.pageId.m_blockId << "] is not in candidate list";
            }
        } else if (type == DSTORE::LN_LRU) {
            if (!buffer->lruNode.IsInLruList()) {
                return testing::AssertionFailure() << "buffer[" << buffer->bufTag.pdbId << "," << buffer->bufTag.pageId.m_fileId
                                                   << "," << buffer->bufTag.pageId.m_blockId << "] is not in lru list";
            }

            if (buffer->lruNode.GetUsage() != usage) {
                return testing::AssertionFailure() << "buffer[" << buffer->bufTag.pdbId << "," << buffer->bufTag.pageId.m_fileId
                                                   << "," << buffer->bufTag.pageId.m_blockId << "] usage is wrong."
                                                   << " expect:" << usage
                                                   << " actual:" << (uint32)buffer->lruNode.GetUsage();
            }
        } else if (type == DSTORE::LN_HOT) {
            if (!buffer->lruNode.IsInHotList()) {
                return testing::AssertionFailure() << "buffer[" << buffer->bufTag.pdbId << "," << buffer->bufTag.pageId.m_fileId
                                                   << "," << buffer->bufTag.pageId.m_blockId << "] is not in hot list";
            }
        }
        return testing::AssertionSuccess();
    }

    testing::AssertionResult check_cr_buffer_cr_info(BufferDesc *cr_buffer,
                                                     BufferDesc *base_buffer,
                                                     CommitSeqNo csn_start = INVALID_CSN,
                                                     CommitSeqNo csn_end = INVALID_CSN)
    {
        if (cr_buffer->GetCrBaseBuffer() != base_buffer) {
            return testing::AssertionFailure() << "base buffer in cr buffer's cr_info is wrong";
        }

        if (base_buffer->GetCrBuffer() != cr_buffer) {
            return testing::AssertionFailure() << "cr buffer in base buffer cr_info is wrong";
        }
        return testing::AssertionSuccess();
    }

    testing::AssertionResult check_free_buffer_cr_info(BufferDesc *buffer_desc)
    {
        if (buffer_desc->crInfo.baseBufferDesc != INVALID_BUFFER_DESC) {
            return testing::AssertionFailure() << "the base buffer in a free cr buffer's cr_info is not INVALID";
        }
        return testing::AssertionSuccess();
    }

    void load_page_into_buffer(BufMgr* buffer_pool, PageId page_id, const char* content = nullptr) {
        BufferDesc* buffer = buffer_pool->Read(g_defaultPdbId, page_id, LW_EXCLUSIVE);
        uint64 set_flags = (Buffer::BUF_VALID | Buffer::BUF_TAG_VALID);
        ASSERT_TRUE(check_buffer(buffer, page_id));
        ASSERT_TRUE(check_buffer_state(buffer, 1U,
                                       (Buffer::BUF_VALID | Buffer::BUF_TAG_VALID),
                                       UNSET_FLAGS | Buffer::BUF_CR_PAGE));
        ASSERT_TRUE(buffer->IsContentLocked(LW_EXCLUSIVE));
        ASSERT_FALSE(LWLockHeldByMe(buffer->controller->GetIoInProgressLwLock()));
        ASSERT_TRUE(check_buffer_lru(buffer, DSTORE::LN_LRU, 1U));

        if (content != nullptr) {
            buffer->GetPage()->Init(0, DSTORE::PageType::HEAP_PAGE_TYPE, page_id);
            errno_t rc = memcpy_s(reinterpret_cast<char *>(buffer->GetPage()) + sizeof(Page::m_header),
                     BLCKSZ - sizeof(Page::m_header), content, strlen(content));
            storage_securec_check(rc, "\0", "\0");
            buffer_pool->MarkDirty(buffer);
            set_flags |= (Buffer::BUF_CONTENT_DIRTY | Buffer::BUF_HINT_DIRTY);
        }

        buffer_pool->UnlockAndRelease(buffer);

        ASSERT_TRUE(check_buffer_state(buffer, 0U, set_flags,
                                       UNSET_FLAGS | Buffer::BUF_CR_PAGE));
        ASSERT_FALSE(buffer->IsHeldContentLockByMe());
        ASSERT_FALSE(LWLockHeldByMe(buffer->controller->GetIoInProgressLwLock()));
    }

    void access_buffer_to_make_it_hot(BufMgr* buffer_pool, PageId page_id)
    {
        for (uint32 i = 1; i < 4; i++) {
            BufferDesc *buffer = buffer_pool->Read(g_defaultPdbId, page_id, LW_SHARED);

            ASSERT_TRUE(check_buffer(buffer, page_id));
            ASSERT_TRUE(check_buffer_state(buffer, 1U,
                                           (Buffer::BUF_VALID | Buffer::BUF_TAG_VALID),
                                           UNSET_FLAGS | Buffer::BUF_CR_PAGE));

            ASSERT_TRUE(buffer->IsContentLocked(LW_SHARED));
            ASSERT_FALSE(LWLockHeldByMe(buffer->controller->GetIoInProgressLwLock()));
            ASSERT_TRUE(check_buffer_lru(buffer, DSTORE::LN_LRU, 1 + i));

            buffer_pool->UnlockContent(buffer);
            buffer_pool->Release(buffer);
            ASSERT_TRUE(check_buffer_state(buffer, 0U,
                                           (Buffer::BUF_VALID | Buffer::BUF_TAG_VALID),
                                           UNSET_FLAGS | Buffer::BUF_CR_PAGE));
            ASSERT_FALSE(buffer->IsHeldContentLockByMe());
            ASSERT_FALSE(LWLockHeldByMe(buffer->controller->GetIoInProgressLwLock()));
        }

        BufferDesc *buffer = buffer_pool->Read(g_defaultPdbId, page_id, LW_SHARED);

        ASSERT_TRUE(check_buffer(buffer, page_id));
        ASSERT_TRUE(check_buffer_state(buffer, 1U,
                                       (Buffer::BUF_VALID | Buffer::BUF_TAG_VALID),
                                       UNSET_FLAGS | Buffer::BUF_CR_PAGE));

        ASSERT_TRUE(buffer->IsContentLocked(LW_SHARED));
        ASSERT_FALSE(LWLockHeldByMe(buffer->controller->GetIoInProgressLwLock()));
        ASSERT_TRUE(check_buffer_lru(buffer, DSTORE::LN_HOT, 0));

        buffer_pool->UnlockContent(buffer);
        buffer_pool->Release(buffer);
        ASSERT_TRUE(check_buffer_state(buffer, 0U,
                                       (Buffer::BUF_VALID | Buffer::BUF_TAG_VALID),
                                       UNSET_FLAGS | Buffer::BUF_CR_PAGE));
        ASSERT_FALSE(buffer->IsHeldContentLockByMe());
        ASSERT_FALSE(LWLockHeldByMe(buffer->controller->GetIoInProgressLwLock()));
    }

    void wait_for_thread_all_finish(std::thread *threads, Size size)
    {
        for (int i = 0; i < size; i++) {
            threads[i].join();
        }
    }

    void check_base_buffer_after_release(BufferDesc *base_buffer, uint64 set_flags = (Buffer::BUF_VALID | Buffer::BUF_TAG_VALID))
    {
        ASSERT_TRUE(check_buffer_state(base_buffer, 0U,
                                      set_flags,
                                      UNSET_FLAGS | Buffer::BUF_CR_PAGE));
        ASSERT_FALSE(LWLockHeldByMe(&base_buffer->contentLwLock));
        ASSERT_FALSE(LWLockHeldByMe(base_buffer->controller->GetIoInProgressLwLock()));
        ASSERT_FALSE(LWLockHeldByMe(base_buffer->controller->GetCrAssignLwLock()));
    }

    void check_cr_buffer_after_release(BufferDesc *cr_buffer)
    {
        ASSERT_TRUE(check_buffer_state(cr_buffer, 0U,
                                       Buffer::BUF_VALID | Buffer::BUF_CR_PAGE,
                                       UNSET_FLAGS
                                            | Buffer::BUF_TAG_VALID | Buffer::BUF_CONTENT_DIRTY |
                                            Buffer::BUF_HINT_DIRTY));
        ASSERT_FALSE(LWLockHeldByMe(&cr_buffer->contentLwLock));
        ASSERT_FALSE(LWLockHeldByMe(cr_buffer->controller->GetIoInProgressLwLock()));
        ASSERT_FALSE(LWLockHeldByMe(cr_buffer->controller->GetCrAssignLwLock()));
    }

    void check_free_buffer_after_release(BufferDesc* buffer_desc)
    {
        ASSERT_TRUE(check_buffer_state(buffer_desc, 0U,
                                       0,
                                       UNSET_FLAGS |
                                       Buffer::BUF_TAG_VALID | Buffer::BUF_CONTENT_DIRTY |
                                       Buffer::BUF_HINT_DIRTY |
                                       Buffer::BUF_VALID | Buffer::BUF_CR_PAGE));
        ASSERT_FALSE(LWLockHeldByMe(&buffer_desc->contentLwLock));
        ASSERT_FALSE(LWLockHeldByMe(buffer_desc->controller->GetIoInProgressLwLock()));
        ASSERT_FALSE(LWLockHeldByMe(buffer_desc->controller->GetCrAssignLwLock()));
        ASSERT_TRUE(check_buffer_lru(buffer_desc, LruNodeType::LN_CANDIDATE, 0));
    }

    void check_base_buffer_after_read(PageId &page_id, BufferDesc *base_buffer, DSTORE::LruNodeType type = LruNodeType::LN_HOT, uint32 usage = 0,
                                        uint64 set_flags = (Buffer::BUF_VALID | Buffer::BUF_TAG_VALID))
    {
        ASSERT_TRUE(check_buffer(base_buffer, page_id));
        ASSERT_TRUE(check_buffer_state(base_buffer, 1U, set_flags, UNSET_FLAGS | Buffer::BUF_CR_PAGE));
        ASSERT_TRUE(LWLockHeldByMeInMode(&base_buffer->contentLwLock, LW_SHARED));
        ASSERT_FALSE(LWLockHeldByMe(base_buffer->controller->GetIoInProgressLwLock()));
        ASSERT_FALSE(LWLockHeldByMe(base_buffer->controller->GetCrAssignLwLock()));
        ASSERT_TRUE(check_buffer_lru(base_buffer, type, usage));
    }

    template<bool is_exists = false, bool is_read = true>
    void check_base_buffer_after_call_read_or_alloc_cr(PageId &page_id, BufferDesc *base_buffer,
                                                       LruNodeType type = LruNodeType::LN_HOT, uint32 usage = 0,
                                                       uint64 set_flags = (Buffer::BUF_VALID |
                                                       Buffer::BUF_TAG_VALID))
    {
        ASSERT_TRUE(check_buffer(base_buffer, page_id));
        if (is_read) {
            ASSERT_TRUE(check_buffer_state(base_buffer, 1U,
                                           set_flags,
                                           UNSET_FLAGS | Buffer::BUF_CR_PAGE));
            ASSERT_TRUE(LWLockHeldByMeInMode(&base_buffer->contentLwLock, LW_SHARED));
        } else {
            ASSERT_TRUE(check_buffer_state(base_buffer, 0U,
                                           set_flags,
                                           UNSET_FLAGS | Buffer::BUF_CR_PAGE));

            ASSERT_FALSE(base_buffer->IsHeldContentLockByMe());
        }

        ASSERT_FALSE(LWLockHeldByMe(base_buffer->controller->GetIoInProgressLwLock()));
        if (is_exists) {
            ASSERT_FALSE(LWLockHeldByMe(base_buffer->controller->GetCrAssignLwLock()));
        } else {
            ASSERT_TRUE(LWLockHeldByMe(base_buffer->controller->GetCrAssignLwLock()));
        }
        ASSERT_TRUE(check_buffer_lru(base_buffer, type, usage));
    }

    void check_closest_cr_buffer_after_read(PageId &page_id, BufferDesc *closest_buffer, BufferDesc *base_buffer, CRRange &range, uint32 usage = 0)
    {
        ASSERT_TRUE(check_buffer(closest_buffer, page_id));
        ASSERT_TRUE(check_buffer_state(closest_buffer, 1U,
                                       Buffer::BUF_CR_PAGE | Buffer::BUF_VALID,
                                       UNSET_FLAGS | (Buffer::BUF_TAG_VALID | Buffer::BUF_CONTENT_DIRTY
                                       | Buffer::BUF_HINT_DIRTY)));

        ASSERT_FALSE(LWLockHeldByMe(&closest_buffer->contentLwLock));
        ASSERT_FALSE(LWLockHeldByMe(closest_buffer->controller->GetIoInProgressLwLock()));
        ASSERT_TRUE(check_buffer_lru(closest_buffer, DSTORE::LN_LRU, usage));
        ASSERT_TRUE(check_cr_buffer_cr_info(closest_buffer, base_buffer, range.first, range.second));
        ASSERT_EQ(0, memcmp(reinterpret_cast<char *>(closest_buffer->GetPage()) + sizeof(Page::m_header),
                            page_contents[range].c_str(),
                            strlen(page_contents[range].c_str())));
    }

    void check_closest_base_buffer_after_read(PageId &page_id, BufferDesc *closest_buffer, BufferDesc *base_buffer)
    {
        ASSERT_TRUE(check_buffer(closest_buffer, page_id));
        ASSERT_TRUE(check_buffer_state(closest_buffer, 1U,
                                       (Buffer::BUF_VALID | Buffer::BUF_TAG_VALID),
                                       UNSET_FLAGS | Buffer::BUF_CR_PAGE));
        ASSERT_FALSE(closest_buffer->IsCrPage());
        ASSERT_TRUE(LWLockHeldByMe(&closest_buffer->contentLwLock));
        ASSERT_FALSE(LWLockHeldByMe(closest_buffer->controller->GetIoInProgressLwLock()));
        ASSERT_TRUE(check_buffer_lru(closest_buffer, DSTORE::LN_HOT, 0U));
        ASSERT_EQ(closest_buffer, base_buffer);
        ASSERT_EQ(0, memcmp(reinterpret_cast<char *>(closest_buffer->GetPage()) + sizeof(Page::m_header),
                            page_contents[range_0].c_str(),
                            page_contents[range_0].length()));
    }

    template<bool is_exists = false>
    void check_cr_buffer_after_call_read_or_alloc_cr(PageId &page_id, BufferDesc *cr_buffer, BufferDesc **base_buffer /*= INVALID_BUFFER_DESC*/,  CommitSeqNo cstart = INVALID_CSN, CommitSeqNo cend = INVALID_CSN)
    {
        uint64 set_flags;
        uint64 unset_flags;
        if (is_exists) {
            set_flags = Buffer::BUF_CR_PAGE | Buffer::BUF_VALID;
            unset_flags = UNSET_FLAGS | (Buffer::BUF_TAG_VALID | Buffer::BUF_CONTENT_DIRTY | Buffer::BUF_HINT_DIRTY);
        } else {
            set_flags = Buffer::BUF_CR_PAGE;
            unset_flags = UNSET_FLAGS | (Buffer::BUF_TAG_VALID | Buffer::BUF_CONTENT_DIRTY | Buffer::BUF_HINT_DIRTY |
                                         Buffer::BUF_VALID);
        }
        ASSERT_TRUE(check_buffer(cr_buffer, page_id));
        ASSERT_TRUE(check_buffer_state(cr_buffer, 1U, set_flags, unset_flags));
        ASSERT_TRUE(cr_buffer->IsCrPage());
        if (*base_buffer == INVALID_BUFFER_DESC) {
            *base_buffer = cr_buffer->GetCrBaseBuffer();
            ASSERT_TRUE(check_buffer(*base_buffer, page_id));
        }

        if (is_exists) {
            ASSERT_TRUE(cr_buffer->IsCrValid());
            ASSERT_FALSE((*base_buffer)->IsCrAssignLocked(DSTORE::LW_EXCLUSIVE));
            ASSERT_TRUE(check_buffer_lru(cr_buffer, DSTORE::LN_LRU, 2));
        } else {
            ASSERT_FALSE(cr_buffer->IsCrValid());
            ASSERT_TRUE((*base_buffer)->IsCrAssignLocked(DSTORE::LW_EXCLUSIVE));
            ASSERT_TRUE(check_buffer_lru(cr_buffer, DSTORE::LN_LRU, 1));
        }

        ASSERT_FALSE(LWLockHeldByMe(&cr_buffer->contentLwLock));
        ASSERT_FALSE(LWLockHeldByMe(cr_buffer->controller->GetIoInProgressLwLock()));
        ASSERT_TRUE(check_cr_buffer_cr_info(cr_buffer, *base_buffer, cstart, cend));
    }

    void check_after_finish_cr_build_fail(BufferDesc *cr_buffer_desc, BufferDesc *base_buffer_desc)
    {
        ASSERT_FALSE(base_buffer_desc->IsCrAssignLocked(DSTORE::LW_EXCLUSIVE));
        ASSERT_TRUE(check_free_buffer_cr_info(cr_buffer_desc));
        ASSERT_NE(base_buffer_desc->GetCrBuffer(), cr_buffer_desc);
        ASSERT_FALSE(cr_buffer_desc->IsCrPage());
        ASSERT_FALSE(cr_buffer_desc->IsCrValid());
    }

    void check_after_finish_cr_build_success(BufferDesc *cr_buffer,
                                             BufferDesc *base_buffer,
                                             CommitSeqNo cstart,
                                             CommitSeqNo cend)
    {
        ASSERT_FALSE(base_buffer->IsCrAssignLocked(DSTORE::LW_EXCLUSIVE));
        ASSERT_TRUE(check_cr_buffer_cr_info(cr_buffer, base_buffer, cstart, cend));
        ASSERT_TRUE(cr_buffer->IsCrPage());
        ASSERT_TRUE(cr_buffer->IsCrValid());
    }
};

#endif
