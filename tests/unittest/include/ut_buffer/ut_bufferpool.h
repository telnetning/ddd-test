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
#ifndef UT_BUFFERPOOL_H
#define UT_BUFFERPOOL_H

#include "ut_utilities/ut_dstore_framework.h"
#include "buffer/dstore_buf_mgr.h"
#include <thread>

namespace DSTORE {

/*
 * UTBufferPoolTest class and its member functions.
 */
class UTBufferPoolTest : virtual public DSTORETEST {
protected:
    /* Helper utility constants */
    /*
     * LRU_MAX_USAGE is not available outside of the LRU code so we copy it here.
     * Note that tests should detect if LRU_MAX_USAGE is changed.
     */
    const uint32 BPOOL_LRU_MAX_USAGE{5u};
    const bool BPOOL_PAGE_UPDATED{true};
    const int BPOOL_MAX_NUMBER_BITS{641};
    const int BPOOL_MAX_NUMBER_BITMASKS{BPOOL_MAX_NUMBER_BITS / BPOOL_BITSET_SIZE};
    /*
     * BPOOL_DEFAULT_LRU_PARTITIONS should be chosen in such a way
     * that we may load BPOOL_DEFAULT_BUFFERPOOL_SIZE into the
     * bufferpool without running out of the free pages in any of the
     * LRU partitions. Note that this number is determined based on
     * the hash value of the buffer tag ID and can't be easily predicted.
     * With bufferpool size 10, 4 partitions will result in a hang while
     * calling bufmgr->Read(thrd->GetPdbId(), ) in a loop without releasing/un-pinning pages.
     * Note: since blocks are assigned to the LRU partitions based on hash of their
     *   PageId modulo number_lru_partitions, we should use prime numbers as the
     *   number of the LRU partitions. Therefore, 4 is a bad choice.
     *
     * Alternatively, we may set BPOOL_DEFAULT_BUFFERPOOL_SIZE higher
     * than the number of pages we read. That will ensure that we never
     * run out of free pages while loading pages.
     */
    static const int BPOOL_DEFAULT_LRU_PARTITIONS{3};
    /* Defaults */
    static const Size BPOOL_DEFAULT_BUFFERPOOL_SIZE{10};
    static const Size BPOOL_DEFAULT_NUM_BLOCKS_ON_DISK{30};
    /* Number of threads for Multi-threaded testing */
    static const Size BPOOL_MULTITHREAD_NUM_OF_THREADS{10};
    static const int BPOOL_PAGE_STRING_END_OFFSET{256};
    static const int BPOOL_BITSET_SIZE{64};
    static constexpr char BPOOL_DEFAULT_FILEPATH[]{"6000"};
    /*
     * Global/static variables to share between threads.
     */
    static const int BPOOL_MAX_NUM_BARRIERS{10};
    static const bool BPOOL_PAGE_NOT_UPDATED{false};
    static constexpr char BPOOL_UPDATED_PREFIX[]{"updated: "};

    /* Protected data members */
    BufMgr *m_bufmgr;
    VFSAdapter *m_vfs;
    Size m_bufferpool_size;
    Size m_num_blocks_on_disk;
    Size m_num_lru_partitions;
    /*
     * We need the following thread context,
     * so that each thread can reference the member variables in the
     * main test class (UTBufferPoolTest). Otherwise, function bodys of
     * the function pointers passed to the thread will not have access
     * to these variables.
     * Additional variables may be added as required.
     */
    struct TestThreadContext {
        BufMgr *bufmgr;
        FileId fileId;
    };
    /* Each test will set its own number of barriers counter */
    Size g_num_barriers;
    /*
     * Barriers for the multi-threaded testing.
     * To make it easier to add new tests, we use the global (static for the file)
     * array of barriers. That way we don't need to name them or pass them in as
     * parameters into each thread excution function.
     */
    static pthread_barrier_t g_barriers[BPOOL_MAX_NUM_BARRIERS];

    /* Protected functions */
    void SetUp() override;
    void TearDown() override;

    /* Perform actual setup of the bufferpool in memory */
    void prepare_buffer_pool(Size bufferpool_size = BPOOL_DEFAULT_BUFFERPOOL_SIZE,
                             Size num_blocks_on_disk = BPOOL_DEFAULT_NUM_BLOCKS_ON_DISK,
                             Size num_LRU_partitions = BPOOL_DEFAULT_LRU_PARTITIONS);
    void PrepareBlocksOnDisk(Size num_blocks_on_disk = BPOOL_DEFAULT_NUM_BLOCKS_ON_DISK);
    /* Function to help parameterize RandomBufferPoolAccess test */
    void RandomBufferpoolAccess(Size num_blocks_on_disk, Size bufferpool_size, const uint number_of_threads);
    /* Wrapper function for RandomBufferpoolMod test */
    void RandomBufferpoolMod(Size num_blocks_on_disk, Size bufferpool_size, const uint number_of_threads);

    /* Class static constants */
    static constexpr FileId m_fileId = 6000;
    static constexpr const char *m_filepath = BPOOL_DEFAULT_FILEPATH;

    /* Structure for multithread_randomMod function below, keeps track of randomly generated numbers  */
    struct RandomNumbers {
        BlockNumber P1;
        BlockNumber P2;
        uint random_num;
    };

    /* Class help functions for UTBufferPoolTest */
    void compare_buffer_with_disk(BufferDesc *buf, const char *sourcefile = __builtin_FILE(),
                                  int sourceline = __builtin_LINE());
    void validate_buffer(BufferDesc *buf, bool is_updated = BPOOL_PAGE_NOT_UPDATED,
                         const char *sourcefile = __builtin_FILE(), int sourceline = __builtin_LINE());
    void check_buffer_state(BufferDesc *buf, uint64 flags, bool should_flags_be_in_state,
                            const char *sourcefile = __builtin_FILE(), int sourceline = __builtin_LINE());
    void wait_for_threads_to_finish(std::thread *threads, Size num_threads);
    virtual void init_global_barriers(Size num_threads, Size num_barriers);
    virtual void destroy_global_barriers();
    bool is_in_candidate_list(BufferDesc *buf);
    bool is_in_lru_list(BufferDesc *buf);
    bool is_in_hot_list(BufferDesc *buf);
    static void update_page(BufferDesc *buf, const char *sourcefile = __builtin_FILE(),
                            int sourceline = __builtin_LINE());
    static void multithread_hot_pages(const TestThreadContext &thd_ctx, const int number_of_pages_to_hit,
                                      std::set<BufferDesc *> *buf_set);
    static void multithread_read_test(const TestThreadContext &thd_ctx, bool check_ref_count);
    static void multithread_write(const TestThreadContext &thd_ctx, int thread_id, int number_of_pages_on_disk);
    static void multithread_read_write_bitmask(const TestThreadContext &thd_ctx, uint thread_bit_idx,
                                               uint number_of_pages_on_disk);
    static std::string generate_token(const int fileId, const BlockNumber block_number, const char *prefix = nullptr);
    static void multithread_randomMod(const TestThreadContext &thd_ctx, int thread_id, uint number_of_rounds,
                                      uint bufferpool_size, bool check_sum, std::vector<RandomNumbers> *rand_tracker);
};

} /* namespace DSTORE */

#endif /* UT_BUFFERPOOL_H */
