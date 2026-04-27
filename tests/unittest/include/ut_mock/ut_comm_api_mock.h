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
#ifndef DSTORE_UT_COMM_API_MOCK_H
#define DSTORE_UT_COMM_API_MOCK_H

/* ut_utilities/ut_thread_pool.h must come before other headers. */
#include "ut_utilities/ut_thread_pool.h"

#include <atomic>
#include <stack>
#include <vector>
#include <mutex>
#include <condition_variable>

namespace DSTORE {

/* The wait interval between receiving thread reading the file. */
#define RPC_RETRY_INTERVAL_USEC (1000)
#define RPC_RETRY_INTERVAL_MILLIS (100)
/* Maximum length for rpc file name. */
#define RPC_FILE_NAME_MAX_LEN (256)
/* Maximum length for node list. */
#define RPC_NODE_LIST_MAX_LEN (64)
/* RPC directory for all rpc files. */
#define RPC_DIRECTORY ("rpc")
/* The max data size allowed. */
#define MOCK_RPC_MAX_DATA_SIZE (1024*1024)
/* The max blocked request contexts allowed. */
#define MAX_MOCK_REQUEST_CONTEXT_NUM (1024)
/* The sentinel value used for memory sanity check. */
#define MOCK_RPC_HEADER_SENTINEL_VALUE (0XFACEBEEF)

/* For MockRPC, we use memContext to store if this is a forward message or using zero-copy. */
#define UT_FORWARD_MESSAGE_FLAG (1)
#define UT_COMM_ZERO_COPY_FLAG (1 << 1)

#ifndef container_of
#define container_of(ptr, type, member)                    \
    ({                                                     \
        const typeof(((type *)0)->member) *__mptr = (ptr); \
        (type *)((char *)__mptr - offsetof(type, member)); \
    })
#endif

/* fopen mode according to c++ reference. */
/* append: Open file for output at the end of a file. The file is created if it does not exist. */
constexpr const char *RPC_FILE_CREATE_APPEND_MODE = "a";
/* read/update: Open a file for update (both for input and output). The file must exist. */
constexpr const char *RPC_FILE_READ_UPDATE_MODE = "r+";
constexpr const int INVALID_FILE_DESC = -1;

/* Global thread local boolean allows UT to check if RPC request was called. */
extern thread_local bool g_hasRPCBeenCalled;

/* The header is embedded to every memory chunck allocated by MockRPC::CommZeroAllocMem. */
struct MockRpcMsgHeader {
    int contextId;
    NodeId senderId;
    uint64 sentinel;
};

#define MOCK_RPC_RESPONSE_GET_CONTAINER(ptr) (container_of(ptr, MockRpcResponseMsg, response))

struct UTRpcFile {
    NodeId m_selfNodeId;
    int m_fileDesc;
    bool m_isRequestFile;
    char m_name[RPC_FILE_NAME_MAX_LEN];
    pthread_mutex_t m_fileMutex = PTHREAD_MUTEX_INITIALIZER;

    /* Constructor for rpc debug file rpc.txt. */
    UTRpcFile();
    /* Constructor for rpc binary file rpc_request_XXX and rpc_response_XXX. */
    UTRpcFile(bool isRequestFile, NodeId selfNodeId);
    /* Clear file content. */
    void ClearContent();
    /* We only support exclusive file lock, because both AppendFile and ReadFile modify the file content. */
    bool Open();
    void Lock();
    void Unlock();
    void Close();

    void Write(const void *buf, size_t count);
};

} /* namespace DSTORE */

#endif /* #define DSTORE_UT_COMM_API_MOCK_H */
