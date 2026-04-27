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
 * dstore_memory_diagnose.h
 *
 * IDENTIFICATION
 *        src/gausskernel/dstore/interface/diagnose/dstore_memory_diagnose.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_MEMORY_DIAGNOSE
#define DSTORE_MEMORY_DIAGNOSE

namespace DSTORE {
#pragma GCC visibility push(default)

const uint32 MAX_THREAD_ID_LENGTH = 32;
const uint32 MAX_SESSION_ID_LENGTH = 32;
const uint32 MAX_CONTEXT_NAME_LENGTH = 128;
const uint32 MAX_FILE_NAME_LENGTH = 128;

enum GetMemInfoFilterType : uint8 {
    GET_SHARED_TOTAL_INFO = 0,
    GET_THREAD_TOTAL_INFO,
    GET_SESSION_TOTAL_INFO,
    GET_SHARED_DETAIL_INFO,
    GET_THREAD_DETAIL_INFO,
    GET_MAX_AND_USED_SIZE
};

struct SharedMemoryDetail {
    /* Shared memory overview in each context. */
    char contextName[MAX_CONTEXT_NAME_LENGTH];
    uint32 level;
    char parent[MAX_CONTEXT_NAME_LENGTH];
    uint64 totalSize;
    uint64 freeSize;
    uint64 usedSize;
    /* Shared memory detail in each context. */
    char fileName[MAX_FILE_NAME_LENGTH];
    int lineNumber;
    uint64 allocSize;
};

struct ThreadMemoryDetail {
    /* Thread memory overview in each context. */
    char threadId[MAX_THREAD_ID_LENGTH];
    ThreadId tid;
    char threadType[MAX_CONTEXT_NAME_LENGTH];
    char contextName[MAX_CONTEXT_NAME_LENGTH];
    uint32 level;
    char parent[MAX_CONTEXT_NAME_LENGTH];
    uint64 totalSize;
    uint64 freeSize;
    uint64 usedSize;
    /* Thread memory detail in each context. */
    char fileName[MAX_FILE_NAME_LENGTH];
    int lineNumber;
    uint64 allocSize;
};

struct SessionMemoryDetail {
    /* Session memory overview in each context. */
    char sessionId[MAX_SESSION_ID_LENGTH];
    ThreadId tid;
    char contextName[MAX_CONTEXT_NAME_LENGTH];
    uint32 level;
    char parent[MAX_CONTEXT_NAME_LENGTH];
    uint64 totalSize;
    uint64 freeSize;
    uint64 usedSize;
    /* Session memory detail in each context. */
    char fileName[MAX_FILE_NAME_LENGTH];
    int lineNumber;
    uint64 allocSize;
};

class MemoryDiagnose {
public:
    static char *PrintMemoryInfo();
    
    static RetStatus GetAllSharedMemoryDetails(SharedMemoryDetail **detailsArray, uint32 *detailsArrayLen);
    
    static RetStatus GetAllThreadsMemoryDetails(ThreadMemoryDetail **detailsArray, uint32 *detailsArrayLen);
    
    static RetStatus GetAllSessionsMemoryDetails(SessionMemoryDetail **detailsArray, uint32 *detailsArrayLen);

    static RetStatus GetSharedMemoryDetail(char *contextName, uint32 contextNameSize, SharedMemoryDetail **detailsArray,
        uint32 *detailsArrayLen);

    static RetStatus GetThreadMemoryDetail(ThreadId tid, char *contextName, uint32 contextNameSize,
        ThreadMemoryDetail **detailsArray, uint32 *detailsArrayLen);

    static RetStatus GetSessionMemoryDetail(char *contextName, uint32 contextNameSize,
        SessionMemoryDetail **detailsArray, uint32 *detailsArrayLen);

    static RetStatus GetUsedSharedMemory(uint64 *usedSize);
};

#pragma GCC visibility pop
} /* namespace MemoryDiagnose */

#endif /* STORAGE_MEMORY_DIAGNOSE */
