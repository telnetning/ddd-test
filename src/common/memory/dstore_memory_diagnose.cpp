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
 * This file defines the MemoryDiagnose interface.
 *
 * IDENTIFICATION
 *        src/commen/memory/dstore_memory_diagnose.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "common/memory/dstore_mctx.h"
#include "framework/dstore_instance.h"
#include "framework/dstore_thread.h"
#include "diagnose/dstore_memory_diagnose.h"

namespace DSTORE {

char *MemoryDiagnose::PrintMemoryInfo()
{
    StringInfoData dumpInfo;
    dumpInfo.init();

    dumpInfo.append("\nMctx size at StorageInstance level\n");

    DstoreAllocSetContext* context = g_storageInstance->GetMemoryMgr()->GetRoot()->allocSet;
    dumpInfo.append("  Root mctx pre-allocated size: %lu, used: %lu \n",
                    context->get_total_size(), context->get_used_size());
    context = g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE)->allocSet;
    dumpInfo.append("  LongLive mctx pre-allocated size: %lu, used: %lu \n",
                    context->get_total_size(), context->get_used_size());
    context = g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_SMGR)->allocSet;
    dumpInfo.append("  Smgr mctx pre-allocated size: %lu, used: %lu \n",
                    context->get_total_size(), context->get_used_size());
    context = g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_ERROR)->allocSet;
    dumpInfo.append("  Error mctx pre-allocated size: %lu, used: %lu \n",
                    context->get_total_size(), context->get_used_size());
    context = g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_BUFFER)->allocSet;
    dumpInfo.append("  Buffer mctx pre-allocated size: %lu, used: %lu \n",
                    context->get_total_size(), context->get_used_size());
    context = g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_TRANSACTION)->allocSet;
    dumpInfo.append("  Transaction mctx pre-allocated size: %lu, used: %lu \n",
                    context->get_total_size(), context->get_used_size());
    context = g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LOCK)->allocSet;
    dumpInfo.append("  Lock mctx pre-allocated size: %lu, used: %lu \n",
                    context->get_total_size(), context->get_used_size());
    context = g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_QUERY)->allocSet;
    dumpInfo.append("  Query mctx pre-allocated size: %lu, used: %lu \n",
                    context->get_total_size(), context->get_used_size());
    context = g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_STACK)->allocSet;
    dumpInfo.append("  Stack mctx pre-allocated size: %lu, used: %lu \n",
                    context->get_total_size(), context->get_used_size());

    dumpInfo.append("\nMctx size at Thread level\n");

    context = thrd->m_memoryMgr->GetRoot()->allocSet;
    dumpInfo.append("  Root mctx pre-allocated size: %lu, used: %lu \n",
                    context->get_total_size(), context->get_used_size());
    context = thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LONGLIVE)->allocSet;
    dumpInfo.append("  LongLive mctx pre-allocated size: %lu, used: %lu \n",
                    context->get_total_size(), context->get_used_size());
    context = thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_SMGR)->allocSet;
    dumpInfo.append("  Smgr mctx pre-allocated size: %lu, used: %lu \n",
                    context->get_total_size(), context->get_used_size());
    context = thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_ERROR)->allocSet;
    dumpInfo.append("  Error mctx pre-allocated size: %lu, used: %lu \n",
                    context->get_total_size(), context->get_used_size());
    context = thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_BUFFER)->allocSet;
    dumpInfo.append("  Buffer mctx pre-allocated size: %lu, used: %lu \n",
                    context->get_total_size(), context->get_used_size());
    context = thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_TRANSACTION)->allocSet;
    dumpInfo.append("  Transaction mctx pre-allocated size: %lu, used: %lu \n",
                    context->get_total_size(), context->get_used_size());
    context = thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LOCK)->allocSet;
    dumpInfo.append("  Lock mctx pre-allocated size: %lu, used: %lu \n",
                    context->get_total_size(), context->get_used_size());
    context = thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_QUERY)->allocSet;
    dumpInfo.append("  Query mctx pre-allocated size: %lu, used: %lu \n",
                    context->get_total_size(), context->get_used_size());
    context = thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_STACK)->allocSet;
    dumpInfo.append("  Stack mctx pre-allocated size: %lu, used: %lu \n",
                    context->get_total_size(), context->get_used_size());

    return dumpInfo.data;
}

typedef bool (*ContextTypeFilter)(DstoreMemoryContext root, void *args);
typedef RetStatus (*ProcessMemDetailsFunction)(GetMemInfoFilterType type, DstoreMemoryContext root, uint32 level,
    void *args);

struct GetInfoProcessFunctionTable {
    GetMemInfoFilterType getType;
    ContextTypeFilter contextTypeFilterFunc;
    ProcessMemDetailsFunction processFunc;
};

struct SharedMemoryDetailArgs {
    SharedMemoryDetail **detailsArray;
    uint32 *detailsArrayLen;
    uint32 *detailsArraySize;
};

struct ThreadMemoryDetailArgs {
    ThreadMemoryDetail **detailsArray;
    uint32 *detailsArrayLen;
    uint32 *detailsArraySize;
};

struct SessionMemoryDetailArgs {
    SessionMemoryDetail **detailsArray;
    uint32 *detailsArrayLen;
    uint32 *detailsArraySize;
};

struct MemctxFilterArgs {
    char *contextName;
    uint32 contextNameSize;
};

struct MemctxInfo {
    uint32 *level;
    char *parent;
    uint64 *totalSize;
    uint64 *freeSize;
    uint64 *usedSize;
};

static RetStatus ExpandDetailsArray(void **detailsArray, uint32 *detailsArraySize, uint32 detailSize)
{
    uint32 tmpArrayLen = (*detailsArraySize) == 0 ? 1 : (2 * (*detailsArraySize));
    uint32 tmpArraySize = tmpArrayLen * detailSize;
    void *tmpArray = nullptr;
    if (tmpArraySize >= MaxAllocSize) {
        tmpArray = (void *)DstoreMemoryContextAllocHugeSize(g_dstoreCurrentMemoryContext, tmpArraySize);
    } else {
        tmpArray = (void *)DstorePalloc(tmpArraySize);
    }
    if (tmpArray == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("Expand details array failed, try allocate size %u.", tmpArraySize));
        return DSTORE_FAIL;
    }
    int rc = memset_s(tmpArray, tmpArraySize, 0, tmpArraySize);
    if (rc != 0) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON,
            ErrMsg("Expand details array memset failed, error code: %d, array size: %u.", rc, tmpArraySize));
        DstorePfreeExt(tmpArray);
        return DSTORE_FAIL;
    }
    if (*detailsArraySize != 0) {
        rc = memcpy_s(tmpArray, tmpArraySize, *detailsArray, *detailsArraySize * detailSize);
        if (rc != 0) {
            ErrLog(DSTORE_ERROR, MODULE_COMMON,
                ErrMsg("Expand details array memcpy failed, error code: %d, dest array size: %u, src array size: %u.",
                rc, tmpArraySize, *detailsArraySize * detailSize));
            DstorePfreeExt(tmpArray);
            return DSTORE_FAIL;
        }
        DstorePfreeExt(*detailsArray);
    }
    *detailsArray = tmpArray;
    *detailsArraySize = tmpArrayLen;
    return DSTORE_SUCC;
}

static RetStatus CopyMemctxInfo(uint32 level, DstoreMemoryContext root, MemctxInfo *memctxInfo)
{
    *(memctxInfo->level) = level;
    if (root->parent != nullptr) {
        int rc = memcpy_s(memctxInfo->parent, MAX_CONTEXT_NAME_LENGTH, root->parent->name, strlen(root->parent->name));
        if (rc != 0) {
            ErrLog(DSTORE_ERROR, MODULE_COMMON,
                ErrMsg("Copy memory context parent name failed, error code: %d, name: %s, level %u.", rc,
                root->parent->name, level));
            return DSTORE_FAIL;
        }
    }
    *(memctxInfo->totalSize) = root->allocSet->get_total_size();
    *(memctxInfo->usedSize) = root->allocSet->get_used_size();
    *(memctxInfo->freeSize) = root->allocSet->get_free_size();
    return DSTORE_SUCC;
}

static bool UseSharedContextTypeFilter(DstoreMemoryContext root, UNUSE_PARAM void *args)
{
    return root->type == MemoryContextType::SHARED_CONTEXT || root->type == MemoryContextType::THREAD_CONTEXT;
}
static bool SharedContextTypeFilter(DstoreMemoryContext root, UNUSE_PARAM void *args)
{
    return root->type == MemoryContextType::SHARED_CONTEXT;
}

static bool ThreadContextTypeFilter(DstoreMemoryContext root, UNUSE_PARAM void *args)
{
    return root->type == MemoryContextType::THREAD_CONTEXT;
}

static bool SessionContextTypeFilter(DstoreMemoryContext root, UNUSE_PARAM void *args)
{
    return root->type == MemoryContextType::SESSION_CONTEXT;
}

static bool MemctxTypeFilter(DstoreMemoryContext root, void *args)
{
    MemctxFilterArgs *filterArgs = static_cast<MemctxFilterArgs *>(args);
    return (strcmp(filterArgs->contextName, root->name) == 0);
}

static uint32 GetDetailSize(GetMemInfoFilterType type)
{
    switch (type) {
        case GET_SHARED_TOTAL_INFO:
        case GET_SHARED_DETAIL_INFO:
            return sizeof(SharedMemoryDetail);
        case GET_THREAD_TOTAL_INFO:
        case GET_THREAD_DETAIL_INFO:
            return sizeof(ThreadMemoryDetail);
        case GET_SESSION_TOTAL_INFO:
            return sizeof(SessionMemoryDetail);
        case GET_MAX_AND_USED_SIZE:
        default:
            return 0;
    }
}

static RetStatus ProcessSharedContextDetail(GetMemInfoFilterType type, DstoreMemoryContext root, uint32 level,
    void *args)
{
    SharedMemoryDetailArgs *sharedArgs = static_cast<SharedMemoryDetailArgs *>(args);
    SharedMemoryDetail **detailsArray = sharedArgs->detailsArray;
    uint32 *detailsArrayLen = sharedArgs->detailsArrayLen;
    uint32 *detailsArraySize = sharedArgs->detailsArraySize;
    if (*detailsArrayLen == *detailsArraySize) {
        if (ExpandDetailsArray((void **)detailsArray, detailsArraySize, GetDetailSize(type)) == DSTORE_FAIL) {
            return DSTORE_FAIL;
        }
    }
    int rc = memcpy_s((*detailsArray)[*detailsArrayLen].contextName, MAX_CONTEXT_NAME_LENGTH, root->name,
        strlen(root->name));
    if (rc != 0) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON,
            ErrMsg("Process shared context detail memcpy failed, error code: %d, name: %s, level: %u.", rc, root->name,
            level));
        return DSTORE_FAIL;
    }
    MemctxInfo memctxInfo = {
        .level = &(*detailsArray)[*detailsArrayLen].level,
        .parent = (*detailsArray)[*detailsArrayLen].parent,
        .totalSize = &(*detailsArray)[*detailsArrayLen].totalSize,
        .freeSize = &(*detailsArray)[*detailsArrayLen].freeSize,
        .usedSize = &(*detailsArray)[*detailsArrayLen].usedSize
    };
    if (CopyMemctxInfo(level, root, &memctxInfo) == DSTORE_FAIL) {
        return DSTORE_FAIL;
    }
    ++(*detailsArrayLen);
    return DSTORE_SUCC;
}

static RetStatus ProcessThreadContextDetail(GetMemInfoFilterType type, DstoreMemoryContext root, uint32 level,
    void *args)
{
    ThreadMemoryDetailArgs *threadArgs = static_cast<ThreadMemoryDetailArgs *>(args);
    ThreadMemoryDetail **detailsArray = threadArgs->detailsArray;
    uint32 *detailsArrayLen = threadArgs->detailsArrayLen;
    uint32 *detailsArraySize = threadArgs->detailsArraySize;
    if (*detailsArrayLen == *detailsArraySize) {
        if (ExpandDetailsArray((void **)detailsArray, detailsArraySize, GetDetailSize(type)) == DSTORE_FAIL) {
            return DSTORE_FAIL;
        }
    }
    int rc = sprintf_s((*detailsArray)[*detailsArrayLen].threadId, MAX_THREAD_ID_LENGTH, "%lu", root->thread_id);
    if (rc == -1) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON,
            ErrMsg("Process thread id detail memcpy failed, error code: %d, name: %s, level: %u.", rc, root->name,
            level));
        return DSTORE_FAIL;
    }
    (*detailsArray)[*detailsArrayLen].tid = root->thread_id;
    rc = memcpy_s((*detailsArray)[*detailsArrayLen].contextName, MAX_CONTEXT_NAME_LENGTH, root->name,
        strlen(root->name));
    if (rc != 0) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON,
            ErrMsg("Process thread context detail memcpy failed, error code: %d, name: %s, level: %u.", rc, root->name,
            level));
        return DSTORE_FAIL;
    }
    /* Attention: thread type not store in cloud native. */
    rc =
        memcpy_s((*detailsArray)[*detailsArrayLen].threadType, MAX_CONTEXT_NAME_LENGTH, "postgres", strlen("postgres"));
    if (rc != 0) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON,
            ErrMsg("Process thread context detail memcpy failed, error code: %d, type: %s, level: %u.", rc, "postgres",
            level));
        return DSTORE_FAIL;
    }
    MemctxInfo memctxInfo = {
        .level = &(*detailsArray)[*detailsArrayLen].level,
        .parent = (*detailsArray)[*detailsArrayLen].parent,
        .totalSize = &(*detailsArray)[*detailsArrayLen].totalSize,
        .freeSize = &(*detailsArray)[*detailsArrayLen].freeSize,
        .usedSize = &(*detailsArray)[*detailsArrayLen].usedSize
    };
    if (CopyMemctxInfo(level, root, &memctxInfo) == DSTORE_FAIL) {
        return DSTORE_FAIL;
    }
    ++(*detailsArrayLen);
    return DSTORE_SUCC;
}

static RetStatus ProcessSessionContextDetail(GetMemInfoFilterType type, DstoreMemoryContext root, uint32 level,
    void *args)
{
    SessionMemoryDetailArgs *threadArgs = static_cast<SessionMemoryDetailArgs *>(args);
    SessionMemoryDetail **detailsArray = threadArgs->detailsArray;
    uint32 *detailsArrayLen = threadArgs->detailsArrayLen;
    uint32 *detailsArraySize = threadArgs->detailsArraySize;
    if (*detailsArrayLen == *detailsArraySize) {
        if (ExpandDetailsArray((void **)detailsArray, detailsArraySize, GetDetailSize(type)) == DSTORE_FAIL) {
            return DSTORE_FAIL;
        }
    }
    int rc = sprintf_s((*detailsArray)[*detailsArrayLen].sessionId, MAX_SESSION_ID_LENGTH, "%lu", root->session_id);
    if (rc != 0) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON,
            ErrMsg("Process session id detail memcpy failed, error code: %d, name: %s, level: %u.", rc, root->name,
            level));
        return DSTORE_FAIL;
    }
    (*detailsArray)[*detailsArrayLen].tid = root->thread_id;
    rc = memcpy_s((*detailsArray)[*detailsArrayLen].contextName, MAX_CONTEXT_NAME_LENGTH, root->name,
        strlen(root->name));
    if (rc != 0) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON,
            ErrMsg("Process session context detail memcpy failed, error code: %d, name: %s, level: %u.", rc, root->name,
            level));
        return DSTORE_FAIL;
    }
    MemctxInfo memctxInfo = {
        .level = &(*detailsArray)[*detailsArrayLen].level,
        .parent = (*detailsArray)[*detailsArrayLen].parent,
        .totalSize = &(*detailsArray)[*detailsArrayLen].totalSize,
        .freeSize = &(*detailsArray)[*detailsArrayLen].freeSize,
        .usedSize = &(*detailsArray)[*detailsArrayLen].usedSize
    };
    if (CopyMemctxInfo(level, root, &memctxInfo) == DSTORE_FAIL) {
        return DSTORE_FAIL;
    }
    ++(*detailsArrayLen);
    return DSTORE_SUCC;
}

static RetStatus CopyMemoryDetailInfo(GetMemInfoFilterType type, GenericChunk chunkBody, uint32 chunkSize, void *args)
{
    if (type == GET_SHARED_DETAIL_INFO) {
        SharedMemoryDetailArgs *details = static_cast<SharedMemoryDetailArgs *>(args);
        SharedMemoryDetail **detailsArray = details->detailsArray;
        for (uint32 idx = 0; idx < *(details->detailsArrayLen); idx++) {
            if ((*detailsArray)[idx].lineNumber == chunkBody->line &&
                strcmp((*detailsArray)[idx].fileName, chunkBody->file) == 0) {
                (*detailsArray)[idx].allocSize += chunkSize;
                return DSTORE_SUCC;
            }
        }
        uint32 *detailsArrayLen = details->detailsArrayLen;
        uint32 *detailsArraySize = details->detailsArraySize;
        if (*detailsArrayLen == *detailsArraySize) {
            if (ExpandDetailsArray((void **)detailsArray, detailsArraySize, GetDetailSize(type)) == DSTORE_FAIL) {
                return DSTORE_FAIL;
            }
        }
        (*detailsArray)[*detailsArrayLen].allocSize = chunkSize;
        (*detailsArray)[*detailsArrayLen].lineNumber = chunkBody->line;
        int rc = memcpy_s((*detailsArray)[*detailsArrayLen].fileName, MAX_FILE_NAME_LENGTH, chunkBody->file,
            strlen(chunkBody->file));
        if (rc != 0) {
            ErrLog(DSTORE_ERROR, MODULE_COMMON,
                ErrMsg("Process shared memctx detail memcpy failed, error code: %d.", rc));
            return DSTORE_FAIL;
        }
        ++(*detailsArrayLen);
    } else {
        ThreadMemoryDetailArgs *details = static_cast<ThreadMemoryDetailArgs *>(args);
        ThreadMemoryDetail **detailsArray = details->detailsArray;
        for (uint32 idx = 0; idx < *(details->detailsArrayLen); idx++) {
            if ((*detailsArray)[idx].lineNumber == chunkBody->line &&
                strcmp((*detailsArray)[idx].fileName, chunkBody->file) == 0) {
                (*detailsArray)[idx].allocSize += chunkSize;
                return DSTORE_SUCC;
            }
        }
        uint32 *detailsArrayLen = details->detailsArrayLen;
        uint32 *detailsArraySize = details->detailsArraySize;
        if (*detailsArrayLen == *detailsArraySize) {
            if (ExpandDetailsArray((void **)detailsArray, detailsArraySize, GetDetailSize(type)) == DSTORE_FAIL) {
                return DSTORE_FAIL;
            }
        }
        (*detailsArray)[*detailsArrayLen].allocSize = chunkSize;
        (*detailsArray)[*detailsArrayLen].lineNumber = chunkBody->line;
        int rc = memcpy_s((*detailsArray)[*detailsArrayLen].fileName, MAX_FILE_NAME_LENGTH, chunkBody->file,
            strlen(chunkBody->file));
        if (rc != 0) {
            ErrLog(DSTORE_ERROR, MODULE_COMMON,
                ErrMsg("Process shared memctx detail memcpy failed, error code: %d.", rc));
            return DSTORE_FAIL;
        }
        ++(*detailsArrayLen);
    }
    return DSTORE_SUCC;
}

static RetStatus ParseChunks(GetMemInfoFilterType type, GenericBlock block, void *args)
{
    char *chunk = STATIC_CAST_PTR_TYPE(block, char *) + ALLOC_BLOCKHDRSZ;
    while (chunk < block->freeptr) {
        GenericChunk chunkBody = STATIC_CAST_PTR_TYPE(chunk, GenericChunk);
        Size chunkSize = chunkBody->size;

        /* If magic num is 0, the chunk is in free list, not allocated. */
        if (chunkBody->alloc_set == nullptr || chunkBody->magicNum != PRE_MAGIC_NUM) {
            chunk += ALLOC_CHUNKHDRSZ + chunkSize;
            continue;
        }

        if (CopyMemoryDetailInfo(type, chunkBody, chunkSize, args) == DSTORE_FAIL) {
            ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("Copy memory detail info failed"));
            return DSTORE_FAIL;
        }
        
        chunk += ALLOC_CHUNKHDRSZ + chunkSize;
    }
    return DSTORE_SUCC;
}

static RetStatus ProcessMemCtxDetail(GetMemInfoFilterType type, DstoreMemoryContext root, uint32 level, void *args)
{
    for (GenericBlock block = STATIC_CAST_PTR_TYPE(root->allocSet->get_block(), GenericBlock);
        block != NULL; block = STATIC_CAST_PTR_TYPE(block->next, GenericBlock)) {
        char *blockBody = ((char *)block) + ALLOC_BLOCKHDRSZ;
        Size blockUsedSize = block->freeptr - blockBody;

        /* If block unused, skip it. */
        if (blockUsedSize <= ALLOC_BLOCKHDRSZ) {
            continue;
        }
        
        /* Process chunks in this block. */
        if (ParseChunks(type, block, args) == DSTORE_FAIL) {
            ErrLog(DSTORE_ERROR, MODULE_COMMON,
                ErrMsg("Process memctx detail failed, name: %s, level: %u.", root->name,
                level));
            return DSTORE_FAIL;
        }
    }
    
    return DSTORE_SUCC;
}

static RetStatus ProcessUsedSize(UNUSE_PARAM GetMemInfoFilterType type, DstoreMemoryContext root,
    UNUSE_PARAM uint32 level, void *args)
{
    uint64 *usedSize = static_cast<uint64 *>(args);
    Size totalSize = root->allocSet->get_total_size();
    if (UINT64_MAX - *(usedSize) < totalSize) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON,
            ErrMsg("Process shared memory max and used size failed, memctx name: %s total size: %lu, current total "
                   "size: %lu, sum out of range.",
            root->name, totalSize, *usedSize));
        return DSTORE_FAIL;
    }
    *usedSize += totalSize;
    return DSTORE_SUCC;
}

static GetInfoProcessFunctionTable g_getMemInfoFunctionTable[] = {
    { GET_SHARED_TOTAL_INFO,    SharedContextTypeFilter,    ProcessSharedContextDetail  },
    { GET_THREAD_TOTAL_INFO,    ThreadContextTypeFilter,    ProcessThreadContextDetail  },
    { GET_SESSION_TOTAL_INFO,   SessionContextTypeFilter,   ProcessSessionContextDetail },
    { GET_SHARED_DETAIL_INFO,   MemctxTypeFilter,           ProcessMemCtxDetail         },
    { GET_THREAD_DETAIL_INFO,   MemctxTypeFilter,           ProcessMemCtxDetail         },
    { GET_MAX_AND_USED_SIZE,    UseSharedContextTypeFilter, ProcessUsedSize             }
};

static RetStatus AccessMemctx(DstoreMemoryContext root, uint32 level, GetMemInfoFilterType type, void *filterArgs,
    void *outputArgs)
{
    if (g_getMemInfoFunctionTable[type].contextTypeFilterFunc(root, filterArgs)) {
        if (g_getMemInfoFunctionTable[type].processFunc(type, root, level, outputArgs) == DSTORE_FAIL) {
            ErrLog(DSTORE_ERROR, MODULE_COMMON,
                ErrMsg("AccessMemctx process memory context information level %u, type %u failed.", level,
                static_cast<uint32>(type)));
            return DSTORE_FAIL;
        }
    }
    for (DstoreMemoryContext curChild = root->firstChild; curChild != nullptr; curChild = curChild->nextChild) {
        if (AccessMemctx(curChild, level + 1, type, filterArgs, outputArgs) == DSTORE_FAIL) {
            ErrLog(DSTORE_ERROR, MODULE_COMMON,
                ErrMsg("AccessMemctx level %u, type %u failed.", level, static_cast<uint32>(type)));
            return DSTORE_FAIL;
        }
    }
    return DSTORE_SUCC;
}

RetStatus MemoryDiagnose::GetAllSharedMemoryDetails(SharedMemoryDetail **detailsArray, uint32 *detailsArrayLen)
{
    DstoreMemoryContext root = g_storageInstance->m_memoryMgr->GetRoot();
    if (root == nullptr || detailsArray == nullptr || detailsArrayLen == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("Get all shared memory detail failed, param invalid."));
        return DSTORE_FAIL;
    }

    uint32 detailsArraySize = 0;
    *detailsArrayLen = 0;
    SharedMemoryDetailArgs outputArgs =  {
        .detailsArray = detailsArray,
        .detailsArrayLen = detailsArrayLen,
        .detailsArraySize = &detailsArraySize
    };

    pthread_rwlock_wrlock(&root->topnode->tree_lock);
    if (AccessMemctx(root, 0, GET_SHARED_TOTAL_INFO, nullptr, &outputArgs) == DSTORE_FAIL) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("Get all shared memory details failed."));
        DstorePfreeExt(*detailsArray);
        *detailsArray = nullptr;
        *detailsArrayLen = 0;
        pthread_rwlock_unlock(&root->topnode->tree_lock);
        return DSTORE_FAIL;
    }
    pthread_rwlock_unlock(&root->topnode->tree_lock);
    return DSTORE_SUCC;
}

RetStatus MemoryDiagnose::GetAllThreadsMemoryDetails(ThreadMemoryDetail **detailsArray, uint32 *detailsArrayLen)
{
    if (detailsArray == nullptr || detailsArrayLen == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("Get all thread memory detail failed, param invalid."));
        return DSTORE_FAIL;
    }
    uint32 detailsArraySize = 0;
    *detailsArrayLen = 0;
    ThreadMemoryDetailArgs outputArgs =  {
        .detailsArray = detailsArray,
        .detailsArrayLen = detailsArrayLen,
        .detailsArraySize = &detailsArraySize
    };
    ThreadCoreMgr::ThreadIterator iter(&(g_storageInstance->m_thrdCoreMgr));
    ThreadCore *core = nullptr;
    while ((core = iter.GetNextThreadCore()) != nullptr) {
        DstoreMemoryContext root = core->threadContext->m_memoryMgr->GetRoot();
        if (root == nullptr) {
            DstorePfreeExt(*detailsArray);
            *detailsArray = nullptr;
            *detailsArrayLen = 0;
            return DSTORE_FAIL;
        }

        if (AccessMemctx(root, 0, GET_THREAD_TOTAL_INFO, nullptr, &outputArgs) == DSTORE_FAIL) {
            ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("Get all threads memory details failed."));
            DstorePfreeExt(*detailsArray);
            *detailsArray = nullptr;
            *detailsArrayLen = 0;
            return DSTORE_FAIL;
        }
    }

    return DSTORE_SUCC;
}

RetStatus MemoryDiagnose::GetAllSessionsMemoryDetails(SessionMemoryDetail **detailsArray, uint32 *detailsArrayLen)
{
    DstoreMemoryContext root = g_storageInstance->m_memoryMgr->GetRoot();
    if (root == nullptr || detailsArray == nullptr || detailsArrayLen == nullptr) {
        return DSTORE_FAIL;
    }

    uint32 detailsArraySize = 0;
    *detailsArrayLen = 0;
    SessionMemoryDetailArgs outputArgs =  {
        .detailsArray = detailsArray,
        .detailsArrayLen = detailsArrayLen,
        .detailsArraySize = &detailsArraySize
    };

    if (AccessMemctx(root, 0, GET_SESSION_TOTAL_INFO, nullptr, &outputArgs) == DSTORE_FAIL) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("Get all session memory details failed."));
        DstorePfreeExt(*detailsArray);
        *detailsArray = nullptr;
        *detailsArrayLen = 0;
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus MemoryDiagnose::GetSharedMemoryDetail(char *contextName, uint32 contextNameSize,
    SharedMemoryDetail **detailsArray, uint32 *detailsArrayLen)
{
    DstoreMemoryContext root = g_storageInstance->m_memoryMgr->GetRoot();
    if (root == nullptr || contextName == nullptr || detailsArray == nullptr || detailsArrayLen == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("Get shared memory detail failed, param invalid."));
        return DSTORE_FAIL;
    }

    uint32 detailsArraySize = 0;
    *detailsArrayLen = 0;
    MemctxFilterArgs filterArgs = {
        .contextName = contextName,
        .contextNameSize = contextNameSize
    };
    SharedMemoryDetailArgs outputArgs =  {
        .detailsArray = detailsArray,
        .detailsArrayLen = detailsArrayLen,
        .detailsArraySize = &detailsArraySize
    };

    if (AccessMemctx(root, 0, GET_SHARED_DETAIL_INFO, &filterArgs, &outputArgs) == DSTORE_FAIL) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("Get shared memctx details failed."));
        DstorePfreeExt(*detailsArray);
        *detailsArray = nullptr;
        *detailsArrayLen = 0;
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus MemoryDiagnose::GetThreadMemoryDetail(ThreadId tid, char *contextName, uint32 contextNameSize,
    ThreadMemoryDetail **detailsArray, uint32 *detailsArrayLen)
{
    if (contextName == nullptr || detailsArray == nullptr || detailsArrayLen == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("Get thread memctx detail failed, param invalid."));
        return DSTORE_FAIL;
    }
    ThreadContext *threadContext = nullptr;
    ThreadCoreMgr::ThreadIterator iter(&(g_storageInstance->m_thrdCoreMgr));
    ThreadCore *core = nullptr;
    while ((core = iter.GetNextThreadCore()) != nullptr) {
        if (core->pid == tid) {
            threadContext = core->threadContext;
            break;
        }
    }
    if (threadContext == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("Get thread memctx detail failed, thread id %lu not exist.", tid));
        return DSTORE_FAIL;
    }
    DstoreMemoryContext root = threadContext->m_memoryMgr->GetRoot();
    if (root == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("Get thread memctx detail failed, root memctx is null."));
        return DSTORE_FAIL;
    }

    uint32 detailsArraySize = 0;
    *detailsArrayLen = 0;
    MemctxFilterArgs filterArgs = {
        .contextName = contextName,
        .contextNameSize = contextNameSize
    };
    ThreadMemoryDetailArgs outputArgs =  {
        .detailsArray = detailsArray,
        .detailsArrayLen = detailsArrayLen,
        .detailsArraySize = &detailsArraySize
    };

    if (AccessMemctx(root, 0, GET_THREAD_DETAIL_INFO, &filterArgs, &outputArgs) == DSTORE_FAIL) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("Get thread memctx details failed."));
        DstorePfreeExt(*detailsArray);
        *detailsArray = nullptr;
        *detailsArrayLen = 0;
        return DSTORE_FAIL;
    }

    return DSTORE_SUCC;
}

RetStatus MemoryDiagnose::GetSessionMemoryDetail(UNUSE_PARAM char *contextName, UNUSE_PARAM uint32 contextNameSize,
    UNUSE_PARAM SessionMemoryDetail **detailsArray, uint32 *detailsArrayLen)
{
    /* Do not support session now */
    *detailsArrayLen = 0;
    return DSTORE_SUCC;
}

RetStatus MemoryDiagnose::GetUsedSharedMemory(uint64 *usedSize)
{
    DstoreMemoryContext root = g_storageInstance->m_memoryMgr->GetRoot();
    if (root == nullptr || usedSize == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("Get used shared memory failed, param invalid."));
        return DSTORE_FAIL;
    }
    
    *usedSize = 0;

    if (AccessMemctx(root, 0, GET_MAX_AND_USED_SIZE, nullptr, usedSize) == DSTORE_FAIL) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("Get used shared memory failed."));
        return DSTORE_FAIL;
    }

    ThreadCoreMgr::ThreadIterator iter(&(g_storageInstance->m_thrdCoreMgr));
    ThreadCore *core = nullptr;
    while ((core = iter.GetNextThreadCore()) != nullptr) {
        DstoreMemoryContext thrdroot = core->threadContext->m_memoryMgr->GetRoot();
        if (thrdroot == nullptr) {
            return DSTORE_FAIL;
        }
 
        if (AccessMemctx(thrdroot, 0, GET_MAX_AND_USED_SIZE, nullptr, usedSize) == DSTORE_FAIL) {
            ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("Get all threads memory details failed."));
            return DSTORE_FAIL;
        }
    }
    return DSTORE_SUCC;
}
}