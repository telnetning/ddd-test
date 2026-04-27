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
 * Description: Fold log implementation
 */
#include "container/linked_list.h"
#include "container/hash_table.h"
#include "syslog/err_log_fold.h"

struct LogFoldContext {
    MemoryContext memoryContext;
    int foldLevel;          /* less than or equal to foldLevel, the error log will be folded */
    uint32_t foldPeriod;    /* log fold period in second */
    uint32_t foldThreshold; /* Greater than foldThreshold in one foldPeriod will be folded */
    DListHead foldLogList;  /* FIFO list to store fold log info, use to serial search fold log by time */
    HashTab *foldHashTable; /* Speed up finding fold log in list */
};

typedef struct LogFoldHashKey LogFoldHashKey;
struct LogFoldHashKey {
    uint64_t threadId;
    int lineNum;
    char fileName[FILE_NAME_MAX_LEN];
} __attribute__((packed));

typedef struct LogFoldNode LogFoldNode;
struct LogFoldNode {
    LogFoldHashKey foldHashKey;
    DListNode node;
    time_t firstLogTime;
    uint64_t logCount;
    char *msgBuf;
    size_t msgLen;
};

static int FoldLogHashCompare(const void *key1, const void *key2, SYMBOL_UNUSED size_t keySize)
{
    ASSERT(key1 != NULL);
    ASSERT(key2 != NULL);
    ASSERT(keySize == sizeof(LogFoldHashKey));
    const LogFoldHashKey *hashKey1 = (const LogFoldHashKey *)key1;
    const LogFoldHashKey *hashKey2 = (const LogFoldHashKey *)key2;
    if (hashKey1->threadId != hashKey2->threadId) {
        return -1;
    }
    if (strcmp(hashKey1->fileName, hashKey2->fileName) != 0) {
        return -1;
    }
    if (hashKey1->lineNum != hashKey2->lineNum) {
        return -1;
    }
    return 0;
}

static bool LogFoldCheckConfigure(int foldLevel, SYMBOL_UNUSED uint32_t foldThreshold, uint32_t foldPeriod)
{
    if (foldLevel > WARNING || foldLevel <= 0) {
        return false;
    }
    uint32_t maxFoldPeriod = 60 * 60; /* Max fold period is 1 hour (60 * 60 seconds) */
    if (foldPeriod > maxFoldPeriod) {
        return false;
    }
    return true;
}

LogFoldContext *LogFoldAllocContext(MemoryContext memoryContext, int foldLevel, uint32_t foldThreshold,
                                    uint32_t foldPeriod)
{
    if (unlikely(!LogFoldCheckConfigure(foldLevel, foldThreshold, foldPeriod))) {
        return NULL;
    }
    LogFoldContext *context = (LogFoldContext *)malloc(sizeof(LogFoldContext));
    if (unlikely(context == NULL)) {
        return NULL;
    }
    context->memoryContext = memoryContext;
    context->foldLevel = foldLevel;
    context->foldPeriod = foldPeriod;
    context->foldThreshold = foldThreshold;
    DListInit(&(context->foldLogList));

    HashCTL hashCtl = {0};
    hashCtl.keySize = sizeof(LogFoldHashKey);
    hashCtl.entrySize = sizeof(LogFoldNode);
    hashCtl.hash = TagHash;
    hashCtl.match = FoldLogHashCompare;
    size_t foldLogHashElemSize = 1024; /* Hash table init element count, will be automatically expanded */
    HashTab *hashTab =
        HashCreate("Fold log hash table", foldLogHashElemSize, &hashCtl, HASH_ELEM | HASH_FUNCTION | HASH_COMPARE);
    if (hashTab == NULL) {
        /* Fold log hash table create failed, fold log ability disabled */
        free(context);
        return NULL;
    }
    context->foldHashTable = hashTab;
    return context;
}

void LogFoldFreeContext(LogFoldContext *context)
{
    if (context != NULL) {
        /* Free hash table */
        if (context->foldHashTable != NULL) {
            HashDestroy(context->foldHashTable);
        }
        free(context);
    }
}

bool LogFoldSetRule(LogFoldContext *context, int foldLevel, uint32_t foldThreshold, uint32_t foldPeriod)
{
    if (context == NULL || !LogFoldCheckConfigure(foldLevel, foldThreshold, foldPeriod)) {
        return false;
    }
    context->foldPeriod = foldPeriod;
    context->foldThreshold = foldThreshold;
    context->foldLevel = foldLevel;
    return true;
}

static void ResetLogFoldNode(LogFoldNode *logFoldNode, time_t curTime)
{
    logFoldNode->firstLogTime = curTime;
    logFoldNode->logCount = 1;
    logFoldNode->msgBuf = NULL;
    logFoldNode->msgLen = 0;
}

static void OutputFoldLogNode(LogFoldContext *context, const LogFoldNode *logFoldNode, LogFoldContent *logFoldContent)
{
    if (logFoldNode->logCount > context->foldThreshold) {
        ASSERT(logFoldNode->msgBuf != NULL);
        logFoldContent->foldCount = logFoldNode->logCount - context->foldThreshold;
        logFoldContent->msgBuf = logFoldNode->msgBuf;
        logFoldContent->msgLen = logFoldNode->msgLen;
    } else {
        /* Do not reach fold threshold, do not need to write fold log */
        ASSERT(logFoldNode->msgBuf == NULL);
        logFoldContent->foldCount = 0;
        logFoldContent->msgBuf = logFoldNode->msgBuf;
    }
}

static bool IncreaseFoldLogNode(LogFoldContext *context, LogFoldNode *logFoldNode, LogFoldContent *newLogFoldContent,
                                LogFoldContent *logFoldContent)
{
    logFoldNode->logCount += 1;
    bool needFold = false;
    if (logFoldNode->logCount > context->foldThreshold) {
        needFold = true; /* Need to fold if greater than threshold */
        logFoldContent->foldCount = 0;
        logFoldContent->msgBuf = logFoldNode->msgBuf;
        logFoldNode->msgBuf = newLogFoldContent->msgBuf;
        logFoldNode->msgLen = newLogFoldContent->msgLen;
    }
    return needFold;
}

static void AddNewLogNode(LogFoldContext *context, const LogIdentifier *logIdentifier, const void *hashKey)
{
    LogFoldNode *newNode = (LogFoldNode *)HashSearch(context->foldHashTable, hashKey, HASH_ENTER, NULL);
    if (newNode != NULL) {
        newNode->foldHashKey.threadId = logIdentifier->threadId;
        newNode->foldHashKey.lineNum = logIdentifier->lineNum;
        (void)memset_s(newNode->foldHashKey.fileName, sizeof(newNode->foldHashKey.fileName), 0,
                       sizeof(newNode->foldHashKey.fileName));
        (void)strcpy_s(newNode->foldHashKey.fileName, sizeof(newNode->foldHashKey.fileName), logIdentifier->fileName);
        ResetLogFoldNode(newNode, logIdentifier->timeSecond);
        DListPushTail(&(context->foldLogList), &(newNode->node));
    }
}

static bool UpdateLogNode(LogFoldContext *context, LogFoldNode *targetNode, time_t curTime,
                          LogFoldContent *newLogFoldContent, LogFoldContent *logFoldContent)
{
    ASSERT(targetNode != NULL);
    ASSERT(targetNode->logCount > 0);
    if (curTime - targetNode->firstLogTime > context->foldPeriod) {
        /* current fold log reach fold period, write fold log if count greater than threshold */
        OutputFoldLogNode(context, targetNode, logFoldContent);
        DListDelete(&(targetNode->node));
        /* Reset log count and first log time */
        ResetLogFoldNode(targetNode, curTime);
        DListPushTail(&(context->foldLogList), &(targetNode->node));
        return false;
    } else {
        return IncreaseFoldLogNode(context, targetNode, newLogFoldContent, logFoldContent);
    }
}

static LogFoldNode *FindExistLogNode(LogFoldContext *context, const void *hashKey)
{
    void *resPtr = HashSearch(context->foldHashTable, hashKey, HASH_FIND, NULL);
    return (LogFoldNode *)resPtr;
}

static void RemoveLogNode(LogFoldContext *context, const void *hashKey)
{
    (void)HashSearch(context->foldHashTable, hashKey, HASH_REMOVE, NULL);
}

bool LogFoldIsNeedFold(LogFoldContext *context, const LogIdentifier *logIdentifier, char *msgBuf, size_t msgLen,
                       LogFoldContent *logFoldContent)
{
    if (context == NULL || context->foldPeriod == 0 || context->foldThreshold == 0 || logIdentifier == NULL ||
        logFoldContent == NULL) {
        /* Fold log is not active or parameters invalid, do not fold */
        return false;
    }

    if (logIdentifier->logLevel > context->foldLevel || logIdentifier->logLevel == LOG) {
        return false;
    }
    logFoldContent->foldCount = 0;
    logFoldContent->msgBuf = NULL;

    LogFoldHashKey foldHashKey;
    foldHashKey.threadId = logIdentifier->threadId;
    foldHashKey.lineNum = logIdentifier->lineNum;
    (void)memset_s(foldHashKey.fileName, sizeof(foldHashKey.fileName), 0, sizeof(foldHashKey.fileName));
    (void)strcpy_s(foldHashKey.fileName, sizeof(foldHashKey.fileName), logIdentifier->fileName);

    LogFoldNode *targetNode = FindExistLogNode(context, &foldHashKey);
    /* a new log, add to fold log list but do not fold */
    if (targetNode == NULL) {
        AddNewLogNode(context, logIdentifier, &foldHashKey);
        return false;
    }
    LogFoldContent newLogFoldContent = {
        .foldCount = 0,
        .msgBuf = msgBuf,
        .msgLen = msgLen,
    };
    return UpdateLogNode(context, targetNode, logIdentifier->timeSecond, &newLogFoldContent, logFoldContent);
}

uint64_t FoldLogGetContent(LogFoldContext *context, bool checkTime, time_t curTime, LogFoldContent *contentArray,
                           size_t arrayLen)
{
    if (context == NULL || contentArray == NULL || arrayLen == 0) {
        return 0;
    }
    uint64_t count = 0;
    DListMutableIter iter;
    DLIST_MODIFY_FOR_EACH(iter, &(context->foldLogList))
    {
        LogFoldNode *curNode = DLIST_CONTAINER(LogFoldNode, node, iter.cur);
        if (checkTime && curTime - curNode->firstLogTime <= context->foldPeriod) {
            break; /* list is FIFO base on first log time, do not need to search next node */
        }
        OutputFoldLogNode(context, curNode, &(contentArray[count]));
        /* Just delete from list, and remove from hash table */
        DListDelete(&(curNode->node));
        RemoveLogNode(context, &(curNode->foldHashKey));

        count += 1;
        if (count >= arrayLen) {
            /* content array already full */
            break;
        }
    }
    return count;
}
