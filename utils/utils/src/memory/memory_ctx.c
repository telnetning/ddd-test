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
 * Description: Implement for memory context
 */

#include <stdint.h>
#include "defines/abort.h"
#include "memory/memory_context_internal.h"
#include "memory/memory_ctx.h"

/* set g_mmapThreshold during system initialization, (size_t)atol(gs_getenv_r("GAUSS_MMAP_THRESHOLD")) */
static const size_t MMAP_THRESHOLD = DEFAULT_MMAP_THRESHOLD;
static MemoryContextMethods g_mCtxMethods[MCTX_METHOD_MAX_ID];

size_t GetMmapThreshold(void)
{
    return MMAP_THRESHOLD;
}

static inline MemoryContext *GetCurrentContext(void)
{
    static __thread MemoryContext currentMctx = NULL;
    return &currentMctx;
}

static inline void AddChild(MemoryContext parent, MemoryContext child)
{
    ASSERT(parent != NULL);
    ASSERT(child != NULL);
    MemoryContext temp = parent->firstChild;
    if (temp != NULL) {
        child->nextChild = temp;
        temp->prevChild = child;
    }
    parent->firstChild = child;
}

static void RemoveChild(MemoryContext parent, MemoryContext child)
{
    /* pop child to parent if parent not null */
    ASSERT(parent != NULL);
    ASSERT(child != NULL);
    for (MemoryContext temp = parent->firstChild; temp != NULL; temp = temp->nextChild) {
        if (temp == child) {
            if (child->prevChild != NULL) {
                child->prevChild->nextChild = child->nextChild;
            } else {
                /* delete child is first child */
                parent->firstChild = child->nextChild;
            }
            if (child->nextChild != NULL) {
                child->nextChild->prevChild = child->prevChild;
            }
            break;
        }
    }
}

static void MemoryContextInit(MemoryContext mctx, MemoryContext parent, MemoryContextType type,
                              MemoryContextMethods *methods, const char *name)
{
    ASSERT(mctx != NULL);
    /* init the memory context header */
    mctx->type = type;
    mctx->methods = methods;
    mctx->parent = parent;
    mctx->firstChild = NULL;
    mctx->prevChild = NULL;
    mctx->nextChild = NULL;
    (void)strncpy_s(mctx->name, sizeof(mctx->name), (name == NULL ? "notset" : name), sizeof(mctx->name) - 1);
    mctx->level = 0;
    mctx->isReset = true;
    mctx->isSilent = false;
    mctx->isPrintSummary = false;
    mctx->threadId = pthread_self();
    SpinLockInit(&mctx->childListLock);
    /* push child to parent if parent not null */
    if (parent != NULL) {
        SpinLockAcquire(&parent->childListLock);
        AddChild(parent, mctx);
        SpinLockRelease(&parent->childListLock);
        mctx->level = parent->level + 1;
    }
}

void RegisterMemoryContextMethod(uint32_t id, MemoryContextMethods *methods)
{
    ASSERT(id < MCTX_METHOD_MAX_ID);
    ASSERT(methods != NULL);
    ASSERT(methods->create != NULL);
    ASSERT(methods->alloc != NULL);
    ASSERT(methods->mfree != NULL);
    ASSERT(methods->realloc != NULL);
    ASSERT(methods->reset != NULL);
    ASSERT(methods->delete != NULL);
    ASSERT(methods->getChunkSpace != NULL);
    ASSERT(g_mCtxMethods[id].create == NULL);

    g_mCtxMethods[id] = *methods;
}

UTILS_EXPORT MemoryContext MemoryContextCreate(MemoryContext parent, MemoryContextType type, const char *name,
                                               size_t initBlockSize, size_t maxBlockSize, size_t maxSize)
{
    MemoryContextMethods *methods;

    ASSERT((parent == NULL) || ((parent->type == type)));
    switch (type) {
        case MEM_CXT_TYPE_GENERIC:
            methods = &g_mCtxMethods[MCTX_GENERIC_ID];
            break;
        case MEM_CXT_TYPE_SHARE:
            methods = &g_mCtxMethods[MCTX_SHARE_ID];
            break;
        case MEM_CXT_TYPE_STACK:
            methods = &g_mCtxMethods[MCTX_STACK_ID];
            break;
        case MEM_CXT_TYPE_SIMPLE_SHARE:
            methods = &g_mCtxMethods[MCTX_SIMPLE_SHARE_ID];
            break;
        case MEM_CXT_TYPE_NULL:
            /* fall through */
        default:
#ifdef MEM_LOG
            (void)printf("argument type is invaild.\n");
#endif
            return NULL;
    }
    if (methods->create == NULL) {
#ifdef MEM_LOG
        (void)printf("this memory type had not register methods.\n");
#endif
        return NULL;
    }

    MemoryContext mctx = methods->create(maxSize, initBlockSize, maxBlockSize, MCTX_UNUSED);
    if (mctx != NULL) {
        MemoryContextInit(mctx, parent, type, methods, name);
    }

    return mctx;
}

UTILS_EXPORT void MemoryContextSetSilent(MemoryContext context, bool isSilent)
{
    if (unlikely(context == NULL)) {
        return;
    }
    context->isSilent = isSilent;
}

UTILS_EXPORT void MemoryContextSetPrintSummary(const MemoryContext context, bool isPrintSummary)
{
    if (unlikely(context == NULL)) {
        return;
    }
    context->isPrintSummary = isPrintSummary;
}

static void DeleteMemoryContext(MemoryContext context)
{
    if (unlikely(context == NULL)) {
        return;
    }

    MemoryContext lastmctx = NULL;
    for (MemoryContext mctx = context->firstChild; mctx != NULL; mctx = mctx->nextChild) {
        DeleteMemoryContext(mctx->prevChild); // this step delete lastmctx, lastmctx = mctx->prevChild
        lastmctx = mctx;
    }
    DeleteMemoryContext(lastmctx);
    SpinLockDestroy(&context->childListLock);
    context->methods->delete (context);
}

UTILS_EXPORT void MemoryContextDelete(MemoryContext context)
{
    if (unlikely(context == NULL)) {
        return;
    }
    MemoryContext parent = context->parent;
    /* first remove context from parent child list */
    if (parent != NULL) {
        SpinLockAcquire(&parent->childListLock);
        RemoveChild(parent, context);
        SpinLockRelease(&parent->childListLock);
    }
    DeleteMemoryContext(context);
}

UTILS_EXPORT void MemoryContextReset(MemoryContext context)
{
    if (unlikely(context == NULL)) {
        return;
    }
    context->isReset = true;
    /* rest will keep the memory context it self, so call alloctor rest directly */
    SpinLockAcquire(&context->childListLock);
    for (MemoryContext mctx = context->firstChild; mctx != NULL; mctx = mctx->nextChild) {
        MemoryContextReset(mctx);
    }
    SpinLockRelease(&context->childListLock);
    ASSERT(context->methods->reset != NULL);
    context->methods->reset(context);
}

#ifdef ENABLE_UT
UTILS_EXPORT uint32_t MemoryContextNums(MemoryContext context)
{
    if (unlikely(context == NULL)) {
        return 0;
    }
    uint32_t ret = 1;
    /* rest will keep the memory context it self, so call alloctor rest directly */
    SpinLockAcquire(&context->childListLock);
    for (MemoryContext mctx = context->firstChild; mctx != NULL; mctx = mctx->nextChild) {
        ret += MemoryContextNums(mctx);
    }
    SpinLockRelease(&context->childListLock);
    return ret;
}
#endif

UTILS_EXPORT void *MemoryContextAllocD(MemoryContext context, size_t size, const char *file, int line)
{
    if (unlikely(context == NULL)) {
        /* if context is null, can't obtain methods */
        return NULL;
    }
    ASSERT(context->methods->alloc != NULL);
    void *ptr = context->methods->alloc(context, size, file, line);
    if (likely(ptr != NULL)) {
        context->isReset = false;
    }
    return ptr;
}

UTILS_EXPORT void *MemoryContextAllocExD(MemoryContext context, size_t size, uint32_t flags, const char *file, int line)
{
    if (unlikely(context == NULL)) {
        /* if context is null, can't obtain methods */
        return NULL;
    }
    void *ptr;
    const MemoryContextMethods *methods = context->methods;
    ASSERT(methods->alloc != NULL);
    switch (flags) {
        case MCTX_ALLOC_FLAG_DEFAULT:
            ptr = methods->alloc(context, size, file, line);
            if (likely(ptr != NULL)) {
                context->isReset = false;
            }
            break;
        case MCTX_ALLOC_FLAG_OOM:
            ptr = methods->alloc(context, size, file, line);
            if (unlikely(ptr == NULL)) {
                Abort();
            } else {
                context->isReset = false;
            }
            break;
        case MCTX_ALLOC_FLAG_ZERO:
            ptr = methods->alloc(context, size, file, line);
            if (likely(ptr != NULL)) {
                (void)memset_s(ptr, size, 0, size);
                context->isReset = false;
            }
            break;
        case MCTX_ALLOC_FLAG_OOM | MCTX_ALLOC_FLAG_ZERO:
            ptr = methods->alloc(context, size, file, line);
            if (likely(ptr != NULL)) {
                (void)memset_s(ptr, size, 0, size);
                context->isReset = false;
            } else {
                Abort();
            }
            break;
        default:
            return NULL;
    }
    return ptr;
}

UTILS_EXPORT void *MemReallocD(void *pointer, size_t size, const char *file, int line)
{
    if (unlikely(pointer == NULL)) {
        /* if pointer is null, the memcontext can't obtain */
        return NULL;
    }
    uint8_t id = MctxChunkGetMethodID(pointer);
    ASSERT(g_mCtxMethods[id].realloc != NULL);
    return g_mCtxMethods[id].realloc(pointer, size, file, line);
}

UTILS_EXPORT void *MemAllocD(size_t size, const char *file, int line)
{
    ASSERT(*GetCurrentContext() != NULL);
    return MemoryContextAllocD(*GetCurrentContext(), size, file, line);
}

UTILS_EXPORT void *MemAlloc0D(size_t size, const char *file, int line)
{
    ASSERT(*GetCurrentContext() != NULL);
    return MemoryContextAllocExD(*GetCurrentContext(), size, MCTX_ALLOC_FLAG_ZERO, file, line);
}

UTILS_EXPORT void *MemAllocExD(size_t size, uint32_t flags, const char *file, int line)
{
    ASSERT(*GetCurrentContext() != NULL);
    return MemoryContextAllocExD(*GetCurrentContext(), size, flags, file, line);
}

UTILS_EXPORT void MemFree(void *pointer)
{
    if (unlikely(pointer == NULL)) {
        return;
    }
    uint8_t id = MctxChunkGetMethodID(pointer);
    ASSERT(g_mCtxMethods[id].mfree != NULL);
    g_mCtxMethods[id].mfree(pointer);
}

UTILS_EXPORT void MemFreeAndSetNull(void **pointer)
{
    if (unlikely(pointer == NULL)) {
        return;
    }
    MemFree(*pointer);
    *pointer = NULL;
}

UTILS_EXPORT MemoryContext MemoryContextSwitchTo(MemoryContext context)
{
    MemoryContext old = *GetCurrentContext();
    *GetCurrentContext() = context;
    return old;
}

UTILS_EXPORT bool MemoryContextIsEmpty(const MemoryContext context)
{
    if (unlikely((context == NULL) || (context->methods->isEmpty == NULL))) {
        /* can't obtain methods when context is null, or when not support isEmpty function, return true */
        return true;
    }
    if (!context->methods->isEmpty(context)) {
        return false;
    }
    bool hasChild = false;
    hasChild = (context->firstChild != NULL);
    return !hasChild;
}

UTILS_EXPORT bool MemoryContextIsValid(const MemoryContext context)
{
    if (unlikely(context == NULL)) {
        /* if context is null, can't obtain methods */
        return false;
    }
    return (context->type == MEM_CXT_TYPE_GENERIC) || (context->type == MEM_CXT_TYPE_STACK) ||
           (context->type == MEM_CXT_TYPE_SIMPLE_SHARE) || (context->type == MEM_CXT_TYPE_SHARE);
}

UTILS_EXPORT const char *GetMemoryContextName(const MemoryContext context)
{
    if (unlikely((context == NULL) || (context->name == NULL))) {
        return "";
    }
    return context->name;
}

UTILS_EXPORT MemoryContext GetMemoryContextParent(const MemoryContext context)
{
    if (unlikely(context == NULL)) {
        return NULL;
    }
    return context->parent;
}

UTILS_EXPORT MemoryContextType GetMemoryContextType(const MemoryContext context)
{
    if (unlikely(context == NULL)) {
        return MEM_CXT_TYPE_NULL;
    }
    return context->type;
}

static inline char *StrdupInternal(MemoryContext context, const char *string, size_t size, const char *file, int line)
{
    size_t len = strlen(string);
    len = Min(len, size);
    if (len + 1 < len) {
        /* wrap happended */
        return NULL;
    }
    char *str = (char *)MemoryContextAllocD(context, len + 1, file, line);
    if (likely(str != NULL)) {
        /* according manual of securec API, this case will not fail forever */
        (void)strncpy_s(str, len + 1, string, len);
    }
    return str;
}

/*
 * MemoryContextStrdup :Like strdup(), but allocate from the specified context
 */
UTILS_EXPORT char *MemoryContextStrdupD(MemoryContext context, const char *string, const char *file, int line)
{
    if (unlikely((context == NULL) || (string == NULL))) {
        return NULL;
    }
    return StrdupInternal(context, string, SIZE_MAX, file, line);
}

UTILS_EXPORT char *MemStrdupD(const char *string, const char *file, int line)
{
    if (unlikely(string == NULL)) {
        return NULL;
    }
    return StrdupInternal(*GetCurrentContext(), string, SIZE_MAX, file, line);
}

UTILS_EXPORT char *MemNStrdupD(const char *string, size_t len, const char *file, int line)
{
    if (unlikely(string == NULL)) {
        return NULL;
    }
    return StrdupInternal(*GetCurrentContext(), string, len, file, line);
}

UTILS_EXPORT size_t MemGetSpace(const void *pointer)
{
    if (unlikely(pointer == NULL)) {
        return 0;
    }
    uint8_t id = MctxChunkGetMethodID(pointer);
    ASSERT(g_mCtxMethods[id].getChunkSpace != NULL);
    return g_mCtxMethods[id].getChunkSpace(pointer);
}

static void GetCurrentStats(const MemoryContext context, bool isPrintToLog, const AllocPosition *pos,
                            MemoryStatType type, MemStat *stat)
{
    ASSERT(context != NULL);
    ASSERT(stat != NULL);

    if (type == MEM_STAT_CURRENT_CTX) {
        ASSERT(context->methods->getStats != NULL);
        context->methods->getStats(context, isPrintToLog, stat);
    } else {
        ASSERT(pos->line >= 0);
        ASSERT(pos->file != NULL);
        ASSERT(type == MEM_STAT_CURRENT_CTX_BY_POSITION);
        ASSERT(context->methods->getStatsByPos != NULL);
        context->methods->getStatsByPos(context, isPrintToLog, pos->file, pos->line, stat);
    }
}

static void GetAllStatsByRecurse(MemoryContext context, bool isPrintToLog, const AllocPosition *pos,
                                 MemoryStatType type, MemStat *stat)
{
    if (context == NULL) {
        return;
    }
    ASSERT(stat != NULL);
    ASSERT(pos != NULL);
    ASSERT((type == MEM_STAT_CURRENT_CTX_BY_POSITION) || (type == MEM_STAT_CURRENT_CTX));

    SpinLockAcquire(&context->childListLock);
    for (MemoryContext mctx = context->firstChild; mctx != NULL; mctx = mctx->nextChild) {
        GetAllStatsByRecurse(mctx, isPrintToLog, pos, type, stat);
    }
    SpinLockRelease(&context->childListLock);
    /* get current memory context statistics */
    MemStat allocStat = {0};
    GetCurrentStats(context, isPrintToLog, pos, type, &allocStat);
    stat->totalSpace += allocStat.totalSpace;
    stat->totalBlocks += allocStat.totalBlocks;
    stat->totalChunks += allocStat.totalChunks;
    stat->freeSpace += allocStat.freeSpace;
    stat->freeChunks += allocStat.freeChunks;
}

UTILS_EXPORT void MemoryContextStats(const MemoryContext context, bool isPrintToLog, const MemStatFilter *filter,
                                     MemStat *stat)
{
    if (unlikely((context == NULL) || (stat == NULL))) {
        return;
    }
    /* clear output parameters */
    (void)memset_s(stat, sizeof(*stat), 0, sizeof(*stat));

    static const struct {
        MemoryStatType type;
        void (*func)(MemoryContext, bool, const AllocPosition *, MemoryStatType, MemStat *);
    } getStatFunc[] = {
        {MEM_STAT_CURRENT_CTX_BY_POSITION, GetCurrentStats},
        {MEM_STAT_CURRENT_CTX, GetCurrentStats},
        {MEM_STAT_CURRENT_CTX_BY_POSITION, GetAllStatsByRecurse},
        {MEM_STAT_CURRENT_CTX, GetAllStatsByRecurse},
    };

    MemoryStatType idx = MEM_STAT_CURRENT_CTX;
    if (likely(filter != NULL)) {
        idx = filter->type;
        getStatFunc[idx].func(context, isPrintToLog, &filter->pos, getStatFunc[idx].type, stat);
    } else {
        getStatFunc[idx].func(context, isPrintToLog, NULL, getStatFunc[idx].type, stat);
    }
}

UTILS_EXPORT void MemoryContextCheck(const MemoryContext context)
{
    if (likely(context != NULL)) {
        ASSERT(context->methods != NULL);
        if (unlikely(context->methods->check == NULL)) {
            return;
        }
        context->methods->check(context);
    }
}

UTILS_EXPORT void *MemoryContextSavePoint(MemoryContext context)
{
    if (unlikely(context == NULL)) {
        return NULL;
    }
    const MemoryContextMethods *methods = context->methods;
    if (likely(methods->save != NULL)) {
        return methods->save(context);
    }
    return NULL;
}

UTILS_EXPORT void MemoryContextRestorePoint(MemoryContext context, void *point)
{
    if (unlikely((context == NULL) || (point == NULL))) {
        return;
    }
    const MemoryContextMethods *methods = context->methods;
    if (likely(methods->restore != NULL)) {
        methods->restore(context, point);
    }
}
