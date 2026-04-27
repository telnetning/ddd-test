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
 * Description: Header file for memory context
 */
#ifndef UTILS_MEMORY_CONTEXT_H
#define UTILS_MEMORY_CONTEXT_H

#include "defines/common.h"
#include "types/data_types.h"

GSDB_BEGIN_C_CODE_DECLS

#define DEFAULT_UNLIMITED_SIZE    (size_t)(-1)
#define DEFAULT_MAX_BLOCKSIZE_1M  (size_t)(1 * 1024 * 1024)
#define DEFAULT_INIT_BLOCKSIZE_8K (size_t)(8 * 1024)
#define DEFAULT_MAXSIZE_8M        (size_t)(8 * 1024 * 1024)
#define MCTX_UNUSED               (size_t)0

/* default NO ZERO and NO OOM , will retun NULL at OS memory not enougth */
#define MCTX_ALLOC_FLAG_DEFAULT (0U)
#define MCTX_ALLOC_FLAG_OOM     (1U << 1)
#define MCTX_ALLOC_FLAG_ZERO    (1U << 2)

typedef enum {
    MEM_CXT_TYPE_NULL,         /* null type, type is invaild, or the input memory context is null pointer */
    MEM_CXT_TYPE_GENERIC,      /* thread no share */
    MEM_CXT_TYPE_STACK,        /* thread no share */
    MEM_CXT_TYPE_SHARE,        /* thread share */
    MEM_CXT_TYPE_SIMPLE_SHARE, /* thread share, no one-time release capability, when delete memory context */
} MemoryContextType;

/* evaluate topN is too difficult, because of the uncertain pos, so that need too temprary memory or introduce hash */
typedef enum MemoryStatType {
    MEM_STAT_CURRENT_CTX_BY_POSITION, /* only acquire the current memory alloc statistics matched file and line,
                                         output data only totalSpace, and totalChunks, are invaild */
    MEM_STAT_CURRENT_CTX,             /* acquire the current memory context total statistics */
    MEM_STAT_ALL_CTX_BY_POSITION,     /* get memory context include all children statistic that matched file and line */
    MEM_STAT_ALL_CTX,                 /* current and recurse all child memory context total statistics */
} MemoryStatType;

typedef struct {
    size_t totalSpace;  /* total allocated memory */
    size_t totalBlocks; /* total allocated Blocks */
    size_t totalChunks; /* total allocated chunks */
    size_t freeSpace;   /* only invaild when memory context type is MEM_CXT_TYPE_GENERIC */
    size_t freeChunks;  /* only invaild when memory context type is MEM_CXT_TYPE_GENERIC */
} MemStat;

typedef struct {
    int line;         /* only used when type is xxx_BY_POSITION */
    const char *file; /* only used when type is xxx_BY_POSITION */
} AllocPosition;

typedef struct {
    MemoryStatType type;
    AllocPosition pos;
} MemStatFilter;

typedef struct MemCtxHdr *MemoryContext;

/*****************  memory context ops  *********/
/**
 * create a memory context by spcific parameter
 *
 * @param[in] parent         - the parent context, if parent is NULL, will create an isolated root
 *                             memory context
 * @param[in] type           - see MemoryContextType type define
 * @param[in] name           - the name of memory context that will be created, name can be NULL
 * @param[in] initBlockSize  - the first block size is initBlockSize, all block size >= initBlockSize,
 *                             this parameter only used in generic memory context
 * @param[in] maxBlockSize   - the max block size, this parameter only used in generic memory context
 * @param[in] maxSize        - the memory context max usage memory limit from OS
 * @return                   - the MemoryContext will not NULL if success
 */
MemoryContext MemoryContextCreate(MemoryContext parent, MemoryContextType type, const char *name, size_t initBlockSize,
                                  size_t maxBlockSize, size_t maxSize);

/**
 * set memory context whether print log to errlog, current only used in errlog to avoid circular dependencies
 * and infinte flush log
 *
 * @param[in] context  - the context want to set to print log behavior
 * @param[in] isSilent - true, is silent, will not print log, false on the contrary, default is false
 */
void MemoryContextSetSilent(MemoryContext context, bool isSilent);

/**
 * set memory context whether print summary to errlog, when alloc memory arrive at limit. because don't want to depends
 * on hash tables, so we can only be traversed multiple times, the summary time complexity is o(n^2), if need display
 * summary statistics of the all memory allocate location. too slow maybe accept hardly. so here only print the current
 * allocate location summary. time complexity o(n), only used in development stage. the summary will show string:
 * "memory context %s, [filename]:[line] total alloc [size]", square brackets is the actual value.
 * only print summary to errlog when had set silent to false, and summary to true, if summary set to false, the
 * only print the total statistics, not classify by the different file:line
 *
 * @param[in] context        - the context want to set to print log behavior
 * @param[in] isPrintSummary - true is print summary, false not. default is false
 */
void MemoryContextSetPrintSummary(const MemoryContext context, bool isPrintSummary);

/**
 * MemoryContextSwitchTo function is for set the current memory context, and return the old current
 * memory contextonly used with generic memory context, and cooperate with MemAlloc,MemAlloc0,
 * MemAllocEx,MemAllocAlignedEx,MemRealloc,MemReallocEx API together to use. This mentioned above
 * API alloc memory in current memory context.
 *
 * @param[in] context - the context want to set to current memory context
 * @return            - the old current memory context
 */
MemoryContext MemoryContextSwitchTo(MemoryContext context);

/**
 * delete the memory context and all the children memory context
 *
 * @param[in] context      - the memory context need to delete
 */
void MemoryContextDelete(MemoryContext context);

/**
 * reset the memory context and all the children memory context. the diffence from MemoryContextDelete is
 * reset only free user area memory, and keep the the memory context Manage data structures with reset it
 * to initialization state
 *
 * @param[in] context      - the memory context need to reset
 */
void MemoryContextReset(MemoryContext context);

/**
 *  below xxxD API have two default arguments
 *  @param[in] file       - the memory allocate function caller file location
 *  @param[in] line       - the caller's line location of file
 */

/*****************  alloc/free  *****************/
/* alloc/free memory by specific memory context */

/**
 * allocate specified size memory on the memory context of context
 *
 * @param[in] context      - memory context where to allocating memory
 * @param[in] size         - caller need allocate memory size
 * @return                 - the user pointer, need check whether is NULL or not
 */
void *MemoryContextAllocD(MemoryContext context, size_t size, const char *file, int line);
#define MemoryContextAlloc(...) MemoryContextAllocD(__VA_ARGS__, __FILE__, __LINE__)

/**
 * allocate specified size memory on the memory context of context with extended flags
 *
 * @param[in] context      - memory context where to allocating memory
 * @param[in] size         - caller need allocate memory size
 * @param[in] flags        - extended flags, support MCTX_ALLOC_FLAG_DEFAULT,MCTX_ALLOC_FLAG_OOM,MCTX_ALLOC_FLAG_ZERO
 * @return                 - the user pointer, need check whether is NULL or not if flags is not MCTX_ALLOC_FLAG_OOM
 */
void *MemoryContextAllocExD(MemoryContext context, size_t size, uint32_t flags, const char *file, int line);
#define MemoryContextAllocEx(...) MemoryContextAllocExD(__VA_ARGS__, __FILE__, __LINE__)

/**
 * allocate specified size memory on the memory context with extended flags, and aligned to alignto
 *
 * @param[in] context      - memory context where to allocating memory
 * @param[in] size         - caller need allocate memory size
 * @param[in] alignto      - aligned to size
 * @param[in] flags        - extended flags, support MCTX_ALLOC_FLAG_DEFAULT,MCTX_ALLOC_FLAG_OOM,MCTX_ALLOC_FLAG_ZERO
 * @return                 - the user pointer, need check whether is NULL or not if flags is not MCTX_ALLOC_FLAG_OOM
 */
void *MemoryContextAllocAlignedExD(MemoryContext context, size_t size, size_t alignto, uint32_t flags, const char *file,
                                   int line);
#define MemoryContextAllocAlignedEx(...) MemoryContextAllocAlignedExD(__VA_ARGS__, __FILE__, __LINE__)

/**
 * allocate specified size memory on the currnet memory context
 *
 * @param[in] size         - caller need allocate memory size
 * @return                 - the user pointer, need check whether is NULL or not
 */
void *MemAllocD(size_t size, const char *file, int line);
#define MemAlloc(...) MemAllocD(__VA_ARGS__, __FILE__, __LINE__)

/**
 * allocate specified size memory on the currnet memory context, and clear memory simultaneously, like to glibc calloc
 *
 * @param[in] size         - caller need allocate memory size
 * @return                 - the user pointer, need check whether is NULL or not
 */
void *MemAlloc0D(size_t size, const char *file, int line);
#define MemAlloc0(...) MemAlloc0D(__VA_ARGS__, __FILE__, __LINE__)

/**
 * allocate specified size memory on the current memory context of context with extended flags
 *
 * @param[in] size         - caller need allocate memory size
 * @param[in] flags        - extended flags, support MCTX_ALLOC_FLAG_DEFAULT,MCTX_ALLOC_FLAG_OOM,MCTX_ALLOC_FLAG_ZERO
 * @return                 - the user pointer, need check whether is NULL or not if flags is not MCTX_ALLOC_FLAG_OOM
 */
void *MemAllocExD(size_t size, uint32_t flags, const char *file, int line);
#define MemAllocEx(...) MemAllocExD(__VA_ARGS__, __FILE__, __LINE__)

/**
 * allocate specified size memory on the current memory context with extended flags, and aligned to alignto
 *
 * @param[in] size         - caller need allocate memory size
 * @param[in] alignto      - aligned to size
 * @param[in] flags        - extended flags, support MCTX_ALLOC_FLAG_DEFAULT,MCTX_ALLOC_FLAG_OOM,MCTX_ALLOC_FLAG_ZERO
 * @return                 - the user pointer, need check whether is NULL or not if flags is not MCTX_ALLOC_FLAG_OOM
 */
void *MemAllocAlignedExD(size_t size, size_t alignto, uint32_t flags, const char *file, int line);
#define MemAllocAlignedEx(...) MemAllocAlignedExD(__VA_ARGS__, __FILE__, __LINE__)

/**
 * reallocate specified size memory
 *
 *
 * @param[in] pointer      - the old pointer need to reallocate, the pointer must be allocated by memory context API
 * @param[in] size         - caller expected new memory size
 * @return                 - the user pointer, need check whether is NULL or not, if allocate fail, return NULL, and
 *                           the old memory(pointer) will not free, but when allocate success, old memory(pointer)
 *                           will free auto.
 */
void *MemReallocD(void *pointer, size_t size, const char *file, int line);
#define MemRealloc(...) MemReallocD(__VA_ARGS__, __FILE__, __LINE__)

/**
 * free memory of pointer
 *
 * @param[in] pointer      - the memory pointer need to free
 */
void MemFree(void *pointer);

/**
 * free memory of *pointer and set it to NULL
 *
 * @param[in] pointer      - the address of a pointer memory need to free
 */
void MemFreeAndSetNull(void **pointer);

/*****************  string duplicate  ***********/
/**
 * string duplicate on the specified memory context
 *
 * @param[in] context      - memory context where to allocating memory
 * @param[in] string       - a string need to duplicate
 * @return                 - the duplicate string, need check whether is NULL or not, if not NULL, string duplicate
 *                           success, and the string memory in context
 */
char *MemoryContextStrdupD(MemoryContext context, const char *string, const char *file, int line);
#define MemoryContextStrdup(...) MemoryContextStrdupD(__VA_ARGS__, __FILE__, __LINE__)

/**
 * string duplicate on current memory context
 *
 * @param[in] string       - a string need to duplicate
 * @return                 - the duplicate string, need check whether is NULL or not, if not NULL, string duplicate
 *                           success, and the string memory in current memory context
 */
char *MemStrdupD(const char *string, const char *file, int line);
#define MemStrdup(...) MemStrdupD(__VA_ARGS__, __FILE__, __LINE__)

/**
 * string duplicate on current memory context in a specificate len, returned string is always have a null terminator
 *
 * @param[in] string       - a string need to duplicate
 * @param[in] len          - the max len to duplicate
 * @return                 - the duplicate string, need check whether is NULL or not, if not NULL, string duplicate
 *                           success, and the string memory in current memory context
 */
char *MemNStrdupD(const char *string, size_t len, const char *file, int line);
#define MemNStrdup(...) MemNStrdupD(__VA_ARGS__, __FILE__, __LINE__)

/*****************  dfx   ***********************/
/**
 * get memory context name
 *
 * @param[in] context      - the memory context need to get name
 * @return                 - the memory context name
 */
const char *GetMemoryContextName(const MemoryContext context);

/**
 * get memory context parent memory context
 *
 * @param[in] context      - the memory context need to get parent
 * @return                 - the parent memory context
 */
MemoryContext GetMemoryContextParent(const MemoryContext context);

/**
 * get memory context type
 *
 * @param[in] context      - the memory context need to get type
 * @return                 - the memory context type
 */
MemoryContextType GetMemoryContextType(const MemoryContext context);

/**
 * judge the memory context whether have no any user momory and no any child memory context, use to check memory leak.
 * only MEM_CXT_TYPE_SIMPLE_SHARE support empty check, because of other memory context not appear memory leak.
 *
 * @param[in] context      - the memory context
 * @return                 - false is not empty, and true is empty or context is NULL, or this type memory
 *                           context not support is empty function
 */
bool MemoryContextIsEmpty(const MemoryContext context);

/**
 * judge the memory context is a vaild memory context or not
 *
 * @param[in] context      - the memory context
 * @return                 - true is vaild/normal memory context, false is not a memory context or corrupted
 */
bool MemoryContextIsValid(const MemoryContext context);

/**
 * check the memory context memory area is legitimate or happened out of bound write, had tainted or corrupted
 *
 * @param[in] context      - the memory context
 */
void MemoryContextCheck(const MemoryContext context);

/**
 * get only specific memory context's statistic, not include its children
 *
 * @param[in] context      - the memory context
 * @param[in] isPrintToLog - whether print error log, true will print all memory.
 *                           format is [memctx:name]<%file>:<%line>,size=<%size>
 * @param[in] filter       - query filter parameters, current include query type and allocate position in file if need
 * @param[out] stat        - result of statistics, there are a lot of statistics, only real-time consistency
 *                           of a single data is guaranteed, because read statistics use atomic read, no lock,
 *                           it may happen that during a memory allocation or release process, 'chunks' are updated,
 *                           but 'totalspace' has not been updated yet.
 */
void MemoryContextStats(const MemoryContext context, bool isPrintToLog, const MemStatFilter *filter, MemStat *stat);

/**
 * get a pointer real hold space, include memory allocator manage data and extra redundancy space
 *
 * @param[in] pointer      - user memory pointer that allocated by memory context API
 * @return                 - the real hold space
 */
size_t MemGetSpace(const void *pointer);

/**
 * record a save point, that can undo(free) all the memory allocate after that save point. currently only
 * used in stack memory context to save stack top pointer
 *
 * @param[in] context      - the memory context
 * @return                 - the (void * type)handle of save point
 */
void *MemoryContextSavePoint(MemoryContext context);

/**
 * restore a save point, the memory allocate after that save point will be undo(free), currently only used
 * in stack memory context to restore stack top pointer
 *
 * @param[in] context      - the memory context
 * @param[in] point        - the save point get via MemoryContextSavePoint API
 */
void MemoryContextRestorePoint(MemoryContext context, void *point);
#ifdef ENABLE_UT
/* count the memory context quantity in the context tree */
uint32_t MemoryContextNums(const MemoryContext context);
#endif

GSDB_END_C_CODE_DECLS
#endif /* UTILS_MEMORY_CONTEXT_H */
