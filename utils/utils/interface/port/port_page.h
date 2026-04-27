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
 * port_page.h
 *
 * Description:
 * Cross-platform OS memory allocation interface.
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef UTILS_PORT_PAGE_H
#define UTILS_PORT_PAGE_H
#include <stdio.h>
#include "securec.h"
#include "defines/common.h"
#include "types/data_types.h"

GSDB_BEGIN_C_CODE_DECLS

/* Memory page size. */
#define MEMORY_PAGE_SIZE 8192

/* Memory page max size */
#define MAX_MEMORY_PAGE_SIZE ((Size)0x40000000) /* 1 gigabyte */

enum MemoryFailureHandleBehavior {
    /* If the memory processing is abnormal, directly abort. */
    MEMORY_FAILURE_HANDLE_BEHAVIOR_ABORT = 1,
    /*
     * If the memory allocation is abnormal, NULL is returned.
     * If other exceptions occur, the corresponding error code is returned.
     */
    MEMORY_FAILURE_HANDLE_BEHAVIOR_RETURN,
    /* Memory exceptions handling end of enumeration Value. */
    MEMORY_FAILURE_HANDLE_BEHAVIOR_END
};
typedef enum MemoryFailureHandleBehavior MemoryFailureHandleBehavior;

/*
 *  The protect describes the desired memory protection of the mapping.
 *  Pages may be executed.
 *  Pages may be read.
 *  Pages may be written.
 */
#define MEM_MAP_PROTECT_EXEC  0x20
#define MEM_MAP_PROTECT_READ  0x40
#define MEM_MAP_PROTECT_WRITE 0x80
/*
 * The flags determines whether updates to the mapping are visible to other
 * processes mapping the same region.and whether updates are carried through
 * to the underlying file.
 * Share this mapping,updates to the mapping are visible to other processes mapping the same region.
 * Create a private copy-on-write mapping,updates to the mapping are not visible to other processes
 * mapping the same file.
 * The anonymous mapping is not backed by any file, its contents are initialized to zero.
 */
#define MEM_MAP_SHARED    0x20
#define MEM_MAP_PRIVATE   0x40
#define MEM_MAP_ANONYMOUS 0x80
/*
 * First or second call to mmap.
 * On the Windows platform, the first process calls CreateFileMapping,and the second process calls OpenFileMapping.
 * On the linux platform, both calls are the same.
 * To adapt to different platforms, this bit is added to the flags field.
 * This bit is not used on the Linux platform.To support cross-platform, application are advised
 * to set this parameter based on the requirements of the Windows platform.
 */
#define MEM_MAP_FIRST_PROCESS  0x100
#define MEM_MAP_SECOND_PROCESS 0x200

#define MEM_MAP_INVALID_FD (-1)
/*
 * The number of mmap function parameters on the Linux platform is six, which exceeds the
 * programming specifications.In addition, the parameters and implementations on the Linux
 * platform and Windows platform are greatly different.
 * Therefore, these input parameters are encapsulated as an input parameter struct.
 */
#define MEM_MAP_ATTRIBUTE_NAME_LEN 64
typedef struct MemMapAttribute MemMapAttribute;
struct MemMapAttribute {
    /*
     * The addr specifies the starting address for the new mapping.If addr is NULL,
     * then the kernel chooses the (page-aligned) address at which to create the mapping;
     * On Linux, the kernel will pick a nearby page boundary (but always above or equal to
     * the value specified by /proc/sys/vm/mmap_min_addr) and attempt to create the mapping there.
     * If another mapping already exists there, the kernel picks a new address that may or may
     * not depend on the hint.
     * This parameter is not used on the Windows platform.
     * To support cross-platform, application are advised to set this parameter to NULL.
     */
    void *addr;
    /*
     * The length specifies the length of the mapping (which must be greater than 0).
     */
    size_t length;
    /*
     * The protect describes the desired memory protection of the mapping,It is the bitwise
     * OR of one or more of the prior flags.
     */
    int protect;
    /*
     * This parameter is valid only on the Linux platform and is ignored on the Windows platform.
     * The meaning of this parameter on the Windows platform is the following parameter name.
     */
    int flags;
    /*
     * The mapping file,if the mapping is not backed by any file,set fd to be -1.
     */
    int fd;
    /*
     * Start offset of the mapping file. The default value is 0.
     */
    off_t offset;
    /*
     * Windows platform represents different namespaces by adding different prefixes to names.
     * The process use the "Global\" prefix to open the global object.
     * The processes can use the "Local\" prefix to explicitly create an object in their session namespace.
     * The "Session\" prefix is reserved for system use and you should not use it in names of kernel objects.
     * These keywords are case sensitive.
     * This parameter is not used on the Linux platform.To support cross-platform, application are advised
     * to set this parameter based on the requirements of the Windows platform.
     */
    char name[MEM_MAP_ATTRIBUTE_NAME_LEN];
};

/*
 * Initializes the attributes of the memory map.
 */
static inline void MemMapAttributeInit(MemMapAttribute *memMapAttribute)
{
    errno_t rc = memset_s(memMapAttribute, sizeof(MemMapAttribute), 0, sizeof(MemMapAttribute));
    if (rc != EOK) {
        memMapAttribute->addr = NULL;
        memMapAttribute->length = 0;
        memMapAttribute->protect = 0;
        memMapAttribute->flags = 0;
        memMapAttribute->offset = 0;
        memMapAttribute->name[0] = '\0';
    }
    memMapAttribute->fd = MEM_MAP_INVALID_FD;
}
/*
 * Set the attribute addr of the memory map.
 */
static inline void MemMapAttributeSetAddr(MemMapAttribute *memMapAttribute, void *addr)
{
    memMapAttribute->addr = addr;
}
/*
 * Set the attribute length of the memory map.
 */
static inline void MemMapAttributeSetLength(MemMapAttribute *memMapAttribute, size_t length)
{
    memMapAttribute->length = length;
}
/*
 * Set the attribute protect of the memory map.
 */
static inline void MemMapAttributeSetProtect(MemMapAttribute *memMapAttribute, int protect)
{
    memMapAttribute->protect = protect;
}
/*
 * Set the attribute flags of the memory map.
 */
static inline void MemMapAttributeSetFlags(MemMapAttribute *memMapAttribute, int flags)
{
    memMapAttribute->flags = flags;
}
/*
 * Set the attribute fd of the memory map.
 */
static inline void MemMapAttributeSetFd(MemMapAttribute *memMapAttribute, int fd)
{
    memMapAttribute->fd = fd;
}
/*
 * Set the attribute offset of the memory map.
 */
static inline void MemMapAttributeSetOffset(MemMapAttribute *memMapAttribute, off_t offset)
{
    memMapAttribute->offset = offset;
}
/*
 * Set the attribute name of the memory map.
 */
static inline void MemMapAttributeSetName(MemMapAttribute *memMapAttribute, char *name)
{
    int i;
    errno_t rc = strcpy_s(memMapAttribute->name, MEM_MAP_ATTRIBUTE_NAME_LEN, name);
    if (rc != EOK) {
        for (i = 0; i < MEM_MAP_ATTRIBUTE_NAME_LEN - 1; i++) {
            memMapAttribute->name[i] = '?';
        }
        memMapAttribute->name[MEM_MAP_ATTRIBUTE_NAME_LEN - 1] = '\0';
    }
}

/*
 * In virtual memory management, the os kernel maintains a table in which it has a mapping
 * of the virtual memory address to a physical address. For every page transaction, the kernel
 * needs to load related mapping. If you have small size pages then you need to load more numbers
 * of pages resulting kernel to load more mapping tables. This decreases performance.
 * Using huge pages means you will need fewer pages. This decreases the number of mapping tables
 * to load by the kernel to a great extent. This increases your kernel-level performance which
 * ultimately benefits your application.
 * HugePages is a feature integrated into the Linux kernel 2.6.
 * HugePages is useful for both 32-bit and 64-bit configurations.
 * HugePage sizes vary from 2 MB to 256 MB, greater than the default (usually 4 KB),depending on
 * the kernel version and the hardware architecture.
 */
enum TransparentHugePageMode {
    TRANSPARENT_HUGE_PAGE_MODE_DEFAULT = 0,      /* Use os default hugepage settings. */
    TRANSPARENT_HUGE_PAGE_MODE_ALWAYS = 1,       /* Always set MADV_HUGEPAGE. */
    TRANSPARENT_HUGE_PAGE_MODE_NEVER = 2,        /* Always set MADV_NOHUGEPAGE. */
    TRANSPARENT_HUGE_PAGE_MODE_NOT_SUPPORTED = 3 /* Hugepage not support detected. */
};
typedef enum TransparentHugePageMode TransparentHugePageMode;

typedef struct PortMemConfig PortMemConfig;
struct PortMemConfig {
    MemoryFailureHandleBehavior failureHandleBehavior;
    size_t osPageSize;
    bool overcommit;
    TransparentHugePageMode transparentHugePageMode;
};

/*
 * Sets the memory failure processing mode.
 */
void SetMemMapFailureHandleBehavior(MemoryFailureHandleBehavior failureHandleBehavior);

/*
 * Get the memory failure processing mode.
 */
MemoryFailureHandleBehavior GetMemMapFailureHandleBehavior(void);
/*
 * Sets the memory page size.
 */
void SetMemMapOsPageSize(size_t osPageSize);
/*
 * Get the memory page size.
 */
size_t GetMemMapOsPageSize(void);
/*
 * Sets the memory page overcommit.
 */
void SetMemMapOvercommit(bool overcommit);

/*
 * Get the memory page overcommit.
 */
bool GetMemMapOvercommit(void);

/*
 * Sets the memory page transparent huge page mode.
 */
void SetMemMapTransparentHugePageMode(TransparentHugePageMode transparentHugePageMode);

/*
 * Get the memory page transparent huge page mode.
 */
TransparentHugePageMode GetMemMapTransparentHugePageMode(void);

/*
 * Free the memory mappings pages.
 */
void MemPagesFree(void *addr, size_t size);
/*
 * Malloc the memory mappings pages.
 */
void *MemPagesAlloc(size_t size);
/*
 * Commit memory pages.
 */
bool MemPagesCommit(void *addr, size_t size);
/*
 * Decommit memory pages.
 */
bool MemPagesDecommit(void *addr, size_t size);
/*
 * Dont dump memory pages.
 */
bool MemPagesDontDump(void *addr, size_t size);
/*
 * Do dump memory pages.
 */
bool MemPagesDoDump(void *addr, size_t size);
/*
 * Mark memory pages guard.
 */
bool MemPagesMarkGuard(void *addr, size_t size);
/*
 * Unmark memory pages guard.
 */
bool MemPagesUnmarkGuard(void *addr, size_t size);
/*
 * Use huge memory pages.
 */
bool MemPagesHuge(void *addr, size_t size);
/*
 * Do not use huge memory pages.
 */
bool MemPagesNoHuge(void *addr, size_t size);
/*
 * Purge memory pages lazy.
 */
bool MemPagesPurgeLazy(void *addr, size_t size);
/*
 * Purge memory pages forced.
 */
bool MemPagesPurgeForced(void *addr, size_t size);
/*
 * Boot memory pages.
 */
bool MemPagesBoot(void);

/*
 * Get the memory check result for mmap.
 */
bool GetMemBootCheckResultForMmap(void);
/*
 * Get the memory check result for madvise dontneed.
 */
bool GetMemBootCheckResultForMadviseDontneed(void);
/*
 * Get the memory check result for pageSize.
 */
bool GetMemBootCheckResultForPageSize(void);

GSDB_END_C_CODE_DECLS

#endif /* UTILS_PORT_PAGE_H */
