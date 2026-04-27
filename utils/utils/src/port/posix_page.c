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
 * ---------------------------------------------------------------------------------
 *
 * posix_page.c
 *
 * Description:
 * 1. Implementation of the linux memory map interface wrapper.
 *
 * ---------------------------------------------------------------------------------
 */
#include <sys/mman.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/prctl.h>
#include "defines/abort.h"
#include "port/posix_errcode.h"
#include "port/posix_page.h"

PortMemConfig g_memConfig = {MEMORY_FAILURE_HANDLE_BEHAVIOR_ABORT, MEMORY_PAGE_SIZE, false,
                             TRANSPARENT_HUGE_PAGE_MODE_DEFAULT};

typedef struct MemBootstrapCheckResult MemBootstrapCheckResult;
struct MemBootstrapCheckResult {
    bool mmap;
    bool madviseDontneed;
    bool pageSize;
};
MemBootstrapCheckResult g_memBootstrapCheckResult = {true, true, true};
/*
 * Get the memory check result for mmap.
 */
bool GetMemBootCheckResultForMmap(void)
{
    return g_memBootstrapCheckResult.mmap;
}
/*
 * Get the memory check result for madvise dontneed.
 */
bool GetMemBootCheckResultForMadviseDontneed(void)
{
    return g_memBootstrapCheckResult.madviseDontneed;
}
/*
 * Get the memory check result for pageSize.
 */
bool GetMemBootCheckResultForPageSize(void)
{
    return g_memBootstrapCheckResult.pageSize;
}
/*
 * Sets the memory failure processing mode.
 */
void SetMemMapFailureHandleBehavior(MemoryFailureHandleBehavior failureHandleBehavior)
{
    g_memConfig.failureHandleBehavior = failureHandleBehavior;
}

/*
 * Get the memory failure processing mode.
 */
MemoryFailureHandleBehavior GetMemMapFailureHandleBehavior(void)
{
    return g_memConfig.failureHandleBehavior;
}
/*
 * Sets the memory page size.
 */
void SetMemMapOsPageSize(size_t osPageSize)
{
    g_memConfig.osPageSize = osPageSize;
}

/*
 * Get the memory page size.
 */
size_t GetMemMapOsPageSize(void)
{
    return g_memConfig.osPageSize;
}
/*
 * Sets the memory page overcommit.
 */
void SetMemMapOvercommit(bool overcommit)
{
    g_memConfig.overcommit = overcommit;
}
/*
 * Get the memory page overcommit.
 */
bool GetMemMapOvercommit(void)
{
    return g_memConfig.overcommit;
}
/*
 * Sets the memory page transparent huge page mode.
 */
void SetMemMapTransparentHugePageMode(TransparentHugePageMode transparentHugePageMode)
{
    g_memConfig.transparentHugePageMode = transparentHugePageMode;
}
/*
 * Get the memory page transparent huge page mode.
 */
TransparentHugePageMode GetMemMapTransparentHugePageMode(void)
{
    return g_memConfig.transparentHugePageMode;
}
/*
 * Bootstrap check MADV_DONTNEED function of madvise.
 * MADV_DONTNEED: Do not expect access in the near future.  (For the time being,
 * the application is finished with the given range,so the kernel can free resources
 * associated with it.)
 * MADV_DONTNEED cannot be applied to locked pages, Huge TLB pages, or VM_PFNMAP pages.
 */
void BootstrapCheckMadviseDontneed(void)
{
    size_t size = MEMORY_PAGE_SIZE;
    void *addr = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, MEM_MAP_INVALID_FD, 0);
    if (addr == MAP_FAILED) {
        if (g_memConfig.failureHandleBehavior == MEMORY_FAILURE_HANDLE_BEHAVIOR_ABORT) {
            Abort();
        } else {
            g_memBootstrapCheckResult.mmap = false;
        }
    }
    g_memBootstrapCheckResult.madviseDontneed = false;
    errno_t rc = memset_s(addr, size, 'B', size);
    if (rc != EOK) {
        unsigned int i;
        char *addrChar = (char *)addr;
        for (i = 0; i < size; i++) {
            addrChar[i] = 'B';
        }
    }
    /*
     * Check that MADV_DONTNEED will actually zero pages on subsequent access.
     */
    if (madvise(addr, size, MADV_DONTNEED) == 0) {
        if (memchr(addr, 'B', size) == NULL) {
            g_memBootstrapCheckResult.madviseDontneed = true;
        }
    }
    if (munmap(addr, size) != 0) {
        if (g_memConfig.failureHandleBehavior == MEMORY_FAILURE_HANDLE_BEHAVIOR_ABORT) {
            Abort();
        } else {
            g_memBootstrapCheckResult.mmap = false;
        }
    } else {
        g_memBootstrapCheckResult.mmap = true;
    }
}
/*
 * Get the OS memory page size.
 */
size_t GetOsPageSize(void)
{
#if defined(__FreeBSD__)
    return getpagesize();
#else
    long result = sysconf(_SC_PAGESIZE);
    if (result == -1) {
        return MEMORY_PAGE_SIZE;
    }
    return (size_t)result;
#endif
}

/*
 * Bootstrap checks whether the configured page size is appropriate.
 */
void BootstrapCheckPageSize(void)
{
    g_memConfig.osPageSize = GetOsPageSize();
    if (g_memConfig.osPageSize <= MEMORY_PAGE_SIZE) {
        g_memBootstrapCheckResult.pageSize = true;
    } else {
        g_memBootstrapCheckResult.pageSize = false;
        if (g_memConfig.failureHandleBehavior == MEMORY_FAILURE_HANDLE_BEHAVIOR_ABORT) {
            Abort();
        }
    }
}

#if defined(__FreeBSD__)
/*
 * Memory Overcommit means that the memory promised by the operating system to a
 * process exceeds the available memory.
 * Overcommit refers to memory application. Memory application is not equal to memory allocation.
 * Memory is allocated only when it is actually used.
 * Linux allows memory overcommit, and I'll give it to you if you apply for memory, hoping that the process
 * doesn't actually use that much memory, but what if it does? Then there will be a "bank run" crisis,
 * and there will be a shortage of cash (memory). Linux designs an OOM killer mechanism (OOM = out-of-memory)
 * to deal with this crisis: select a process to kill to free up some memory. If not, continue to kill...
 * You can also set the kernel parameter vm.panic_on_oom to automatically restart the system when OOM occurs.
 * This is a risky mechanism. Restarting the process may interrupt services. Killing the process may also
 * interrupt services.
 * 0: Heuristic overcommit. This is the default value.  It allows overcommit if it thinks it's reasonable
 * and it rejects overcommit if it thinks it's unreasonable.
 * 1: Always overcommit. The memory application is allowed to be overcommitted.
 * 2 - Don't overcommit.
 */
bool GetOsMemOvercommit()
{
    int overcommit;
    size_t size = sizeof(overcommit);
#if defined(VM_OVERCOMMIT)
#define MIB_OVERCOMMIT_ARRARY_SIZE 2
    int mib[MIB_OVERCOMMIT_ARRARY_SIZE];
    mib[0] = CTL_VM;
    mib[1] = VM_OVERCOMMIT;
    if (sysctl(mib, MIB_OVERCOMMIT_ARRARY_SIZE, &overcommit, &size, NULL, 0) != 0) {
        return false;
    }
#else
    if (sysctlbyname("vm.overcommit", &overcommit, &size, NULL, 0) != 0) {
        return false;
    }
#endif
    return ((overcommit & 0x3) == 0);
}

#elif defined(__linux__)
#if defined(O_CLOEXEC)
#define OPEN_FILE_FLAG (O_RDONLY | O_CLOEXEC)
#else
#define OPEN_FILE_FLAG (O_RDONLY)
#endif
/*
 * Use syscall rather than {open,read,close} to avoid reentry if another
 * library has interposed system call memory alloc.
 */
int PageOpenFile(char *fileName)
{
    int fd;
#if defined(SYS_open)
    fd = (int)syscall(SYS_open, fileName, OPEN_FILE_FLAG);
#elif defined(SYS_openat)
    fd = (int)syscall(SYS_openat, AT_FDCWD, fileName, OPEN_FILE_FLAG);
#else
    fd = open(fileName, OPEN_FILE_FLAG);
#endif
#if !defined(O_CLOEXEC)
    if (fd != -1) {
        fcntl(fd, F_SETFD, fcntl(fd, F_GETFD) | FD_CLOEXEC);
    }
#endif
    return fd;
}

ssize_t PageReadFile(int fd, void *buf, size_t count)
{
#if defined(SYS_read)
    long result = syscall(SYS_read, fd, buf, count);
#else
    ssize_t result = read(fd, buf, count);
#endif
    return (ssize_t)result;
}

void PageCloseFile(int fd)
{
#if defined(SYS_close)
    syscall(SYS_close, fd);
#else
    int rc;
    rc = close(fd);
    if (rc != 0) {
        return;
    }
#endif
}

bool GetOsMemOvercommit(void)
{
    int fd;
    char buf[1];
    fd = PageOpenFile("/proc/sys/vm/overcommit_memory");
    if (fd < 0) {
        return false;
    }
    ssize_t readSize = PageReadFile(fd, buf, sizeof(buf));
    if (readSize < 1) {
        PageCloseFile(fd);
        return false;
    }
    PageCloseFile(fd);
    /*
     * /proc/sys/vm/overcommit_memory meanings:
     * 0: Heuristic overcommit.
     * 1: Always overcommit.
     * 2: Never overcommit.
     */
    return (buf[0] == '0' || buf[0] == '1');
}
#elif defined(__NetBSD__)
bool GetOsMemOvercommit()
{
    return true;
}
#else
bool GetOsMemOvercommit()
{
    return false;
}
#endif

#if defined(__linux__)
TransparentHugePageMode GetOsTransparentHugePageMode(void)
{
    static const char MADVISE[] = "always [madvise] never\n";
    static const char ALWAYS[] = "[always] madvise never\n";
    static const char NEVER[] = "always madvise [never]\n";
    char buf[sizeof(MADVISE)];

    int fd = PageOpenFile("/sys/kernel/mm/transparent_hugepage/enabled");
    if (fd < 0) {
        return TRANSPARENT_HUGE_PAGE_MODE_NOT_SUPPORTED;
    }
    ssize_t readSize = PageReadFile(fd, buf, sizeof(buf));
    if (readSize < 0) {
        PageCloseFile(fd);
        return TRANSPARENT_HUGE_PAGE_MODE_NOT_SUPPORTED;
    }
    PageCloseFile(fd);

    if (strncmp(buf, MADVISE, (size_t)readSize) == 0) {
        return TRANSPARENT_HUGE_PAGE_MODE_DEFAULT;
    } else if (strncmp(buf, ALWAYS, (size_t)readSize) == 0) {
        return TRANSPARENT_HUGE_PAGE_MODE_ALWAYS;
    } else if (strncmp(buf, NEVER, (size_t)readSize) == 0) {
        return TRANSPARENT_HUGE_PAGE_MODE_NEVER;
    } else {
        return TRANSPARENT_HUGE_PAGE_MODE_NOT_SUPPORTED;
    }
}
#else
TransparentHugePageMode GetOsTransparentHugePageMode()
{
    return TRANSPARENT_HUGE_PAGE_MODE_DEFAULT;
}
#endif

/*
 * Configure the VMA name of an anonymous memory segment applied for by the MMAP, name must
 * be a static variable or a variable on the heap.
 * While parsing `/proc/<pid>/maps` file, the name could appear in the content line.
 */
#if defined(__linux__)
void SetMemMapName(void *addr, size_t size, const char *name)
{
#ifdef PR_SET_VMA
    int ret = prctl(PR_SET_VMA, PR_SET_VMA_ANON_NAME, (uintptr_t)addr, size, (uintptr_t)name);
    if (ret != 0) {
        if (g_memConfig.failureHandleBehavior == MEMORY_FAILURE_HANDLE_BEHAVIOR_ABORT) {
            Abort();
        }
    }
#else
    (void)addr;
    (void)size;
    (void)name;
#endif
}
#else
void SetMemMapName(void *addr, size_t size, const char *name)
{
    (void)addr;
    (void)size;
    (void)name;
    return;
}
#endif

/*
 * Creates a new mapping in the virtual address space of the calling process.
 */
ErrorCode MemMap(MemMapAttribute *memMapAttribute, MemMapBuffer *memMapBuffer)
{
    unsigned int prot = 0;
    unsigned int iprotect = (unsigned int)memMapAttribute->protect;
    if (iprotect & MEM_MAP_PROTECT_EXEC) {
        prot = prot | PROT_EXEC;
    }
    if (iprotect & MEM_MAP_PROTECT_READ) {
        prot = prot | PROT_READ;
    }
    if (iprotect & MEM_MAP_PROTECT_WRITE) {
        prot = prot | PROT_WRITE;
    }
    unsigned int flags = 0;
    unsigned int iflags = (unsigned int)memMapAttribute->flags;
    if (iflags & MEM_MAP_SHARED) {
        flags = flags | MAP_SHARED;
    }
    if (iflags & MEM_MAP_PRIVATE) {
        flags = flags | MAP_PRIVATE;
    }
    if (iflags & MEM_MAP_ANONYMOUS) {
        flags = flags | MAP_ANONYMOUS;
    }
    void *addr = NULL;
    ErrorCode errCode = ERROR_SYS_OK;
    addr = mmap(memMapAttribute->addr, memMapAttribute->length, (int)prot, (int)flags, memMapAttribute->fd,
                memMapAttribute->offset);
    if (addr == MAP_FAILED) {
        PosixErrorCode2PortErrorCode(errno, &errCode);
        memMapBuffer->addr = NULL;
        memMapBuffer->length = 0;
    } else {
        memMapBuffer->addr = addr;
        memMapBuffer->length = memMapAttribute->length;
    }
    return errCode;
}
/*
 * Removes the mappings for pages.
 */
void MemUnmap(MemMapBuffer *memMapBuffer)
{
    int rc;
    if (memMapBuffer->addr != NULL) {
        rc = munmap(memMapBuffer->addr, memMapBuffer->length);
        if (rc != 0) {
            Abort();
        }
        memMapBuffer->addr = NULL;
        memMapBuffer->length = 0;
    }
}

/*
 * Free the memory mappings pages.
 */
void MemPagesFree(void *addr, size_t size)
{
    int rc;
    rc = munmap(addr, size);
    if (rc != 0) {
        if (g_memConfig.failureHandleBehavior == MEMORY_FAILURE_HANDLE_BEHAVIOR_ABORT) {
            Abort();
        }
    }
}

/*
 * Malloc the memory mappings pages.
 */
void *MemPagesAlloc(size_t size)
{
    if (size > MAX_MEMORY_PAGE_SIZE) {
        return NULL;
    }
    void *addr = NULL;
    addr = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, MEM_MAP_INVALID_FD, 0);
    if (addr == MAP_FAILED) {
        if (g_memConfig.failureHandleBehavior == MEMORY_FAILURE_HANDLE_BEHAVIOR_ABORT) {
            Abort();
        } else {
            addr = NULL;
        }
    }
    return addr;
}
/*
 * Check mmap result.
 */
bool MemPagesResultCheck(void *result, void *addr, size_t size)
{
    if (result == MAP_FAILED) {
        return false;
    }
    if (result != addr) {
        int rc = munmap(addr, size);
        if (rc != 0) {
            Abort();
        }
        return false;
    }
    return true;
}
/*
 * Commit memory pages.
 */
bool MemPagesCommit(void *addr, size_t size)
{
    if (size > MAX_MEMORY_PAGE_SIZE) {
        return false;
    }
    void *result = NULL;
    result = mmap(addr, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON | MAP_FIXED, MEM_MAP_INVALID_FD, 0);
    return MemPagesResultCheck(result, addr, size);
}

/*
 * Decommit memory pages.
 */
bool MemPagesDecommit(void *addr, size_t size)
{
    if (size > MAX_MEMORY_PAGE_SIZE) {
        return false;
    }
    void *result = NULL;
    result = mmap(addr, size, PROT_NONE, MAP_PRIVATE | MAP_ANON | MAP_FIXED, MEM_MAP_INVALID_FD, 0);
    return MemPagesResultCheck(result, addr, size);
}

/*
 * Dont dump memory pages.
 */
bool MemPagesDontDump(void *addr, size_t size)
{
    int rc;
    rc = madvise(addr, size, MADV_DONTDUMP);
    if (rc != 0) {
        return false;
    }
    return true;
}

/*
 * Do dump memory pages.
 */
bool MemPagesDoDump(void *addr, size_t size)
{
    int rc;
    rc = madvise(addr, size, MADV_DODUMP);
    if (rc != 0) {
        return false;
    }
    return true;
}
/*
 * Mark memory pages guard.
 */
bool MemPagesMarkGuard(void *addr, size_t size)
{
    int rc;
    rc = mprotect(addr, size, PROT_NONE);
    if (rc != 0) {
        return false;
    }
    return true;
}

/*
 * Unmark memory pages guard.
 */
bool MemPagesUnmarkGuard(void *addr, size_t size)
{
    int rc;
    rc = mprotect(addr, size, PROT_READ | PROT_WRITE);
    if (rc != 0) {
        return false;
    }
    return true;
}

/*
 * Use huge memory pages.
 */
bool MemPagesHuge(void *addr, size_t size)
{
    int rc;
    rc = madvise(addr, size, MADV_HUGEPAGE);
    if (rc != 0) {
        return false;
    }
    return true;
}
/*
 * Do not use huge memory pages.
 */
bool MemPagesNoHuge(void *addr, size_t size)
{
    int rc;
    rc = madvise(addr, size, MADV_NOHUGEPAGE);
    if (rc != 0) {
        return false;
    }
    return true;
}
/*
 * Purge memory pages lazy.
 */
bool MemPagesPurgeLazy(void *addr, size_t size)
{
    int rc;
#ifdef MADV_FREE
    rc = madvise(addr, size, MADV_FREE);
#else
    rc = madvise(addr, size, MADV_DONTNEED);
#endif
    if (rc != 0) {
        return false;
    }
    return true;
}

/*
 * Purge memory pages forced.
 */
bool MemPagesPurgeForced(void *addr, size_t size)
{
    int rc;
    rc = madvise(addr, size, MADV_DONTNEED);
    if (rc != 0) {
        return false;
    }
    return true;
}

/*
 * Boot memory pages.
 */
bool MemPagesBoot(void)
{
    BootstrapCheckMadviseDontneed();
    BootstrapCheckPageSize();
    return true;
}
