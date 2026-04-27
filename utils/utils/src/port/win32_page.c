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
 * win32_page.c
 *
 * Description:
 * 1. Implementation of the windows memory map interface wrapper.
 *
 * ---------------------------------------------------------------------------------
 */

#include "port/win32_page.h"

/*
 * Creates a new mapping in the virtual address space of the calling process.
 */
ErrorCode MemMap(MemMapAttribute *memMapAttribute, MemMapBuffer *memMapBuffer)
{
    HANDLE hFile;
    if (memMapAttribute->fd == MEM_MAP_INVALID_FD) {
        hFile = INVALID_HANDLE_VALUE;
    } else {
        hFile = (HANDLE)_get_osfhandle(fd);
    }
    DWORD flProtect = 0;
    DWORD dwDesiredAccess = 0;
    if (memMapAttribute->protect & MEM_MAP_PROTECT_EXEC) {
        flProtect = flProtect | PAGE_EXECUTE_READ;
        dwDesiredAccess = dwDesiredAccess | FILE_MAP_EXECUTE;
    }
    if (memMapAttribute->protect & MEM_MAP_PROTECT_READ) {
        flProtect = flProtect | PAGE_READONLY;
        dwDesiredAccess = dwDesiredAccess | FILE_MAP_READ;
    }
    if (memMapAttribute->protect & MEM_MAP_PROTECT_WRITE) {
        flProtect = flProtect | PAGE_READWRITE;
        dwDesiredAccess = dwDesiredAccess | FILE_MAP_WRITE;
    }
    DWORD dwMaximumSizeHigh = memMapAttribute->length >> 32;
    DWORD dwMaximumSizeLow = memMapAttribute->length & 0xFFFFFFFF;
    ErrorCode errCode = ERROR_SYS_OK;
    memMapBuffer->mapFile = NULL;
    memMapBuffer->addr = NULL;
    HANDLE hMapFile;
    if (memMapAttribute->flags & MEM_MAP_FIRST_PROCESS) {
        hMapFile =
            CreateFileMapping(hFile, NULL, flProtect, dwMaximumSizeHigh, dwMaximumSizeLow, memMapAttribute->name);
    } else {
        hMapFile = OpenFileMapping(dwDesiredAccess, FALSE, memMapAttribute->name);
    }
    if (hMapFile == NULL) {
        WindowsErrorCode2PortErrorCode(GetLastError(), &errCode);
        return errCode;
    }

    LPCTSTR pBuf;
    DWORD dwFileOffsetHigh = memMapAttribute->offset >> 32;
    DWORD dwFileOffsetLow = memMapAttribute->offset & 0xFFFFFFFF;
    SIZE_T dwNumberOfBytesToMap = memMapAttribute->length;
    pBuf = (LPTSTR)MapViewOfFile(hMapFile, dwDesiredAccess, dwFileOffsetHigh, dwFileOffsetLow, 0, dwNumberOfBytesToMap);
    if (pBuf == NULL) {
        WindowsErrorCode2PortErrorCode(GetLastError(), &errCode);
        CloseHandle(hMapFile);
        return errCode;
    }
    memMapBuffer->mapFile = hMapFile;
    memMapBuffer->addr = (PVOID)pBuf;
    return errCode;
}
/*
 * Removes the mappings for pages.
 */
void MemUnmap(MemMapBuffer *memMapBuffer)
{
    if (memMapBuffer->addr != NULL) {
        UnmapViewOfFile((LPCTSTR)memMapBuffer->addr);
    }
    if (memMapBuffer->mapFile != NULL) {
        CloseHandle(memMapBuffer->mapFile);
        memMapBuffer->mapFile = NULL;
        memMapBuffer->length = 0;
    }
}
/*
 * Free the memory mappings pages.
 */
void MemPagesFree(void *addr, size_t size)
{
    BOOL rc;
    rc = VirtualFree(addr, 0, MEM_RELEASE);
    if (!rc) {
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
    void *addr = NULL;
    addr = VirtualAlloc(addr, size, MEM_RESERVE | MEM_COMMIT, PAGE_READWRITE);
    if (addr == NULL) {
        if (g_memConfig.failureHandleBehavior == MEMORY_FAILURE_HANDLE_BEHAVIOR_ABORT) {
            Abort();
        }
    }
    return addr;
}
/*
 * Commit memory pages.
 */
bool MemPagesCommit(void *addr, size_t size)
{
    void *allocAddr = NULL;
    allocAddr = VirtualAlloc(addr, size, MEM_COMMIT, PAGE_READWRITE);
    if (allocAddr == NULL) {
        return false;
    }
    return true;
}

/*
 * Decommit memory pages.
 */
bool MemPagesDecommit(void *addr, size_t size)
{
    BOOL decommit;
    decommit = VirtualFree(addr, size, MEM_DECOMMIT);
    if (!decommit) {
        return false;
    }
    return true;
}

/*
 * Dont dump memory pages.
 */
bool MemPagesDontDump(void *addr, size_t size)
{
    /*
     * The Windows operating system does not support the setting of whether to dump the memory.
     * Always returns true.
     */
    return true;
}

/*
 * Do dump memory pages.
 */
bool MemPagesDoDump(void *addr, size_t size)
{
    /*
     * The Windows operating system does not support the setting of whether to dump the memory.
     * Always returns true.
     */
    return true;
}

/*
 * Mark memory pages guard.
 */
bool MemPagesMarkGuard(void *addr, size_t size)
{
    return MemPagesDecommit(addr, size);
}

/*
 * Unmark memory pages guard.
 */
bool MemPagesUnmarkGuard(void *addr, size_t size)
{
    return MemPagesCommit(addr, size);
}

/*
 * Use huge memory pages.
 */
bool MemPagesHuge(void *addr, size_t size)
{
    /*
     * The Windows operating system does not support the setting of whether to dump the memory.
     * Always returns true.
     */
    return true;
}
/*
 * Do not use huge memory pages.
 */
bool MemPagesNoHuge(void *addr, size_t size)
{
    /*
     * The Windows operating system does not support the setting of whether to dump the memory.
     * Always returns true.
     */
    return true;
}
/*
 * Purge memory pages lazy.
 */
bool MemPagesPurgeLazy(void *addr, size_t size)
{
    void *allocAddr = NULL;
    allocAddr = VirtualAlloc(addr, size, MEM_RESET, PAGE_READWRITE);
    if (allocAddr == NULL) {
        return false;
    }
    return true;
}

/*
 * Purge memory pages forced.
 */
bool MemPagesPurgeLazy(void *addr, size_t size)
{
    return MemPagesCommit(addr, size);
}

/*
 * Boot memory pages.
 */
bool MemPagesBoot()
{
    /*
     * No extra check is required for the Windows platform.
     */
    return true;
}