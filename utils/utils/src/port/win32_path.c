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
 * win32_path.c
 *
 * Description:
 * 1. Implementation of the file path operation API on the Windows platform.
 *
 * ---------------------------------------------------------------------------------
 */
#include "port/win32_path.h"

/**
 * canonicalize path
 *
 * @param path in path
 * @param resolvedPath out path
 * @return if success return true, failed return false
 */
bool CanonicalizePath(const char *path, char *resolvedPath)
{
    return PathCanonicalize(path, resolvedPath);
}

/**
 * Obtain the file name in the path.This function does not modify the input path content.
 * @param path : File name that includes the path.
 * @return : File name.
 */
char *Basename(char *path)
{
    return PathFindFileName(path);
}

/**
 * Delete the file name and backslash (\) following the path.This function modifies the input path content.
 * @param path : File name that includes the path.
 * @return : File path.
 */
char *Dirname(char *path)
{
    BOOL rc;
    rc = PathRemoveFileSpec(path);
    if (rc) {
        return path;
    } else {
        return NULL;
    }
}

ErrorCode MakeDirectory(const char *directoryName, unsigned int mode)
{
    ErrorCode errCode = ERROR_SYS_OK;
    int rc = _mkdir(directoryName);
    if (rc != 0) {
        WindowsErrorCode2PortErrorCode(errno, &errCode);
    }
    return errCode;
}

/**
 * Deletes a directory, which must be empty.
 * @param directoryName
 * @return
 */
ErrorCode DestroyDirectory(const char *directoryName)
{
    ErrorCode errCode = ERROR_SYS_OK;
    int rc = _rmdir(directoryName);
    if (rc != 0) {
        WindowsErrorCode2PortErrorCode(errno, &errCode);
    }
    return errCode;
}

/**
 * Get current working directory.
 * @param buf
 * @param size
 * @return
 */
ErrorCode GetCurrentWorkingDirectory(char *buf, size_t size)
{
    char *pathName;
    char processPath[MAX_PATH];
    HANDLE processHandle = GetCurrentProcess();
    if (GetModuleFileNameEx(processHandle, NULL, processPath, MAX_PATH) > 0) {
        pathName = PathRemoveFileSpec(processPath);
        errno_t rc = strcpy_s(buf, len, pathName);
        if (rc != EOK) {
            ERROR_UTILS_PORT_UNKNOWN;
        }
        return ERROR_SYS_OK;
    }
    return ERROR_UTILS_PORT_UNKNOWN;
}

#define MAX_PATH 1024

ErrorCode GetCurrentProcessName(char *processName, Size len)
{
    char *fileName;
    char processPath[MAX_PATH];
    HANDLE processHandle = GetCurrentProcess();
    if (GetModuleFileNameEx(processHandle, NULL, processPath, MAX_PATH) > 0) {
        char *processPathStr = processPath;
        fileName = PathFindFileName(processPathStr);
        errno_t rc = strcpy_s(processName, len, fileName);
        if (rc != EOK) {
            return ERROR_UTILS_PORT_UNKNOWN;
        }
        return ERROR_SYS_OK;
    }
    return ERROR_UTILS_PORT_UNKNOWN;
}

#define DIR_TYPE_DIRECTORY    0
#define DIR_TYPE_REGULAR_FILE 1
#define DIR_TYPE_UNKNOWN      2
/* The dirent wrapper. */
typedef struct DirectoryEntry DirectoryEntry;
struct DirectoryEntry {
    char name[MAX_PATH];
    int type;
};

/* The DIR wrapper. */
typedef struct Directory Directory;
struct Directory {
    HANDLE hFindFile;
    DirectoryEntry temp;
    bool fetched;
};

/* Open one Directory. */
ErrorCode OpenDirectory(const char *name, Directory *dirPointer)
{
    ErrorCode errCode = ERROR_SYS_OK;
    WIN32_FIND_DATA FindFileData;
    dirPointer->hFindFile = FindFirstFile(name, &FindFileData);
    if (dirPointer->hFindFile == INVALID_HANDLE_VALUE) {
        errCode = ERROR_UTILS_PORT_UNKNOWN;
        return errCode;
    }
    errno_t rc = strcpy_s(dirPointer->temp.name, MAX_PATH, FindFileData.cFileName);
    if (rc != EOK) {
        errCode = ERROR_UTILS_PORT_UNKNOWN;
        return errCode;
    }
    if (FindFileData.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) {
        dirPointer->temp.type = DIR_TYPE_DIRECTORY;
    } else {
        dirPointer->temp.type = DIR_TYPE_REGULAR_FILE;
    }
    dirPointer->fetched = false;
    return errCode;
}

/* Read the directory. */
bool ReadDirectory(Directory *dirPointer, DirectoryEntry *dirEntry)
{
    if (!dirPointer->fetched) {
        errno_t rc = strcpy_s(dirEntry->name, MAX_PATH, dirPointer->temp.name);
        if (rc != EOK) {
            return false;
        }
        dirEntry->type = dirPointer->temp.type;
        dirPointer->fetched = true;
        return true;
    }
    WIN32_FIND_DATA nextFindFileData;
    BOOL rc = FindNextFile(dirPointer->hFindFile, &nextFindFileData);
    if (!rc) {
        return false;
    }
    errno_t rc = strcpy_s(dirEntry->name, MAX_PATH, nextFindFileData.cFileName);
    if (rc != EOK) {
        errCode = ERROR_UTILS_PORT_UNKNOWN;
        return false;
    }
    if (nextFindFileData.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) {
        dirEntry->type = DIR_TYPE_DIRECTORY;
    } else {
        dirEntry->type = DIR_TYPE_REGULAR_FILE;
    }
    return true;
}

/* Close the directory. */
void CloseDirectory(Directory *dirPointer)
{
    BOOL rc = FindClose(dirPointer->hFindFile);
    dirPointer->hFindFile = NULL;
}