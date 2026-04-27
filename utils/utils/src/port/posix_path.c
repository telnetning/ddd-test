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
 * posix_path.c
 *
 * Description:
 * 1. Implementation of the file path operation API on the Linux platform.
 *
 * ---------------------------------------------------------------------------------
 */
#include <errno.h>
#include <libgen.h>
#include "port/posix_path.h"
/**
 * canonicalize path
 *
 * @param path in path
 * @param resolvedPath out path
 * @return if success return true, failed return false
 */
UTILS_EXPORT bool CanonicalizePath(const char *path, char *resolvedPath)
{
    if (path == NULL || resolvedPath == NULL) {
        return false;
    }
    char *res = realpath(path, resolvedPath);
    if (res == NULL) {
        return false;
    }
    return true;
}
/**
 * Obtain the file name in the path.This function does not modify the input path content.
 * @param path : File name that includes the path.
 * @return : File name.
 */
UTILS_EXPORT char *Basename(char *path)
{
    return basename(path);
}

/**
 * Delete the file name and backslash (\) following the path.This function modifies the input path content.
 * @param path : File name that includes the path.
 * @return : File path.
 */
UTILS_EXPORT char *Dirname(char *path)
{
    return dirname(path);
}

/**
 * Create directory.
 * @param directoryName
 * @param mode
 * @return
 */
UTILS_EXPORT ErrorCode MakeDirectory(const char *directoryName, unsigned int mode)
{
    ErrorCode errCode = ERROR_SYS_OK;
    int rc = mkdir(directoryName, mode);
    if (rc != 0) {
        PosixErrorCode2PortErrorCode(errno, &errCode);
    }
    return errCode;
}

/**
 * Deletes a directory, which must be empty.
 * @param directoryName
 * @return
 */
UTILS_EXPORT ErrorCode DestroyDirectory(const char *directoryName)
{
    ErrorCode errCode = ERROR_SYS_OK;
    int rc = rmdir(directoryName);
    if (rc != 0) {
        PosixErrorCode2PortErrorCode(errno, &errCode);
    }
    return errCode;
}

/**
 * Get current working directory.
 * @param buf
 * @param size
 * @return
 */
UTILS_EXPORT ErrorCode GetCurrentWorkingDirectory(char *buf, size_t size)
{
    ErrorCode errCode = ERROR_SYS_OK;
    char *cwd = getcwd(buf, size);
    if (cwd == NULL) {
        PosixErrorCode2PortErrorCode(errno, &errCode);
    }
    return errCode;
}

/**
 * Get the current process name.
 * @param processName
 * @param len
 * @return
 */
UTILS_EXPORT ErrorCode GetCurrentProcessName(char *processName, Size len)
{
    char *pathEnd;
    char processPath[MAX_PATH];
    errno_t rc;
    rc = memset_s(processPath, MAX_PATH, 0, MAX_PATH);
    if (rc != EOK) {
        return ERROR_UTILS_PORT_UNKNOWN;
    }
    if (readlink("/proc/self/exe", processPath, MAX_PATH) <= 0) {
        return ERROR_UTILS_PORT_UNKNOWN;
    }
    processPath[MAX_PATH - 1] = '\0'; /* Ensure string has terminal byte */
    pathEnd = strrchr(processPath, '/');
    if (pathEnd == NULL) {
        return ERROR_UTILS_PORT_UNKNOWN;
    }
    ++pathEnd;
    rc = strcpy_s(processName, len, pathEnd);
    if (rc != EOK) {
        return ERROR_UTILS_PORT_UNKNOWN;
    }
    return ERROR_SYS_OK;
}

/* Open one Directory. */
UTILS_EXPORT ErrorCode OpenDirectory(const char *name, Directory *dirPointer)
{
    ErrorCode errCode = ERROR_SYS_OK;
    if (name == NULL) {
        return ERROR_UTILS_PORT_EINVAL;
    }
    dirPointer->dirPointer = opendir(name);
    if (dirPointer->dirPointer == NULL) {
        PosixErrorCode2PortErrorCode(errno, &errCode);
    }
    return errCode;
}

/* Read the directory. */
UTILS_EXPORT bool ReadDirectory(Directory *dirPointer, DirectoryEntry *dirEntry)
{
    struct dirent *dirEnt = readdir(dirPointer->dirPointer);
    if (dirEnt != NULL) {
        errno_t rc = strcpy_s(dirEntry->name, MAX_PATH, dirEnt->d_name);
        if (rc != EOK) {
            return false;
        }
        if (dirEnt->d_type == DT_DIR) {
            dirEntry->type = DIR_TYPE_DIRECTORY;
        } else if (dirEnt->d_type == DT_REG) {
            dirEntry->type = DIR_TYPE_REGULAR_FILE;
        } else {
            dirEntry->type = DIR_TYPE_UNKNOWN;
        }
        return true;
    } else {
        return false;
    }
}

/* Close the directory. */
UTILS_EXPORT void CloseDirectory(Directory *dirPointer)
{
    (void)closedir(dirPointer->dirPointer);
    dirPointer->dirPointer = NULL;
}