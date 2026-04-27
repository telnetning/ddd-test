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
 * win32_path.h
 *
 * Description:Defines the file path operation API for the Windows platform.
 *
 * ---------------------------------------------------------------------------------
 */
#ifndef UTILS_WIN32_PATH_H
#define UTILS_WIN32_PATH_H

#include <stdlib.h>
#include <shlwapi.h>

GSDB_BEGIN_C_CODE_DECLS

/**
 * canonicalize path
 *
 * @param path in path
 * @param resolvedPath out path
 * @return if success return true, failed return false
 */
bool CanonicalizePath(const char *path, char *resolvedPath);

/**
 * Obtain the file name in the path.This function does not modify the input path content.
 * @param path : File name that includes the path.
 * @return : File name.
 */
char *Basename(char *path);

/**
 * Delete the file name and backslash (\) following the path.This function modifies the input path content.
 * @param path : File name that includes the path.
 * @return : File path.
 */
char *Dirname(char *path);

/**
 * Get the current process name.
 * @param processName
 * @param len
 * @return
 */
ErrorCode GetCurrentProcessName(char *processName, Size len);

/**
 * Get current working directory.
 * @param buf
 * @param size
 * @return
 */
ErrorCode GetCurrentWorkingDirectory(char *buf, size_t size);

/* These macros are not provided by older MinGW, nor by MSVC */
#ifndef S_IRUSR
#define S_IRUSR _S_IREAD
#endif
#ifndef S_IWUSR
#define S_IWUSR _S_IWRITE
#endif
#ifndef S_IXUSR
#define S_IXUSR _S_IEXEC
#endif
#ifndef S_IRWXU
#define S_IRWXU (S_IRUSR | S_IWUSR | S_IXUSR)
#endif

/* Default mode for creating directories */
#define DIRECTORY_MODE_OWNER S_IRWXU

/**
 * Create directory.
 * @param directoryName
 * @param mode
 * @return
 */
ErrorCode MakeDirectory(const char *directoryName, unsigned int mode);

/**
 * Deletes a directory, which must be empty.
 * @param directoryName
 * @return
 */
ErrorCode DestroyDirectory(const char *directoryName);

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
ErrorCode OpenDirectory(const char *name, Directory *dirPointer);

/* Read the directory. */
bool ReadDirectory(Directory *dirPointer, DirectoryEntry *dirEntry);

/* Close the directory. */
void CloseDirectory(Directory *dirPointer);

GSDB_END_C_CODE_DECLS
#endif /* UTILS_WIN32_PATH_H */
