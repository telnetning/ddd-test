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
 * posix_path.h
 *
 * Description:Defines the file path operation API for the Linux platform.
 *
 * ---------------------------------------------------------------------------------
 */
#ifndef UTILS_POSIX_PATH_H
#define UTILS_POSIX_PATH_H

#include <stdlib.h>
#include <sys/types.h>
#include <dirent.h>
#include <sys/stat.h>
#include <stdbool.h>
#include <unistd.h>
#include "securec.h"
#include "defines/err_code.h"
#include "defines/common.h"
#include "types/data_types.h"
#include "port/posix_errcode.h"

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

/**
 * Get current working directory.
 * @param buf
 * @param size
 * @return
 */
ErrorCode GetCurrentWorkingDirectory(char *buf, size_t size);

/**
 * Get the current process name.
 * @param processName
 * @param len
 * @return
 */
ErrorCode GetCurrentProcessName(char *processName, Size len);
#define DIR_TYPE_DIRECTORY    0
#define DIR_TYPE_REGULAR_FILE 1
#define DIR_TYPE_UNKNOWN      2
/* The DIR wrapper. */
typedef struct Directory Directory;
struct Directory {
    DIR *dirPointer;
};

/*
 * The dirent wrapper.
 * MAX_PATH is a macro constant defined by the #define instruction in the C language runtime library.
 * It defines the length of the longest full path name supported by the compiler.
 * Library: stdlib.h
 * In Linux, the value is PATH_MAX. Generally, the value cannot exceed 4096.
 */
#ifdef PATH_MAX
#define MAX_PATH PATH_MAX
#else
#define MAX_PATH 1024
#endif

typedef struct DirectoryEntry DirectoryEntry;
struct DirectoryEntry {
    char name[MAX_PATH];
    int type;
};

/* Open one Directory. */
ErrorCode OpenDirectory(const char *name, Directory *dirPointer);
/* Read the directory. */
bool ReadDirectory(Directory *dirPointer, DirectoryEntry *dirEntry);
/* Close the directory. */
void CloseDirectory(Directory *dirPointer);

GSDB_END_C_CODE_DECLS
#endif /* UTILS_POSIX_PATH_H */
