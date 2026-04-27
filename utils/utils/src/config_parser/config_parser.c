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
 * Description: Config parser used for json file, could load/store json file and CRUD with it.
 *
 * REQUIRED:    CJSON version at least <1.7.14>
 * ---------------------------------------------------------------------------------
 */
#include "config_parser/config_parser.h"

#include <math.h>
#include <float.h>
#include <cjson/cJSON.h>

#include "syslog/err_log.h"

#ifdef LOCAL_MODULE_NAME
#undef LOCAL_MODULE_NAME
#endif
#define LOCAL_MODULE_NAME "config_parser"

struct ConfigParserHandle {
    cJSON cJson;
    bool isUpdated;
};

static ErrorCode FindDiff(cJSON *latest, cJSON *root, char path[CONFIGPARSER_PATH_MAXIMUM],
                          char *array[CONFIGPARSER_ARRAY_MAXIMUM], uint32_t *size);

/*
 * Use to add extra field in cJSON struct.
 * *ONLY FOR CJSON, NOT FOR PARSER*
 */
static void *MyAllocate(size_t size)
{
    size_t extraSpace = 4; // define extra field space
    void *ptr = calloc(1, size + extraSpace);
    if (ptr == NULL) {
        return NULL;
    }
    return ptr;
}

/*
 * Initial malloc for cJSON to add field of struct.
 */
UTILS_INLINE static void ConfigParserInit(void)
{
    cJSON_Hooks tmpHook = {MyAllocate, free};
    cJSON_InitHooks(&tmpHook);
}

UTILS_INLINE static cJSON *GetCJson(ConfigParserHandle *handle)
{
    if (handle == NULL) {
        return NULL;
    }
    return &(handle->cJson);
}

UTILS_INLINE static ConfigParserHandle *GetHandle(void *root)
{
    return (ConfigParserHandle *)root;
}

/* securely comparison of floating-point variables */
UTILS_INLINE static bool CompareDouble(double a, double b)
{
    double maxVal = fabs(a) > fabs(b) ? fabs(a) : fabs(b);
    return (fabs(a - b) <= maxVal * DBL_EPSILON);
}

static ErrorCode ReadFromFile(VirtualFileSystem *vfs, const char *filePath, char **str)
{
    ASSERT(filePath != NULL);
    ASSERT(str != NULL);
    FileDescriptor *fd = NULL;
    ErrorCode errorCode = Open(vfs, filePath, FILE_READ_AND_WRITE_FLAG, &fd);
    if (errorCode != ERROR_SYS_OK) {
        ErrLog(ERROR, ErrMsg("Failed to open the file: %s, error code:0x%lx.0x%lx.0x%lx", filePath,
                             ERROR_GET_COMPONENT(errorCode), ERROR_GET_MODULE(errorCode), ERROR_GET_CODE(errorCode)));
        return errorCode;
    }
    int64_t fileSize = 0;
    errorCode = GetSize(fd, &fileSize);
    if (errorCode != ERROR_SYS_OK) {
        ErrLog(ERROR, ErrMsg("Failed to get size of the file: %s.", filePath));
        (void)Close(fd);
        return errorCode;
    }
    char *buffer = (char *)malloc((uint64_t)fileSize + 1);
    if (buffer == NULL) {
        ErrLog(ERROR, ErrMsg("Can't malloc for %ld memory space in loading file: %s.", fileSize, filePath));
        (void)Close(fd);
        return CONFIG_PARSER_ALLOCATOR_FAILED;
    }
    int64_t readSize = 0;
    errorCode = Read(fd, buffer, (uint64_t)fileSize, &readSize);
    (void)Close(fd);
    if (errorCode != ERROR_SYS_OK) {
        ErrLog(ERROR, ErrMsg("Failed to read from the file: %s.", filePath));
        free(buffer);
        return errorCode;
    }
    buffer[readSize] = '\0';
    *str = buffer;
    return ERROR_SYS_OK;
}

UTILS_EXPORT ErrorCode ConfigParserLoadFile(const char *configFilePath, ConfigParserHandle **handle)
{
    if (handle == NULL) {
        ErrLog(WARNING, ErrMsg("Given handle for load file is NULL."));
        return CONFIG_PARSER_PARAMETER_INCORRECT;
    }
    if (configFilePath == NULL) {
        ErrLog(WARNING, ErrMsg("Given file path for load file is NULL."));
        return CONFIG_PARSER_PARAMETER_INCORRECT;
    }
    if (strlen(configFilePath) >= CONFIGPARSER_FILE_MAXIMUM) {
        ErrLog(WARNING, ErrMsg("Given file path [%s] over the limit %d.", configFilePath, CONFIGPARSER_FILE_MAXIMUM));
        return CONFIG_PARSER_PARAMETER_INCORRECT;
    }
    ConfigParserInit();
    bool needExit = false;
    if (InitVfsModule(NULL) == ERROR_SYS_OK) {
        needExit = true;
    }
    VirtualFileSystem *vfs;
    ErrorCode errorCode = GetStaticLocalVfsInstance(&vfs);
    if (errorCode != ERROR_SYS_OK) {
        ErrLog(ERROR, ErrMsg("Failed to get static VFS, out of memory error when load file: %s.", configFilePath));
        if (needExit) {
            (void)ExitVfsModule();
        }
        return errorCode;
    }
    char *jsonStr;
    errorCode = ReadFromFile(vfs, configFilePath, &jsonStr);
    if (needExit) {
        (void)ExitVfsModule();
    }
    if (errorCode != ERROR_SYS_OK) {
        return errorCode;
    }
    cJSON *jsonRoot = cJSON_Parse(jsonStr);
    free(jsonStr);
    if (jsonRoot == NULL) {
        ErrLog(ERROR, ErrMsg("Can't load the JSON from file: %s, check the file format if JSON.", configFilePath));
        return CONFIG_PARSER_HANDLE_IS_NULL;
    }
    *handle = GetHandle(jsonRoot);
    return ERROR_SYS_OK;
}

UTILS_EXPORT ErrorCode ConfigParserLoadString(const char *jsonStr, ConfigParserHandle **handle)
{
    if (handle == NULL) {
        ErrLog(WARNING, ErrMsg("Given handle for load string is NULL."));
        return CONFIG_PARSER_PARAMETER_INCORRECT;
    }
    if (jsonStr == NULL) {
        ErrLog(WARNING, ErrMsg("Given json string is NULL."));
        return CONFIG_PARSER_PARAMETER_INCORRECT;
    }
    ConfigParserInit();
    cJSON *jsonRoot = cJSON_Parse(jsonStr);
    if (jsonRoot == NULL) {
        ErrLog(ERROR, ErrMsg("parse json string failed, check the format if JSON."));
        return CONFIG_PARSER_HANDLE_IS_NULL;
    }
    *handle = GetHandle(jsonRoot);
    return ERROR_SYS_OK;
}

/*
 * DFS recurse iterate the root to find modified item and copy it to latest, and mark it as not modified.
 */
static ErrorCode IncreaseCopy(cJSON *latest, cJSON *root, char *path, uint32_t size)
{
    if (root == NULL) {
        return ERROR_SYS_OK;
    }
    if (cJSON_IsArray(root) || cJSON_IsObject(root)) {
        cJSON *curr = NULL;
        if (strcat_s(path, CONFIGPARSER_PATH_MAXIMUM, CONFIGPARSER_DELIM) != EOK) {
            ErrLog(WARNING, ErrMsg("Using strcat_s() with [%s] failed, check if over limit %d, current path: [%s].",
                                   CONFIGPARSER_DELIM, CONFIGPARSER_PATH_MAXIMUM, path));
            return CONFIG_PARSER_ALLOCATOR_FAILED;
        }
        if (cJSON_IsObject(root)) {
            cJSON_ArrayForEach(curr, root)
            { // backtracking
                if (strcat_s(path, CONFIGPARSER_PATH_MAXIMUM, curr->string) != EOK) {
                    ErrLog(WARNING, ErrMsg("Using strcat_s() with [%s] failed, check if over limit %d, "
                                           "current path: [%s].",
                                           curr->string, CONFIGPARSER_PATH_MAXIMUM, path));
                    return CONFIG_PARSER_ALLOCATOR_FAILED;
                }
                ErrorCode ec = IncreaseCopy(latest, curr, path, size);
                if (ec != ERROR_SYS_OK) {
                    return ec;
                }
                path[strlen(path) - strlen(curr->string)] = '\0';
            }
        } else if (cJSON_IsArray(root)) {
            int i = 0;
            char str[CONFIGPARSER_NUM_STR_SIZE] = {};
            cJSON_ArrayForEach(curr, root)
            { // backtracking
                if (snprintf_s(str, sizeof(str), sizeof(str) - 1, "%d", i) == -1) {
                    ErrLog(WARNING, ErrMsg("Using snprintf_s() convert number to string failed, "
                                           "check if size of array over limit %d.",
                                           CONFIGPARSER_NUM_STR_SIZE));
                    return CONFIG_PARSER_ALLOCATOR_FAILED;
                }
                if (strcat_s(path, CONFIGPARSER_PATH_MAXIMUM, str) != EOK) {
                    ErrLog(WARNING, ErrMsg("Using strcat_s() with [%s] failed, "
                                           "check if over limit %d, current path: [%s].",
                                           str, CONFIGPARSER_PATH_MAXIMUM, path));
                    return CONFIG_PARSER_ALLOCATOR_FAILED;
                }
                ErrorCode ec = IncreaseCopy(latest, curr, path, size);
                if (ec != ERROR_SYS_OK) {
                    return ec;
                }
                path[strlen(path) - strlen(str)] = '\0';
                i++;
            }
        }
        path[strlen(path) - 1] = '\0';
    } else {
        if (GetHandle(root)->isUpdated) {
            cJSON *newJson = cJSON_Duplicate(root, true);
            if (newJson == NULL) {
                ErrLog(ERROR, ErrMsg("Duplicate handle failed."));
                return CONFIG_PARSER_ALLOCATOR_FAILED;
            }
            if (ConfigParserSetHandle(GetHandle(latest), path, GetHandle(newJson)) != ERROR_SYS_OK) {
                cJSON_Delete(newJson);
                ErrLog(ERROR, ErrMsg("Set duplicated handle failed."));
                return CONFIG_PARSER_ALLOCATOR_FAILED;
            }
        }
    }
    GetHandle(root)->isUpdated = false;
    return ERROR_SYS_OK;
}

/*
 * Modify the file path, add extra extension to make extra file.
 * Case 1: The length of file path is enough in range to add extension, just append <.lock>.
 * Case 2: Not enough to append, if the rest and file name is long enough, replace last some chars to extension.
 * Case 3: Not enough to append, and the file name is short, replace last char.
 */
static void GetValidExtraFilePath(const char *configFilePath, char *extraFileName, uint32_t maxSize,
                                  const char *extraExt)
{
    size_t rest = ((maxSize - strlen(configFilePath)) - 1) + strlen(basename(configFilePath));
    if (strlen(configFilePath) + strlen(extraExt) < maxSize) { // Case 1
        size_t index = strlen(configFilePath);
        for (size_t i = 0; i <= strlen(extraExt); i++) { // include '\0'
            extraFileName[index++] = extraExt[i];
        }
    } else if (rest > strlen(extraExt)) { // Case 2
        size_t index = (maxSize - strlen(extraExt)) - 1;
        for (size_t i = 0; i <= strlen(extraExt); i++) { // include '\0'
            extraFileName[index++] = extraExt[i];
        }
    } else { // Case 3
        if (strcmp(extraExt, ".lock") == 0) {
            if (configFilePath[strlen(configFilePath) - 1] != 'l') { // change last char to make lock file
                extraFileName[strlen(extraFileName) - 1] = 'l';
            } else {
                extraFileName[strlen(extraFileName) - 1] = 'L';
            }
        } else {
            if (configFilePath[strlen(configFilePath) - 1] != 't') { // change last char to make temp file
                extraFileName[strlen(extraFileName) - 1] = 't';
            } else {
                extraFileName[strlen(extraFileName) - 1] = 'T';
            }
        }
    }
}

/*
 * Create lock file as a file lock to avoid process-competition, make sure the store function work.
 */
static ErrorCode InterProcessLock(VirtualFileSystem *vfs, const char *configFilePath, char *lockFileName,
                                  uint32_t fileNameMaxSize, FileDescriptor **lock)
{
    ASSERT(lock != NULL);
    GetValidExtraFilePath(configFilePath, lockFileName, fileNameMaxSize, ".lock");
    FileDescriptor *fd;
    FileParameter filePara = {.storeSpaceName = "storeSpaceName1",
                              .streamId = VFS_DEFAULT_FILE_STREAM_ID,
                              .flag = IN_PLACE_WRITE_FILE,
                              .fileSubType = DATA_FILE_TYPE,
                              .rangeSize = DEFAULT_RANGE_SIZE,
                              .maxSize = 1,
                              .recycleTtl = 0,
                              .mode = FILE_READ_AND_WRITE_MODE,
                              .isReplayWrite = false};
    ErrorCode errorCode = Create(vfs, lockFileName, filePara, &fd);
    if (errorCode != ERROR_SYS_OK && errorCode != VFS_ERROR_CREATE_FILE_EXIST) {
        ErrLog(ERROR, ErrMsg("Failed to create the temp file for [%s].", configFilePath));
        return errorCode;
    } else if (errorCode == VFS_ERROR_CREATE_FILE_EXIST) { // if exist then just open it
        errorCode = Open(vfs, lockFileName, FILE_READ_AND_WRITE_FLAG, &fd);
        if (errorCode != ERROR_SYS_OK) {
            ErrLog(ERROR, ErrMsg("Failed to open the temp file for [%s].", configFilePath));
            return errorCode;
        }
    }
    errorCode = LockFile(fd, 0, 1, FILE_EXCLUSIVE_LOCK, 0);
    if (errorCode != ERROR_SYS_OK) {
        (void)Close(fd);
        ErrLog(ERROR, ErrMsg("Failed to lock the lock file for [%s].", configFilePath));
        return errorCode;
    }
    *lock = fd;
    return ERROR_SYS_OK;
}

static void InterProcessUnlock(FileDescriptor *lock)
{
    ASSERT(lock != NULL);
    (void)UnlockFile(lock, 0, 1);
    (void)Close(lock);
}

/*
 * Write the string into file
 * Implementation method Use tmp file and rename.
 */
static ErrorCode WriteToFile(VirtualFileSystem *vfs, const char *configFilePath, char *tmpFileName,
                             uint32_t fileNameMaxSize, const char *str)
{
    ASSERT(str != NULL);
    // same as create lock file, since share the space, should use same length extension as lock file
    GetValidExtraFilePath(configFilePath, tmpFileName, fileNameMaxSize, ".temp");
    FileDescriptor *fd;
    FileParameter filePara = {.storeSpaceName = "storeSpaceName1",
                              .streamId = VFS_DEFAULT_FILE_STREAM_ID,
                              .flag = IN_PLACE_WRITE_FILE,
                              .fileSubType = DATA_FILE_TYPE,
                              .rangeSize = DEFAULT_RANGE_SIZE,
                              .maxSize = 0xFFFFFFFFU,
                              .recycleTtl = 0,
                              .mode = FILE_READ_AND_WRITE_MODE,
                              .isReplayWrite = false};
    ErrorCode errorCode = Create(vfs, tmpFileName, filePara, &fd);
    if (errorCode != ERROR_SYS_OK && errorCode != VFS_ERROR_CREATE_FILE_EXIST) {
        ErrLog(ERROR, ErrMsg("Failed to create the temp file for [%s].", configFilePath));
        return errorCode;
    } else if (errorCode == VFS_ERROR_CREATE_FILE_EXIST) { // if exist then just open it
        errorCode = Open(vfs, tmpFileName, FILE_READ_AND_WRITE_FLAG, &fd);
        if (errorCode != ERROR_SYS_OK) {
            ErrLog(ERROR, ErrMsg("Failed to open the temp file for [%s].", configFilePath));
            return errorCode;
        }
    }
    int64_t writeSize = 0;
    errorCode = WriteSync(fd, str, strlen(str), &writeSize);
    (void)Close(fd);
    if (errorCode != ERROR_SYS_OK) {
        ErrLog(ERROR, ErrMsg("Failed to write to the temp file for [%s].", configFilePath));
        return errorCode;
    }
    if (rename(tmpFileName, configFilePath) == -1) {
        ErrLog(ERROR, ErrMsg("Failed to rename file from [%s] to [%s].", tmpFileName, configFilePath));
        return CONFIG_PARSER_FILE_IO_ERROR;
    }
    return ERROR_SYS_OK;
}

/**
 * Persistent storage root to file, load the latest version config file and write the update with it into file.
 * If the given file is a json, it would load it and update cover it with root.
 * If file is not json file, it would just write into file.
 */
UTILS_EXPORT ErrorCode ConfigParserStoreFile(ConfigParserHandle *handle, const char *configFilePath)
{
    if (handle == NULL) {
        ErrLog(WARNING, ErrMsg("The handle for store file is NULL."));
        return CONFIG_PARSER_HANDLE_IS_NULL;
    }
    if (configFilePath == NULL) {
        ErrLog(WARNING, ErrMsg("The filepath for store file is NULL."));
        return CONFIG_PARSER_PARAMETER_INCORRECT;
    }
    if (configFilePath[0] == '\0') {
        ErrLog(WARNING, ErrMsg("The file path is empty."));
        return CONFIG_PARSER_PARAMETER_INCORRECT;
    }
    char filePath[CONFIGPARSER_FILE_MAXIMUM] = {};
    if (strcpy_s(filePath, sizeof(filePath), configFilePath) != EOK) {
        ErrLog(WARNING, ErrMsg("Using strcpy_s() for [%s] failed, check limit %lu.", configFilePath, sizeof(filePath)));
        return CONFIG_PARSER_ALLOCATOR_FAILED;
    }
    bool needExit = false;
    if (InitVfsModule(NULL) == ERROR_SYS_OK) {
        needExit = true;
    }
    VirtualFileSystem *vfs;
    ErrorCode errorCode = GetStaticLocalVfsInstance(&vfs);
    if (errorCode != ERROR_SYS_OK) {
        ErrLog(ERROR, ErrMsg("Failed to get static VFS, out of memory error when store file: %s.", configFilePath));
        if (needExit) {
            (void)ExitVfsModule();
        }
        return errorCode;
    }
    FileDescriptor *lock = NULL;
    ErrorCode errcode = InterProcessLock(vfs, configFilePath, filePath, sizeof(filePath), &lock);
    if (errcode != ERROR_SYS_OK) {
        return errcode;
    }
    ConfigParserHandle *latest = NULL;
    (void)ConfigParserLoadFile(configFilePath, &latest);
    char *str = NULL;
    if (latest != NULL) {
        char path[CONFIGPARSER_PATH_MAXIMUM] = {};
        ErrorCode ec = IncreaseCopy(GetCJson(latest), GetCJson(handle), path, sizeof(path));
        if (ec != ERROR_SYS_OK) { // update latest with modified items
            (void)ConfigParserDelete(latest, "");
            InterProcessUnlock(lock);
            return ec;
        }
        str = cJSON_Print(GetCJson(latest));
        (void)ConfigParserDelete(latest, "");
    } else {
        str = cJSON_Print(GetCJson(handle));
    }
    if (str == NULL) {
        ErrLog(WARNING, ErrMsg("fail to render the cJSON entity in the last to text"));
        errcode = CONFIG_PARSER_WRONG_DATA_TYPE;
    } else {
        errcode = WriteToFile(vfs, configFilePath, filePath, sizeof(filePath), str);
        free(str);
    }
    InterProcessUnlock(lock);
    if (needExit) {
        (void)ExitVfsModule();
    }
    return errcode;
}

/*
 * Add found path into array.
 * !WARNING! The elements in array need to be free by caller.
 */
static ErrorCode AddPath(char path[CONFIGPARSER_PATH_MAXIMUM], char *array[CONFIGPARSER_ARRAY_MAXIMUM], uint32_t *size)
{
    if (*size >= CONFIGPARSER_ARRAY_MAXIMUM) {
        ErrLog(WARNING, ErrMsg("The updated items are over limit %d, ignore current path: [%s].",
                               CONFIGPARSER_ARRAY_MAXIMUM, path));
        return ERROR_SYS_OK;
    }
    char *res = malloc(strlen(path) + 1); // +1 for '0'
    if (res == NULL) {
        ErrLog(ERROR, ErrMsg("Can't add apply for memory, [%s].", path));
        return CONFIG_PARSER_ALLOCATOR_FAILED;
    }
    errno_t rc = strcpy_s(res, strlen(path) + 1, path);
    if (rc != EOK) {
        free(res);
        ErrLog(ERROR, ErrMsg("Can't copy the path, [%s].", path));
        return CONFIG_PARSER_ALLOCATOR_FAILED;
    }
    array[(*size)++] = res;
    return ERROR_SYS_OK;
}

/*
 * Assume the json format is complex, DFS recurse iterate the handle and latest to find different item and record it.
 */
static ErrorCode CheckComplexTypeAndUpdate(cJSON *latest, cJSON *root, char path[CONFIGPARSER_PATH_MAXIMUM],
                                           char *array[CONFIGPARSER_ARRAY_MAXIMUM], uint32_t *size)
{
    if (cJSON_GetArraySize(latest) > cJSON_GetArraySize(root)) {
        ErrorCode ec = AddPath(path, array, size);
        if (ec != ERROR_SYS_OK) { // latest add new item, just record the array or object
            return ec;
        }
    } else {
        cJSON *curr1 = latest->child;
        cJSON *curr2 = NULL;
        int i = 0;
        if (strcat_s(path, CONFIGPARSER_PATH_MAXIMUM, CONFIGPARSER_DELIM) != EOK) {
            ErrLog(WARNING, ErrMsg("Using strcat_s() with [%s] failed, check if over limit %d, current path: [%s].",
                                   CONFIGPARSER_DELIM, CONFIGPARSER_PATH_MAXIMUM, path));
            return CONFIG_PARSER_ALLOCATOR_FAILED;
        }
        cJSON_ArrayForEach(curr2, root)
        {                               // backtracking
            if (cJSON_IsObject(root)) { // for object
                if (strcat_s(path, CONFIGPARSER_PATH_MAXIMUM, curr2->string) != EOK) {
                    ErrLog(WARNING, ErrMsg("Using strcat_s() with [%s] failed, check if over limit %d, "
                                           "current path: [%s].",
                                           curr2->string, CONFIGPARSER_PATH_MAXIMUM, path));
                    return CONFIG_PARSER_ALLOCATOR_FAILED;
                }
                ErrorCode ec = FindDiff(curr1, curr2, path, array, size);
                if (ec != ERROR_SYS_OK) {
                    return ec;
                }
                path[strlen(path) - strlen(curr2->string)] = '\0';
            } else if (cJSON_IsArray(latest) && cJSON_IsArray(root)) { // for array
                char str[CONFIGPARSER_NUM_STR_SIZE] = {};
                if (snprintf_s(str, sizeof(str), sizeof(str) - 1, "%d", i) == -1) {
                    ErrLog(WARNING, ErrMsg("Using snprintf_s() convert number to string failed, "
                                           "check if size of array over limit %d.",
                                           CONFIGPARSER_NUM_STR_SIZE));
                    return CONFIG_PARSER_ALLOCATOR_FAILED;
                }
                if (strcat_s(path, CONFIGPARSER_PATH_MAXIMUM, str) != EOK) {
                    ErrLog(WARNING, ErrMsg("Using strcat_s() with [%s] failed, check if over limit %d, "
                                           "current path: [%s].",
                                           str, CONFIGPARSER_PATH_MAXIMUM, path));
                    return CONFIG_PARSER_ALLOCATOR_FAILED;
                }
                ErrorCode ec = FindDiff(curr1, curr2, path, array, size);
                if (ec != ERROR_SYS_OK) {
                    return ec;
                }
                path[strlen(path) - strlen(str)] = '\0';
            }
            if (curr1 != NULL) {
                curr1 = curr1->next;
            }
            i++;
        }
        path[strlen(path) - 1] = '\0';
    }
    return ERROR_SYS_OK;
}

/*
 * Assume the json format is same, check the content if same or updated.
 */
static ErrorCode CheckSimpleTypeAndUpdate(cJSON *latest, cJSON *root, char path[CONFIGPARSER_PATH_MAXIMUM],
                                          char *array[CONFIGPARSER_ARRAY_MAXIMUM], uint32_t *size)
{
    ErrorCode ec = ERROR_SYS_OK;
    if (cJSON_IsString(root) && strcmp(cJSON_GetStringValue(latest), cJSON_GetStringValue(root)) != 0) {
        if (!GetHandle(root)->isUpdated) {
            ec = AddPath(path, array, size);
        } else {
            ErrLog(WARNING, ErrMsg("Modified same place, [%s] latest: %s, current: %s.", path,
                                   cJSON_GetStringValue(latest), cJSON_GetStringValue(root)));
        }
    } else if (cJSON_IsNumber(root) && !CompareDouble(cJSON_GetNumberValue(latest), cJSON_GetNumberValue(root))) {
        if (!GetHandle(root)->isUpdated) {
            ec = AddPath(path, array, size);
        } else {
            ErrLog(WARNING, ErrMsg("Modified same place, [%s] latest: %f, current: %f.", path,
                                   cJSON_GetNumberValue(latest), cJSON_GetNumberValue(root)));
        }
    } else if (cJSON_IsBool(root) &&
               !((cJSON_IsTrue(latest) && cJSON_IsTrue(root)) || (cJSON_IsFalse(latest) && cJSON_IsFalse(root)))) {
        if (!GetHandle(root)->isUpdated) {
            ec = AddPath(path, array, size);
        } else {
            ErrLog(WARNING, ErrMsg("Modified same place, [%s].", path));
        }
    }
    return ec;
}

/*
 * Figure out the difference between latest version and current given version to record the updated.
 */
static ErrorCode FindDiff(cJSON *latest, cJSON *root, char path[CONFIGPARSER_PATH_MAXIMUM],
                          char *array[CONFIGPARSER_ARRAY_MAXIMUM], uint32_t *size)
{
    if (latest == NULL || root == NULL) { // check not null
        ErrLog(WARNING, ErrMsg("Latest or root is Null, [%s] can't compare to find different.", path));
        return ERROR_SYS_OK;
    }
    if (latest->type != root->type) { // make sure same type
        if (!GetHandle(root)->isUpdated) {
            ErrorCode ec = AddPath(path, array, size);
            if (ec != ERROR_SYS_OK) { // latest add new item, just record the array or object
                return ec;
            }
        } else {
            ErrLog(WARNING, ErrMsg("Modified same place, [%s], type is not same.", path));
        }
        return ERROR_SYS_OK;
    }
    if (latest->string != NULL && root->string != NULL && strcmp(latest->string, root->string) != 0) {
        // check the field should have same key
        if (!GetHandle(root)->isUpdated) {
            ErrorCode ec = AddPath(path, array, size);
            if (ec != ERROR_SYS_OK) { // latest add new item, just record the array or object
                return ec;
            }
        } else {
            ErrLog(WARNING, ErrMsg("Modified same place, [%s] key is not same.", path));
        }
        return ERROR_SYS_OK;
    }
    if (cJSON_IsArray(root) || cJSON_IsObject(root)) { // check complicated type
        // base on root to iterate, if not same length means modify same item
        return CheckComplexTypeAndUpdate(latest, root, path, array, size);
    } else { // check simple data type
        return CheckSimpleTypeAndUpdate(latest, root, path, array, size);
    }
}

/**
 * Reload the config file, figure out modified item and announce it, then update it with current change.
 * After others store file, this function should be called.
 */
UTILS_EXPORT ErrorCode ConfigParserReloadFile(char const *configFilePath, ConfigParserHandle *handle,
                                              ConfigParserHandle **newHandle,
                                              int (*callback)(char *updatedPaths[], uint32_t size))
{
    if (configFilePath == NULL || handle == NULL || newHandle == NULL) {
        ErrLog(WARNING, ErrMsg("Using NULL parameter filepath, handle or newHandle for Reload function."));
        return CONFIG_PARSER_PARAMETER_INCORRECT;
    }
    ConfigParserHandle *latest = NULL;
    ErrorCode errcode = ConfigParserLoadFile(configFilePath, &latest);
    if (errcode != ERROR_SYS_OK) {
        return errcode; // if file error, just return
    }

    char path[CONFIGPARSER_PATH_MAXIMUM] = {};
    if (callback != NULL) {
        char *updatedPaths[CONFIGPARSER_ARRAY_MAXIMUM] = {};
        uint32_t size = 0;
        ErrorCode ec = FindDiff(GetCJson(latest), GetCJson(handle), path, updatedPaths, &size);
        if (ec != ERROR_SYS_OK) {
            for (uint32_t i = 0; i < size; i++) { // clear paths
                free(updatedPaths[i]);
            }
            (void)ConfigParserDelete(latest, "");
            return ec;
        }
        callback(updatedPaths, size);
        for (uint32_t i = 0; i < size; i++) { // clear paths
            free(updatedPaths[i]);
        }
    }
    ErrorCode ec = IncreaseCopy(GetCJson(latest), GetCJson(handle), path, sizeof(path));
    if (ec != ERROR_SYS_OK) {
        (void)ConfigParserDelete(latest, "");
        return ec;
    }
    *newHandle = latest; // set as handle
    return ERROR_SYS_OK;
}

UTILS_EXPORT ErrorCode ConfigParserPrintJson(ConfigParserHandle *handle, char **string)
{
    if (handle == NULL) {
        ErrLog(WARNING, ErrMsg("ParserPrintJson() handle is NULL."));
        return CONFIG_PARSER_HANDLE_IS_NULL;
    }
    if (string == NULL) {
        ErrLog(WARNING, ErrMsg("ParserPrintJson() string is NULL."));
        return CONFIG_PARSER_PARAMETER_INCORRECT;
    }
    *string = cJSON_Print(GetCJson(handle));
    return ERROR_SYS_OK;
}

UTILS_EXPORT ErrorCode ConfigParserDeepCopy(ConfigParserHandle *oldHandle, ConfigParserHandle **newHandle)
{
    if (oldHandle == NULL || newHandle == NULL) {
        ErrLog(WARNING, ErrMsg("ParserDeepCopy handle is NULL."));
        return CONFIG_PARSER_HANDLE_IS_NULL;
    }
    char *jsonStr = cJSON_Print(GetCJson(oldHandle));
    if (jsonStr == NULL) {
        return CONFIG_PARSER_ALLOCATOR_FAILED;
    }
    cJSON *jsonRoot = cJSON_Parse(jsonStr);
    free(jsonStr);
    if (jsonRoot == NULL) {
        ErrLog(ERROR, ErrMsg("parse json string failed."));
        return CONFIG_PARSER_HANDLE_IS_NULL;
    }
    *newHandle = GetHandle(jsonRoot);
    return ERROR_SYS_OK;
}

/*
 * check if pure number
 */
UTILS_INLINE static int IsDigitStr(char *str)
{
    return (strspn(str, "0123456789") == strlen(str));
}

/*
 * Use for tracking the current path node info.
 */
typedef struct Node {
    cJSON *parent;
    cJSON *current;
    char *lastStr;
    int lastInt;
} Node;

/*
 * Convert string to int32, only for positive number, return -1 if failed.
 */
static int StrToPositiveInt(char *str)
{
    ASSERT(str != NULL);
    if (strlen(str) >= CONFIGPARSER_NUM_STR_SIZE) { // check if in range of int
        ErrLog(WARNING, ErrMsg("Number %s over the limit %d digits.", str, CONFIGPARSER_NUM_STR_SIZE));
        return -1;
    }
    char *endPtr;
    int num = (int)strtol(str, &endPtr, 10); // since require 8 digits in int range, convert to int
    if (*endPtr != '\0') {
        ErrLog(WARNING, ErrMsg("convert string[%s] to num[%d] failed, end with: %s.", str, num, endPtr));
        return -1;
    }
    return num;
}

/*
 * find the target item by given path.
 */
static bool FindTarget(Node *node, char *token)
{
    if (IsDigitStr(token)) { // need an array
        if (!cJSON_IsArray(node->current)) {
            ErrLog(WARNING, ErrMsg("Find path failed, current path is not an array."));
            return false;
        }
        int num = StrToPositiveInt(token);
        if (num == -1) {
            return false;
        }
        node->parent = node->current;
        node->lastInt = num;
        node->lastStr = NULL;
        node->current = cJSON_GetArrayItem(node->current, num);
    } else { // need an object
        if (!cJSON_IsObject(node->current)) {
            ErrLog(WARNING, ErrMsg("Find path failed, current path is not an object."));
            return false;
        }
        node->parent = node->current;
        node->lastStr = token;
        node->lastInt = -1;
        node->current = cJSON_GetObjectItemCaseSensitive(node->current, token);
    }
    return true;
}

/*
 * Create array item by given cJSON root in node and index.
 */
static bool CreateArrayItem(Node *node, int index)
{
    cJSON_bool check = true;
    if (!cJSON_IsArray(node->current)) {
        if (node->parent == NULL) {
            ErrLog(WARNING, ErrMsg("Create path failed, can't change outer data structure, from object to array."));
            return false;
        }
        node->current = cJSON_CreateArray();
        if (cJSON_IsObject(node->parent)) {
            check = cJSON_ReplaceItemInObjectCaseSensitive(node->parent, node->lastStr, node->current);
        } else {
            check = cJSON_ReplaceItemInArray(node->parent, node->lastInt, node->current);
        }
        if (!check) {
            ErrLog(ERROR, ErrMsg("Create path failed, can't replace array item, "
                                 "check if create array is null or nested reference."));
            return false;
        }
    }
    node->parent = node->current;
    node->lastInt = index;
    node->lastStr = NULL;
    for (int i = 0; i <= index; i++) {
        if (cJSON_GetArrayItem(node->current, i) == NULL) {
            check = cJSON_AddItemToArray(node->parent, cJSON_CreateNull());
            if (!check) {
                ErrLog(ERROR, ErrMsg("Create path failed, can't add item to array, check if nested reference."));
                return false;
            }
        }
    }
    node->current = cJSON_GetArrayItem(node->current, index);
    return true;
}

/*
 * Create object item by given cJSON root in node and token (field).
 */
static bool CreateObjectItem(Node *node, char *token)
{
    if (!cJSON_IsObject(node->current)) {
        if (node->parent == NULL) {
            ErrLog(WARNING, ErrMsg("Create path failed, can't change outer data structure, from array to object."));
            return false;
        }
        node->current = cJSON_CreateObject();
        cJSON_bool check;
        if (cJSON_IsObject(node->parent)) {
            check = cJSON_ReplaceItemInObjectCaseSensitive(node->parent, node->lastStr, node->current);
        } else {
            check = cJSON_ReplaceItemInArray(node->parent, node->lastInt, node->current);
        }
        if (!check) {
            ErrLog(ERROR, ErrMsg("Create path failed, can't replace object item, "
                                 "check if create object is null or nested reference."));
            return false;
        }
    }
    node->parent = node->current;
    node->lastStr = token;
    node->lastInt = -1;
    node->current = cJSON_GetObjectItemCaseSensitive(node->current, token);
    if (node->current == NULL) { // if current is NULL, then create
        node->current = cJSON_AddNullToObject(node->parent, token);
    }
    return true;
}

/*
 * Create the target item by given path.
 */
static bool CreateTarget(Node *node, char *token)
{
    if (IsDigitStr(token)) { // check if pure number, if so then it should be an array
        int num = StrToPositiveInt(token);
        if (num == -1) {
            return false;
        }
        return CreateArrayItem(node, num);
    }
    return CreateObjectItem(node, token); // alphabet should be an object
}

/*
 * parse the path to find target item. could choose to create the given path.
 * According to the path, create responding object or array and then keep creating recursive.
 */
static bool ParsePath(Node *node, char *destPath, uint32_t size, bool isCreate)
{
    ASSERT(node != NULL);
    ASSERT(destPath != NULL);
    char curr[CONFIGPARSER_PATH_MAXIMUM + 1] = {}; // +1 for prefix '.'
    char *endPtr;
    char *token = strtok_s(destPath, CONFIGPARSER_DELIM, &endPtr);
    while (token != NULL) {
        bool check = isCreate ? CreateTarget(node, token) : FindTarget(node, token);
        if (!check || node->current == NULL) {
            ErrLog(WARNING, ErrMsg("Parse path failed, stop at path: [%s].", curr));
            return false;
        }
        if (strcat_s(curr, sizeof(curr), CONFIGPARSER_DELIM) || strcat_s(curr, sizeof(curr), token)) {
            ErrLog(ERROR, ErrMsg("Using strcat_s() with [%s] failed, check the current path [%s] with limit %u.", token,
                                 curr, size));
            return false;
        }
        token = strtok_s(NULL, CONFIGPARSER_DELIM, &endPtr);
    }
    return true;
}

/*
 * Check getter parameter and return the target handle from root by path
 */
static ErrorCode CheckParamAndGetTarget(ConfigParserHandle *handle, const char *path, void *value, cJSON **target)
{
    if (handle == NULL) {
        ErrLog(WARNING, ErrMsg("The root for getter is NULL."));
        return CONFIG_PARSER_HANDLE_IS_NULL;
    }
    if (path == NULL) {
        ErrLog(WARNING, ErrMsg("The path for getter is NULL."));
        return CONFIG_PARSER_PARAMETER_INCORRECT;
    }
    if (value == NULL) {
        ErrLog(WARNING, ErrMsg("The value for getter is NULL."));
        return CONFIG_PARSER_PARAMETER_INCORRECT;
    }
    char destPath[CONFIGPARSER_PATH_MAXIMUM] = {};
    if (strcpy_s(destPath, sizeof(destPath), path) != EOK) {
        ErrLog(WARNING, ErrMsg("Using strcpy_s() in getter for [%s] failed, check limit %lu.", path, sizeof(destPath)));
        return CONFIG_PARSER_ALLOCATOR_FAILED;
    }

    Node node = {.parent = NULL, .current = GetCJson(handle), .lastStr = NULL, .lastInt = -1};
    if (!ParsePath(&node, destPath, sizeof(destPath), false)) { // go to the target node by Path
        ErrLog(WARNING, ErrMsg("Parse Target path [%s] failed.", path));
        return CONFIG_PARSER_PARSE_PATH_FAILED;
    }
    *target = node.current;
    return ERROR_SYS_OK;
}

UTILS_EXPORT ErrorCode ConfigParserGetStrValue(ConfigParserHandle *handle, const char *path, char **value)
{
    cJSON *target = NULL;
    ErrorCode errcode = CheckParamAndGetTarget(handle, path, value, &target);
    if (errcode != ERROR_SYS_OK) {
        return errcode;
    }
    if (!cJSON_IsString(target)) {
        ErrLog(WARNING, ErrMsg("Target config by path [%s] is not a string value.", path));
        return CONFIG_PARSER_WRONG_DATA_TYPE;
    }
    *value = cJSON_GetStringValue(target);
    return ERROR_SYS_OK;
}

UTILS_EXPORT ErrorCode ConfigParserGetIntValue(ConfigParserHandle *handle, const char *path, int32_t *value)
{
    // reuse getDouble since it is same but the type from double to int32.
    if (value == NULL) {
        ErrLog(WARNING, ErrMsg("The value for getter is NULL."));
        return CONFIG_PARSER_PARAMETER_INCORRECT;
    }
    double number;
    ErrorCode errcode = ConfigParserGetDoubleValue(handle, path, &number);
    if (errcode != ERROR_SYS_OK) {
        return errcode;
    }
    *value = (int32_t)number;
    return ERROR_SYS_OK;
}

UTILS_EXPORT ErrorCode ConfigParserGetDoubleValue(ConfigParserHandle *handle, const char *path, double *value)
{
    cJSON *target = NULL;
    ErrorCode errcode = CheckParamAndGetTarget(handle, path, value, &target);
    if (errcode != ERROR_SYS_OK) {
        return errcode;
    }
    if (!cJSON_IsNumber(target)) {
        ErrLog(WARNING, ErrMsg("Target config by path [%s] is not a number value.", path));
        return CONFIG_PARSER_WRONG_DATA_TYPE;
    }
    *value = cJSON_GetNumberValue(target);
    return ERROR_SYS_OK;
}

UTILS_EXPORT ErrorCode ConfigParserGetBoolValue(ConfigParserHandle *handle, const char *path, bool *value)
{
    cJSON *target = NULL;
    ErrorCode errcode = CheckParamAndGetTarget(handle, path, value, &target);
    if (errcode != ERROR_SYS_OK) {
        return errcode;
    }
    if (!cJSON_IsBool(target)) {
        ErrLog(WARNING, ErrMsg("Target config by path [%s] is not a boolean value.", path));
        return CONFIG_PARSER_WRONG_DATA_TYPE;
    }
    *value = cJSON_IsTrue(target) ? true : false;
    return ERROR_SYS_OK;
}

UTILS_EXPORT ErrorCode ConfigParserGetNullValue(ConfigParserHandle *handle, char const *path, bool *value)
{
    cJSON *target = NULL;
    ErrorCode errcode = CheckParamAndGetTarget(handle, path, value, &target);
    if (errcode != ERROR_SYS_OK) {
        return errcode;
    }
    *value = cJSON_IsNull(target) ? true : false;
    return ERROR_SYS_OK;
}

UTILS_EXPORT ErrorCode ConfigParserGetHandle(ConfigParserHandle *handle, const char *path, ConfigParserHandle **value)
{
    cJSON *target = NULL;
    ErrorCode errcode = CheckParamAndGetTarget(handle, path, value, &target);
    if (errcode != ERROR_SYS_OK) {
        return errcode;
    }
    *value = GetHandle(target);
    return ERROR_SYS_OK;
}

/*
 * Check setter parameter and put the target handle from root by path
 */
static ErrorCode CheckParamAndSetTarget(ConfigParserHandle *handle, const char *path, cJSON *value)
{
    ASSERT(value != NULL);
    if (handle == NULL) {
        ErrLog(WARNING, ErrMsg("The root for setter is NULL."));
        return CONFIG_PARSER_HANDLE_IS_NULL;
    }
    if (path == NULL) {
        ErrLog(WARNING, ErrMsg("The path for setter is NULL."));
        return CONFIG_PARSER_PARAMETER_INCORRECT;
    }
    char destPath[CONFIGPARSER_PATH_MAXIMUM] = {};
    if (strcpy_s(destPath, sizeof(destPath), path) != EOK) {
        ErrLog(WARNING, ErrMsg("Using strcpy_s() in setter for [%s] failed, check limit %lu.", path, sizeof(destPath)));
        return CONFIG_PARSER_ALLOCATOR_FAILED;
    }

    Node node = {.parent = NULL, .current = GetCJson(handle), .lastStr = NULL, .lastInt = -1};
    if (!ParsePath(&node, destPath, sizeof(destPath), true) || node.parent == NULL) { // create target path
        ErrLog(WARNING, ErrMsg("Create target path [%s] fail.", path));
        return CONFIG_PARSER_CREATE_PATH_FAILED;
    }
    cJSON_bool check;
    if (node.lastStr == NULL) {
        check = cJSON_ReplaceItemInArray(node.parent, node.lastInt, value);
    } else {
        check = cJSON_ReplaceItemInObjectCaseSensitive(node.parent, node.lastStr, value);
    }
    if (!check) {
        ErrLog(ERROR, ErrMsg("Can't create or replace item."));
        return CONFIG_PARSER_CREATE_PATH_FAILED;
    }
    GetHandle(value)->isUpdated = true;
    return ERROR_SYS_OK;
}

UTILS_EXPORT ErrorCode ConfigParserSetStrValue(ConfigParserHandle *handle, const char *path, const char *value)
{
    if (value == NULL) {
        ErrLog(WARNING, ErrMsg("The string for setter is NULL."));
        return CONFIG_PARSER_PARAMETER_INCORRECT;
    }
    cJSON *strJson = cJSON_CreateString(value);
    if (strJson == NULL) {
        ErrLog(ERROR, ErrMsg("Can't create string handle item."));
        return CONFIG_PARSER_ALLOCATOR_FAILED;
    }
    ErrorCode errcode = CheckParamAndSetTarget(handle, path, strJson);
    if (errcode != ERROR_SYS_OK) {
        cJSON_Delete(strJson);
    }
    return errcode;
}

UTILS_EXPORT ErrorCode ConfigParserSetIntValue(ConfigParserHandle *handle, const char *path, int32_t value)
{
    // reuse setDouble since it is same but the type from int32 to double.
    return ConfigParserSetDoubleValue(handle, path, value);
}

UTILS_EXPORT ErrorCode ConfigParserSetDoubleValue(ConfigParserHandle *handle, const char *path, double value)
{
    cJSON *numJson = cJSON_CreateNumber(value);
    if (numJson == NULL) {
        ErrLog(ERROR, ErrMsg("Can't create number handle item."));
        return CONFIG_PARSER_ALLOCATOR_FAILED;
    }
    ErrorCode errcode = CheckParamAndSetTarget(handle, path, numJson);
    if (errcode != ERROR_SYS_OK) {
        cJSON_Delete(numJson);
    }
    return errcode;
}

UTILS_EXPORT ErrorCode ConfigParserSetBoolValue(ConfigParserHandle *handle, const char *path, bool value)
{
    cJSON *boolJson = cJSON_CreateBool((cJSON_bool)value);
    if (boolJson == NULL) {
        ErrLog(ERROR, ErrMsg("Can't create boolean handle item."));
        return CONFIG_PARSER_ALLOCATOR_FAILED;
    }
    ErrorCode errcode = CheckParamAndSetTarget(handle, path, boolJson);
    if (errcode != ERROR_SYS_OK) {
        cJSON_Delete(boolJson);
    }
    return errcode;
}

UTILS_EXPORT ErrorCode ConfigParserSetNullValue(ConfigParserHandle *handle, const char *path)
{
    cJSON *nullJson = cJSON_CreateNull();
    if (nullJson == NULL) {
        ErrLog(ERROR, ErrMsg("Can't create null handle item."));
        return CONFIG_PARSER_ALLOCATOR_FAILED;
    }
    ErrorCode errcode = CheckParamAndSetTarget(handle, path, nullJson);
    if (errcode != ERROR_SYS_OK) {
        cJSON_Delete(nullJson);
    }
    return errcode;
}

UTILS_EXPORT ErrorCode ConfigParserSetHandle(ConfigParserHandle *handle, const char *path, ConfigParserHandle *value)
{
    if (value == NULL) {
        ErrLog(WARNING, ErrMsg("The value for set handle is NULL."));
        return CONFIG_PARSER_PARAMETER_INCORRECT;
    }
    return CheckParamAndSetTarget(handle, path, GetCJson(value));
}

UTILS_EXPORT ErrorCode ConfigParserDelete(ConfigParserHandle *handle, const char *path)
{
    if (handle == NULL) {
        ErrLog(WARNING, ErrMsg("The handle for delete is NULL."));
        return CONFIG_PARSER_HANDLE_IS_NULL;
    }
    if (path == NULL) {
        ErrLog(WARNING, ErrMsg("The path for delete is NULL."));
        return CONFIG_PARSER_PARAMETER_INCORRECT;
    }
    char destPath[CONFIGPARSER_PATH_MAXIMUM] = {};
    if (strcpy_s(destPath, sizeof(destPath), path) != EOK) {
        ErrLog(WARNING, ErrMsg("Using strcpy_s() in delete for [%s] failed, check limit %lu.", path, sizeof(destPath)));
        return CONFIG_PARSER_ALLOCATOR_FAILED;
    }

    Node node = {.parent = NULL, .current = GetCJson(handle), .lastStr = NULL, .lastInt = -1};
    if (!ParsePath(&node, destPath, sizeof(destPath), false)) { // find target path
        ErrLog(WARNING, ErrMsg("Delete target path [%s] fail.", path));
        return CONFIG_PARSER_PARSE_PATH_FAILED;
    }
    if (node.parent == NULL) { // empty path, free the handle
        cJSON_Delete(node.current);
        return ERROR_SYS_OK;
    }
    if (node.lastStr == NULL) {
        cJSON_DeleteItemFromArray(node.parent, node.lastInt);
    } else {
        cJSON_DeleteItemFromObjectCaseSensitive(node.parent, node.lastStr);
    }
    GetHandle(node.parent)->isUpdated = true;
    return ERROR_SYS_OK;
}

UTILS_EXPORT ErrorCode ConfigParserCreateObjectHandle(ConfigParserHandle **handle)
{
    if (handle == NULL) {
        ErrLog(WARNING, ErrMsg("The out param handle for create object is NULL."));
        return CONFIG_PARSER_PARAMETER_INCORRECT;
    }
    ConfigParserInit();
    cJSON *jsonRoot = cJSON_CreateObject();
    if (jsonRoot == NULL) {
        ErrLog(ERROR, ErrMsg("Create object handle failed."));
        return CONFIG_PARSER_ALLOCATOR_FAILED;
    }
    *handle = GetHandle(jsonRoot);
    return ERROR_SYS_OK;
}

UTILS_EXPORT ErrorCode ConfigParserCreateArrayHandle(ConfigParserHandle **handle)
{
    if (handle == NULL) {
        ErrLog(WARNING, ErrMsg("The out param handle for create array is NULL."));
        return CONFIG_PARSER_PARAMETER_INCORRECT;
    }
    ConfigParserInit();
    cJSON *jsonRoot = cJSON_CreateArray();
    if (jsonRoot == NULL) {
        ErrLog(ERROR, ErrMsg("Create array handle failed."));
        return CONFIG_PARSER_ALLOCATOR_FAILED;
    }
    *handle = GetHandle(jsonRoot);
    return ERROR_SYS_OK;
}

UTILS_EXPORT ErrorCode ConfigParserGetArraySize(ConfigParserHandle *arrayHandle, uint32_t *size)
{
    if (arrayHandle == NULL) {
        ErrLog(WARNING, ErrMsg("The root for get array size is NULL."));
        return CONFIG_PARSER_HANDLE_IS_NULL;
    }
    if (size == NULL) {
        ErrLog(WARNING, ErrMsg("The size for get array size is NULL."));
        return CONFIG_PARSER_PARAMETER_INCORRECT;
    }
    cJSON *jsonRoot = GetCJson(arrayHandle);
    if (!cJSON_IsArray(jsonRoot) && !cJSON_IsObject(jsonRoot)) {
        ErrLog(WARNING, ErrMsg("The handle for get array size is not an array or an object."));
        return CONFIG_PARSER_PARAMETER_INCORRECT;
    }
    *size = (uint32_t)cJSON_GetArraySize(jsonRoot);
    return ERROR_SYS_OK;
}

ErrorCode ConfigParserHandleGetName(ConfigParserHandle *handle, char **name)
{
    if (name == NULL) {
        ErrLog(WARNING, ErrMsg("The name is NULL."));
        return CONFIG_PARSER_PARAMETER_INCORRECT;
    }
    cJSON *jsonRoot = GetCJson(handle);
    if (jsonRoot == NULL) {
        ErrLog(WARNING, ErrMsg("The handle is NULL."));
        return CONFIG_PARSER_HANDLE_IS_NULL;
    }
    *name = jsonRoot->string;
    return ERROR_SYS_OK;
}

ConfigParserHandle *ConfigParserHandleGetChild(ConfigParserHandle *handle)
{
    cJSON *jsonRoot = GetCJson(handle);
    if (jsonRoot == NULL) {
        return NULL;
    }
    return GetHandle(jsonRoot->child);
}

ConfigParserHandle *ConfigParserHandleGetNext(ConfigParserHandle *handle)
{
    cJSON *jsonRoot = GetCJson(handle);
    if (jsonRoot == NULL) {
        return NULL;
    }
    return GetHandle(jsonRoot->next);
}
