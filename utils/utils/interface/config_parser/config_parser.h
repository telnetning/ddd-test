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
 * Description:
 *   config parser used for json file, includes file write and load, CRUD access.
 *
 * Notes:
 *   The number for config parser, int32 and uint32 is supporting, and double support convert to long in range 53 bits,
 *   2^53 ~ -2^53
 *   Set operation according to path, make sure the given path could have responding value if set successfully.
 *   Thus, if the target index is out of range of array, it will insert NULL until the target index.
 *   Free the handle by ConfigParserDelete(handle, "").
 *
 * ***The configuration parser is not thread-safe but process-safe.***
 *   Most service scenarios use config parser to parser and read only the config, some may need to modify
 *   the config by parser, but using the one thread process to handle the config, which is serialization.
 *   Therefore, currently we are not considering about the thread conflict.
 *
 *
 */

#ifndef UTILS_CONFIG_PARSER_H
#define UTILS_CONFIG_PARSER_H

#include <stdbool.h>
#include "defines/err_code.h"
#include "defines/common.h"
#include "defines/utils_errorcode.h"
#include "vfs/vfs_interface.h"

GSDB_BEGIN_C_CODE_DECLS

#define CONFIG_PARSER_FILE_IO_ERROR       MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_CONFIG_PARSER_MODULE_ID, 0x0001)
#define CONFIG_PARSER_HANDLE_IS_NULL      MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_CONFIG_PARSER_MODULE_ID, 0x0002)
#define CONFIG_PARSER_PARSE_PATH_FAILED   MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_CONFIG_PARSER_MODULE_ID, 0x0003)
#define CONFIG_PARSER_CREATE_PATH_FAILED  MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_CONFIG_PARSER_MODULE_ID, 0x0004)
#define CONFIG_PARSER_WRONG_DATA_TYPE     MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_CONFIG_PARSER_MODULE_ID, 0x0005)
#define CONFIG_PARSER_PARAMETER_INCORRECT MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_CONFIG_PARSER_MODULE_ID, 0x0006)
#define CONFIG_PARSER_ALLOCATOR_FAILED    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_CONFIG_PARSER_MODULE_ID, 0x0007)

#define CONFIGPARSER_DELIM         "."               /* Path separator */
#define CONFIGPARSER_NUM_STR_SIZE  (8 + 1)           /* The limit for the number of digits in path */
#define CONFIGPARSER_PATH_MAXIMUM  2048              /* The limit for the length of parser path include '\0' */
#define CONFIGPARSER_ARRAY_MAXIMUM 128               /* The limit for the size of updated array */
#define CONFIGPARSER_FILE_MAXIMUM  FILE_PATH_MAX_LEN /* The limit for the file path length which is from VFS */

/*
 * The config root type used to manipulate by parser
 */
typedef struct ConfigParserHandle ConfigParserHandle;

/* ---------------General function--------------- */
/**
 * Load JSON file by given file path.
 * @param[in] configFilePath limit to CONFIGPARSER_FILE_MAXIMUM
 * @param[out] handle handle to access
 * @return ERROR_SYS_OK = 0 if success
 *
 * @WANRING User is responsible to free the handle.
 */
UTILS_EXPORT ErrorCode ConfigParserLoadFile(const char *configFilePath, ConfigParserHandle **handle);

/**
 * Load JSON by given string.
 * @param[in] jsonStr json string
 * @param[out] handle handle to access
 * @return ERROR_SYS_OK = 0 if success
 *
 * @WANRING User is responsible to free the handle.
 */
UTILS_EXPORT ErrorCode ConfigParserLoadString(const char *jsonStr, ConfigParserHandle **handle);

/**
 * Reload the config file with the updated handle did and return new handle.
 * @param[in] configFilePath limit to CONFIGPARSER_FILE_MAXIMUM
 * @param[in] handle current handle
 * @param[out] newHandle new handle to access
 * @param[in] callback callback function to announce the update item
 * @return ERROR_SYS_OK = 0 if success
 * @note If the processes modify same place and current process doesn't store it,
 *       the callback after reload will not record the path and keep the current, not update.
 *       Therefore, in this case, the latest version shall prevail.
 *
 * @WANRING User is responsible to free the old handle and new handle.
 */
UTILS_EXPORT ErrorCode ConfigParserReloadFile(char const *configFilePath, ConfigParserHandle *handle,
                                              ConfigParserHandle **newHandle,
                                              int (*callback)(char *updatedPaths[], uint32_t size));

/**
 * If the given file is a json, it would load it and update cover it with handle.
 * If load file is not json or any problem, it would log error but still try to write into file.
 * @param[in] handle handle to write
 * @param[in] configFilePath file to write, limit to CONFIGPARSER_FILE_MAXIMUM
 * @return ERROR_SYS_OK = 0 if success
 * @note use file block lock to avoid process conflict, make process-safe. It generates lock file by:
 *       Case 1: The length of file path is enough in range to add extension, just append <.lock>.
 *       Case 2: Not enough to append, if the rest and file name is long enough, replace last some chars to extension.
 *       Case 3: Not enough to append, and the file name is short, replace last char.
 */
UTILS_EXPORT ErrorCode ConfigParserStoreFile(ConfigParserHandle *handle, const char *configFilePath);

/**
 * Print the formatted json of handle.
 * @param[in] handle current handle
 * @param[out] string formatted json string
 * @return ERROR_SYS_OK = 0 if success
 *
 * @WARNING User is responsible to free the string.
 */
UTILS_EXPORT ErrorCode ConfigParserPrintJson(ConfigParserHandle *handle, char **string);

/**
 * Deep copy the old handle and return new handle.
 * @param[in] oldHandle current handle
 * @param[out] newHandle new handle to access
 * @return ERROR_SYS_OK = 0 if success
 *
 * @WANRING User is responsible to free the old handle and new handle.
 */
UTILS_EXPORT ErrorCode ConfigParserDeepCopy(ConfigParserHandle *oldHandle, ConfigParserHandle **newHandle);

/* ---------------Query getter function--------------- */
/**
 * Get string value by path.
 * @param[in] handle handle to access
 * @param[in] path path to item, "." or "" means itself
 * @param[out] value string
 * @return ERROR_SYS_OK = 0 if success
 *
 * @WARNING Don't free the value string. The string is not deep copy, user should not modify.
 */
UTILS_EXPORT ErrorCode ConfigParserGetStrValue(ConfigParserHandle *handle, const char *path, char **value);

/**
 * Get integer value by path.
 * Support int32 and uint32
 * @param[in] handle handle to access
 * @param[in] path path to item, "." or "" means itself
 * @param[out] value integer
 * @return ERROR_SYS_OK = 0 if success
 */
UTILS_EXPORT ErrorCode ConfigParserGetIntValue(ConfigParserHandle *handle, const char *path, int32_t *value);

/**
 * Get double value by path. (double is 64 bits)
 * @param[in] handle handle to access
 * @param[in] path path to item, "." or "" means itself
 * @param[out] value double
 * @return ERROR_SYS_OK = 0 if success
 */
UTILS_EXPORT ErrorCode ConfigParserGetDoubleValue(ConfigParserHandle *handle, const char *path, double *value);

/**
 * Get boolean value by path.
 * @param[in] handle handle to access
 * @param[in] path path to item, "." or "" means itself
 * @param[out] value true or false
 * @return ERROR_SYS_OK = 0 if success
 */
UTILS_EXPORT ErrorCode ConfigParserGetBoolValue(ConfigParserHandle *handle, const char *path, bool *value);

/**
 * Get null value by path.
 * @param[in] handle handle to access
 * @param[in] path path to item, "." or "" means itself
 * @param[out] value true or false means exist or not
 * @return ERROR_SYS_OK = 0 if success
 */
UTILS_EXPORT ErrorCode ConfigParserGetNullValue(ConfigParserHandle *handle, char const *path, bool *value);

/**
 * Get handle by path.
 * @param[in] handle handle to access
 * @param[in] path path to item, "." or "" means itself
 * @param[out] handle *not deep copy*
 * @return ERROR_SYS_OK = 0 if success
 */
UTILS_EXPORT ErrorCode ConfigParserGetHandle(ConfigParserHandle *handle, const char *path, ConfigParserHandle **value);

/**
 * Get size of array handle.
 * @param[in] array given array handle
 * @param[out] size count of array element
 * @return ERROR_SYS_OK = 0 if success
 */
UTILS_EXPORT ErrorCode ConfigParserGetArraySize(ConfigParserHandle *arrayHandle, uint32_t *size);

/* ---------------Update setter function--------------- */
/**
 * Set string value by path.
 * If add out of array size, it will set to target with insert null value in between.
 * @param[in] handle handle to access
 * @param[in] path path to item, "." or "" means itself, can't modify itself
 * @param[in] value string
 * @return ERROR_SYS_OK = 0 if success
 * @note Failed if try to change the outer data structure.
 */
UTILS_EXPORT ErrorCode ConfigParserSetStrValue(ConfigParserHandle *handle, const char *path, const char *value);

/**
 * Set integer value by path.
 * If add out of array size, it will set to target with insert null value in between.
 * @param[in] handle handle to access
 * @param[in] path path to item, "." or "" means itself, can't modify itself
 * @param[in] value integer
 * @return ERROR_SYS_OK = 0 if success
 * @note Failed if try to change the outer data structure.
 */
UTILS_EXPORT ErrorCode ConfigParserSetIntValue(ConfigParserHandle *handle, const char *path, int32_t value);

/**
 * Set double value by path.
 * If add out of array size, it will set to target with insert null value in between.
 * @param[in] handle handle to access
 * @param[in] path path to item, "." or "" means itself, can't modify itself
 * @param[in] value double
 * @return ERROR_SYS_OK = 0 if success
 * @note Failed if try to change the outer data structure.
 */
UTILS_EXPORT ErrorCode ConfigParserSetDoubleValue(ConfigParserHandle *handle, const char *path, double value);

/**
 * Set boolean value by path.
 * If add out of array size, it will set to target with insert null value in between.
 * @param[in] handle handle to access
 * @param[in] path path to item, "." or "" means itself, can't modify itself
 * @param[in] value true or false
 * @return ERROR_SYS_OK = 0 if success
 * @note Failed if try to change the outer data structure.
 */
UTILS_EXPORT ErrorCode ConfigParserSetBoolValue(ConfigParserHandle *handle, const char *path, bool value);

/**
 * Set null value by path.
 * If add out of array size, it will set to target with insert null value in between.
 * @param[in] handle handle to access
 * @param[in] path path to item, "." or "" means itself, can't modify itself
 * @return ERROR_SYS_OK = 0 if success
 * @note Failed if try to change the outer data structure.
 */
UTILS_EXPORT ErrorCode ConfigParserSetNullValue(ConfigParserHandle *handle, const char *path);

/**
 * Set handle by path.
 * If add out of array size, it will set to target with insert null value in between.
 * @param[in] handle handle to access
 * @param[in] path path to item, "." or "" means itself, can't modify itself
 * @param[in] value newHandle to set to target
 * @return ERROR_SYS_OK = 0 if success
 * @note Failed if try to change the outer data structure.
 * @note Be careful about Parent-son reference to each other
 */
UTILS_EXPORT ErrorCode ConfigParserSetHandle(ConfigParserHandle *handle, const char *path, ConfigParserHandle *value);

/* ---------------Remove function--------------- */
/**
 * Remove and delete target node recursive.
 * @param[in] handle handle to access
 * @param[in] path path to item, "" or "." means itself
 * @return ERROR_SYS_OK = 0 if success
 *
 * @WARNING Delete handle itself only includes it and its sub-root but not parent-root. So only call this
 *          function with root handle when want to delete itself, Delete(root, "") == free(root)
 */
UTILS_EXPORT ErrorCode ConfigParserDelete(ConfigParserHandle *handle, const char *path);

/* ---------------Create function--------------- */
/**
 * Create empty object handle.
 * @param[out] handle
 * @return ERROR_SYS_OK = 0 if success
 *
 * @WANRING User is responsible to free the handle.
 */
UTILS_EXPORT ErrorCode ConfigParserCreateObjectHandle(ConfigParserHandle **handle);

/**
 * Create empty array handle.
 * @param[out] handle
 * @return ERROR_SYS_OK = 0 if success
 *
 * @WANRING User is responsible to free the handle.
 */
UTILS_EXPORT ErrorCode ConfigParserCreateArrayHandle(ConfigParserHandle **handle);

/**
 * Get handle name.
 * @param[in] handle handle to access
 * @param[out] name string
 * @return ERROR_SYS_OK = 0 if success
 *
 * @WARNING Don't free the name string. The string is not deep copy, user should not modify.
 */
UTILS_EXPORT ErrorCode ConfigParserHandleGetName(ConfigParserHandle *handle, char **name);

/**
 * Only for iterator, ignore this.
 */
UTILS_EXPORT ConfigParserHandle *ConfigParserHandleGetChild(ConfigParserHandle *handle);

/**
 * Only for iterator, ignore this.
 */
UTILS_EXPORT ConfigParserHandle *ConfigParserHandleGetNext(ConfigParserHandle *handle);

/**
 * Macro for iterating over an array or object.
 * In the loop body, use get path = itself to extract the value.
 *
 * @param[out] element The element handle pointer that use to access the element of array
 * @param[in] array the array handle that need to iterating
 */
#define CONFIGPARSER_HANDLE_FOR_EACH(element, array)                                                  \
    for ((element) = ((array) != NULL) ? ConfigParserHandleGetChild(array) : NULL; (element) != NULL; \
         (element) = ConfigParserHandleGetNext(element))

GSDB_END_C_CODE_DECLS

#endif /* UTILS_CONFIG_PARSER_H */