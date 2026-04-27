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
 * Description: Implement dynamic c string
 */

#ifndef UTILS_STRING_INFO_H
#define UTILS_STRING_INFO_H

#include "defines/common.h"
#include "types/data_types.h"
#include "memory/memory_ctx.h"

GSDB_BEGIN_C_CODE_DECLS

/*
 * StringInfoData holds information about an extensible string.
 *		data    is the current buffer for the string (allocated with MemAlloc).
 *		len	    is the current string length.  There is a terminating '\0' at data[len].
 *		maxLen  is the maximum size of data that the string can store.
 *              maxLen always greater than len.
 *		cursor  is initialized to zero by MakeString or InitString,
 *              but is not otherwise touched by the string_info.c routines.
 *              Some routines use it to scan through a StringInfo.
 */
typedef struct StringInfoData StringInfoData;
struct StringInfoData {
    char *data;
    size_t len;
    size_t maxLen;
    size_t cursor;
};

typedef StringInfoData *StringInfo;

/*
 * There are two ways to create a StringInfo object:
 *
 * 1.StringInfo stringPtr = MakeString();
 *		Both the StringInfoData and the data buffer are MemAlloc'd.
 *
 * 2.StringInfoData string;
 *   InitString(&string);
 *		The data buffer is MemAlloc'd but the StringInfoData is just local.
 *
 * In order to destroy a StringInfo, MemFree() could be used to free
 * the space of the data buffer, and the space of StringInfoData.
 *
 * NOTE: some routines build up a string using StringInfo, and then
 * release the StringInfoData but return the data string itself to their
 * caller. At that point the data string looks like a plain malloc'd
 * string.
 *
 */

/*
 * Create an empty 'StringInfoData' & return a pointer to it.
 * @param context[in], the memory context.
 * @return, StringInfo if success, otherwise NULL.
 */
StringInfo MakeString(MemoryContext context);

/*
 * Destroy a 'StringInfo', use this function when call MakeString.
 * @param str[in], the pointer of StringInfoData.
 */
void DestroyString(StringInfo str);

/*
 * Initialize a StringInfoData struct (with previously undefined contents).
 * to describe an empty string.
 * @param context[in], the memory context.
 * @param str[out], the pointer of StringInfoData.
 * @return, true if success, otherwise false.
 */
bool InitString(MemoryContext context, StringInfo str);

/*
 * Free a 'StringInfoData', use this function when call InitString.
 * @param str[in], the pointer of StringInfoData.
 */
void FreeString(StringInfo str);

/*
 * Free a StringInfo's data get by GetCStrOfString or TransferString.
 * @param data[in], the data pointer of StringInfo.
 */
void FreeStringData(char *data);

/*
 * Returns the data pointer of StringInfo.
 * @param str[in], the pointer of StringInfoData.
 * @return, the data pointer of StringInfo.
 */
char *GetCStrOfString(const StringInfo str);

/*
 * Returns the maxLen of StringInfo.
 * @param str[in], the pointer of StringInfoData.
 * @return, the maxLen of StringInfo.
 */
size_t GetCapacityOfString(const StringInfo str);

/*
 * Returns the len of StringInfo.
 * @param str[in], the pointer of StringInfoData.
 * @return, the len of StringInfo.
 */
size_t GetLengthOfString(const StringInfo str);

/*
 * Avoid deep copy. Assign the StringInfo data pointer to toCStr.
 * @param toCStr[out], the pointer of toCStr's pointer.
 * @param from[in], the pointer of StringInfoData.
 */
void TransferString(char **toCStr, StringInfo from);

/*
 * Clears the current content of the StringInfo, if any. The
 * StringInfo remains valid.
 * @param str[in], the pointer of StringInfoData.
 */
void ResetString(StringInfo str);

/*
 * Format text data under the control of fmt (an sprintf-style format string)
 * and append it to the end of str.  More space is allocated to str if necessary.
 * This is similar to the combination of sprintf and strcat.
 * @param str[in], the pointer of StringInfoData.
 * @param fmt[in], sprintf-style format string.
 * @return, true if success, otherwise false.
 */
bool AppendString(StringInfo str, const char *fmt, ...) GsAttributePrintf(2, 3);

/*
 * Attempt to format text data under the control of fmt (an sprintf-style
 * format string) and append it to whatever is already in str.	If successful
 * return 0; if is truncated (because there's not enough space), return an
 * estimate of the space needed needed value ; if failed return -1 without
 * modifying str. Typically the caller would enlarge str on positive integer return
 * --- see AppendString for standard usage pattern.
 * @param str[in], the pointer of StringInfoData.
 * @param fmt[in], sprintf-style format string.
 * @param args[in], arg list.
 * @return, 0 or needed length if success, otherwise false.
 */
int32_t AppendStringVA(StringInfo str, const char *fmt, va_list args) GsAttributePrintf(2, 0);

/*
 * Append a null-terminated string to str.
 * Like AppendString(str, "%s", s) but faster.
 * @param str[in], the pointer of StringInfoData.
 * @param toCstr[in], the pointer of toCStr.
 * @return, true if success, otherwise false.
 */
bool AppendStringString(StringInfo str, const char *cStr);

/*
 * Append a single byte to str.
 * Like AppendString(str, "%c", ch) but much faster.
 * @param str[in], the pointer of StringInfoData.
 * @param ch[in], a single byte need to be added to str.
 * @return, true if success, otherwise false.
 */
bool AppendStringChar(StringInfo str, char ch);

/**
 * Compared to AppendStringChar, AppendStringCharFast runs
 * faster when there are enough space for adding a character.
 */
static inline bool AppendStringCharFast(StringInfo str, char ch)
{
    if (likely(str->len + 1 < str->maxLen)) {
        str->data[str->len] = ch;
        str->len++;
        str->data[str->len] = '\0';
        return true;
    }

    return AppendStringChar(str, ch);
}

/*
 * Append a given number of spaces to str.
 * @param str[in], the pointer of StringInfoData.
 * @param count[in], the number of spaces.
 * @return, true if success, otherwise false.
 */
bool AppendStringSpaces(StringInfo str, size_t count);

/*
 * Append arbitrary binary data to a StringInfo, allocating more space
 * if necessary.
 * @param str[in], the pointer of StringInfoData.
 * @param data[in], the data pointer of StringInfo.
 * @param dataLen[in], the length of StringInfo data pointer.
 * @return, true if success, otherwise false.
 */
bool AppendBinaryString(StringInfo str, const char *data, size_t dataLen);

/*
 * Append arbitrary binary data to a StringInfo, allocating more space if necessary.
 * Does not ensure a trailing null-byte exists.
 * @param str[in], the pointer of StringInfoData.
 * @param data[in], the data pointer of StringInfo.
 * @param dataLen[in], the length of StringInfo data pointer.
 * @return, true if success, otherwise false.
 */
bool AppendBinaryStringNotTrailNull(StringInfo str, const char *data, size_t dataLen);

/*
 * Deep copy StringInfo.
 * @param to[in], the pointer of StringInfoData.
 * @param from[in], the pointer of StringInfoData.
 * @return, true if success, otherwise false.
 */
bool CopyString(StringInfo to, StringInfo from);

/*
 * Make sure a StringInfo's buffer can hold at least 'needed' more bytes.
 * @param str[in], the pointer of StringInfoData.
 * @param needed[in], the size of string that needed to be expand.
 * @return, true if success, otherwise false.
 */
bool EnlargeString(StringInfo str, size_t needed);

/*
 * Realloc a StringInfo's buffer with initial size 1024.
 * @param str[in], the pointer of StringInfoData.
 * @return, true if success, otherwise false.
 */
bool ReInitString(StringInfo str);

/*
 * Removes the white space at the beginning of the C string.
 * @param cStr[in], the pointer of cStr.
 */
void LeftTrim(char *cStr);

/*
 * Removes the white space at the beginning of the StringInfo.
 * @param str[in], the pointer of StringInfoData.
 */
void LeftTrimString(StringInfo str);

/*
 * Removes the white space at the end of the C string.
 * @param cStr[in], the pointer of cStr.
 */
void RightTrim(char *cStr);

/*
 * Removes the white space at the end of the StringInfo.
 * @param str[in], the pointer of StringInfoData.
 */
void RightTrimString(StringInfo str);

GSDB_END_C_CODE_DECLS
#endif // UTILS_STRING_INFO_H
