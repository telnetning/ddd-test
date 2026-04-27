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
 * Description: Implement dynamic c string. StringInfo provides an extensible string data type.
 * It can be used to buffer either ordinary C strings (null-terminated text) or arbitrary binary data.
 * All storage is allocated with MemoryContextAlloc() (falling back to malloc in frontend code).
 */

#include "securec.h"
#include "memory/memory_ctx.h"
#include "defines/abort.h"
#include "fault_injection/fault_injection.h"
#include "syslog/err_log.h"
#include "container/string_info.h"

#define INITIAL_STRING_SIZE    1024                 /* Initial default string size. */
#define MAX_STRING_BUFFER_SIZE ((size_t)0x3fffffff) /* 1 gigabyte - 1 */

UTILS_EXPORT void ResetString(StringInfo str)
{
    if (unlikely(str == NULL)) {
        return;
    }

    if (likely(str->data != NULL)) {
        str->data[0] = '\0';
    }

    str->len = 0;
    str->cursor = 0;
}

UTILS_EXPORT bool InitString(MemoryContext context, StringInfo str)
{
    if (unlikely(str == NULL)) {
        return false;
    }

    FAULT_INJECTION_CALL_REPLACE(MOCK_STRING_INIT_MALLOC_FAILED, &str->data)
    str->data = (char *)MemoryContextAlloc(context, INITIAL_STRING_SIZE);
    FAULT_INJECTION_CALL_REPLACE_END;
    if (unlikely(str->data == NULL)) {
        str->maxLen = 0;
        return false;
    }
    str->maxLen = INITIAL_STRING_SIZE;
    ResetString(str);

    return true;
}

UTILS_EXPORT StringInfo MakeString(MemoryContext context)
{
    StringInfo str = (StringInfo)MemoryContextAlloc(context, sizeof(StringInfoData));
    if (unlikely(str == NULL)) {
        return NULL;
    }

    bool ret = InitString(context, str);
    if (unlikely(!ret)) {
        MemFree(str);
        return NULL;
    }

    return str;
}

UTILS_EXPORT void FreeString(StringInfo str)
{
    /*
     * If data of StringInfo has been transferred, data was NULL.Therefore, check whether data is NULL.
     */
    if (likely(str != NULL && str->data != NULL)) {
        MemFree(str->data);
        str->data = NULL;
    }
}

UTILS_EXPORT void FreeStringData(char *data)
{
    if (unlikely(data == NULL)) {
        return;
    }

    MemFree(data);
}

UTILS_EXPORT void DestroyString(StringInfo str)
{
    if (unlikely(str == NULL)) {
        return;
    }

    FreeString(str);
    MemFree(str);
}

UTILS_EXPORT int32_t AppendStringVA(StringInfo str, const char *fmt, va_list args)
{
    if (unlikely((str == NULL) || (str->data == NULL))) {
        return -1;
    }
    /* return 0 indicates success. A value greater than 0 indicates the size of the space to be expanded. -1 failed */
    size_t avail = str->maxLen - str->len;
#define MIN_STRING_INFO_GAP 32
    if (avail < MIN_STRING_INFO_GAP) {
        /**
         * Return MIN_STRING_INFO_GAP is because expanding maxLen double size in default,
         * when EnlargeString(MIN_STRING_INFO_GAP), we EnlargeString(str->maxLen)
         * or EnlargeString(MAX_PER_EXTEND) in actually
         */
        return MIN_STRING_INFO_GAP;
    }

    int ret = vsnprintf_s(str->data + str->len, avail, avail - 1, fmt, args);
    if (ret == -1) {
        if (str->data[str->len] == '\0') {
            return -1;
        }
        /**
         * according to huawei securec help document, this case indicates truncation occurred, so avail not enough.
         * we need tell caller to expand current maxLen to double default, so return avail. attention: here we not
         * reset the str->data[str->len] to '\0', it's because caller maybe ignore the case of return greater than 0,
         * so we leave the truncate chars in data, no matter if caller uses it or not
         */
        return (int32_t)avail;
    }
    /* successful */
    /* The ret must greater than 0 */
    str->len += (size_t)(long long)ret;
    return 0;
}

UTILS_EXPORT bool EnlargeString(StringInfo str, size_t needed)
{
    if (unlikely((str == NULL) || (str->data == NULL))) {
        return false;
    }

    if (needed >= (MAX_STRING_BUFFER_SIZE - str->len)) {
        Abort();
        return false;
    }

    /* The total space required */
    size_t needLen = needed + str->len + 1;

    if (needLen <= str->maxLen) {
        return true;
    }
    /* Double string info space once a time, but limit to 1 gigabyte */
    size_t newMaxLen;

    for (newMaxLen = str->maxLen; newMaxLen < needLen; newMaxLen <<= 1) {
    }

    char *newData = (char *)MemRealloc(str->data, newMaxLen);
    if (unlikely(newData == NULL)) {
        /* if MemRealloc return NULL, the str->data memory not freed, so return directly is ok */
        return false;
    }
    /* successful */
    str->maxLen = newMaxLen;
    str->data = newData;

    return true;
}

UTILS_EXPORT bool AppendString(StringInfo str, const char *fmt, ...)
{
    if (unlikely(str == NULL)) {
        return false;
    }

    for (;;) {
        va_list args;
        /* Try to format the data. */
        va_start(args, fmt);
        int32_t needed = AppendStringVA(str, fmt, args);
        va_end(args);
        /* Fail. */
        if (needed == -1) {
            return false;
        }
        /* Success. */
        if (needed == 0) {
            break;
        }
        /**
         * Double the buffer size and try again.
         * The value of needed must greater than 0.
         */
        bool ret = EnlargeString(str, (size_t)(long long)needed);
        if (unlikely(!ret)) {
            return ret;
        }
    }
    return true;
}

UTILS_EXPORT bool AppendBinaryStringNotTrailNull(StringInfo str, const char *data, size_t dataLen)
{
    if (unlikely(str == NULL || data == NULL)) {
        return false;
    }

    bool ret = EnlargeString(str, dataLen);
    if (unlikely(!ret)) {
        return ret;
    }

    errno_t errCode = memcpy_s(str->data + str->len, str->maxLen - str->len, data, dataLen);
    if (unlikely(errCode != EOK)) {
        return false;
    }

    str->len += dataLen;
    return true;
}

UTILS_EXPORT bool AppendBinaryString(StringInfo str, const char *data, size_t dataLen)
{
    if (unlikely(str == NULL || data == NULL)) {
        return false;
    }

    bool ret = AppendBinaryStringNotTrailNull(str, data, dataLen);
    if (unlikely(!ret)) {
        return ret;
    }

    str->data[str->len] = '\0';

    return true;
}

UTILS_EXPORT bool AppendStringString(StringInfo str, const char *cStr)
{
    if (unlikely(str == NULL || cStr == NULL)) {
        return false;
    }

    return AppendBinaryString(str, cStr, strlen(cStr));
}

UTILS_EXPORT bool AppendStringChar(StringInfo str, char ch)
{
    if (unlikely(str == NULL)) {
        return false;
    }

    bool ret = EnlargeString(str, 1);
    if (unlikely(!ret)) {
        return ret;
    }

    str->data[str->len] = ch;
    str->data[++str->len] = '\0';

    return true;
}

UTILS_EXPORT bool AppendStringSpaces(StringInfo str, size_t count)
{
    if (unlikely(str == NULL)) {
        return false;
    }

    bool ret = EnlargeString(str, count);
    if (unlikely(!ret)) {
        return ret;
    }

    size_t end = str->len + count;
    for (; str->len < end; str->len++) {
        str->data[str->len] = ' ';
    }
    str->data[str->len] = '\0';

    return true;
}

UTILS_EXPORT bool CopyString(StringInfo to, StringInfo from)
{
    if (unlikely(to == NULL || from == NULL)) {
        return false;
    }
    ResetString(to);
    return AppendBinaryString(to, from->data, from->len);
}

UTILS_EXPORT bool ReInitString(StringInfo str)
{
    if (unlikely(str == NULL)) {
        return false;
    }

    size_t initSize = INITIAL_STRING_SIZE;
    if (str->maxLen > initSize) {
        char *newData = (char *)MemRealloc(str->data, initSize);
        if (unlikely(newData == NULL)) {
            return false;
        }
        str->data = newData;
        str->maxLen = initSize;
    }
    ResetString(str);
    return true;
}

UTILS_EXPORT char *GetCStrOfString(const StringInfo str)
{
    ASSERT(str != NULL);
    return str->data;
}

UTILS_EXPORT size_t GetCapacityOfString(const StringInfo str)
{
    ASSERT(str != NULL);
    return str->maxLen;
}

UTILS_EXPORT size_t GetLengthOfString(const StringInfo str)
{
    ASSERT(str != NULL);
    return str->len;
}

UTILS_EXPORT void TransferString(char **toCStr, StringInfo from)
{
    ASSERT(toCStr != NULL);
    ASSERT(from != NULL);
    *toCStr = from->data;
    from->data = NULL;
    from->maxLen = 0;
    from->len = 0;
    from->cursor = 0;
}

UTILS_EXPORT void LeftTrim(char *cStr)
{
    if (unlikely(cStr == NULL)) {
        return;
    }

    size_t destMax = strlen(cStr) + 1;
    char *p = cStr;
    unsigned long len = 0;
    while (*p == ' ' || *p == '\t') {
        p++;
        ++len;
    }
    (void)memmove_s(cStr, destMax, p, destMax - len);
}

UTILS_EXPORT void LeftTrimString(StringInfo str)
{
    ASSERT(str != NULL);
    LeftTrim(str->data);
}

UTILS_EXPORT void RightTrim(char *cStr)
{
    if (unlikely(cStr == NULL)) {
        return;
    }

    int i = (int)(strlen(cStr) - 1);
    while ((cStr[i] == ' ' || cStr[i] == '\t') && i >= 0) {
        i--;
    }
    cStr[i + 1] = '\0';
}

UTILS_EXPORT void RightTrimString(StringInfo str)
{
    ASSERT(str != NULL);
    RightTrim(str->data);
}
