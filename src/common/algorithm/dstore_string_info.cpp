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
 * ---------------------------------------------------------------------------------------
 * IDENTIFICATION
 *        src\gausskernel\dstore\src\common\dstore_string_info.cpp
 *
 * ---------------------------------------------------------------------------------------
 *
 * stringinfo.cpp
 *
 * StringInfo provides an indefinitely-extensible string data type.
 * It can be used to buffer either ordinary C strings (null-terminated text)
 * or arbitrary binary data.  All storage is allocated with palloc().
 *
 * -------------------------------------------------------------------------
 */
#include "common/algorithm/dstore_string_info.h"
#include <securec.h>
#include "common/dstore_datatype.h"
#include "common/memory/dstore_mctx.h"
#include "errorcode/dstore_common_error_code.h"
#include "framework/dstore_thread.h"

namespace DSTORE {

/*
 * Initialize a StringInfoData struct (with previously undefined contents)
 * to describe an empty string.
 */
bool StringInfoData::init(int size)
{
    data = static_cast<char *>(DstorePalloc(size));
    if (STORAGE_VAR_NULL(data)) {
        return false;
    }
    max_len = size;
    reset();
    return true;
}

/*
 * Reset the StringInfo: the data buffer remains valid, but its
 * previous content, if any, is cleared.
 */
void StringInfoData::reset()
{
    data[0] = '\0';
    len = 0;
    cursor = 0;
}

/*
 * Append arbitrary binary data to a StringInfo, allocating more space
 * if necessary.
 */
RetStatus StringInfoData::append_binary(const char *newData, int dataLen)
{
    if (STORAGE_VAR_NULL(data)) {
        return DSTORE_FAIL;
    }

    /* Make more room if needed */
    RetStatus ret = enlarge(dataLen);
    if (STORAGE_FUNC_FAIL(ret)) {
        return ret;
    }

    /* OK, append the data */
    errno_t rc = memcpy_s(data + len, (size_t)(max_len - len), newData, (size_t)dataLen);
    storage_securec_check(rc, "\0", "\0");
    len += dataLen;

    /*
     * Keep a trailing null in place, even though it's probably useless for
     * binary data.  (Some callers are dealing with text but call this because
     * their input isn't null-terminated.)
     */
    data[len] = '\0';
    return DSTORE_SUCC;
}

/*
 * Make sure there is enough space for StringInfo
 *
 * External callers usually need not concern themselves with this, since
 * all stringinfo.c routines do it automatically.  However, if a caller
 * knows that a StringInfo will eventually become X bytes large, it
 * can save some palloc overhead by enlarging the buffer before starting
 * to store data in it.
 *
 * NB: because we use repalloc() to enlarge the buffer, the string buffer
 * will remain allocated in the same memory context that was current when
 * initStringInfo was called, even if another context is now current.
 * This is the desired and indeed critical behavior!
 */
RetStatus StringInfoData::enlarge(int needed)
{
    int new_len;

    /*
     * Guard against out-of-range "needed" values. Without this, we can get
     * an overflow or infinite loop in the following.
     */
    /* should not happen */
    if (needed < 0) {
        storage_set_error(STRING_ERROR_INVALID_STRING_ENLARGE, needed);
        StorageAssert(0);
    }
    if (((Size)len > MaxAllocSize) || ((Size)needed) >= (MaxAllocSize - (Size)len)) {
        storage_set_error(STRING_ERROR_CANNOT_ENLARGE_BUFFER, len, needed);
        StorageAssert(0);
    }

    needed += len + 1; /* total space required now */

    /* Because of the above test, we now have needed <= MaxAllocSize */
    if (needed <= max_len) {
        return DSTORE_SUCC; /* got enough space already */
    }

    /*
     * We don't want to allocate just a little more space with each append;
     * for efficiency, double the buffer size each time it overflows.
     * Actually, we might need to more than double it if 'needed' is big...
     */
    new_len = 2 * max_len;
    while (needed > new_len) {
        new_len = 2 * new_len;
    }

    /*
     * Clamp to MaxAllocSize in case we went past it.  Note we are assuming
     * here that MaxAllocSize <= INT_MAX/2, else the above loop could
     * overflow.  We will still have newlen >= needed.
     */
    if (new_len > (int)MaxAllocSize) {
        new_len = (int)MaxAllocSize;
    }

    if (g_dstoreCurrentMemoryContext->type == MemoryContextType::STACK_CONTEXT) {
        char* dataTmp = data;
        data = (char *)DstoreMemoryContextAlloc(thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_QUERY), new_len);
        if (STORAGE_VAR_NULL(data)) {
            return DSTORE_FAIL;
        }
        int rc = memcpy_s(data, max_len, dataTmp, max_len);
        storage_securec_check(rc, "\0", "\0");
    } else {
        char* dataTmp = static_cast<char *>(DstoreRepalloc(data, new_len));
        if (STORAGE_VAR_NULL(dataTmp)) {
            return DSTORE_FAIL;
        }
        data = dataTmp;
    }

    max_len = new_len;
    return DSTORE_SUCC;
}

/*
 * Create an empty 'StringInfoData' & return a pointer to it.
 */
StringInfo StringInfoData::make(void)
{
    StringInfo res;

    res = static_cast<StringInfo>(DstorePalloc(sizeof(StringInfoData)));
    if (STORAGE_VAR_NULL(res)) {
        return nullptr;
    }

    bool ret = res->init();
    if (ret == false) {
        DstorePfreeExt(res);
        return nullptr;
    }

    return res;
}

/*
 * Format text data under the control of fmt (an sprintf-style format string)
 * and append it to whatever is already in str.  More space is allocated
 * to str if necessary.  This is sort of like a combination of sprintf and
 * strcat.
 */
RetStatus StringInfoData::append(const char *fmt, ...)
{
    for (;;) {
        va_list args;
        bool success = false;

        /* Try to format the data. */
        va_start(args, fmt);
        success = append_VA(fmt, args);
        va_end(args);

        if (success) {
            break;
        }

        /* Double the buffer size and try again. */
        RetStatus ret = enlarge(max_len);
        if (STORAGE_FUNC_FAIL(ret)) {
            return ret;
        }
    }
    return DSTORE_SUCC;
}

/*
 * Attempt to format text data under the control of fmt (an sprintf-style
 * format string) and append it to whatever is already in str. If successful
 * return true; if not (because there's not enough space), return false
 * without modifying str.  Typically the caller would enlarge str and retry
 * on false return --- see append_string_info for standard usage pattern.
 *
 * XXX This API is ugly, but there seems no alternative given the C spec's
 * restrictions on what can portably be done with va_list arguments: you have
 * to redo va_start before you can rescan the argument list, and we can't do
 * that from here.
 */
bool StringInfoData::append_VA(const char *fmt, va_list args)
{
    if (STORAGE_VAR_NULL(data)) {
        return false;
    }

    int avail, nprinted;

    /*
     * If there's hardly any space, don't bother trying, just fail to make the
     * caller enlarge the buffer first.
     */
    avail = max_len - len - 1;
    if (avail < 16) {
        return false;
    }

    /*
     * Assert check here is to catch buggy vsnprintf that overruns the
     * specified buffer length.  Solaris 7 in 64-bit mode is an example of a
     * platform with such a bug.
     */
#ifdef DSTORE_USE_ASSERT_CHECKING
    data[max_len - 1] = '\0';
#endif

    nprinted = vsnprintf_s(data + len, (size_t)(max_len - len), (size_t)avail, fmt, args);

    StorageAssert(data[max_len - 1] == '\0');

    /*
     * Note: some versions of vsnprintf return the number of chars actually
     * stored, but at least one returns -1 on failure. Be conservative about
     * believing whether the print worked.
     */
    if (nprinted >= 0 && nprinted < avail - 1) {
        /* Success.  Note nprinted does not include trailing null. */
        len += nprinted;
        return true;
    }

    /* Restore the trailing null so that str is unmodified. */
    data[len] = '\0';
    return false;
}

/*
 * Append a null-terminated string to str.
 * Like appendStringInfo(str, "%s", s) but faster.
 */
RetStatus StringInfoData::AppendString(const char *s)
{
    return append_binary(s, strlen(s));
}

/*
 * Append a single byte to str.
 * Like appendStringInfo(str, "%c", ch) but much faster.
 */
RetStatus  StringInfoData::append_char(char ch)
{
    /* Make more room if needed */
    if (len + 1 >= max_len) {
        RetStatus ret = enlarge(1);
        if (STORAGE_FUNC_FAIL(ret)) {
            return ret;
        }
    }

    /* OK, append the character */
    data[len] = ch;
    len++;
    data[len] = '\0';
    return DSTORE_SUCC;
}

struct StringInfoData *StringInfoData::duplicate()
{
    struct StringInfoData *str = StringInfoData::make();
    if (STORAGE_VAR_NULL(str)) {
        return nullptr;
    }
    copy_into(str);
    return str;
}

/*
 * Deep copy: Data part is copied too.   Cursor of the destination is
 * initialized to zero.
 */
void StringInfoData::copy_into(struct StringInfoData *to)
{
    to->reset();
    to->append_binary(data, len);
}

}  // namespace DSTORE
