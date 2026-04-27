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
 *        include/common/algorithm/dstore_string_info.h
 *
 * ---------------------------------------------------------------------------------------
*
* stringinfo.h
*	  Declarations/definitions for "StringInfo" functions.
*
* StringInfo provides an indefinitely-extensible string data type.
* It can be used to buffer either ordinary C strings (null-terminated text)
* or arbitrary binary data.  All storage is allocated with palloc().
*
* Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
* Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
* Portions Copyright (c) 1994, Regents of the University of California
* Portions Copyright (c) 2010-2012 Postgres-XC Development Group
*
* $PostgreSQL: pgsql/src/include/lib/stringinfo.h,v 1.35 2008/01/01 19:45:57 momjian Exp $
*
* -------------------------------------------------------------------------
*/

#ifndef DSTORE_DSTORE_STRING_INFO_H
#define DSTORE_DSTORE_STRING_INFO_H

#include <cstdarg>
#include <netinet/in.h>
#include "securec.h"
#include "common/dstore_datatype.h"
#include "errorcode/dstore_common_error_code.h"
#include "common/error/dstore_error.h"
#include "common/memory/dstore_mctx.h"
#include "common/log/dstore_log.h"

namespace DSTORE {

/* -------------------------
 * StringInfoData holds information about an extensible string.
 *		data	is the current buffer for the string (allocated with palloc).
 *		len		is the current string length.  There is guaranteed to be
 *				a terminating '\0' at data[len], although this is not very
 *				useful when the string holds binary data rather than text.
 *		maxlen	is the allocated size in bytes of 'data', i.e. the maximum
 *				string size (including the terminating '\0' char) that we can
 *				currently store in 'data' without having to reallocate
 *				more space.  We must always have maxlen > len.
 *		cursor	is initialized to zero by make_string_info or initStringInfo,
 *				but is not otherwise touched by the stringinfo.c routines.
 *				Some routines use it to scan through a StringInfo.
 * -------------------------
 */
/* ------------------------
 * There are two ways to create a StringInfo object initially:
 *
 * StringInfo stringptr = StringInfoData::make();
 *		Both the StringInfoData and the data buffer are palloc'd.
 *
 * StringInfoData string;
 * string.init();
 *		The data buffer is palloc'd but the StringInfoData is just local.
 *		This is the easiest approach for a StringInfo object that will
 *		only live as long as the current routine.
 *
 * To destroy a StringInfo, DstorePfree() the data buffer, and then DstorePfree() the
 * StringInfoData if it was palloc'd.  There's no special support for this.
 *
 * NOTE: some routines build up a string using StringInfo, and then
 * release the StringInfoData but return the data string itself to their
 * caller.	At that point the data string looks like a plain palloc'd
 * string.
 * -------------------------
 */
typedef struct StringInfoData {
public:
    char *data;
    int len;
    int max_len;
    int cursor;

    StringInfoData() : data(nullptr), len(0), max_len(0), cursor(0) {}

    /* ------------------------
    * Create an empty 'StringInfoData' & return a pointer to it.
    */
    static struct StringInfoData *make(void);

    /* ------------------------
    * Initialize a StringInfoData struct (with previously undefined contents)
    * to describe an empty string.
    */
    bool init(int size = 1024);

    /* ------------------------
    * Clears the current content of the StringInfo, if any. The
    * StringInfo remains valid.
    */
    void reset();

    /* ------------------------
    * Format text data under the control of fmt (an sprintf-style format string)
    * and append it to whatever is already in str.  More space is allocated
    * to str if necessary.  This is sort of like a combination of sprintf and
    * strcat.
    */
    RetStatus append(const char *fmt, ...)
        /* This extension allows gcc to check the format string */
        __attribute__((format(printf, 2, 3)));

    /* ------------------------
    * Attempt to format text data under the control of fmt (an sprintf-style
    * format string) and append it to whatever is already in str.	If successful
    * return true; if not (because there's not enough space), return false
    * without modifying str.  Typically the caller would enlarge str and retry
    * on false return --- see append_string_info for standard usage pattern.
    */
    bool append_VA(const char* fmt, va_list args) __attribute__((format(printf, 2, 0)));

    /* ------------------------
    * Append a null-terminated string to str.
    * Like append_string_info(str, "%s", s) but faster.
    */
    RetStatus AppendString(const char* s);

    /* ------------------------
    * Append a single byte to str.
    * Like appendStringInfo(str, "%c", ch) but much faster.
    */
    RetStatus  append_char(char ch);

    /* ------------------------
    * Append arbitrary binary data to a StringInfo, allocating more space
    * if necessary.
    */
    RetStatus append_binary(const char* newData, int dataLen);

    /* ------------------------
    * Make sure a StringInfo's buffer can hold at least 'needed' more bytes.
    */
    RetStatus enlarge(int needed);

    /* -----------------------
    * Get new StringInfo and copy the original to it.
    */
    struct StringInfoData *duplicate();

    /* ------------------------
    * Copy StringInfo. Deep copy: Data will be copied too.
    * cursor of "to" will be initialized to zero.
    */
    void copy_into(struct StringInfoData *to);

    /* --------------------------------------------------------------------------
    * The following interfaces are used for message encapsulation and parsing.
    */
    /* append a binary [u]int8 to a StringInfo buffer */
    inline RetStatus SendInt8(uint8 i)
    {
        RetStatus res = enlarge(sizeof(uint8));
        if (STORAGE_FUNC_FAIL(res)) {
            return DSTORE_FAIL;
        }
        errno_t rc = memcpy_s(data + len, sizeof(uint8), &i, sizeof(uint8));
        storage_securec_check(rc, "\0", "\0");
        len += sizeof(uint8);
        return DSTORE_SUCC;
    }

    /* append a binary [u]int16 to a StringInfo buffer */
    inline RetStatus send_int16(uint16 i)
    {
        RetStatus res = enlarge(sizeof(uint16));
        if (STORAGE_FUNC_FAIL(res)) {
            return DSTORE_FAIL;
        }
        uint16 ni = htons(i);
        errno_t rc = memcpy_s(data + len, sizeof(uint16), &ni, sizeof(uint16));
        storage_securec_check(rc, "\0", "\0");
        len += sizeof(uint16);
        return DSTORE_SUCC;
    }

    /* append a binary [u]int32 to a StringInfo buffer */
    inline RetStatus SendInt32(uint32 i)
    {
        RetStatus res = enlarge(sizeof(uint32));
        if (STORAGE_FUNC_FAIL(res)) {
            return DSTORE_FAIL;
        }
        uint32 ni = htonl(i);
        errno_t rc = memcpy_s(data + len, sizeof(uint32), &ni, sizeof(uint32));
        storage_securec_check(rc, "\0", "\0");
        len += sizeof(uint32);
        return DSTORE_SUCC;
    }

    /* append a binary [u]int64 to a StringInfo buffer */
    inline RetStatus SendInt64(uint64 i)
    {
        RetStatus res = enlarge(sizeof(uint64));
        if (STORAGE_FUNC_FAIL(res)) {
            return DSTORE_FAIL;
        }
        uint32 n32;
        errno_t rc = EOK;

        /* High order half first, since we're doing MSB-first */
        n32 = (uint32)(i >> 32);
        n32 = htonl(n32);
        rc = memcpy_s(data + len, sizeof(uint32), &n32, sizeof(uint32));
        storage_securec_check(rc, "\0", "\0");
        len += sizeof(uint32);

        /* Now the low order half */
        n32 = (uint32)i;
        n32 = htonl(n32);
        rc = memcpy_s(data + len, sizeof(uint32), &n32, sizeof(uint32));
        storage_securec_check(rc, "\0", "\0");
        len += sizeof(uint32);
        return DSTORE_SUCC;
    }

    /* append a binary byte to a StringInfo buffer */
    inline RetStatus send_byte(uint8 byt)
    {
        return SendInt8(byt);
    }

    /* append raw data to a StringInfo buffer */
    inline RetStatus SendBytes(const char* rawData, int datalen)
    {
        return append_binary(rawData, datalen);
    }

    /* append a float to a StringInfo buffer */
    inline RetStatus send_float(float f)
    {
        union {
            float f;
            uint32 i;
        } swap = { .i = 0 };

        swap.f = f;
        return SendInt32(swap.i);
    }

    /* append a double to a StringInfo buffer */
    inline RetStatus send_double(double f)
    {
        union {
            double f;
            int64 i;
        } swap = { .i = 0 };

        swap.f = f;
        return SendInt64(swap.i);
    }

    /* get a raw byte from a message buffer */
    inline char get_byte()
    {
        if (unlikely (cursor >= len)) {
            ErrLog(DSTORE_PANIC,
                MODULE_COMMON, ErrMsg("len is %d, cursor is %d", len, cursor));
        }
        return (char)data[cursor++];
    }

    /* get raw data from a message buffer */
    inline const char* get_bytes(int datalen)
    {
        if (unlikely (datalen < 0 || datalen > (len - cursor))) {
            ErrLog(DSTORE_PANIC,
                MODULE_COMMON, ErrMsg("datalen is %d, cursor is %d, len is %d", datalen, cursor, len));
        }
        const char* result = &data[cursor];
        cursor += datalen;
        return result;
    }

    /* copy raw data from a message buffer */
    inline void copy_bytes(char* buf, int datalen)
    {
        if (unlikely(datalen < 0 || datalen > (len - cursor))) {
            ErrLog(DSTORE_PANIC,
                MODULE_COMMON, ErrMsg("datalen is %d, cursor is %d, len is %d", datalen, cursor, len));
        }
        if (likely(datalen > 0)) {
            int rcs = memcpy_s(buf, datalen, &data[cursor], datalen);
            storage_securec_check(rcs, "\0", "\0");
            cursor += datalen;
        }
    }

    /* get a int8 from a message buffer */
    inline uint8 GetInt8()
    {
        return (uint8)get_byte();
    }

    /* get a int16 from a message buffer */
    inline uint16 get_int16()
    {
        uint16 n16;
        copy_bytes((char*)&n16, 2);
        return ntohs(n16);
    }

    /* get a int32 from a message buffer */
    inline uint32 GetInt32()
    {
        uint32 n32;
        copy_bytes((char*)&n32, 4);
        return ntohl(n32);
    }

    /* get a binary 8-byte int from a message buffer */
    inline uint64 GetInt64()
    {
        uint64 result;
        uint32 h32;
        uint32 l32;

        copy_bytes((char*)&h32, 4);
        copy_bytes((char*)&l32, 4);
        h32 = ntohl(h32);
        l32 = ntohl(l32);

        result = h32;
        result <<= 32;
        result |= l32;

        return result;
    }

    /* get a float from a message buffer */
    inline float get_float()
    {
        union {
            float f;
            uint32 i;
        } swap = { .i = 0 };

        swap.i = GetInt32();
        return swap.f;
    }

    /* get a double from a message buffer */
    inline double get_double()
    {
        union {
            double f;
            uint64 i;
        } swap = { .i = 0 };

        swap.i = GetInt64();
        return swap.f;
    }
} StringInfoData;

typedef StringInfoData* StringInfo;

class StringLog {
public:
    StringInfoData stringData;

    StringLog() : stringData()
    {
        stringData.init();
    }

    DISALLOW_COPY(StringLog);

    StringLog(StringLog &&other)
    {
        stringData.data = other.stringData.data;
        stringData.len = other.stringData.len;
        stringData.max_len = other.stringData.max_len;
        stringData.cursor = other.stringData.cursor;
        other.stringData.data = nullptr;
        other.stringData.len = 0;
        other.stringData.max_len = 0;
        other.stringData.cursor = 0;
    }

    StringLog &operator=(StringLog &&other)
    {
        stringData.data = other.stringData.data;
        stringData.len = other.stringData.len;
        stringData.max_len = other.stringData.max_len;
        stringData.cursor = other.stringData.cursor;
        other.stringData.data = nullptr;
        other.stringData.len = 0;
        other.stringData.max_len = 0;
        other.stringData.cursor = 0;
        return *this;
    }

    ~StringLog()
    {
        DstorePfreeExt(this->stringData.data);
        stringData.data = nullptr;
        stringData.len = 0;
        stringData.max_len = 0;
        stringData.cursor = 0;
    }

    const char *CString() const
    {
        if (STORAGE_VAR_NULL(stringData.data)) {
            return "";
        }
        return stringData.data;
    }
};
}
#endif  // DSTORE_STORAGE_STRING_INFO_H
