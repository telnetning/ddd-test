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
 *        include/common/error/dstore_error.h
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_ERROR_H
#define DSTORE_ERROR_H

#include "common/dstore_datatype.h"
#include "errorcode/dstore_error_struct.h"
#include "common/memory/dstore_mctx.h"

namespace DSTORE {

class Error : public BaseObject {
public:
    DSTORE_EXPORT Error();
    Error(ErrorDetails **errorCodeMap, int errorCodeMapSize);
    ~Error()
    {
        m_error_code_map = nullptr;
    }

    DISALLOW_COPY_AND_MOVE(Error);

    DSTORE_EXPORT void ClearError();
    void SetError(const char *fileName, int lineNumber, const char *functionName, ErrorCode errorCode, ...);
    void CopyError(Error *error);
    DSTORE_EXPORT void SetErrorCodeOnly(ErrorCode errorCode);
    void SetErrorWithNodeId(const char *fileName, int lineNumber, const char *functionName, NodeId nodeId,
        ErrorCode errorCode, ...);

    ErrorCode GetErrorCode() const;
    const char *GetMessage() const;
    const char *GetErrorName() const;
    void GetLocation(const char *&fileName, int &lineNumber) const;
    void GetFunctionName(const char *&functionName) const;
    NodeId GetErrorNodeId() const;
    DSTORE_EXPORT char *GetErrorInfo() const;

    static const size_t MAX_TOKENSTRING_LEN = 256;
    static const size_t MAX_ERRMSG_LEN = MAX_TOKENSTRING_LEN * 2;
    static const size_t MAX_FILENAME_LEN = 256;
    static const size_t MAX_FUNCTIONNAME_LEN = 256;

private:
    void SetErrorCommon(const char *fileName, int lineNumber, const char *functionName, ErrorCode errorCode,
        va_list args);
    ErrorDetails  **m_error_code_map;
    int           m_error_code_map_size;
    ErrorCode     m_error_code = STORAGE_OK;
    char          m_error_msg [MAX_ERRMSG_LEN + 1] = {0};
    char          m_token_string[MAX_TOKENSTRING_LEN + 1] = {0};
    char          m_file_name[MAX_FILENAME_LEN + 1] = {0};
    int           m_line_number = 0;
    char          m_function_name[MAX_FUNCTIONNAME_LEN + 1] = {0};
    NodeId        m_errorNodeId = 0;
};

extern void StorageSetError(const char *fileName, int lineNumber, const char *functionName, ErrorCode errorCode, ...);
#define storage_set_error(...)  StorageSetError(__FILE__, __LINE__, __FUNCTION__, __VA_ARGS__);

void StorageSetErrorWithNodeId(const char *fileName, int lineNumber, const char *functionName, NodeId nodeId,
                                   ErrorCode errorCode, ...);
#define storage_set_error_with_nodeId(nodeId, ...)  StorageSetErrorWithNodeId(__FILE__, __LINE__, __FUNCTION__, nodeId, __VA_ARGS__); 

extern void StorageClearError();
extern bool StorageIsErrorSet();
extern ErrorCode StorageGetErrorCode();
extern const char *StorageGetErrorName();
extern void StorageSetErrorCodeOnly(ErrorCode errcode);
extern const char *StorageGetMessage();
extern void StorageGetFunctionName(const char *&functionName);
extern NodeId StorageGetErrorNodeId();

} /* namespace DSTORE */

#endif
