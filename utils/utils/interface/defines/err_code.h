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
 * err_code.h
 *
 * Description:
 * 1. define global error code .
 *
 * ---------------------------------------------------------------------------------
 */

#ifndef UTILS_ERR_CODE_H
#define UTILS_ERR_CODE_H

#include <stdint.h>
#include "defines/common.h"

GSDB_BEGIN_C_CODE_DECLS

/* -------------------------------------------------------------------------------------------------------
 * ErrorCode is global error code, defined by 64 bit. its memory structure:
 * |--------32 bit----------|-----8 bit----|-----8 bit-----|---15 bit---|---1bit--|
 *  module inner error code   module id     component id      reserved   sign bit
 *
 * Component id must be uniformly allocated. We can define module id for modules in component.
 * in addition, We also can define error code for modules that don't affect each other.
 * usage:
 * #define COMPONENT_XXXX          xx    //0x00 ~ 0x0F
 * #define MODULE_XXXX             xx    //0x00 ~ 0xFF
 * #define ERROR_{MODULE_NAME}_{DESC}    MAKE_ERROR_CODE(COMPONENT_XXXX, MODULE_XXXX, module_errno)
 *
 * for example:
 * #define COMPONENT_TENANT              0x01
 * #define MODULE_RESOURCE               0x01
 * #define MODULE_SERVICE                0x02
 *
 * #define ERROR_RESOURCE_NOT_ENOUGH     MAKE_ERROR_CODE(COMPONENT_TENANT, MODULE_RESOURCE, 0x00000001)
 * #define ERROR_RESOURCE_OUT_OF_RANGE   MAKE_ERROR_CODE(COMPONENT_TENANT, MODULE_RESOURCE, 0x00000002)
 * -----------------------------------------------------------------------------------------------------------
 */

typedef long long ErrorCode;

/* error code for ok */
#define ERROR_SYS_OK 0

#define ERROR_COMPONENT_MASK 0x0000FF0000000000
#define ERROR_MODULE_MASK    0x000000FF00000000
#define ERROR_CODE_MASK      0x00000000FFFFFFFF

#define ERROR_COMPONENT_SHIFT 40
#define ERROR_MODULE_SHIFT    32

#define MAKE_ERROR_COMPONENT(comId) ((ErrorCode)(comId) << ERROR_COMPONENT_SHIFT)
#define MAKE_ERROR_MODULE(moduleId) ((ErrorCode)(moduleId) << ERROR_MODULE_SHIFT)

/**
 * make error code macro
 */
#define MAKE_ERROR_CODE(com, module, errNo) (MAKE_ERROR_COMPONENT(com) | MAKE_ERROR_MODULE(module) | (errNo))

#define ERROR_GET_COMPONENT(_errorCode) ((((uint64_t)(_errorCode)) & ERROR_COMPONENT_MASK) >> ERROR_COMPONENT_SHIFT)
#define ERROR_GET_MODULE(_errorCode)    ((((uint64_t)(_errorCode)) & ERROR_MODULE_MASK) >> ERROR_MODULE_SHIFT)
#define ERROR_GET_CODE(_errorCode)      (((uint64_t)(_errorCode)) & ERROR_CODE_MASK)

/**
 * error code to error infomation
 */
typedef const char *(*ErrorDescInfoCallback)(uint64_t errorCode);

/**
 * get the error info of errorCode
 *
 * @param[in] errorCode    - the error code
 * @return                 - the error info string (format is MACRO-STRING:<space>error-string),
 *                           but if the error code not register, (maybe miss by developer),
 *                           will return "NOTFOUNT:undefined error code infomation"
 */
const char *GetErrorCodeInfo(uint64_t errorCode);
/* internal used by DECLEAR_ERROR_CODE_DESC_XXX */
void RegisterErrorDescInfo(uint8_t componentId, uint8_t moduleId, const char **array, size_t size);

/**
 * register the custom function of get error description information
 *
 * @param[in] componentId  - caller component id
 * @param[in] moduleId     - caller module id
 * @param[in] callback     - the function will be called when someone use GetErrorCodeInfo to get the infomation of
 *                           one error code defined by caller.
 */
void RegisterErrorDescInfoCallback(uint8_t componentId, uint8_t moduleId, ErrorDescInfoCallback callback);

/* in c++, error code must start with 0 and continuously at one module. otherwise it will appear
 * "sorry, unimplemented: non-trivial designated initializers not supported",
 * But gcc is support out-of-order initialization */
// clang-format off
#define DECLEAR_ERROR_CODE_DESC_BEGIN(componentId, moduleId) const char *errCode_##componentId##moduleId[] = {
/* [ERROR_GET_CODE(errorCode)] will lead to clang format tool report " not support Objective-C ", so we defined the
 * '[' -> LEFT_SQUARE_BRACKET and ']' -> RIGHT_SQUARE_BRACKET ugly */
#define LEFT_SQUARE_BRACKET   [
#define RIGHT_SQUARE_BRACKET  ]
#define DECLEAR_ERROR_CODE_DESC(errorCode, errorInfo)                                                                \
                        LEFT_SQUARE_BRACKET ERROR_GET_CODE(errorCode) RIGHT_SQUARE_BRACKET = #errorCode ": " errorInfo,
#define DECLEAR_ERROR_CODE_DESC_END(componentId, moduleId)   };                                                      \
__attribute__((weak, constructor)) void InitErrorCode##componentId##moduleId(void)                                   \
{                                                                                                                    \
    RegisterErrorDescInfo(componentId, moduleId, errCode_##componentId##moduleId,                                    \
                          sizeof(errCode_##componentId##moduleId) / sizeof(errCode_##componentId##moduleId[0]));     \
}
// clang-format on

GSDB_END_C_CODE_DECLS
#endif /* UTILS_ERR_CODE_H */
