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
 */

#ifndef UT_ERROR_CODE_MAP_H
#define UT_ERROR_CODE_MAP_H

#include "common/dstore_datatype.h"
#include "ut_error_code.h"

namespace DSTORE {

static ErrorDetails ut_error_code_map[] = {
    [STORAGE_OK] = {"STORAGE_OK", ""},
    [ERROR_GET_CODE(UT_ERROR_BASIC)] = {"UT_ERROR_BASIC", "Testing Error without tokens"},
    [ERROR_GET_CODE(UT_ERROR_PERCENT)] = {"UT_ERROR_PERCENT", "Testing Error ends with %%"},
    [ERROR_GET_CODE(UT_ERROR_PERCENT2)] ={"UT_ERROR_PERCENT2", "Testing Error including %% in the middle"},
    [ERROR_GET_CODE(UT_ERROR_WITH_TOKEN)] ={"UT_ERROR_WITH_TOKEN", "%s is my token"},
    [ERROR_GET_CODE(UT_ERROR_TOKEN_IN_THE_END)] ={"UT_ERROR_TOKEN_IN_THE_END", "My token is in the end: %s"},
    [ERROR_GET_CODE(UT_ERROR_MULTIPLE_TOKENS)] ={"UT_ERROR_MULTIPLE_TOKENS", "The first token: %s and the second token: %s and the third token: %s"},
    [ERROR_GET_CODE(UT_ERROR_LONG_MESSAGE)] ={"UT_ERROR_LONG_MESSAGE",
      "This is a really long message. Just repeat the message until it exceeds 512 characters.\
       This is a really long message. Just repeat the message until it exceeds 512 characters.\
       This is a really long message. Just repeat the message until it exceeds 512 characters.\
       This is a really long message. Just repeat the message until it exceeds 512 characters.\
       This is a really long message. Just repeat the message until it exceeds 512 characters.\
       This is a really long message. Just repeat the message until it exceeds 512 characters."},
    [ERROR_GET_CODE(UT_ERROR_TOKEN_PERCENTD)] ={"UT_ERROR_TOKEN_PERCENTD", "Testing %%d token i1 = %d, i2 = %d"},
    [ERROR_GET_CODE(UT_ERROR_TOKEN_SINGLE_PERCENT)] ={"UT_ERROR_TOKEN_SINGLE_PERCENT", "A single percent % in the middle"}
};

static ErrorDetails *g_ut_error_code_map[] = {
    [MODULE_UT] = ut_error_code_map
};

} /* namespace DSTORE */

#endif
