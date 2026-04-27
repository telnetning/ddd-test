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
#include <cstdlib>
#include <cstdio>
#include "common/error/dstore_error.h"

using namespace DSTORE;

int main(int argc, char **argv)
{
    const int numArgsRequired = 2;
    if (argc != numArgsRequired) {
        (void)printf("Usage:\n whatiserrcode <errorCode>\n errorCode should be a dec value or hex value.\n");
        return -1;
    }

    if (STORAGE_FUNC_FAIL(CreateMemoryContextForTool("whatiserrcode memcontext"))){
        printf("Failed to create memory context for tool\n");
        return -1;
    }
    Error err;
    err.SetErrorCodeOnly(static_cast<ErrorCode>(std::strtoll(argv[argc-1], nullptr, 0)));
    char *errorInfo = err.GetErrorInfo();
    if (errorInfo == nullptr) {
        printf("Wrong error code specified\n");
        printf("Usage:\n whatiserrcode <errorCode>\n errorCode should be a dec value or hex value.\n");
        return -1;
    }
    (void)printf("%s", errorInfo);
    err.ClearError();

    return 0;
}
