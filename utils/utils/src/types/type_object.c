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
 *
 * type_object.c
 *
 * Description:
 * 1. This file implements the relevant contents in the type_object.h file, including the
 *    definition of functions and the definition of global structures necessary for building
 *    the type system.
 *
 * ---------------------------------------------------------------------------------------
 */
#include "types/type_object.h"

DEFINE_TYPE_SYSTEM_MEMORY_FACILITY

/*
 * Class constructors and destructors must be defined, even if nothing is done.
 */
static ErrorCode TypeObjectInit(SYMBOL_UNUSED TypeObject *self, SYMBOL_UNUSED TypeInitParams *initData)
{ /* Empty */
    return ERROR_SYS_OK;
}

static void TypeObjectFinalize(SYMBOL_UNUSED TypeObject *self)
{ /* Empty */
}

static ErrorCode GetObjectInfo(SYMBOL_UNUSED const TypeObject *self, SYMBOL_UNUSED char info[],
                               SYMBOL_UNUSED size_t size)
{
    ASSERT(self != NULL);
    return ERROR_SYS_OK;
}

/*
 * This function must be defined, and the virtual function of the class
 * will be initialized here.
 */
static void TypeObjectOpsInit(TypeObjectOps *self)
{
    ASSERT(self != NULL);
    GET_FOPS(TypeObject)->getObjectInfo = GetObjectInfo;
}

DEFINE_ROOT_TYPED_CLASS(TypeObject)

UTILS_EXPORT ErrorCode GetTypeObjectInfo(const TypeObject *self, char info[], size_t size)
{
    ASSERT(self != NULL);
    return GET_FAP(TypeObject)->getObjectInfo(self, info, size);
}
