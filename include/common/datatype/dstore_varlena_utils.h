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
 * dstore_varlena_utils.h
 *
 * IDENTIFICATION
 *        dstore/include/common/datatype/dstore_varlena_utils.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_DSTORE_VARLENA_UTILS_H
#define DSTORE_DSTORE_VARLENA_UTILS_H

#include "common/dstore_datatype.h"

namespace DSTORE {

/*
 * This should be large enough that most strings will fit, but small enough
 * that we feel comfortable putting it on the stack
 */
constexpr uint16 TEXTBUFLEN = 1024;

/* VarstrCmp()
 * Comparison function for text strings with given lengths.
 * Includes locale support, but must copy strings to temporary memory
 *	to allow null-termination for inputs to strcoll().
 * Returns an integer less than, equal to, or greater than zero, indicating
 * whether arg1 is less than, equal to, or greater than arg2.
 */
int VarstrCmp(char* arg1, int len1, char* arg2, int len2, Oid collid);

/* TextCmp()
 * Internal comparison function for text strings.
 * Returns -1, 0 or 1
 */
int TextCmp(text* arg1, text* arg2, Oid collid);

}

#endif // DSTORE_STORAGE_VARLENA_UTILS_H
