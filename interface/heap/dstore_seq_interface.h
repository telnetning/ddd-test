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
 * dstore_seq_interface.h
 *
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/dstore/interface/seq/dstore_seq_interface.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_SEQ_INTERFACE_H
#define DSTORE_SEQ_INTERFACE_H

#include "heap/dstore_heap_struct.h"
#include "tuple/dstore_tuple_struct.h"

namespace SeqInterface {
    DSTORE::RetStatus GetNextValue(DSTORE::StorageRelation relation, DSTORE::SeqContext *seqCtx, __int128_t *result);
    DSTORE::RetStatus SetSeqValue(DSTORE::StorageRelation relation, DSTORE::SeqContext *seqCtx,
        bool isReset, __int128_t *newLastValue, bool isCalled);
    DSTORE::RetStatus GetSeqTuple(DSTORE::StorageRelation relation, DSTORE::SeqContext *seqCtx);

}; /* end SeqInterface */

#endif /* end DSTORE_SEQ_INTERFACE_H */