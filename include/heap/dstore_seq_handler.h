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
 * dstore_seq_handler.h
 *
 * IDENTIFICATION
 *        include/heap/dstore_seq_handler.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_SEQENCE_HANDLER_H
#define DSTORE_SEQENCE_HANDLER_H

#include "framework/dstore_instance.h"
#include "heap/dstore_heap_lock_tuple.h"
#include "page/dstore_heap_page.h"
#include "catalog/dstore_sequence_struct.h"

namespace DSTORE {

class SeqHandler final : virtual public HeapHandler {
public:
    SeqHandler(StorageInstance *instance, ThreadContext *thread, StorageRelation heapRel,
                    bool isLobOperation = false);
    ~SeqHandler() override = default;

    RetStatus GetNextValue(SeqContext *seqCtx, int128 *result);
    RetStatus SetSeqValue(SeqContext *seqCtx, bool isReset, int128 *newLastValue, bool isCalled);
    RetStatus CopySeqTuple(SeqContext *seqCtx);

private:
    RetStatus InitBufferDesc(ItemPointerData ctid, LWLockMode lockMode);
    RetStatus DoGetNextValue(SeqContext *seqCtx, int128 *result);
    RetStatus DoSetSeqValue(SeqContext *seqCtx, bool isReset, int128 *newLastValue, bool isCalled);
    RetStatus DoUpdateSeqInfo(HeapTuple *seqTuple, HeapTuple *newTuple);
    RetStatus DoUpdateSeqBuffer(HeapPage *page, OffsetNumber offset, HeapTuple *seqTuple);
    RetStatus DoCopySeqTuple(SeqContext *seqCtx);
    RetStatus AdvanceSeqValue(SeqContext *seqCtx, const FormData_gs_sequence *seqform,
        int128 &pushVal, int128 &resultVal);
    RetStatus AdvanceAscSeqValue(SeqContext *seqCtx, const FormData_gs_sequence *seqform,
        int128 &fetchNum, int128 &cachedVal, int128 &resultVal);
    RetStatus AdvanceDescSeqValue(SeqContext *seqCtx, const FormData_gs_sequence *seqform,
        int128 &fetchNum, int128 &cachedVal, int128 &resultVal);
};

} /* namespace DSTORE */

#endif
