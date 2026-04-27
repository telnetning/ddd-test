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
 * dstore_data_tuple.h
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/dstore/include/tuple/dstore_data_tuple.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_GAUSSKERNEL_DSTORE_INCLUDE_TUPLE_DSTORE_DATA_TUPLE_H
#define SRC_GAUSSKERNEL_DSTORE_INCLUDE_TUPLE_DSTORE_DATA_TUPLE_H

#include <cstddef>
#include "catalog/dstore_fake_attribute.h"
#include "common/dstore_datatype.h"
#include "tuple/dstore_tuple_struct.h"

namespace DSTORE {

constexpr uint8 INVALID_TD_SLOT = 0xFF;


enum TupleTdStatus : uint8 {
    ATTACH_TD_AS_NEW_OWNER = 0, /* can directly fetch info from td slot */
    ATTACH_TD_AS_HISTORY_OWNER, /* need fetch info from undo */
    DETACH_TD,                  /* disconnect with tdslot, decide by csn value */
};

struct DisassembleDataContext {
    TupleAttrContext &attrContext;
    int start;
    int end;
    char *tupleValues;
    char *nullBits;
    char *lobValues;
};

struct CalculateOffsetSlowContext {
    int attNum;
    int natts;
    Form_pg_attribute *att;
    int &off;
    char *tp;
    bool needCheckLobValue;
    int &loboff;
};

template <typename TupleType>
struct AssembleDataContext {
    Datum value;
    TupleType *diskTuple;
    Form_pg_attribute att;
    char *&tupleValues;
    Size remainLength;
    bool hasLob;
    char *&lobValues;
};

struct DataTuple {
public:
    static Size ComputeDataSize(TupleDesc desc, const Datum *values, const bool *isnull);

    static Size ComputeDataSize(TupleDesc desc, const Datum *values, const bool *isnull, Size &lobSize);

    /*
     * GetBitmapLen(int nAttrs)
     *      Computes size of null bitmap given number of data columns.
     */
    static uint32 GetBitmapLen(int nAttrs)
    {
        return static_cast<uint32>(
            (static_cast<uint32>(nAttrs) + static_cast<uint32>(BYTE_TO_BIT_MULTIPLIER - 1U)) / BYTE_TO_BIT_MULTIPLIER);
    }

    template<typename TupleType>
    void CalculateOffset(int attNum, int natts, Form_pg_attribute *att, int& off);

    template <typename TupleType>
    void CalculateOffsetSlow(CalculateOffsetSlowContext &context);

    inline static bool DataTupleAttrIsNull(uint32 attnum, char *nullbit)
    {
        return !(static_cast<uint32>(nullbit[attnum >> BYTE_TO_BIT_SHIFT]) & (1U << (attnum & 0x07)));
    }

    template <bool hasnulls>
    static void DisassembleData(DisassembleDataContext &context);

    static Datum DisassembleColumnData(TupleDesc tupleDesc, Form_pg_attribute att,
                                       char *tupleValues, char *&lobValues);

    template <bool hasNull, typename TupleType>
    static void AssembleData(TupleDesc tupleDesc, Datum *values, const bool *isnull, char *tuple, Size dataSize);

    template <typename TupleType>
    static Size AssembleColumnData(AssembleDataContext<TupleType> &context);
};

}  // namespace DSTORE
#endif /* SRC_GAUSSKERNEL_DSTORE_INCLUDE_TUPLE_STORAGE_DATA_TUPLE_H */
