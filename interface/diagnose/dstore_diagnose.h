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
 * dstore_diagnose.h
 *
 * IDENTIFICATION
 *        dstore/interface/diagnose/dstore_diagnose.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_DIAGNOSE_H
#define DSTORE_DIAGNOSE_H
#include "common/dstore_common_utils.h"

namespace DSTORE {

struct DiagnoseItem {
public:
    ~DiagnoseItem() = default;

protected:
    DiagnoseItem() = default;
};

class DiagnoseIterator {
public:
    DISALLOW_COPY_AND_MOVE(DiagnoseIterator);
    virtual ~DiagnoseIterator() = default;

    /* Do initialization for the iterator, FALSE means some error. */
    virtual bool Begin() = 0;

    /* While FALSE, it is required to check the error code. */
    virtual bool HasNext() = 0;

    /*
     * Do invoke HasNext before GetNext.
     * While nullptr, it is required to check the error code.
     */
    virtual DiagnoseItem* GetNext() = 0;

    /* clear for the iterator. */
    virtual void End() = 0;

protected:
    DiagnoseIterator() = default;
};

}
#endif /* DSTORE_DIAGNOSE_H */
