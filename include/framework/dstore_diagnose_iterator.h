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
 * dstore_diagnose_iterator.h
 *
 * IDENTIFICATION
 *        include/framework/dstore_diagnose_iterator.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_DIAGNOSE_ITERATOR_H
#define DSTORE_DIAGNOSE_ITERATOR_H

#include  "common/memory/dstore_mctx.h"
#include  "diagnose/dstore_diagnose.h"

namespace DSTORE {

class AbstractDiagnoseIterator : public DiagnoseIterator, public BaseObject {
public:
    DISALLOW_COPY_AND_MOVE(AbstractDiagnoseIterator);
    virtual ~AbstractDiagnoseIterator() = default;

    virtual bool Begin() = 0;
    virtual bool HasNext() = 0;
    virtual DiagnoseItem* GetNext() = 0;
    virtual void End() = 0;

protected:
    AbstractDiagnoseIterator() = default;
};

}
#endif /* DSTORE_DIAGNOSE_ITERATOR_H */
