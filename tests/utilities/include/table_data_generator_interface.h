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

#ifndef BASE_TABLE_DATA_GENERATOR_H
#define BASE_TABLE_DATA_GENERATOR_H

#include "common/dstore_common_utils.h"
using namespace DSTORE;
 
enum Rule{
    NotNull,
    SetMinVal,
    SetMaxVal,
    SetLen
};

class TableDataGeneratorInterface
{
public:
    virtual ~TableDataGeneratorInterface() = default;
    virtual bool Create(Oid colTypes[], uint32_t colNum) = 0;
    virtual bool Describe(uint32_t colNum, Rule rule, int64_t value = 0) = 0;
    virtual bool GenerateData(uint32_t rowNum) = 0;
};
#endif
