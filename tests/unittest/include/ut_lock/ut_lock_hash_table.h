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
#ifndef UT_LOCK_HASH_TABLE_H
#define UT_LOCK_HASH_TABLE_H

#include "lock/dstore_lock_hash_table.h"

using namespace DSTORE;

/*
 * This class is defined to add count for LockHashTable.
 */
class LockHashTableForUT : public LockHashTable {
public:
    LockHashTableForUT() {}
    virtual ~LockHashTableForUT() {}

    uint32 GetEntryCount()
    {
        return hash_get_num_entries(this->m_lockTable);
    }
};

#endif