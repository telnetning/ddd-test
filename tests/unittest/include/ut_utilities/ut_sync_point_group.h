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
#ifndef DSTORE_UT_SYNC_POINT_GROUP_H
#define DSTORE_UT_SYNC_POINT_GROUP_H

#include <semaphore.h>
#include <assert.h>

class SyncPointGroup {
public:
    SyncPointGroup(int num);
    ~SyncPointGroup();

    void SyncPoint(int id);

private:
    sem_t ** m_syncPoints;
    int m_number;
};

#endif //DSTORE_UT_CONFIG_FILE_H