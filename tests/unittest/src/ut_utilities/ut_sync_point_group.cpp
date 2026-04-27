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
#include "ut_utilities/ut_sync_point_group.h"

#include <string>
#include <fcntl.h>
#include <string.h>

SyncPointGroup::SyncPointGroup(int num):m_syncPoints{nullptr}, m_number{num}
{
    m_syncPoints = (sem_t**)malloc(sizeof(sem_t*) * num);
    for (int i = 0; i < num; i++) {
        std::string semNameStr = "/SyncPointGroups" + std::to_string(i) + "_" + std::getenv("USER");
        const char *semName = semNameStr.c_str();
        /* Unlink the old semaphore if exists. */
        sem_unlink(semName);
        m_syncPoints[i] = sem_open(semName, O_CREAT, S_IRWXO, 0);
        if (m_syncPoints[i] == SEM_FAILED) {
            printf("Create semaphore %s failed, error code: %s\n", semName, strerror(errno));
            assert(false);
        }
        sem_init(m_syncPoints[i], 1, 0);
    }
}

SyncPointGroup::~SyncPointGroup()
{
    for (int i = 0; i < m_number; i++) {
        sem_close(m_syncPoints[i]);
        std::string semNameStr = "/SyncPointGroups" + std::to_string(i) + "_" + std::getenv("USER");
        const char *semName = semNameStr.c_str();
        sem_unlink(semName);
    }
    free(m_syncPoints);
}

void SyncPointGroup::SyncPoint(int id)
{
    assert(id <= m_number);
    assert(id > 0);

    int index = id - 1;

    if (index == 0) {
        for (int i = 1; i < m_number; i++) {
            assert(sem_wait(m_syncPoints[index]) >= 0);
        }
        for (int i = 1; i < m_number; i++) {
            assert(sem_post(m_syncPoints[i]) >= 0);
        }
    } else {
        assert(sem_post(m_syncPoints[0]) >= 0);
        assert(sem_wait(m_syncPoints[index]) >= 0);
    }
}