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

 * Description: CloudNativeDatabase CommonPersistentHandler(Common data structure persistent class)
 */

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <assert.h>
#include "common_persistence_handler.h"
#include "securec.h"
#include <sys/file.h>

CommonPersistentHandler::CommonPersistentHandler()
    : m_fd(-1), m_obj(nullptr), m_init(false), m_size(0)
{}

CommonPersistentHandler::~CommonPersistentHandler()
{
    Sync();
    Close();
}
bool CommonPersistentHandler::IsExist(const char *filePath)
{
    return access(filePath, 0) == 0;
}

void CommonPersistentHandler::Create(const char *filePath, void* obj, size_t size)
{
    assert(!IsExist(filePath));
    if (m_init) {
        return;
    }
    m_fd = open(filePath, O_RDWR | O_CREAT | O_APPEND, (S_IRUSR | S_IWUSR));
    long nwrite = write(m_fd, obj, size);
    if (nwrite == -1) {
        assert(0);
    }
    (void)fsync(m_fd);
    m_size = size;
    m_obj = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, m_fd, 0);
    m_init = true;
}

void CommonPersistentHandler::Open(const char *filePath)
{
    assert(IsExist(filePath));
    if (m_init) {
        return;
    }
    struct stat statBuf;
    if (lstat(filePath, &statBuf) < 0) {
        perror(filePath);
        assert(0);
    }
    m_size = statBuf.st_size;

    m_fd = open(filePath, O_RDWR | O_APPEND, (S_IRUSR | S_IWUSR));
    if (m_fd < 0) {
        perror(filePath);
        assert(0);
    }
    m_obj = mmap(nullptr, m_size, PROT_READ | PROT_WRITE, MAP_SHARED, m_fd, 0);
    m_init = true;
}

void CommonPersistentHandler::Write()
{
    if (m_fd != -1) {
        long nwrite = write(m_fd, m_obj, m_size);
        if (nwrite == -1) {
            assert(0);
        }
        (void)fsync(m_fd);
    }
}

void CommonPersistentHandler::Sync()
{
    flock(m_fd, LOCK_EX);
    if(m_obj) {
        (void)msync(m_obj, m_size, MS_SYNC);
    }
    flock(m_fd, LOCK_UN);
}

void CommonPersistentHandler::Close()
{
    if (m_obj) {
        (void)munmap(m_obj, m_size);
    }
    if (m_fd != -1) {
        (void)close(m_fd);
    }
    m_init = false;
}
