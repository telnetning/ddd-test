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
#ifndef COMMON_PERSISTENCE_HANDLER_H
#define COMMON_PERSISTENCE_HANDLER_H
#include <cstdint>

class CommonPersistentHandler {
public:
    CommonPersistentHandler();
    ~CommonPersistentHandler();

    bool IsExist(const char *filePath);
    void Create(const char *filePath, void* obj, size_t size);
    void Open(const char *filePath);
    void Write();
    void Sync();
    void Close();

    inline void *GetObject() const
    {
        return m_obj;
    }
    inline size_t GetSize() const {
        return m_size;
    }

public:
    int             m_fd;
    void            *m_obj;
    bool            m_init = false;
    size_t           m_size;
};
#endif