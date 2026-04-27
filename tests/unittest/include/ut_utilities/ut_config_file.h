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
#ifndef DSTORE_UT_CONFIG_FILE_H
#define DSTORE_UT_CONFIG_FILE_H

#include <string>

class UTConfigFile {
public:
    UTConfigFile() = default;
    ~UTConfigFile() = default;

    void SetUpFilePath();

    std::string ReadString(std::string key);
    int         ReadInteger(std::string key);

private:
    std::string m_filePath = "";
};

#endif //DSTORE_UT_CONFIG_FILE_H
