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
#include "common/dstore_datatype.h"
#include "ut_utilities/ut_config_file.h"
#include <fstream>
#include <sstream>
#include "unistd.h"

using namespace std;
using namespace DSTORE;

void UTConfigFile::SetUpFilePath()
{
    /* config file is at the same directory as unittest. */
    const int PATH_SIZE = 1024;
    char executeName[PATH_SIZE];
    int rslt = readlink("/proc/self/exe", executeName, PATH_SIZE);
    StorageAssert(rslt > 0);
    std::string fullPath(executeName);
    size_t pos = fullPath.find_last_of("/");
    std::string dir = fullPath.substr(0, pos);
    m_filePath = dir + "/config";
}

string UTConfigFile::ReadString(string key)
{
    ifstream file(m_filePath);
    string line;
    string lineKey;
    string lineValue;

    if (file.fail()) {
        printf("If you see this, config file:%s may not have been installed correctly. "
               "Try 'make install' after 'make' command, or click Build->Install button if you are using clion.\n",
               m_filePath.c_str());
        return "";
    }

    while (getline(file, line)) {
        istringstream lineStream(line);
        if (getline(lineStream, lineKey, '=')) {
            if (lineKey != key) {
                continue;
            }
            if (getline(lineStream, lineValue)) {
                return lineValue;
            }
        }
    }

    return "";
}

int UTConfigFile::ReadInteger(string key)
{
    string value = ReadString(key);
    if (value.empty()) {
        return -1;
    }
    return stoi(value);
}

