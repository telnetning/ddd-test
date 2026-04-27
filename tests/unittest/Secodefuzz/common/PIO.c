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
/*

本模块对外提供读写文件操作
屏蔽系统差异

不需要初始化函数，可重入


*/

#include "PCommon.h"

#ifdef HAS_IO
//#if LIBFUZZER_POSIX

void ReadFromFile(char** data, int* len, char* Path)
{
    FILE* fp;
    int filesize;

    if ((fp = fopen(Path, "rb")) == NULL) {
        *len = 0;
        *data = NULL;
        return;
    }

    fseek(fp, 0, SEEK_END);
    filesize = ftell(fp);

    *data = (char*)hw_Malloc(filesize + 1);
    (*data)[filesize] = 0;

    // rewind(fp);
    fseek(fp, 0L, SEEK_SET);
    fread(*data, 1, filesize, fp);
    fclose(fp);

    //需要返回真实的文件长度
    *len = filesize;
    return;
}

void WriteToFile(char* data, int len, char* Path)
{
    // Use raw C interface because this function may be called from a sig handler.
    FILE* Out = fopen(Path, "w");
    if (!Out)
        return;
    fwrite(data, len, 1, Out);
    fclose(Out);
}

void WriteToFileFail(char* data, int len, char* Path)
{
    // Use raw C interface because this function may be called from a sig handler.
    FILE* Out = fopen(Path, "a+");
    if (!Out)
        return;
    fprintf(Out, "%s", data);
    fclose(Out);
}
//#endif//LIBFUZZER_POSIX

#else
//如果不支持，对外提供的函数不做动作
void WriteToFile(char* data, int len, char* Path)
{
    return;
}
void WriteToFileFail(char* data, int len, char* Path)
{
    return;
}
void ReadFromFile(char** data, int* len, char* Path)
{
    *len = 0;
    *data = NULL;
    return;
}

#endif  // LIBFUZZER_POSIX
