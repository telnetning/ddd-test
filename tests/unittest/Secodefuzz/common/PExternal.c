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

所有对外依赖的接口
因为最后提供的lib可能会被裁剪系统，最小系统使用，所以要做到尽量少的依赖
这里把所有对外的依赖封装，供移植方便

不允许在其他文件里直接调用外部函数，会直接降低整体代码的可移植性
*/
#include "PCommon.h"

// 无系统库注释16行
//#define     LIBDEFINE

// malloc
// free
// memset
// memcpy
// memmove
// srand
// rand
// time

#undef RAND_MAX
#define RAND_MAX 0x7fffffff

char g_date[40] = {0};

//返回离1970年的秒数
int hw_Get_Time(void)
{
#ifdef LIBDEFINE
    time_t t;
    int j;
    j = time(&t);
    return j;
#else
    return 0;
#endif
}

char* hw_Get_Date(void)
{
#ifdef LIBDEFINE
    time_t timep;
    struct tm* p;

    time(&timep);
    p = localtime(&timep);
    g_date[0] = 0;

    hw_sprintf(g_date,
        "%d_%02d%02d_%02d_%02d_%02d",
        1900 + p->tm_year,
        1 + p->tm_mon,
        p->tm_mday,
        p->tm_hour,
        p->tm_min,
        p->tm_sec);
    return g_date;
#else
    hw_sprintf(g_date, "2000_0000_00_00_00");
    return g_date;
#endif
}
int hw_time()
{
#ifdef LIBDEFINE
    return (int)time(NULL);
#else
    return 1;
#endif
}

// size不能为小于0,最大值不能大于MALLOC_MAX=0xFFFF
// malloc
char* hw_Malloc(size_t size)
{

#ifndef __KERNEL__
    char* ptr = (char*)malloc(size);
#else
    char* ptr = (char*)kmalloc(size, GFP_KERNEL);
#endif
    return ptr;
}
// free
void hw_Free(void* ptemp)
{
#ifndef __KERNEL__
    free(ptemp);
#else
    kfree(ptemp);
#endif
}

// memset
void* hw_Memset(void* s, int ch, size_t n)
{
    return memset(s, ch, n);
}

// memcpy
void* hw_Memcpy(void* dest, const void* src, size_t n)
{
    return memcpy(dest, src, n);
}
// memmove
void* hw_Memmove(void* dest, const void* src, size_t n)
{
    return memmove(dest, src, n);
}

int hw_Memcmp(const void* buf1, const void* buf2, unsigned int count)
{
    return memcmp(buf1, buf2, count);
}

long int hw_Strtol(const char* nptr, char** endptr, int base)
{
#ifndef __KERNEL__
    return (strtol(nptr, endptr, base));
#else
    return (simple_strtol(nptr, endptr, base));
#endif
}

int hw_Strcmp(const char* s1, const char* s2)
{
    return (strcmp(s1, s2));
}

int hw_RAND_MAX()
{
    return RAND_MAX;
}
// rand

#ifdef LIBDEFINE
#else
static unsigned int next = 1;
#endif
//实现rand()函数，在网上找的函数原型，随机性并没有glibc里边的强
//貌似没有加锁，可能多线程会有问题，谁知道呢
int hw_Rand()
{
#ifdef LIBDEFINE
    return rand();
#else
    next = next * 1103515245 + 12345;
    return (*(unsigned int*)&next) & 0x7FFFFFFF;
#endif
}
// srand
//实现srand函数
void hw_Srand(unsigned int temp)
{
#ifdef LIBDEFINE
    srand(temp);
#else
    next = temp;
#endif
}
