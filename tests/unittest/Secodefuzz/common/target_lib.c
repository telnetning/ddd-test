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

#define _GNU_SOURCE
#include <stdio.h>
#include <string.h>
#include <stdlib.h>  ///

#include <stdint.h>

typedef uint8_t u8;
typedef uint16_t u16;
typedef uint32_t u32;
#ifdef __x86_64__  //这是什么鬼，估计到产品线得改
typedef unsigned long long u64;
#else
typedef uint64_t u64;
#endif /* ^sizeof(...) */

typedef int8_t s8;
typedef int16_t s16;
typedef int32_t s32;
typedef int64_t s64;

int a001 = 0;
int a002 = 0;
int a003 = 0;
int a004 = 0;
int a005 = 0;
int a006 = 0;
int a007 = 0;
int a008 = 0;
int a009 = 0;
int a010 = 0;

int a011 = 0;
int a012 = 0;
int a013 = 0;
int a014 = 0;
int a015 = 0;
int a016 = 0;
int a017 = 0;
int a018 = 0;
int a019 = 0;
int a020 = 0;

int a021 = 0;
int a022 = 0;
int a023 = 0;
int a024 = 0;
int a025 = 0;
int a026 = 0;
int a027 = 0;
int a028 = 0;
int a029 = 0;
int a030 = 0;

int aaa[1000000] = {0};

void clean_a(void)
{

    a001 = 0;
    a002 = 0;
    a003 = 0;
    a004 = 0;
    a005 = 0;
    a006 = 0;
    a007 = 0;
    a008 = 0;
    a009 = 0;
    a010 = 0;

    a011 = 0;
    a012 = 0;
    a013 = 0;
    a014 = 0;
    a015 = 0;
    a016 = 0;
    a017 = 0;
    a018 = 0;
    a019 = 0;
    a020 = 0;

    a021 = 0;
    a022 = 0;
    a023 = 0;
    a024 = 0;
    a025 = 0;
    a026 = 0;
    a027 = 0;
    a028 = 0;
    a029 = 0;
    a030 = 0;
}

int fun_branch(s32 tint, s8 tint1, char* tchar, char* tchar1)
{

    if (tint == 1234)
        return 2;

    //验证random
    if (tint1 == 115) {
        if (a001 == 0) {
            a001 = 1;
            printf("---------------------------a001 pass\r\n");
        }
    }

    //验证hook_memcmp
    if ((memcmp(tchar1, "888889", 6)) == 0) {
        if (a002 == 0) {
            a002 = 1;
            printf("---------------------------a002 pass\r\n");
        }
    }

    //验证hook_strcmp
    if ((strcmp(tchar1, "qwerty")) == 0) {
        if (a003 == 0) {
            a003 = 1;
            printf("---------------------------a003 pass\r\n");
        }
    }

    //验证hook_strncmp
    if ((strncmp(tchar1, "strncmp", 6)) == 0) {
        if (a004 == 0) {
            a004 = 1;
            printf("---------------------------a004 pass\r\n");
        }
    }
#ifndef _WIN32
    //验证hook_strcasecmp
    if ((strcasecmp(tchar, "hook_strcasecmp")) == 0) {
        if (a005 == 0) {
            a005 = 1;
            printf("---------------------------a005 pass\r\n");
        }
    }

    //验证hook_strncasecmp
    if ((strncasecmp(tchar, "yanzhengHHC", 11)) == 0) {
        if (a006 == 0) {
            a006 = 1;
            printf("---------------------------a006 pass\r\n");
        }
    }
#endif
    //验证cxx.dict
    if ((memcmp(tchar, "12345abcde", 10)) == 0) {
        if (a007 == 0) {
            a007 = 1;
            printf("---------------------------a007 pass\r\n");
        }
    }

    //验证cmp
    if (tint == 0x31313232) {
        if (a008 == 0) {
            a008 = 1;
            printf("---------------------------a008 pass\r\n");
        }
    }

    //验证switch
    switch (tint) {
        case 0x1278:
            if (a009 == 0) {
                a009 = 1;
                printf("---------------------------a009 pass\r\n");
            }
            break;
        case 45671234:
            if (a010 == 0) {
                a010 = 1;
                printf("---------------------------a010 pass\r\n");
            }
            break;
    }

    //验证div
    int ccc = 566667;
    if (12345678 / ccc - tint == 0) {
        if (a011 == 0) {
            a011 = 1;
            printf("---------------------------a011 pass\r\n");
        }
    }

    //验证gep

    aaa[65578] = 0x8667799;
    if ((0 < tint) && (tint < 1000000)) {
        if (aaa[tint] == aaa[65578])
            if (a012 == 0) {
                a012 = 1;
                printf("---------------------------a012 pass\r\n");
            }
    }

    //验证strstr
    char ddd[15];
    memcpy(ddd, tchar1, 15);
    ddd[14] = 0;
    if (strstr(ddd, "FUZZ")) {
        if (a013 == 0) {
            a013 = 1;
            printf("---------------------------a013 pass\r\n");
        }
    }
#ifndef _WIN32
    //验证strcasestr
    if (strcasestr(tchar1, "aBcD")) {
        if (a014 == 0) {
            a014 = 1;
            printf("---------------------------a014 pass\r\n");
        }
    }

    //验证memmem
    if (memmem(tchar1, 15, "kuku", 4)) {
        if (a015 == 0) {
            a015 = 1;
            printf("---------------------------a015 pass\r\n");
        }
    }
#endif
    //验证多参数
    if ((memcmp(tchar, "AddCorpus", 9)) == 0) {
        if (a020 == 0) {
            a020 = 1;
            printf("---------------------------a020 pass\r\n");
        }

        //验证样本
        if ((memcmp(tchar1, "5432111", 7)) == 0) {
            if (a021 == 0) {
                a021 = 1;
                printf("---------------------------a021 pass\r\n");
            }

            //验证DT_LibFuzzAddCorpus
            if (tint == 0x44557788) {
                if (a022 == 0) {
                    a022 = 1;
                    printf("---------------------------a022 pass\r\n");
                }

                //验证random
                if (tint1 == 111) {
                    if (a023 == 0) {
                        printf("---------------------------a023 pass\r\n");
                        // exit(0);
                        //*(int *)0 = 0;
                        a023 = 1;
                        // int a[10]={0};a[5]=a[10];
                        {  // test timeout
                           //	int i=0,j=0,k=0;
                           //	for( i=0;i<10000;i++)
                           //		for( j=0;j<100000;j++)
                           //			k++;
                        }
                    }
                }
            }
        }
    }

    return 1;
}

int fun_multype(int numberRange, int numberEnum, s32 tint, char* tchar, char* stringenum)
{
    if (numberRange == 0x1278) {
        if (a001 == 0) {
            a001 = 1;
            printf("---------------------------a001 pass\r\n");
        }
    }

    if ((strcmp(stringenum, "zhangpeng")) == 0) {
        if (a002 == 0) {
            a002 = 1;
            printf("---------------------------a002 pass\r\n");
        }
    }

    if (numberEnum == 0x456789) {
        //验证hook_memcmp
        if ((memcmp(tchar, "888889", 6)) == 0) {
            if (a003 == 0) {
                a003 = 1;
                printf("---------------------------a003 pass\r\n");
                // int a[10]={0}; a[5] =a[10];
            }
        }

        //验证hook_strcmp
        if ((strcmp(tchar, "qwerty")) == 0) {
            if (a004 == 0) {
                a004 = 1;
                printf("---------------------------a004 pass\r\n");
            }
        }

        //验证hook_strncmp
        if ((strncmp(tchar, "strncmp", 6)) == 0) {
            if (a005 == 0) {
                a005 = 1;
                printf("---------------------------a005 pass\r\n");
            }
        }
#ifndef _WIN32
        //验证hook_strcasecmp
        if ((strcasecmp(tchar, "hook_strcasecmp")) == 0) {
            if (a006 == 0) {
                a006 = 1;
                printf("---------------------------a006 pass\r\n");
            }
        }

        //验证hook_strncasecmp
        if ((strncasecmp(tchar, "yanzhengHHC", 11)) == 0) {
            if (a007 == 0) {
                a007 = 1;
                printf("---------------------------a007 pass\r\n");
            }
        }

#endif

        //验证CMP
        if (tint == 84821838) {
            if (a008 == 0) {
                a008 = 1;
                printf("---------------------------a008 pass\r\n");
            }
        }

        //验证switch
        switch (tint) {
            case 0x1278:
                if (a009 == 0) {
                    a009 = 1;
                    printf("---------------------------a009 pass\r\n");
                }
                break;
            case 45671234:
                if (a010 == 0) {
                    a010 = 1;
                    printf("---------------------------a010 pass\r\n");
                }
                break;
        }

        //验证div
        int ccc = 566667;
        if (12345678 / ccc - tint == 0) {
            if (a011 == 0) {
                a011 = 1;
                printf("---------------------------a011 pass\r\n");
            }
        }

        //验证gep

        aaa[65578] = 0x8667799;
        if ((0 < tint) && (tint < 1000000)) {
            if (aaa[tint] == aaa[65578])
                if (a012 == 0) {
                    a012 = 1;
                    printf("---------------------------a012 pass\r\n");
                }
        }

        //验证strstr
        char ddd[15];
        memcpy(ddd, tchar, 15);
        ddd[14] = 0;
        if (strstr(ddd, "FUZZ")) {
            if (a013 == 0) {
                a013 = 1;
                printf("---------------------------a013 pass\r\n");
            }
        }
#ifndef _WIN32
        //验证strcasestr
        if (strcasestr(tchar, "aBcD")) {
            if (a014 == 0) {
                a014 = 1;
                printf("---------------------------a014 pass\r\n");
            }
        }

        //验证memmem
        if (memmem(tchar, 15, "kuku", 4)) {
            if (a015 == 0) {
                a015 = 1;
                printf("---------------------------a015 pass\r\n");
            }
        }
#endif
    }

    //验证多参数组合分支
    if (numberEnum == 0x897654) {

        if ((memcmp(tchar, "AddCorpus", 9)) == 0) {
            if (a020 == 0) {
                a020 = 1;
                printf("---------------------------a020 pass\r\n");
            }

            if ((memcmp(tchar + 9, "5432111", 7)) == 0) {
                if (a021 == 0) {
                    a021 = 1;
                    printf("---------------------------a021 pass\r\n");
                }

                if (tint == 0x44557788) {
                    if (a022 == 0) {
                        a022 = 1;
                        printf("---------------------------a022 pass\r\n");
                    }

                    //验证random
                    if ((strcmp(stringenum, "wanghao")) == 0) {
                        if (a023 == 0) {
                            printf("---------------------------a023 pass\r\n");

                            // exit(0);
                            a023 = 1;
                            // int a[10]={0};
                            // a[5]=a[10];
                            // char * tempp ;
                            // a021 = *tempp;
                        }
                    }
                }
            }
        }
    }
    return 1;
}

int fun_bug(int number1, int number2, char* stringe1, char* stringe2)
{

    //验证多参数组合分支
    if (number1 == 0x897654) {
        if (number2 == 0x3145926) {
            if ((strlen(stringe1) >= 7) && (memcmp(stringe1, "5432111", 7) == 0)) {
                if ((strcmp(stringe2, "wanghao")) == 0) {

                    printf("----bug has been found\r\n");
                    int a[10] = {0};
                    a[5] = a[10];
                }
            }
        }
    }
    return 1;
}

int fun_example(int number, char* string, char* buf)
{

    //验证多参数组合分支
    if (number == 0x123455) {
        if (a001 == 0) {
            a001 = 1;
            printf("---------------------------a001 number  pass\r\n");
        }
    }

    if ((strcmp(string, "zhangpengzhangpengzhangpengzhangpengzhangpengzhangpengzhangpeng")) == 0) {

        if (a002 == 0) {
            a002 = 1;
            printf("---------------------------a002 string  pass\r\n");
        }
    }

    if (memcmp(buf, "aaaaaabcde", 10) == 0) {

        if (a003 == 0) {
            a003 = 1;
            printf("---------------------------a003 blob  pass\r\n");
        }
    }
    return 1;
}

int fun_RightRange(int number, char* stringe)
{

    //验证多参数组合分支
    if (number == 0x1234) {
        if ((strcmp(stringe, "wanghao")) == 0) {
            printf("----bug has been found\r\n");
            return 1;
        }
    }
    return 0;
}

int fun_for(s32 tint, char* tchar)
{
    if (tint > 10000)
        return 0;
    int j = 0;
    int k = 1;
    int i = 0;

    for (i = 0; i < tint; i++) {
        if (k == 1)
            j++;
        else if (k == 0)
            j--;
    }
    return 0;
}

int fun_cycle(char* tchar)
{
    int i = 0;
    int k = 0;
    int len = strlen(tchar);

    for (i = 0; i < len; i++) {
        k = (k + 1) * (k + 2);
    }
    return 0;
}

/******************************************

调用测试例,ok

******************************************/

#define MIN(_a, _b) ((_a) > (_b) ? (_b) : (_a))

static volatile int Sink;
int Switch(const uint8_t* Data, size_t Size)
{
    int X;
    if (Size < sizeof(X))
        return 0;
    memcpy(&X, Data, sizeof(X));
    switch (X) {
        case 1:
            Sink = __LINE__;
            break;
        case 101:
            Sink = __LINE__;
            break;
        case 1001:
            Sink = __LINE__;
            break;
        case 10001:
            Sink = __LINE__;
            break;
        case 100001:
            Sink = __LINE__;
            break;
        case 1000001:
            Sink = __LINE__;
            break;
        case 10000001:
            Sink = __LINE__;
            break;
        case 100000001:
            return 1;
    }
    return 0;
}

int ShortSwitch(const uint8_t* Data, size_t Size)
{
    short X;
    if (Size < sizeof(short))
        return 0;
    memcpy(&X, Data, sizeof(short));
    switch (X) {
        case 42:
            Sink = __LINE__;
            break;
        case 402:
            Sink = __LINE__;
            break;
        case 4002:
            Sink = __LINE__;
            break;
        case 5002:
            Sink = __LINE__;
            break;
        case 7002:
            Sink = __LINE__;
            break;
        case 9002:
            Sink = __LINE__;
            break;
        case 14002:
            Sink = __LINE__;
            break;
        case 21402:
            return 1;
    }
    return 0;
}

int LLVMFuzzerTestOneInput1(const uint8_t* Data, size_t Size)
{
    if (Size >= 4 && Switch(Data, Size) && Size >= 12 && Switch(Data + 4, Size - 4) && Size >= 14 &&
        ShortSwitch(Data + 12, 2)) {
        printf("----------------------------test1 ok!!!!!!!!!");
        return 1;
    }
    return 0;
}

int LLVMFuzzerTestOneInput2(const uint8_t* Data, size_t Size)
{

    if (Size > 0 && Data[0] == 'H') {
        Sink = 1;
        if (Size > 1 && Data[1] == 'i') {
            Sink = 2;
            if (Size > 2 && Data[2] == '!') {
                printf("----------------------------test2 ok!!!!!!!!!");
                return 1;
            }
        }
    }
    return 0;
}

int LLVMFuzzerTestOneInput3(const uint8_t* Data, size_t Size)
{
    const char* S = (const char*)Data;
    if (Size >= 8 && strncmp(S, "123", 8))
        Sink = 1;
    if (Size >= 8 && strncmp(S, "01234567", 8) == 0) {
        if (Size >= 12 && strncmp(S + 8, "ABCD", 4) == 0) {
            if (Size >= 14 && strncmp(S + 12, "XY", 2) == 0) {
                if (Size >= 17 && strncmp(S + 14, "KLM", 3) == 0) {
                    printf("----------------------------test3 ok!!!!!!!!!");
                    return 1;
                }
            }
        }
    }
    return 0;
}

/******************************************

两个被测函数

******************************************/

int funtesta(s32 tint, char* tchar)
{
    if (tint == 1234)
        return 2;

    return 1;
}

int funtestb(s32 tint, char* tipv4)
{
    if (tint == 1234)
        return 2;

    return 1;
}

typedef struct {
    int value;
    char name[16];
    int value1;
} TS_TEST2;

int funtestc(s32 tint, TS_TEST2* test2, char* tchar)
{
    if (tint == 1234)
        return 2;

    if ((tint == 0x3689000) && (test2->value == 0x12349999) && (strcmp(test2->name, "01234567") == 0) &&
        (test2->value1 == 0x55664433) && (strcmp(tchar, "76543210") == 0)) {
        if (a001 == 0) {
            a001 = 1;
            printf("---------------------------a001 pass\r\n");
        }
    }

    return 1;
}

int funtestbug(s32 tint, char* tchar)
{
    int i = 0;

    // printf("----tint = %d\r\n" , tint);
    if (tint == -1258289966) {
        printf("----bug has been found\r\n");
        i = 0 / 0;
        return 2;
    }

    i = i + 1;

    return 1;
}
