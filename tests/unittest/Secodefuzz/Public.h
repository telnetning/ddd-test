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
Creat at 2017-01-19

//编程风格，请扩展的兄弟务必遵守统一的编程风格

1.对外提供的接口，第一个单词全大写，接着下划线，其他字母第一个字母大写，其他小写
DT_Set_MaxOutputSize
2.内部各模块之间调用的接口，第一个单词全小写，接着下划线，其他字母第一个字母大写，其他小写
init_StringStatic
3.
*/
#ifndef __MUTATOR_PUBLIC_H__
#define __MUTATOR_PUBLIC_H__

#ifdef __cplusplus
extern "C" {
#endif

#ifndef __KERNEL__
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <math.h>  // sqrt cos log

#define __TIME_ENABLE__
#ifdef __TIME_ENABLE__
#include <time.h>  // time
#endif

#else
#include <linux/module.h>
#include <linux/slab.h> /* kmalloc() */
#endif

//====================== 宏开关 ======================
#define LIBDEFINE  // 系统库宏定义
#define HAS_IO
#define HAS_HOOK
#define HAS_TRACE_PC
#define HAS_LEAKCHECK
#define HAS_SIGNAL
//#define HAS_KCOV

// vs编译器要裁剪的
#ifdef _MSC_VER

#ifndef __clang__
#undef HAS_HOOK
#undef HAS_TRACE_PC
#undef HAS_LEAKCHECK
#undef HAS_SIGNAL
#endif
#undef HAS_KCOV
#endif

//编进内核态，大多裁剪了
#ifdef __KERNEL__
#undef LIBDEFINE
#undef HAS_IO
#undef HAS_HOOK
#undef HAS_TRACE_PC
#undef HAS_LEAKCHECK
#undef HAS_SIGNAL
#undef HAS_KCOV
#endif

/******************************************

定义一些规格

******************************************/

#ifdef __KERNEL__
#define max_para_num 30               //定义一个测试例支持的最大参数数量
#define max_corpus_num 300            //定义一个测试例支持的最大样本数量
#define MAX_valueBuf 100000           //定义变异出来的字符串的缓冲区的最大长度，变异的最大长度不能设置超过这个值
#define Default_maxOutputSize 100000  //定义默认的maxOutputSize
#else
#define max_para_num 256                //定义一个测试例支持的最大参数数量
#define max_corpus_num 300              //定义一个测试例支持的最大样本数量
#define MAX_valueBuf 10000000           //定义变异出来的字符串的缓冲区的最大长度，变异的最大长度不能设置超过这个值
#define Default_maxOutputSize 10000000  //定义默认的maxOutputSize
#endif

//#define little_mem 			1				//内存非常非常吃紧的时候使用这个宏

#ifdef little_mem
#define max_para_num 10             //定义一个测试例支持的最大参数数量
#define max_corpus_num 1            //定义一个测试例支持的最大样本数量
#define MAX_valueBuf 1000           //定义变异出来的字符串的缓冲区的最大长度，变异的最大长度不能设置超过这个值
#define Default_maxOutputSize 1000  //定义默认的maxOutputSize

#undef LIBDEFINE
#undef HAS_IO
#undef HAS_HOOK
#undef HAS_TRACE_PC
#undef HAS_LEAKCHECK
#undef HAS_SIGNAL
#undef HAS_KCOV
#endif

#define MAX_Name_Len 32

//====================================================

#ifdef HAS_KCOV
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <fcntl.h>
#endif

#ifdef HAS_SIGNAL
#include <signal.h>
#endif

/******************************************

整数类型声明

******************************************/

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

/******************************************

枚举声明

******************************************/

// 变异算法枚举，按照使用频率排列貌似可以增加运行效率:)
enum enum_Mutated {

    enum_DataElementBitFlipper = 0,
    enum_DataElementBitFill,
    enum_DataElementBitZero,
    enum_DataElementChangeASCIIInteger,
    enum_DataElementMBitFlipper,
    enum_DataElementDuplicate,
    enum_DataElementLengthEdgeCase,
    enum_DataElementLengthRandom,
    enum_DataElementLengthGauss,
    enum_DataElementLengthRepeatPart,
    enum_DataElementReduce,
    enum_DataElementByteRandom,
    enum_DataElementOneByteInsert,
    enum_DataElementSwapTwoPart,
    enum_DataElementStringStatic,
    enum_DataElementCopyPartOf,
    enum_DataElementInsertPartOf,
    enum_DataElementAFL,
    enum_DataElementMagic,

    enum_NumberEdgeCase,
    enum_NumberEdgeRange,
    enum_NumberRandom,
    enum_NumberVariance,
    enum_NumberSmallRange,
    enum_NumberPowerRandom,
    enum_NumberMagic,
    enum_NumberEnum,
    enum_NumberRange,

    enum_StringAsciiRandom,
    enum_StringLengthAsciiRandom,
    enum_StringCaseLower,
    enum_StringCaseRandom,
    enum_StringCaseUpper,
    enum_StringLengthEdgeCase,
    enum_StringLengthRandom,
    enum_StringLengthGauss,
    enum_StringUtf8Bom,
    enum_StringUtf8BomLength,
    enum_StringUtf8BomStatic,
    enum_StringStatic,
    enum_StringMagic,
    enum_StringEnum,

    enum_BlobChangeBinaryInteger,
    enum_BlobChangeFromNull,
    enum_BlobChangeRandom,
    enum_BlobChangeSpecial,
    enum_BlobChangeToNull,
    enum_BlobExpandAllRandom,
    enum_BlobExpandSingleIncrementing,
    enum_BlobExpandSingleRandom,
    enum_BlobExpandZero,
    enum_BlobMagic,
    enum_BlobEnum,

    enum_CustomNumber,
    enum_CustomString,
    enum_CustomBlob,

    enum_MIpv4,
    enum_MIpv6,
    enum_MMac,

    //	enum_MutatedAFL,
    enum_MutatedMAX = 100,
};

//用户自定义数据类型，目前看来是必须要重新编译lib代码的，而且要修改多处代码，咋能简单点呢?
enum enum_type {
    enum_NumberU = 0,   //	无符号数字
    enum_NumberS,       //	有符号数字
    enum_Number_Enum,   //	枚举数字，只在指定值范围内变异
    enum_Number_Range,  //	范围数字，只在指定值范围内变异
    enum_String,        //	字符串
    enum_String_Enum,   //	枚举字符串，只在指定值范围内变异
    enum_Blob,          //	buffer 可变长内存块           不可变长内存块是否要?
    enum_Blob_Enum,     //	枚举内存块，只在指定值范围内变异
    enum_FixBlob,       //    不可变长内存块
    enum_AFL,
    enum_Ipv4,
    enum_Ipv6,
    enum_Mac,
    enum_MAX = 20,
};

enum enum_whether {
    enum_No = 0,
    enum_Yes = 1,
};

/******************************************

结构体声明

******************************************/

typedef struct {
    char name[MAX_Name_Len];
    char mutater_name[MAX_Name_Len];
    int type;
    int len;
    int max_len;
    char* value;
} S_para;

typedef struct __ELEMENT {
    S_para para;
    int isHasInitValue;  //是否有初始值
    int isNeedFree;
    int isAddWholeRandom;  // 这个用来干啥
    int isAddWholeSequence;
    int sequenceStartPos;

    char numberValue[8];  //专门用来保存数字类型初始值的，省的单独分配内存

    s64 inLen;  //数据的长度，以bit为单位，string必须被8整除,blob也是
    char* inBuf;
    int pos;  //目前执行的测试用例

    //输出的数据，只读
    int count;             //一共多少测试例
    int isNeedFreeOutBuf;  // 释放outBuf标志
    int length;
    int sign;  // 0=无符号，有符号

    //用户可以改变
    int isMutatedClose[enum_MutatedMAX];  //可以针对某个元素单独关闭变异算法，

    //内部变量，请别动
    int isMutatedSupport[enum_MutatedMAX];
    int posStart[enum_MutatedMAX];
    int num[enum_MutatedMAX];
    int numbak[enum_MutatedMAX];

    int* Enum_number_table;
    char** Enum_string_table;
    char** Enum_blob_table;
    int* Enum_blob_l_table;
    int Enum_count;

    int min;
    int max;

    void* init;  // S_ElementInit
} S_Element;

typedef struct {
    int first;
    int isuse;

    int type;  //
    int isHasInitValue;
    int isneedfree;
    char* initValueBuffer;
    u64 initValue;

    int len;  // number单位bit   other单位byte
    int maxLen;
    // 用于结构体
    void* structPtr;
    int structStart;
    int structLength;

    int* Enum_number_table;
    char** Enum_string_table;
    char** Enum_blob_table;
    int* Enum_blob_l_table;
    int Enum_count;

    int min;
    int max;

    S_Element* element;
} S_ElementInit;

/************************************************************************

0.一些辅助功能接口

************************************************************************/
// 0.设置是否在crash时打印crash样本在屏幕上,默认打印，1为打印，0为不打印
extern void DT_Set_If_Show_crash(int is_show_all);

// 1.设置是否在测试例结束时打印样本在屏幕上,默认不打印，1为打印，0为不打印
extern void DT_Set_If_Show_Corpus(int is_show_all);

// 2.设置打印测试例执行报告的路径，如果为空则不打印测试例报告
extern void DT_Set_Report_Path(char* path);

// 3.设置是否输出覆盖率sancov文件，在clang编译器下有效，需要同时打开环境变量export ASAN_OPTIONS=coverage=1
//默认不输出，1为输出，0为不输出
extern void DT_Set_Is_Dump_Coverage(int is_dump_coverage);

// 4.设置最大输出尺寸，单位byte,	默认值为Default_maxOutputSize
extern void DT_Set_MaxOutputSize(int imaxOutputSize);

// 5.得到secodefuzz的版本信息
extern char* DT_Get_Version(void);

// 6.设置单个测试例执行一次超过多长时间报bug,如果为0，则不检测超时bug
extern void DT_Set_TimeOut_Second(int second);

// 7.将二进制样本转换成文本字符串形式并打印,参数为二进制文件路径
extern void DT_Printf_Bin_To_Corpus(char* Path);

// 8.将二进制样本转换成文本字符串形式并写到文件里,参数为二进制文件路径
extern void DT_Write_Bin_To_Corpus(char* Path);

// 9.将样本字符串转换为二进制文件，并写到文件里，,参数为样本文件路径
extern void DT_Write_Corpus_To_Bin(char* Path);

// 10.设置是否开始或者停止监控覆盖反馈，一般不需要调用
extern void DT_Enable_TracePC(int isenable);

// 11.主动打印当前样本和写入文件，利用函数返回值判断错误的时候使用
extern void DT_Show_Cur_Corpus(void);

// 12.控制debug开关函数，默认关闭，1为开启，0为关闭
extern void DT_Enable_Log(int isenable);

// 13.得到变异数据的长度
extern int DT_GET_MutatedValueLen(S_ElementInit* init);

// 13.1得到某个参数是否被变异
extern int DT_GET_IsBeMutated(S_ElementInit* init);

// 14.关闭所有变异算法，默认全部开启，1为开启，0为关闭
extern void DT_Enable_AllMutater(int isenable);

// 15.关闭某个变异算法，默认开启，1为开启，0为关闭
extern void DT_Enable_OneMutater(enum enum_Mutated MutatedNum, int isenable);

// 16.设置字符串变异是否最后添加\0 (如果长度非0),默认为添加
extern void DT_Set_String_Has_Terminal(int is_string_has_terminal);

// 17.设置第一次运行到代码块，打印块pc指针到屏幕，默认关闭
extern void DT_Set_Is_Print_New_PC(int isPrintPC);

// 18.设置单个测试例运行时间，如果设置的运行次数没到而运行时间到了，提前结束
extern void DT_Set_Running_Time_Second(int second);

// 19.打开样本进化过程对循环语句的支持，因为会一定程序影响测试效率，默认关闭
extern void DT_Enable_Support_Loop(int isenable);

// 20.使能每运行一次测试用例检测一次内存泄露，会严重减慢测试速度，默认关闭
//最好是在已经发现内存泄露的情况下，再打开这个功能找到引起内存泄露的测试例
extern void DT_Enable_Leak_Check(int isenable);

/************************************************************************

1.封装测试例循环的声明

************************************************************************/

//
extern int g_iteration;
extern int g_iteration_start;
extern int g_iteration_end;
extern S_ElementInit g_Element[max_para_num];
extern S_ElementInit g_ElementS[max_para_num];
extern int fuzz_seed;
extern int fuzz_i;
extern int fuzz_start;
extern int fuzz_end;

extern void dt_start(int seed, int count, char* testcase_name, int is_reproduce);
extern void dt_for_start(void);
extern void dt_for_end(void);
extern void dt_end(void);
extern int Running_Time_Is_Over(void);

#define DT_FUZZ_START(seed, count, testcase_name, is_reproduce)  \
    {                                                            \
        dt_start(seed, count, testcase_name, is_reproduce);      \
        for (fuzz_i = fuzz_start; fuzz_i < fuzz_end; fuzz_i++) { \
            dt_for_start();

#define DT_FUZZ_END()           \
    dt_for_end();               \
    if (Running_Time_Is_Over()) \
        break;                  \
    }                           \
    dt_end();                   \
    }

//对外部提供的接口，第一个单词大写，然后用下划线与后边隔开，其他每个单词首字母大写

/******************************************

2.接口一DT_SetGet系列接口

******************************************/
// init 元素结构体指针,使用&g_Element[n],每个测试例n从0开始，必须连续
// initValue 初始值或指向初始值的指针
//返回值,指向变异值的指针
char* DT_SetGetS64(S_ElementInit* init, s64 initValue);
char* DT_SetGetS32(S_ElementInit* init, s32 initValue);
char* DT_SetGetS16(S_ElementInit* init, s16 initValue);
char* DT_SetGetS8(S_ElementInit* init, s8 initValue);
char* DT_SetGetU64(S_ElementInit* init, u64 initValue);
char* DT_SetGetU32(S_ElementInit* init, u32 initValue);
char* DT_SetGetU16(S_ElementInit* init, u16 initValue);
char* DT_SetGetU8(S_ElementInit* init, u8 initValue);
//变异一个一字节的数值，变异后的每个值都在枚举表里，不会变异出其他的
char* DT_SetGetNumberEnum_U8(S_ElementInit* init, u8 initValue, u8* eunmTable, int eunmCount);
//变异后的每个值都在枚举表里，不会变异出其他的
//例如:
// initValue 		= 0x1234
// int eunmTable[]	={2,4,6,1234,0x1234,0x897654,0x456789}
// eunmCount		=7
char* DT_SetGetNumberEnum(S_ElementInit* init, s32 initValue, int* eunmTable, int eunmCount);
//变异后的每个值都在枚举表之外
//例如
// initValue 		= 0xff
// int eunmTable[]	={2,4,6,1234,0x1234};
// eunmCount		=5
char* DT_SetGetNumberEnum_EX(S_ElementInit* init, s32 initValue, int* eunmTable, int eunmCount);
// min最小值max最大值,变异会包含边界值
//例如
// initValue		=0x1234
// min			=0x1200
// max			=0x1300
char* DT_SetGetNumberRange(S_ElementInit* init, s32 initValue, int min, int max);
// min最小值max最大值,变异不会包含边界值
//例如
// initValue		=0xff
// min			=0x1200
// max			=0x1300
char* DT_SetGetNumberRange_EX(S_ElementInit* init, s32 initValue, int min, int max);
char* DT_SetGetFloat(S_ElementInit* init, float initValue);
char* DT_SetGetDouble(S_ElementInit* init, double initValue);
// length 		初始值的内存长度,strlen(initValue)+1
// maxLength 	设置变异的最大长度，如果为0，则为默认最大长度，默认为Default_maxOutputSize
//例如
// length  		   	=6
// maxLength			=20
// initValue			="11111"
char* DT_SetGetString(S_ElementInit* init, int length, int maxLength, char* initValue);
//变异后的每个值都在枚举表里，不会变异出其他的
//例如
// length  		   	=6
// maxLength			=20
// initValue			="1.1.1"
// char* eunmTableS[]	={"123","abc","zhangpeng","1.1.1","wanghao" ,"12345"};
// eunmCount			=6
char* DT_SetGetStringEnum(
    S_ElementInit* init, int length, int maxLength, char* initValue, char* eunmTableS[], int eunmCount);
//变异后的每个值都在枚举表之外
//例如
// length  		   	=6
// maxLength			=20
// initValue			="11111"
// char* eunmTableS[]	={"123","abc","zhangpeng","1.1.1","wanghao" ,"12345"};
// eunmCount			=6
char* DT_SetGetStringEnum_EX(
    S_ElementInit* init, int length, int maxLength, char* initValue, char* eunmTableS[], int eunmCount);
// length 		初始值(initValue)的内存长度
// maxLength 	设置变异的最大长度，如果为0，则为默认最大长度，默认为Default_maxOutputSize
//例如
// length  		   	=15
// maxLength			=40
// initValue			="12345678900000"
char* DT_SetGetBlob(S_ElementInit* init, int length, int maxLength, char* initValue);
//变异后的每个值都在枚举表里，不会变异出其他的
//例如
// length  		   	=14
// maxLength			=20
// initValue			="6666666666666"
// char* eunmTableB[]	={"123111","abcaaa","\x00\x01\x02\x00\xff","6666666666666"};
// int eunmTableL[]	={7,7,5,14};
// eunmCount 		=4
char* DT_SetGetBlobEnum(S_ElementInit* init, int length, int maxLength, char* initValue, char* eunmTableB[],
    int eunmTableL[], int eunmCount);
//变异后的每个值都在枚举表之外
//例如
// length  		   	=14
// maxLength			=20
// initValue			="6666666666677"
// char* eunmTableB[]	={"123111","abcaaa","\x00\x01\x02\x00\xff","6666666666666"};
// int eunmTableB[]	={7,7,5,14};
// eunmCount 		=4
char* DT_SetGetBlobEnum_EX(S_ElementInit* init, int length, int maxLength, char* initValue, char* eunmTableB[],
    int eunmTableL[], int eunmCount);
// length初始值的长度，变异后的所有值长度与这个长度相同
// maxLengt这个参数被忽略
char* DT_SetGetFixBlob(S_ElementInit* init, int length, int maxLength, char* initValue);
//例如
// u8 temp_ipv4[4] 	={192,168,0,1};
// initValue			=(char *)temp_ipv4)
char* DT_SetGetIpv4(S_ElementInit* init, char* initValue);
//例如
// u8 temp_ipv6[16] 	={0x20,0x02,0x00,0x00,  0x00,0x00,0x00,0x00,  0x00,0x00,0x00,0x00,  0x00,0x00,0x00,0x01};
// initValue			=(char*)temp_ipv6
char* DT_SetGetIpv6(S_ElementInit* init, char* initValue);
//例如
// u8 temp_mac[6] 	={0x28,0x6e,0xd4,0x89,0x26,0xa8};
// initValue			=(char*)temp_mac
char* DT_SetGetMac(S_ElementInit* init, char* initValue);

/******************************************

6.提供几个简单的变异函数，非常简单，欢迎脑洞增加之
只能在有效值之间来回变异，每次都变

******************************************/
extern int Mutater_NumberRange(int min, int max);
extern int Mutater_NumberEnum(int Number_table[], int count);
extern char* Mutater_StringEnum(char* String_table[], int count);

//增加类型时不要改动原有类型的值
enum argtype {
    ARG_UNDEFINED = 0,
    ARG_CPU = 1,
    ARG_InterfaceIndex = 2,

    ARG_MAX1 = 100,
};

extern int register_Mutater_fill_arg(enum argtype argType, unsigned long (*fill_arg)(void));
extern unsigned long Mutater_fill_arg(enum argtype argType);

/******************************************

7.demo 示例函数

******************************************/
extern void test(void);

/************************************************************************

注:一些早期支持的变异函数被移动到PCommon.h里，
不在推荐，想要使用需要引用PCommon.h

************************************************************************/

#ifdef __cplusplus
}
#endif
#endif
