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
#ifndef _COMMON_H
#define _COMMON_H

#ifdef __cplusplus
extern "C" {
#endif

#include "../Public.h"

// Corpus

//变异出来的最大长度为MAX_valueBuf，一个内存最多4个字节表示
//比如\x00  多分配1000字节表示名字等号啥的:)
//定义样本文件一行最大的字数
#define max_one_line (MAX_valueBuf * 4 + 1000)
#define switch_corpus_count 100  //定义样本间切换的迭代次数间隔
#define switch_sub_count 4       //定义一次样本使用中，使用变异值再次变异的迭加次数

// Peport
#define max_file_path 200  //定义最大的文件路径字符串长度

// Trace_PC
#define MAX_PC_NUM (1 << 21)   //定义保存最大的代码块pc指针数量
#define MAX_MODULE_NUM 4096    //定义最大模块数量
#define MAX_PcDescr_SIZE 8142  //定义最大描述pc打印字符串的尺寸

// LLVMData
#define SIZE_llvmmem_table 10240          //内存hook保存的最大内存数据的数量
#define SIZE_llvmmem_data 100             //每个数据不应超过100字节，太大了貌似也没啥意义 ::大约1M内存占用
#define SIZE_llvmnumber_table_u64 102400  //内存hook保存的最大数字数据的数量

// Kcov
#define MAX_Kernel_PC_NUM (1 << 21)

// Common
#define debugmaxOutputSize 1000  // 打印输出单测试例最大字节数

#define MAXCOUNT (500)  // 一个变异算法 测试例的最大数量
#define String_Number_len (33)

#define MIN(_a, _b) ((_a) > (_b) ? (_b) : (_a))
#define MAX(_a, _b) ((_a) > (_b) ? (_a) : (_b))

#define IsAddone(x) ((x) > (0) ? (1) : (0))

#define SET_ElementOutBuf(ptr, index, value)       \
    do {                                           \
        ((char*)((ptr)->outBuf))[index] = (value); \
    } while (0)
#define GET_ElementInBuf(ptr, index) ((char*)((ptr)->inBuf))[index]
#define PosOriginal 0xffffffff

// Internal
#define Is_Use_Global_Malloc 1  //设置为变异数据分配的内存是否使用一次性分配的大内存

////单bit翻转
#define FLIP_BIT(_ar, _b)                       \
    do {                                        \
        u8* _arf = (u8*)(_ar);                  \
        u32 _bf = (_b);                         \
        _arf[(_bf) >> 3] ^= (128 >> ((_bf)&7)); \
    } while (0)

//单bit清0
#define ZERO_BIT(_ar, _b)                        \
    do {                                         \
        u8* _arf = (u8*)(_ar);                   \
        u32 _bf = (_b);                          \
        _arf[(_bf) >> 3] &= ~(128 >> ((_bf)&7)); \
    } while (0)

//单bit置1
#define FILL_BIT(_ar, _b)                       \
    do {                                        \
        u8* _arf = (u8*)(_ar);                  \
        u32 _bf = (_b);                         \
        _arf[(_bf) >> 3] |= (128 >> ((_bf)&7)); \
    } while (0)

#define ONE_IN(x) ((hw_Rand() % x) == 0)  // limit of RAND_MAX-1
#define RAND_16() (hw_Rand() & 0xFFFF)
#define RAND_32() (((0UL | hw_Rand()) << 1) | (hw_Rand() & 1))
// rand_64为什么要这样写
#define RAND_64() (((0ULL | hw_Rand()) << 33) | ((0ULL | hw_Rand()) << 2) | (hw_Rand() & 0x3))
#define RAND_BOOL() (hw_Rand() & 1)
#define RAND_BYTE() (hw_Rand() & 0xff)

//#define RAND_RANGE(min, max)	(min + hw_Rand() / (RAND_MAX / (max - min + 1) + 1))
#define RAND_RANGE(min, max) ((min) + (hw_Rand() % ((max) - (min) + 1)))

#define RAND_RANGE64(min, max) ((min) + (RAND_64() % ((max) - (min) + 1)))

/* ------------------------------------------------------------------------------- */
//====================== 宏开关 ======================
#define ENABLE_ASSERT 1  // enable assert

//====================================================

#if ENABLE_ASSERT
#define ASSERT(value)                \
    do {                             \
        if (value) {                 \
            M_DEBUG(1, "assert!\n"); \
            while (1)                \
                ;                    \
        }                            \
    } while (0)
#else
#define ASSERT(value)
#endif
#define ASSERT_NULL(value) ASSERT(NULL == (value))
#define ASSERT_ZERO(value) ASSERT(0 == (value))
#define ASSERT_NEGATIVE(value) ASSERT(0 > (value))

#ifndef __KERNEL__
// public
#define hw_printf(fmt, ...) printf(fmt, ##__VA_ARGS__)
#else
#define hw_printf(fmt, ...) printk(fmt, ##__VA_ARGS__)
#endif

#ifndef _WIN32
#define hw_sprintf(buf, fmt, ...) sprintf(buf, fmt, ##__VA_ARGS__)
#else

#ifdef __clang__
#define hw_sprintf(buf, fmt, ...) _snprintf(buf, 50000, fmt, ##__VA_ARGS__)
#else
#define hw_sprintf(buf, fmt, ...) sprintf(buf, fmt, ##__VA_ARGS__)
#endif

#endif

//#ifndef _MSC_VER //为了核心网，先注释掉
#define hw_sscanf(buf, fmt, ...) sscanf(buf, fmt, ##__VA_ARGS__)
//#else
//#define hw_sscanf(buf,fmt, ...)			sscanf_s(buf,fmt, ##__VA_ARGS__)
//#endif

//  DEBUG
extern int debug_level;
#define DEBUG_PREFIX "##"
#ifdef _WIN32
#define __s_func__ __FUNCTION__
#else
#define __s_func__ __func__
#endif

#define DEBUG_LEVEL_CRIT 0      // critical
#define DEBUG_LEVEL_ERR 1       // error
#define DEBUG_LEVEL_WARNNING 2  // warning
#define DEBUG_LEVEL_NOTICE 3    // normalbut
#define DEBUG_LEVEL_INFO 4      // informational
#define DEBUG_LEVEL_DEBUG 5     // debug

#define __m_print_0(msg, ...)                                            \
    do {                                                                 \
        if (debug_level >= DEBUG_LEVEL_CRIT) {                           \
            hw_printf("CRIT:%s[%s]%d ", __FILE__, __s_func__, __LINE__); \
            hw_printf(msg, ##__VA_ARGS__);                               \
        }                                                                \
    } while (0)

#define __m_print_1(msg, ...)                                           \
    do {                                                                \
        if (debug_level >= DEBUG_LEVEL_ERR) {                           \
            hw_printf("ERR:%s[%s]%d ", __FILE__, __s_func__, __LINE__); \
            hw_printf(msg, ##__VA_ARGS__);                              \
        }                                                               \
    } while (0)

#define __m_print_2(msg, ...)                                            \
    do {                                                                 \
        if (debug_level >= DEBUG_LEVEL_WARNNING) {                       \
            hw_printf("WARN:%s[%s]%d ", __FILE__, __s_func__, __LINE__); \
            hw_printf(msg, ##__VA_ARGS__);                               \
        }                                                                \
    } while (0)

#define __m_print_3(msg, ...)                                              \
    do {                                                                   \
        if (debug_level >= DEBUG_LEVEL_NOTICE) {                           \
            hw_printf("NOTICE:%s[%s]%d ", __FILE__, __s_func__, __LINE__); \
            hw_printf(msg, ##__VA_ARGS__);                                 \
        }                                                                  \
    } while (0)

#define __m_print_4(msg, ...)                  \
    do {                                       \
        if (debug_level >= DEBUG_LEVEL_INFO) { \
            hw_printf(msg, ##__VA_ARGS__);     \
        }                                      \
    } while (0)

#define __m_print_5(msg, ...)                   \
    do {                                        \
        if (debug_level >= DEBUG_LEVEL_DEBUG) { \
            hw_printf(msg, ##__VA_ARGS__);      \
        }                                       \
    } while (0)

#define M_DEBUG(level, msg, ...) __m_print_##level(msg, ##__VA_ARGS__)

/* ------------------------------------------------------------------------------- */

//如果变异算法做成链表，则会做的更灵活，用户自定义变异算法就不需要重新编译lib代码了
struct Mutater_group {
    const char* name;
    int (*getCount)(S_Element* pElement);
    char* (*getValue)(S_Element* pElement, int pos);
    int (*getIsSupport)(S_Element* pElement);
    int isMutater;  //可以通过这个变量禁止这个变异算法
};

//全局变量
extern u64 EdgeCase_table[];
extern int maxOutputSize;
extern int isLogOpen;
extern const struct Mutater_group* g_Mutater_group[enum_MutatedMAX];
extern int g_IsMutatedClose[enum_MutatedMAX];
extern int addWholeRandomNum;

extern int addWholeSequenceNum;
extern int tempSequenceStartPos;

//内部模块间函数，首单词小写，然后用下划线与其他隔开，其他单词首字母大写

enum enum_operation_type {
    enum_Insert = 0,
    enum_Overwrite = 1,
    enum_replace = 2,
};

// internal
extern u32 in_GetBitNumber(u32 n);
extern int in_StringIsNumber(S_Element* pElement);
extern char* in_GetStringFromType(int type);
extern int in_GetTypeFromString(char* type_name);
extern char* set_ElementOriginalValue(S_Element* pElement);
extern char* set_ElementInitoutBuf(S_Element* pElement, int len);
extern char* set_ElementInitoutBuf_ex(S_Element* pElement, int len);
extern char* magic_getvalue(S_Element* pElement, char* data, int len, int type);
extern int in_GetBufZeroNumber(char* string, int len);
extern void in_GetRegion(int length, int* outStart, int* outLength);
extern int in_GetLetterNumber(char* string);
extern int in_toupper(int c);
extern int in_tolower(int c);
extern int in_IsLetter(char c);
extern int in_isascii(char c);
extern int in_isprint(char c);

extern int in_isxdigit(char c);
extern int in_isdigit(char c);
extern u32 in_strlen(const char* s);
extern u64 in_sqrt(u64 x);
extern char* in_ltoa(s64 value, char* string, int radix);
extern s64 in_atol(char* string);
extern s64 in_htol(char* s);
extern void in_delSpace(char* str);

extern int in_parse_string_to_bin(char* Str, char* buf);
extern int in_parse_bin_to_string(char* Str, char* buf, int len);

// external
extern int hw_time(void);
extern char* hw_Malloc(size_t size);
extern void hw_Free(void* ptemp);
extern void* hw_Memset(void* s, int ch, size_t n);
extern void* hw_Memcpy(void* dest, const void* src, size_t n);
extern void* hw_Memmove(void* dest, const void* src, size_t n);
extern int hw_Memcmp(const void* buf1, const void* buf2, unsigned int count);
extern long int hw_Strtol(const char* nptr, char** endptr, int base);
extern int hw_Strcmp(const char* s1, const char* s2);
extern int hw_RAND_MAX(void);
extern int hw_Rand(void);
extern void hw_Srand(unsigned int temp);
extern int hw_Get_Time(void);
extern char* hw_Get_Date(void);

// random
extern u32 gaussRandu32(u32 pos);
extern s32 gaussRands32(u32 pos);
extern u64 gaussRandu64(u32 pos);
extern s64 gaussRands64(u32 pos);
extern int get_IsMutated(void);

// Mutator
extern void init_DataElementBitFill(void);
extern void init_DataElementBitFlipper(void);
extern void init_DataElementBitZero(void);
extern void init_DataElementChangeASCIIInteger(void);
extern void init_DataElementMBitFlipper(void);
extern void init_DataElementDuplicate(void);
extern void init_DataElementLengthEdgeCase(void);
extern void init_DataElementLengthRandom(void);
extern void init_DataElementLengthGauss(void);
extern void init_DataElementLengthRepeatPart(void);
extern void init_DataElementReduce(void);
extern void init_DataElementByteRandom(void);
extern void init_DataElementOneByteInsert(void);
extern void init_DataElementSwapTwoPart(void);

extern void init_DataElementStringStatic(void);
extern void init_DataElementCopyPartOf(void);
extern void init_DataElementInsertPartOf(void);
extern void init_DataElementAFL(void);
extern void init_DataElementMagic(void);

extern void init_NumberEdgeCase(void);
extern void init_NumberEdgeRange(void);
extern void init_NumberRandom(void);
extern void init_NumberVariance(void);
extern void init_NumberSmallRange(void);
extern void init_NumberPowerRandom(void);
extern void init_NumberMagic(void);
extern void init_NumberEnum(void);
extern void init_NumberRange(void);

extern void init_StringAsciiRandom(void);
extern void init_StringLengthAsciiRandom(void);
extern void init_StringCaseLower(void);
extern void init_StringCaseRandom(void);
extern void init_StringCaseUpper(void);
extern void init_StringLengthEdgeCase(void);
extern void init_StringLengthRandom(void);
extern void init_StringLengthGauss(void);
extern void init_StringUtf8Bom(void);
extern void init_StringUtf8BomLength(void);
extern void init_StringUtf8BomStatic(void);
extern void init_StringStatic(void);
extern void init_StringMagic(void);
extern void init_StringEnum(void);

extern void init_BlobChangeBinaryInteger(void);
extern void init_BlobChangeFromNull(void);
extern void init_BlobChangeRandom(void);
extern void init_BlobChangeSpecial(void);
extern void init_BlobChangeToNull(void);
extern void init_BlobExpandAllRandom(void);
extern void init_BlobExpandSingleIncrementing(void);
extern void init_BlobExpandSingleRandom(void);
extern void init_BlobExpandZero(void);
extern void init_BlobMagic(void);
extern void init_BlobEnum(void);

extern void init_Ipv4(void);
extern void init_Ipv6(void);
extern void init_Mac(void);

extern void init_CustomNumber(void);
extern void init_CustomString(void);
extern void init_CustomBlob(void);

extern void init_pc_counters(void);
extern void init_8bit_counters(void);
extern void init_kcov(void);
extern void init_llvmdata(void);

extern void init_Corpus(void);
extern void init_SignalCallback(void);

// llvmdata
extern void llvmdata_number_add_value(void* caller_pc, u64 s1, u64 s2);
extern u64 llvmdata_number_get_value(void);
extern int llvmdata_number_get_count(void);
extern void llvmdata_mem_add_value(void* caller_pc, const char* s1, const char* s2, size_t n1, size_t n2);
extern void llvmdata_mem_add_value_ex(void* caller_pc, const char* s1, const char* s2);
extern char* llvmdata_mem_get_value(int* len);
extern int llvmdata_mem_get_count(void);

// llvm
extern int llvmhook_is_support(void);
extern void llvmhook_register_asan_callback(void (*fun)(void));
extern void llvmhook_print_stack_trace(void);

// llvmTracePC
extern int llvmtracepc_is_has_new_feature(void);
extern void llvmtracepc_start_feature(void);
extern void llvmtracepc_end_feature(void);

// llvmpccounters
extern void Llvm_Record_Guards(uint32_t* Start, uint32_t* Stop);
extern void Llvm_Record_pc_counters(int Idx, uintptr_t PC);
extern void Llvm_Set_Is_Dump_Coverage(int is_dump_coverage);
extern void Llvm_Set_Is_Print_New_PC(int isPrintPC);
extern void Llvm_Dump_Coverage(void);

// llvm8bitcounters
extern void Llvm_Record_8bit_counters(int Idx);
extern void Llvm_Do_8bit_counters(void);
extern void Llvm_Enable_Do_8bit_counters(void);
extern void Llvm_Disable_Do_8bit_counters(void);

// llvmLeakCheck
extern void Llvm_Enable_Leak_Check(void);
extern void Llvm_Disable_Leak_Check(void);
extern void Llvm_Do_Leak_Check(void);

// kcov
extern int kcov_is_has_new_feature(void);
extern void kcov_start_feature(void);
extern void kcov_end_feature(void);

// IO
extern void WriteToFile(char* data, int len, char* Path);
extern void WriteToFileFail(char* data, int len, char* Path);
extern void ReadFromFile(char** data, int* len, char* Path);

// Hash
extern char* Hash(char* buf, int len);

// Common
extern int register_Mutater(const struct Mutater_group* bbbb, enum enum_Mutated cccc);

// Mother
extern unsigned long get_cpu1(void);

// Report
extern void Report_Set_Path(char* path);
extern void Report_write_failed_testcase(void);
extern void Report_write_succeed_testcase(char* testcase_name, int seed, int run_count, int run_time);
extern int Report_get_time(void);
extern void Report_Set_Running_Testcase_Name(char* name);

// Corpus
extern void Corpus_Set_If_show_crash(int is_show_all);
extern void Corpus_Set_If_Show(int is_show_all);
extern void Corpus_bin_printf(char* Path);
extern void Corpus_bin_write(char* Path);
extern void Corpus_corpus_write(char* Path);
extern void Corpus_Start(int is_reproduce);
extern void Corpus_End(void);
extern void Corpus_Show_ALL(void);
extern void Corpus_Set_Path(char* path);
extern void Corpus_start_feature(void);
extern void Corpus_end_feature(void);
extern void Corpus_Show_Cur(void);

// common
extern char* GET_Version(void);

// time
extern void TimeOut_get_start_time(void);
extern void TimeOut_Is_Bug(void);

extern void Running_Time_get_start(void);
extern int Running_Time_Is_Over(void);

//控制debug开关函数
extern void OPEN_Log(void);
extern void CLOSE_Log(void);

//
void INIT_FuzzEnvironment(void);
void CLEAR_FuzzEnvironment(void);

//////////////////////////////////////////////////

// Platform detection.
#ifdef __linux__
#define LIBFUZZER_APPLE 0
#define LIBFUZZER_LINUX 1
#define LIBFUZZER_WINDOWS 0
#elif __APPLE__
#define LIBFUZZER_APPLE 1
#define LIBFUZZER_LINUX 0
#define LIBFUZZER_WINDOWS 0
#elif _WIN32
#define LIBFUZZER_APPLE 0
#define LIBFUZZER_LINUX 0
#define LIBFUZZER_WINDOWS 1
#else
#error "Support for your platform has not been implemented"
#endif

#define LIBFUZZER_POSIX LIBFUZZER_APPLE || LIBFUZZER_LINUX

#ifdef __x86_64
#define ATTRIBUTE_TARGET_POPCNT __attribute__((target("popcnt")))
#else
#define ATTRIBUTE_TARGET_POPCNT
#endif

#ifdef __clang__  // avoid gcc warning.
#define ATTRIBUTE_NO_SANITIZE_MEMORY __attribute__((no_sanitize("memory")))
#define ALWAYS_INLINE __attribute__((always_inline))
#else
#define ATTRIBUTE_NO_SANITIZE_MEMORY
#define ALWAYS_INLINE
#endif  // __clang__

#define ATTRIBUTE_NO_SANITIZE_ADDRESS __attribute__((no_sanitize_address))

#if defined(__has_feature)
#if __has_feature(address_sanitizer)
#define ATTRIBUTE_NO_SANITIZE_ALL ATTRIBUTE_NO_SANITIZE_ADDRESS
#elif __has_feature(memory_sanitizer)
#define ATTRIBUTE_NO_SANITIZE_ALL ATTRIBUTE_NO_SANITIZE_MEMORY
#else
#define ATTRIBUTE_NO_SANITIZE_ALL
#endif
#else
#define ATTRIBUTE_NO_SANITIZE_ALL
#endif

#if LIBFUZZER_WINDOWS
#define ATTRIBUTE_INTERFACE __declspec(dllexport)
#else
#define ATTRIBUTE_INTERFACE __attribute__((visibility("default")))
#endif

//////////////////////////

/******************************************

8.demo 示例函数

******************************************/

extern void TEST_DT_DT1(void);
extern void TEST_DT_DT2(void);
extern void TEST_DT_DT3(void);

extern void TEST_GetOneValueNumber(void);
extern void TEST_GetOneValueNumber1(void);
extern void TEST_GetOneValueNumberEnum(void);
extern void TEST_GetOneValueNumberEnum_EX(void);
extern void TEST_GetOneValueNumberRange(void);
extern void TEST_GetOneValueNumberRange_EX(void);
extern void TEST_GetOneValueFloat(void);
extern void TEST_GetOneValueDouble(void);
extern void TEST_GetOneValueString(void);
extern void TEST_GetOneValueStringEnum(void);
extern void TEST_GetOneValueStringEnum_EX(void);
extern void TEST_GetOneValueBlob(void);
extern void TEST_GetOneValueBlobEnum(void);
extern void TEST_GetOneValueBlobEnum_EX(void);
extern void TEST_GetOneValueFixBlob(void);
extern void TEST_GetOneValueIpv4(void);
extern void TEST_GetOneValueIpv6(void);
extern void TEST_GetOneValueMac(void);

extern void TEST_GetOneValueStruct(void);
extern void TEST_GetMultipleValue(void);
extern void TEST_GetMultipleValue1(void);

extern void TEST_Mutater_NumberRange(void);
extern void TEST_Mutater_NumberEnum(void);
extern void TEST_Mutater_NumberRange(void);
extern void TEST_Mutater_StringEnum(void);

extern void TEST_Mutater_fill_arg(void);

extern void TEST_SET_MaxOutputSize(void);

extern void TEST_Reproduce(void);
extern void TEST_Log1(void);

/******************************************

9.test 示例函数

******************************************/

extern void TEST_MutableNumber(int MutatedNo);
extern void TEST_MutableNumberEnum(int MutatedNo);
extern void TEST_MutableNumberRange(int MutatedNo);
extern void TEST_MutableString(int MutatedNo);
extern void TEST_MutableStringEnum(int MutatedNo);
extern void TEST_MutableBlob(int MutatedNo);
extern void TEST_MutableBlobEnum(int MutatedNo);
extern void TEST_MutableFixBlob(int MutatedNo);
extern void TEST_Ipv4(void);
extern void TEST_Ipv6(void);
extern void TEST_Mac(void);
extern void TEST_CREAT_Element(void);
extern void TEST_GetSequenceValue(void);
extern void TEST_GetRandomValue(void);
extern void TEST_GetS32SequenceValue(void);
extern void TEST_GetNumberEnumSequenceValue(void);
extern void TEST_GetNumberRangeSequenceValue(void);
extern void TEST_GetStringSequenceValue(void);
extern void TEST_GetStringEnumSequenceValue(void);
extern void TEST_GetBlobSequenceValue(void);
extern void TEST_GetBlobEnumSequenceValue(void);
extern void TEST_GetFixBlobSequenceValue(void);
extern void TEST_GetS32RandomValue(void);
extern void TEST_GetNumberEnumRandomValue(void);
extern void TEST_GetNumberRangeRandomValue(void);
extern void TEST_GetStringRandomValue(void);
extern void TEST_GetStringEnumRandomValue(void);
extern void TEST_GetBlobRandomValue(void);
extern void TEST_GetBlobEnumRandomValue(void);
extern void TEST_GetFixBlobRandomValue(void);
extern void TEST_INIT_Seed(int MutatedNo);
extern void TEST_WholeMultiVarientRandom(void);
extern void TEST_WholeMutiVarientSequence(void);
extern void TEST_CloseAllMutater(void);
extern void TEST_OpenAllMutater(void);
extern void TEST_CloseOneMutater(void);
extern void TEST_OpenOneMutater(void);
extern void TEST_ElementCloseAllMutater(void);
extern void TEST_ElementOpenAllMutater(void);
extern void TEST_ElementCloseOneMutater(void);
extern void TEST_ElementOpenOneMutater(void);
extern void TEST_Log(void);
extern void TEST_HEX_Dump(void);
extern void TEST_DEBUG_Element(void);

/******************************************

3.接口二，这套接口不建议使用了

******************************************/
void DT_initValueBuffer(S_ElementInit* init, int length, char* initValue);

#define DT_SetNumber(init, Length, Type, Value) \
    do {                                        \
        if (init.first != 1) {                  \
            init.isuse = 1;                     \
            init.type = Type;                   \
            init.len = Length;                  \
            init.initValue = Value;             \
        }                                       \
    } while (0)

#define DT_SetS64(init, initValue) DT_SetNumber(init, 64, enum_NumberS, initValue)
#define DT_SetS32(init, initValue) DT_SetNumber(init, 32, enum_NumberS, initValue)
#define DT_SetS16(init, initValue) DT_SetNumber(init, 16, enum_NumberS, initValue)
#define DT_SetS8(init, initValue) DT_SetNumber(init, 8, enum_NumberS, initValue)
#define DT_SetU64(init, initValue) DT_SetNumber(init, 64, enum_NumberU, initValue)
#define DT_SetU32(init, initValue) DT_SetNumber(init, 32, enum_NumberU, initValue)
#define DT_SetU16(init, initValue) DT_SetNumber(init, 16, enum_NumberU, initValue)
#define DT_SetU8(init, initValue) DT_SetNumber(init, 8, enum_NumberU, initValue)
#define DT_SetFloat(init, initValue)                          \
    do {                                                      \
        float __tmp_ = initValue;                             \
        DT_SetNumber(init, 32, enum_NumberU, *(u32*)&__tmp_); \
    } while (0)
#define DT_SetDouble(init, initValue)                         \
    do {                                                      \
        double __tmp_ = initValue;                            \
        DT_SetNumber(init, 64, enum_NumberU, *(u64*)&__tmp_); \
    } while (0)

#define DT_SetNumberEnum(init, initValue, eunmTable, eunmCount) \
    do {                                                        \
        if (init.first != 1) {                                  \
            init.isuse = 1;                                     \
            init.type = enum_Number_Enum;                       \
            init.len = 32;                                      \
            init.Enum_number_table = eunmTable;                 \
            init.Enum_count = eunmCount;                        \
            init.initValue = initValue;                         \
        }                                                       \
    } while (0)

#define DT_SetNumberRange(init, initValue, min, max) \
    do {                                             \
        if (init.first != 1) {                       \
            init.isuse = 1;                          \
            init.type = enum_Number_Range;           \
            init.len = 32;                           \
            init.min = min;                          \
            init.max = max;                          \
            init.initValue = initValue;              \
        }                                            \
    } while (0)

// length,字符串长度，strlen(str) + 1
#define DT_SetString(init, length, maxLength, str)  \
    do {                                            \
        if (init.first != 1) {                      \
            init.isuse = 1;                         \
            init.type = enum_String;                \
            init.len = length;                      \
            init.maxLen = maxLength;                \
            DT_initValueBuffer(&init, length, str); \
        }                                           \
    } while (0)

#define DT_SetStringEnum(init, length, maxLength, str, eunmTableS, eunmCount) \
    do {                                                                      \
        if (init.first != 1) {                                                \
            init.isuse = 1;                                                   \
            init.type = enum_String;                                          \
            init.len = length;                                                \
            init.maxLen = maxLength;                                          \
            init.Enum_string_table = eunmTableS;                              \
            init.Enum_count = eunmCount;                                      \
            DT_initValueBuffer(&init, length, str);                           \
        }                                                                     \
    } while (0)

#define DT_SetBlob(init, length, maxLength, buf)    \
    do {                                            \
        if (init.first != 1) {                      \
            init.isuse = 1;                         \
            init.type = enum_Blob;                  \
            init.len = length;                      \
            init.maxLen = maxLength;                \
            DT_initValueBuffer(&init, length, buf); \
        }                                           \
    } while (0)

#define DT_SetBlobEnum(init, length, maxLength, buf, eunmTableB, eunmTableL, eunmCount) \
    do {                                                                                \
        if (init.first != 1) {                                                          \
            init.isuse = 1;                                                             \
            init.type = enum_Blob_Enum;                                                 \
            init.len = length;                                                          \
            init.maxLen = maxLength;                                                    \
            init.Enum_blob_table = eunmTableB;                                          \
            init.Enum_blob_l_table = eunmTableL;                                        \
            init.Enum_count = eunmCount;                                                \
            DT_initValueBuffer(&init, length, buf);                                     \
        }                                                                               \
    } while (0)

#define DT_SetFixBlob(init, length, maxLength, buf) \
    do {                                            \
        if (init.first != 1) {                      \
            init.isuse = 1;                         \
            init.type = enum_FixBlob;               \
            init.len = length;                      \
            init.maxLen = maxLength;                \
            DT_initValueBuffer(&init, length, buf); \
        }                                           \
    } while (0)

#define DT_SetIpv4(init, buf)                  \
    do {                                       \
        if (init.first != 1) {                 \
            init.isuse = 1;                    \
            init.type = enum_Ipv4;             \
            init.len = 4;                      \
            init.maxLen = 0;                   \
            DT_initValueBuffer(&init, 4, buf); \
        }                                      \
    } while (0)

#define DT_SetIpv6(init, buf)                   \
    do {                                        \
        if (init.first != 1) {                  \
            init.isuse = 1;                     \
            init.type = enum_Ipv6;              \
            init.len = 16;                      \
            init.maxLen = 0;                    \
            DT_initValueBuffer(&init, 16, buf); \
        }                                       \
    } while (0)

#define DT_SetMac(init, buf)                   \
    do {                                       \
        if (init.first != 1) {                 \
            init.isuse = 1;                    \
            init.type = enum_Mac;              \
            init.len = 6;                      \
            init.maxLen = 0;                   \
            DT_initValueBuffer(&init, 6, buf); \
        }                                      \
    } while (0)

//结构体相关

#define DT_Set_Struct(init, element, member)                                       \
    do {                                                                           \
        (init).structPtr = (void*)&((element).member);                             \
        (init).structStart = (size_t) & ((element).member) - (size_t) & (element); \
        (init).structLength = sizeof((element).member);                            \
    } while (0)

#define DT_SetS64_Struct(init, element, member, value) \
    do {                                               \
        DT_SetS64(init, value);                        \
        DT_Set_Struct(init, element, member);          \
    } while (0)

#define DT_SetS32_Struct(init, element, member, value) \
    do {                                               \
        DT_SetS32(init, value);                        \
        DT_Set_Struct(init, element, member);          \
    } while (0)

#define DT_SetS16_Struct(init, element, member, value) \
    do {                                               \
        DT_SetS16(init, value);                        \
        DT_Set_Struct(init, element, member);          \
    } while (0)

#define DT_SetS8_Struct(init, element, member, value) \
    do {                                              \
        DT_SetS8(init, value);                        \
        DT_Set_Struct(init, element, member);         \
    } while (0)

#define DT_SetU64_Struct(init, element, member, value) \
    do {                                               \
        DT_SetU64(init, value);                        \
        DT_Set_Struct(init, element, member);          \
    } while (0)

#define DT_SetU32_Struct(init, element, member, value) \
    do {                                               \
        DT_SetU32(init, value);                        \
        DT_Set_Struct(init, element, member);          \
    } while (0)

#define DT_SetU16_Struct(init, element, member, value) \
    do {                                               \
        DT_SetU16(init, value);                        \
        DT_Set_Struct(init, element, member);          \
    } while (0)

#define DT_SetU8_Struct(init, element, member, value) \
    do {                                              \
        DT_SetU8(init, value);                        \
        DT_Set_Struct(init, element, member);         \
    } while (0)

#define DT_SetNumberEnum_Struct(init, element, eunmTable, eunmCount, member, value) \
    do {                                                                            \
        DT_SetNumberEnum(init, value, eunmTable, eunmCount);                        \
        DT_Set_Struct(init, element, member);                                       \
    } while (0)

#define DT_SetNumberRange_Struct(init, element, min, max, member, value) \
    do {                                                                 \
        DT_SetNumberRange(init, value, min, max);                        \
        DT_Set_Struct(init, element, member);                            \
    } while (0)

#define DT_SetFloat_Struct(init, element, member, value) \
    do {                                                 \
        DT_SetFloat(init, value);                        \
        DT_Set_Struct(init, element, member);            \
    } while (0)

#define DT_SetDouble_Struct(init, element, member, value) \
    do {                                                  \
        DT_SetDouble(init, value);                        \
        DT_Set_Struct(init, element, member);             \
    } while (0)

#define DT_SetString_Struct(init, element, member, length, maxLength, value) \
    do {                                                                     \
        DT_SetString(init, length, maxLength, value);                        \
        DT_Set_Struct(init, element, member);                                \
    } while (0)

#define DT_SetStringEnum_Struct(init, element, eunmTableS, eunmCount, member, length, maxLength, value) \
    do {                                                                                                \
        DT_SetStringEnum(init, length, maxLength, value, eunmTableS, eunmCount);                        \
        DT_Set_Struct(init, element, member);                                                           \
    } while (0)

#define DT_SetBlob_Struct(init, element, member, length, maxLength, value) \
    do {                                                                   \
        DT_SetBlob(init, length, maxLength, value);                        \
        DT_Set_Struct(init, element, member);                              \
    } while (0)

#define DT_SetBlobEnum_Struct(init, element, member, length, maxLength, value, eunmTableB, eunmTableL, eunmCount) \
    do {                                                                                                          \
        DT_SetBlobEnum(init, length, maxLength, value, eunmTableB, eunmTableL, eunmCount);                        \
        DT_Set_Struct(init, element, member);                                                                     \
    } while (0)

#define DT_SetFixBlob_Struct(init, element, member, length, maxLength, value) \
    do {                                                                      \
        DT_SetFixBlob(init, length, maxLength, value);                        \
        DT_Set_Struct(init, element, member);                                 \
    } while (0)

#define DT_SetIpv4_Struct(init, element, member, value) \
    do {                                                \
        DT_SetIpv4(init, value);                        \
        DT_Set_Struct(init, element, member);           \
    } while (0)

#define DT_SetIpv6_Struct(init, element, member, value) \
    do {                                                \
        DT_SetIpv6(init, value);                        \
        DT_Set_Struct(init, element, member);           \
    } while (0)

#define DT_SetMac_Struct(init, element, member, value) \
    do {                                               \
        DT_SetMac(init, value);                        \
        DT_Set_Struct(init, element, member);          \
    } while (0)

#define DT_GetStructFuzzValue(init, Value, memcount) \
    DT_GetFuzzValueEX((S_ElementInit*)&(init), memcount, (char*)&(Value))

#define DT_GetFuzzValue(init, Value) \
    DT_GetFuzzValueEX((S_ElementInit*)&(init), sizeof(init) / sizeof(S_ElementInit), (char*)&(Value))

void DT_CleanElementInit(S_ElementInit* pInit, int count);
#define DT_Clear(init) DT_CleanElementInit((S_ElementInit*)&(init), sizeof(init) / sizeof(S_ElementInit))

//下边两个为内部函数

extern char* DT_GetFuzzValueEX(S_ElementInit* pInit, int count, char* pValue);
//================================================================================

/******************************************

4.接口三，这套接口不建议使用了

******************************************/

//下边为封装后的创建元素结构体的函数，节省用户代码行数

//创建元素结构体函数，即对外提供，又内部调用
extern void CREAT_ElementEX(S_Element* pElement, int isNeedFree, int isHasInitValue, int type, char* inBuf, int inLen);
extern void FREE_MutatedValue(S_Element* pElement);
extern void FREE_Element(S_Element* pElement);

//
extern S_Element* CREAT_ElementS64(int isHasInitValue, s64 initValue);
extern S_Element* CREAT_ElementS32(int isHasInitValue, s32 initValue);
extern S_Element* CREAT_ElementS16(int isHasInitValue, s16 initValue);
extern S_Element* CREAT_ElementS8(int isHasInitValue, s8 initValue);
extern S_Element* CREAT_ElementU64(int isHasInitValue, u64 initValue);
extern S_Element* CREAT_ElementU32(int isHasInitValue, u32 initValue);
extern S_Element* CREAT_ElementU16(int isHasInitValue, u16 initValue);
extern S_Element* CREAT_ElementU8(int isHasInitValue, u8 initValue);
extern S_Element* CREAT_ElementNumberEnum(int isHasInitValue, u32 initValue, int* eunmTable, int eunmCount);
extern S_Element* CREAT_ElementNumberRange(int isHasInitValue, u32 initValue, int min, int max);
extern S_Element* CREAT_ElementFloat(int isHasInitValue, float initValue);
extern S_Element* CREAT_ElementDouble(int isHasInitValue, double initValue);
extern S_Element* CREAT_ElementString(
    int isHasInitValue, char* initValue, int len, int maxLen);  // len字符串长度，strlen(str) + 1
extern S_Element* CREAT_ElementStringEnum(
    int isHasInitValue, char* initValue, int len, int maxLen, char* eunmTableS[], int eunmCount);
extern S_Element* CREAT_ElementBlob(int isHasInitValue, char* initValue, int len, int maxLen);
extern S_Element* CREAT_ElementBlobEnum(
    int isHasInitValue, char* initValue, int len, int maxLen, char* eunmTableB[], int eunmTableL[], int eunmCount);
extern S_Element* CREAT_ElementFixBlob(int isHasInitValue, char* initValue, int len, int maxLen);
extern S_Element* CREAT_ElementIpv4(int isHasInitValue, char* initValue);
extern S_Element* CREAT_ElementIpv6(int isHasInitValue, char* initValue);
extern S_Element* CREAT_ElementMac(int isHasInitValue, char* initValue);

//获取测试例的统一接口，获取后需自己强制转换成自己想要的类型
extern char* GET_MutatedValueRandom(S_Element* pElement);
extern char* GET_MutatedValueSequence(S_Element* pElement, int pos);
extern int GET_MutatedValueLen(S_Element* pElement);
extern int GET_IsBeMutated(S_Element* pElement);

extern s64 GET_MutatedValueSequenceS64(S_Element* pElement, int pos);
extern s32 GET_MutatedValueSequenceS32(S_Element* pElement, int pos);
extern s16 GET_MutatedValueSequenceS16(S_Element* pElement, int pos);
extern s8 GET_MutatedValueSequenceS8(S_Element* pElement, int pos);
extern u64 GET_MutatedValueSequenceU64(S_Element* pElement, int pos);
extern u32 GET_MutatedValueSequenceU32(S_Element* pElement, int pos);
extern u16 GET_MutatedValueSequenceU16(S_Element* pElement, int pos);
extern u8 GET_MutatedValueSequenceU8(S_Element* pElement, int pos);
extern s32 GET_MutatedValueSequenceNumberEnum(S_Element* pElement, int pos);
extern s32 GET_MutatedValueSequenceNumberRange(S_Element* pElement, int pos);
extern float GET_MutatedValueSequenceFloat(S_Element* pElement, int pos);
extern double GET_MutatedValueSequenceDouble(S_Element* pElement, int pos);
extern char* GET_MutatedValueSequenceString(S_Element* pElement, int pos);
extern char* GET_MutatedValueSequenceStringEnum(S_Element* pElement, int pos);
extern char* GET_MutatedValueSequenceBlob(S_Element* pElement, int pos);
extern char* GET_MutatedValueSequenceBlobEnum(S_Element* pElement, int pos);
extern char* GET_MutatedValueSequenceFixBlob(S_Element* pElement, int pos);
extern char* GET_MutatedValueSequenceIpv4(S_Element* pElement, int pos);
extern char* GET_MutatedValueSequenceIpv6(S_Element* pElement, int pos);
extern char* GET_MutatedValueSequenceMac(S_Element* pElement, int pos);

extern s64 GET_MutatedValueRandomS64(S_Element* pElement);
extern s32 GET_MutatedValueRandomS32(S_Element* pElement);
extern s16 GET_MutatedValueRandomS16(S_Element* pElement);
extern s8 GET_MutatedValueRandomS8(S_Element* pElement);
extern u64 GET_MutatedValueRandomU64(S_Element* pElement);
extern u32 GET_MutatedValueRandomU32(S_Element* pElement);
extern u16 GET_MutatedValueRandomU16(S_Element* pElement);
extern u8 GET_MutatedValueRandomU8(S_Element* pElement);
extern s32 GET_MutatedValueRandomNumberEnum(S_Element* pElement);
extern s32 GET_MutatedValueRandomNumberRange(S_Element* pElement);
extern float GET_MutatedValueRandomFloat(S_Element* pElement);
extern double GET_MutatedValueRandomDouble(S_Element* pElement);
extern char* GET_MutatedValueRandomString(S_Element* pElement);
extern char* GET_MutatedValueRandomStringEnum(S_Element* pElement);
extern char* GET_MutatedValueRandomBlob(S_Element* pElement);
extern char* GET_MutatedValueRandomBlobEnum(S_Element* pElement);
extern char* GET_MutatedValueRandomFixBlob(S_Element* pElement);
extern char* GET_MutatedValueRandomIpv4(S_Element* pElement);
extern char* GET_MutatedValueRandomIpv6(S_Element* pElement);
extern char* GET_MutatedValueRandomMac(S_Element* pElement);

/******************************************

5.其他辅助接口

******************************************/

extern void INIT_Common(void);
extern void CLEAR_Common(void);

//设置变异种子，同样的seed和range回放出来的测试例是一样的
//如果seed设置为0，则系统会随机根据time函数生成seed,如果系统不支持time函数，则seed设置成1
extern void INIT_Seed(int INIT_Seed, int startrange);

//全局随机函数
extern void ADD_WholeRandom(S_Element* pElement);
extern void LEAVE_WholeRandom(S_Element* pElement);

//全局顺序测试函数
extern void ADD_WholeSequence(S_Element* pElement);
extern void LEAVE_WholeSequence(S_Element* pElement);
extern int GET_WholeSequenceTotalNumber(void);

//下边函数操作全局变异算法开关，对外提供
extern void SET_CloseAllMutater(void);
extern void SET_OpenAllMutater(void);
extern void SET_CloseOneMutater(enum enum_Mutated MutatedNum);
extern void SET_OpenOneMutater(enum enum_Mutated MutatedNum);

//下边函数操作单独元素变异算法开关，对外提供
extern void SET_ElementCloseAllMutater(S_Element* pElement);
extern void SET_ElementOpenAllMutater(S_Element* pElement);
extern void SET_ElementCloseOneMutater(S_Element* pElement, enum enum_Mutated MutatedNum);
extern void SET_ElementOpenOneMutater(S_Element* pElement, enum enum_Mutated MutatedNum);

// 16进制buf打印函数
extern void HEX_Dump(u8* buf, u32 len);

//调试函数
extern void DEBUG_Element(S_Element* pElement);

#ifdef __cplusplus
}
#endif
#endif
