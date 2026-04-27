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
 * test the code of fuzz_test
 */

#ifndef UT_DEMO_H_
#define UT_DEMO_H_

#include <gtest/gtest.h>

#include "mockcpp/mockcpp.hpp"
#include "../Secodefuzz/Public.h"
#include "gunit_test.h"

class fuzz_test : public testing::Test {
    GUNIT_TEST_SUITE(fuzz_test);

public:
    virtual void SetUp();
    virtual void TearDown();

public:
    void test_start();

private:
};

extern "C" {
typedef struct {
    int value;
    char name[16];
    int value1;
} TS_TEST2;

extern int fun_branch(s32 tint, s8 tint1, char* tchar, char* tchar1);
extern int fun_multype(int numberRange, int numberEnum, s32 tint, char* tchar, char* stringenum);

extern int LLVMFuzzerTestOneInput1(const uint8_t* Data, size_t Size);
extern int LLVMFuzzerTestOneInput2(const uint8_t* Data, size_t Size);
extern int LLVMFuzzerTestOneInput3(const uint8_t* Data, size_t Size);

extern int fun_bug(int number1, int number2, char* stringe1, char* stringe2);
extern int fun_RightRange(int number, char* stringe);

int funtestbug(s32 tint, char* tchar);

/******************************************

算法库例子文件，大家在这基础上修改即可

******************************************/
extern int funtesta(s32 tint, char* tchar);
extern int funtestb(s32 tint, char* tipv4);

extern int funtestc(s32 tint, TS_TEST2* test2, char* tchar);

extern int fun_for(s32 tint, char* tchar);

extern void ReadFromFile(char** data, int* len, char* Path);
extern void WriteToFile(char* data, int len, char* Path);
extern int fun_example(int number, char* string, char* buf);
extern void clean_a(void);
}
#endif /* FUZZ_TEST_H_ */
