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
 * ---------------------------------------------------------------------------------
 *
 * ut_type_object.cpp
 * Unit Test Cases of C Language OOP Framework.
 *
 * ---------------------------------------------------------------------------------
 */

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <gtest/gtest.h>
#include <mockcpp/mockcpp.hpp>
#include "securec.h"
#include "types/type_object.h"

GSDB_BEGIN_C_CODE_DECLS

#define ERR_UT_COMPONENT_UTILS       0x01
#define ERR_UT_CTYPE_MODULE          0x01

#define E_UT_UTILS_SECUREC_ERROR   MAKE_ERROR_CODE(ERR_UT_COMPONENT_UTILS, ERR_UT_CTYPE_MODULE, 1)
#define E_UT_UTILS_INVALID_ARGS    MAKE_ERROR_CODE(ERR_UT_COMPONENT_UTILS, ERR_UT_CTYPE_MODULE, 2)
#define E_UT_UTILS_CONSTRUCTOR_ERR MAKE_ERROR_CODE(ERR_UT_COMPONENT_UTILS, ERR_UT_CTYPE_MODULE, 3)

constexpr const char* NAME = "Mr.Li";

/*
 * Define test class 1: Person, It is the base class of all test classes.
*/
#define MAX_PERSON_NAME_LEN 32
#define MAX_STUDENT_COUNT   256

typedef struct Person Person;
struct Person {
    TypeObject super;
    char name[MAX_PERSON_NAME_LEN];
    int age;
};

typedef struct PersonOps PersonOps;
struct PersonOps {
    TypeObjectOps super;
    ErrorCode (*getName)(Person *self, char name[], size_t size);
    int       (*getAge)(Person *self);
};

typedef struct PersonInitParams PersonInitParams;
struct PersonInitParams {
    TypeInitParams super;
    const char *name;
    int age;
};

ErrorCode GetPersonName(Person *self, char name[], size_t size);
int GetPersonAge(Person *self);

DECLARE_NEW_TYPED_CLASS(Person)

static int g_isPersonClassInited = 0;
static int g_personObjectCount = 0;

static ErrorCode PersonInit(Person *self, TypeInitParams *initData)
{
    PersonInitParams *param = DOWN_TYPE_CAST(initData, PersonInitParams);
    if (initData == NULL) {
        return E_UT_UTILS_CONSTRUCTOR_ERR;
    }

    errno_t err = strcpy_s(self->name, MAX_PERSON_NAME_LEN, param->name);
    if (err != 0) {
        /* A precise error code mapping is required, but the impact is small. */
        return E_UT_UTILS_SECUREC_ERROR;
    }
    self->age = param->age;
    ++g_personObjectCount;
    return ERROR_SYS_OK;
}

static ErrorCode GetPersonNameImpl(Person *self, char name[], size_t size)
{
    if (strlen(self->name) >= size) {
        return E_UT_UTILS_INVALID_ARGS;
    }
    errno_t err = strcpy_s(name, MAX_PERSON_NAME_LEN, self->name);
    if (err != 0) {
        /* A precise error code mapping is required, but the impact is small. */
        return E_UT_UTILS_SECUREC_ERROR;
    }
    return ERROR_SYS_OK;
}

static int GetPersonAgeImpl(Person *self)
{
    return self->age;
}

static void PersonFinalize(Person *self)
{
    --g_personObjectCount;
}

static void PersonOpsInit(PersonOps *self)
{
    ASSERT(self != NULL);
    GET_FOPS(Person)->getName = GetPersonNameImpl;
    GET_FOPS(Person)->getAge = GetPersonAgeImpl;
    ++g_isPersonClassInited;
}

DEFINE_NEW_TYPED_CLASS(Person, TypeObject)

ErrorCode GetPersonName(Person *self, char name[], size_t size)
{
    ASSERT(self != NULL);
    return GET_FAP(Person)->getName(self, name, size);
}

int GetPersonAge(Person *self)
{
    ASSERT(self != NULL);
    return GET_FAP(Person)->getAge(self);
}

/*
 * Define test class 2: Student, which derived from the Person class.
*/
typedef struct Student Student;
struct Student {
    Person super;
    int grade;
};

typedef struct StudentOps StudentOps;
struct StudentOps {
    PersonOps super;
    int (*getGrade)(Student *self);
};

typedef struct StudentInitParams StudentInitParams;
struct StudentInitParams {
    PersonInitParams super;
    int grade;
};

int getGrade(Student *self);

DECLARE_NEW_TYPED_CLASS(Student)

static int g_isStudentClassInited = 0;
static int g_studentObjectCount = 0;

static ErrorCode StudentInit(Student *self, TypeInitParams *initData)
{
    StudentInitParams *param = DOWN_TYPE_CAST(initData, StudentInitParams);
    self->grade = param->grade;
    ++g_studentObjectCount;
    return ERROR_SYS_OK;
}

static int getGradeImpl(Student *self)
{
    return self->grade;
}

static void StudentFinalize(Student *self)
{
    --g_studentObjectCount;
}

static void StudentOpsInit(Student *self)
{
    ASSERT(self != NULL);
    GET_FOPS(Student)->getGrade = getGradeImpl;
    ++g_isStudentClassInited;
}

DEFINE_NEW_TYPED_CLASS(Student, Person)

int getGrade(Student *self)
{
    ASSERT(self != NULL);
    return GET_FAP(Student)->getGrade(self);
}


/*
 * Define test class 3: Teacher, which derived from the Person class.
*/
typedef struct Teacher Teacher;
struct Teacher {
    Person super;
};

typedef struct TeacherOps TeacherOps;
struct TeacherOps {
    PersonOps super;
};

typedef struct TeacherInitParams TeacherInitParams;
struct TeacherInitParams {
    PersonInitParams super;
    bool createFailedTest;
};

DECLARE_NEW_TYPED_CLASS(Teacher)

static int g_isTeacherClassInited = 0;
static int g_teacherObjectCount = 0;

static ErrorCode TeacherInit(Teacher *self, TypeInitParams *initData)
{
    TeacherInitParams *param = DOWN_TYPE_CAST(initData, TeacherInitParams);
    if (param->createFailedTest) {
        return E_UT_UTILS_CONSTRUCTOR_ERR;
    }
    ++g_teacherObjectCount;
    return ERROR_SYS_OK;
}

static void TeacherFinalize(Teacher *self)
{
    --g_teacherObjectCount;
}

static void TeacherOpsInit(Teacher *self)
{
    ASSERT(self != NULL);
    ++g_isTeacherClassInited;
}

DEFINE_NEW_TYPED_CLASS(Teacher, Person)


/*
 * Define test class 4: Farmer, which derived from the Person class.
*/
typedef struct Farmer Farmer;
struct Farmer {
    Person super;
};

typedef struct FarmerOps FarmerOps;
struct FarmerOps {
    PersonOps super;
    int (*work)(Farmer *self);
};

DECLARE_NEW_TYPED_CLASS(Farmer)

static ErrorCode FarmerInit(Farmer *self, TypeInitParams *initData)
{
    ASSERT(self != NULL);
    if (initData == NULL) {
        return E_UT_UTILS_CONSTRUCTOR_ERR;
    } else {
        return ERROR_SYS_OK;
    }
}

static void FarmerFinalize(Farmer *self)
{
    ASSERT(self != NULL);
    return;
}

static void FarmerOpsInit(Farmer *self)
{
    ASSERT(self != NULL);
    GET_FOPS(Farmer)->work = NULL;
}

DEFINE_NEW_TYPED_CLASS(Farmer, Person)

GSDB_END_C_CODE_DECLS

/*****************************************************************************************************************/

class TypeObjectTest : public testing::Test {
public:
    void SetUp() override {
        g_personObjectCount = 0;
        g_studentObjectCount = 0;
    };

    void TearDown() override {
        g_personObjectCount = 0;
        g_studentObjectCount = 0;
    };
};


TEST_F(TypeObjectTest, CreateAndDeleteClassObjectTest)
{
    StudentInitParams paramStudent = {
        .super = {
            .super = TYPE_INIT_PARAMS_DEFAULT,
            .name = NAME,
            .age = 8,
        },
        .grade = 3,
    };

    ASSERT_TRUE(g_personObjectCount == 0);
    ASSERT_TRUE(g_studentObjectCount == 0);

    ErrorCode err = ERROR_SYS_OK;
    Student *student = NewStudent(CONSTRUCTOR_PARAM(&paramStudent), &err);
    ASSERT_TRUE(student != NULL);
    ASSERT_TRUE(g_isPersonClassInited == 1);
    ASSERT_TRUE(g_isStudentClassInited == 1);
    ASSERT_TRUE(g_personObjectCount == 1);
    ASSERT_TRUE(g_studentObjectCount == 1);

    TeacherInitParams paramTeacher = {
        .super = {
            .super = TYPE_INIT_PARAMS_DEFAULT,
            .name = NAME,
            .age = 38,
        },
        .createFailedTest = true,
    };

    Teacher *teacher = NewTeacher(CONSTRUCTOR_PARAM(&paramTeacher), &err);
    ASSERT_TRUE(teacher == NULL);
    ASSERT_TRUE(g_isTeacherClassInited == 1);
    ASSERT_TRUE(g_personObjectCount == 1);
    ASSERT_TRUE(g_studentObjectCount == 1);
    ASSERT_TRUE(g_teacherObjectCount == 0);

    paramTeacher.createFailedTest= false;
    teacher = NewTeacher(CONSTRUCTOR_PARAM(&paramTeacher), &err);
    ASSERT_TRUE(teacher != NULL);
    ASSERT_TRUE(g_isTeacherClassInited == 1);
    ASSERT_TRUE(g_personObjectCount == 2);
    ASSERT_TRUE(g_studentObjectCount == 1);
    ASSERT_TRUE(g_teacherObjectCount == 1);

    FreeTeacher(teacher);
    ASSERT_TRUE(g_isTeacherClassInited == 1);
    ASSERT_TRUE(g_personObjectCount == 1);
    ASSERT_TRUE(g_studentObjectCount == 1);
    ASSERT_TRUE(g_teacherObjectCount == 0);

    PersonInitParams paramPerson = {
        .super = TYPE_INIT_PARAMS_DEFAULT,
        .name = NAME,
        .age = 8,
    };

    Person *person = NewPerson(CONSTRUCTOR_PARAM(&paramPerson), &err);
    ASSERT_TRUE(person != NULL);
    ASSERT_TRUE(g_isPersonClassInited == 1);
    ASSERT_TRUE(g_isStudentClassInited == 1);
    ASSERT_TRUE(g_personObjectCount == 2);
    ASSERT_TRUE(g_studentObjectCount == 1);
    ASSERT_TRUE(g_teacherObjectCount == 0);

    FreePerson(person);
    ASSERT_TRUE(g_personObjectCount == 1);
    ASSERT_TRUE(g_studentObjectCount == 1);
    ASSERT_TRUE(g_teacherObjectCount == 0);

    FreeStudent(student);
    ASSERT_TRUE(g_personObjectCount == 0);
    ASSERT_TRUE(g_studentObjectCount == 0);
    ASSERT_TRUE(g_teacherObjectCount == 0);
}


TEST_F(TypeObjectTest, CallVirtualFunctionTest)
{
    StudentInitParams paramStudent = {
        .super = {
            .super = TYPE_INIT_PARAMS_DEFAULT,
            .name = NAME,
            .age = 8,
        },
        .grade = 3,
    };
    ErrorCode err = ERROR_SYS_OK;
    char name[MAX_PERSON_NAME_LEN] = {0};
    Student *student = NewStudent(CONSTRUCTOR_PARAM(&paramStudent), &err);
    ASSERT_TRUE(student != NULL);
    ASSERT_TRUE(GetPersonAge(UP_TYPE_CAST(student, Person)) == 8);
    ASSERT_TRUE(GetPersonName(UP_TYPE_CAST(student, Person), name, sizeof(name)) == ERROR_SYS_OK);
    ASSERT_TRUE(strcmp(name, NAME) == 0);
    ASSERT_TRUE(getGrade(student) == 3);
    FreeStudent(student);
}


TEST_F(TypeObjectTest, NULLVirtualFunctionTest)
{
    PersonInitParams paramPerson = {
        .super = TYPE_INIT_PARAMS_DEFAULT,
        .name = NAME,
        .age = 8,
    };
    ErrorCode err = ERROR_SYS_OK;
    Farmer *farmer = NewFarmer(CONSTRUCTOR_PARAM(&paramPerson), &err);
    ASSERT_TRUE(farmer == NULL);
    ASSERT_TRUE(err == E_UTILS_TYPE_OPS_FUNC_NULL);
}


TEST_F(TypeObjectTest, AlignedNewObjectTest)
{
    StudentInitParams paramStudent = {
        .super = {
            .super = TYPE_INIT_PARAMS_DEFAULT,
            .name = NAME,
            .age = 8,
        },
        .grade = 3,
    };
    ErrorCode err = ERROR_SYS_OK;
    char name[MAX_PERSON_NAME_LEN] = {0};
    Student *student = NewStudentAligned(CONSTRUCTOR_PARAM(&paramStudent), &err);
    ASSERT_TRUE(student != NULL);
    ASSERT_TRUE((uint64_t)student % GS_CACHE_LINE_SIZE == 0);
    ASSERT_TRUE(GetPersonAge(UP_TYPE_CAST(student, Person)) == 8);
    ASSERT_TRUE(GetPersonName(UP_TYPE_CAST(student, Person), name, sizeof(name)) == ERROR_SYS_OK);
    ASSERT_TRUE(strcmp(name, NAME) == 0);
    ASSERT_TRUE(getGrade(student) == 3);
    FreeStudent(student);

    Student *studentArray[MAX_STUDENT_COUNT];
    for (int index = 0; index < MAX_STUDENT_COUNT; ++index) {
        studentArray[index] = NewStudentAligned(CONSTRUCTOR_PARAM(&paramStudent), &err);
        ASSERT_TRUE(studentArray[index] != NULL);
        ASSERT_TRUE((uint64_t)studentArray[index] % GS_CACHE_LINE_SIZE == 0);
    }

    for (int index = 0; index < MAX_STUDENT_COUNT; ++index) {
        FreeStudent(studentArray[index]);
    }
}
