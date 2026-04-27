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
 * ---------------------------------------------------------------------------------------
 *
 * IDENTIFICATION
 *        src/test/ut/include/gunit_test.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef GTEST_BACKEND_H
#define GTEST_BACKEND_H

#include "gtest/gtest.h"

#define GUNIT_TEST_SUITE(test_fixture) \
public:                                \
    test_fixture(){};                  \
    virtual ~test_fixture(){};

#define GUNIT_TEST_REGISTRATION(test_fixture, test_name) \
    TEST_F(test_fixture, test_name)                      \
    {                                                    \
        test_fixture##_##test_name##_Test().test_name(); \
    }

#endif
