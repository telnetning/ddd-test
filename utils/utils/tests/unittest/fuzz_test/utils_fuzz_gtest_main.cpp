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
 * Description: utils UT main function
 * ---------------------------------------------------------------------------------
 */

#include <cstdio>
#include "gtest/gtest.h"

GTEST_API_ int main(int argc, char **argv) {
    printf("Running main() from %s\n", __FILE__);
    auto fake = new char *[argc + 1];
    for (int i = 0; i < argc; i++) {
        fake[i] = argv[i];
    }
    fake[argc++] = (char *)"--gtest_death_test_use_fork=true";
    testing::InitGoogleTest(&argc, fake);
    delete[] fake;
    return RUN_ALL_TESTS();
}
