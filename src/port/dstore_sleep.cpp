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
 * Description:
 * Portable delay handling.
 */
#include <chrono>
#include <thread>
#include "common/dstore_datatype.h"
namespace DSTORE {
/*
 * GaussUsleep --- delay the specified number of microseconds.
 *
 * Note: Although  the delay is specified in microseconds, on most Unixen,
 * the effective resolution is only 1/HZ, or 10 milliseconds.  Expect
 * the requested delay to be rounded up to the next resolution boundary.
 *
 * On machines where "long" is 32 bits, the maximum delay is ~2000 seconds.
 */
void GaussUsleep(long microsec)
{
    if (unlikely(microsec == 0)) {
        return;
    }
    std::this_thread::sleep_for(std::chrono::microseconds(microsec));
}

}
