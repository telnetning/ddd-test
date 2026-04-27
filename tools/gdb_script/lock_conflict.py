#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (C) 2026 Huawei Technologies Co.,Ltd.
#
# dstore is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# dstore is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. if not, see <https://www.gnu.org/licenses/>.
from enum import IntEnum

NO_LOCK = 0
ACCESS_SHARE_LOCK = 1
ROW_SHARE_LOCK = 2
ROW_EXCLUSIVE_LOCK = 3
SHARE_UPDATE_EXCLUSIVE_LOCK = 4
SHARE_LOCK = 5
SHARE_ROW_EXCLUSIVE_LOCK = 6
EXCLUSIVE_LOCK = 7
ACCESS_EXCLUSIVE_LOCK = 8
LOCK_MODE_MAX = 9

def GetLockMask(mode: int):
    return 1 << mode

# This is coppied from storage_lock_entry.cpp and barely modified

lockConflictMap = [
    0,
    # /* ACCESS_SHARE_LOCK */
    GetLockMask(ACCESS_EXCLUSIVE_LOCK),

    # /* ROW_SHARE_LOCK */
    GetLockMask(EXCLUSIVE_LOCK) | GetLockMask(ACCESS_EXCLUSIVE_LOCK),

    # /* ROW_EXCLUSIVE_LOCK */
    GetLockMask(SHARE_LOCK) | GetLockMask(SHARE_ROW_EXCLUSIVE_LOCK) | GetLockMask(EXCLUSIVE_LOCK) |
    GetLockMask(ACCESS_EXCLUSIVE_LOCK),

    # /* SHARE_UPDATE_EXCLUSIVE_LOCK */
    GetLockMask(SHARE_UPDATE_EXCLUSIVE_LOCK) | GetLockMask(SHARE_LOCK) | GetLockMask(SHARE_ROW_EXCLUSIVE_LOCK) |
    GetLockMask(EXCLUSIVE_LOCK) | GetLockMask(ACCESS_EXCLUSIVE_LOCK),

    # /* SHARE_LOCK */
    GetLockMask(ROW_EXCLUSIVE_LOCK) | GetLockMask(SHARE_UPDATE_EXCLUSIVE_LOCK) |
    GetLockMask(SHARE_ROW_EXCLUSIVE_LOCK) | GetLockMask(EXCLUSIVE_LOCK) | GetLockMask(ACCESS_EXCLUSIVE_LOCK),

    # /* SHARE_ROW_EXCLUSIVE_LOCK */
    GetLockMask(ROW_EXCLUSIVE_LOCK) | GetLockMask(SHARE_UPDATE_EXCLUSIVE_LOCK) | GetLockMask(SHARE_LOCK) |
    GetLockMask(SHARE_ROW_EXCLUSIVE_LOCK) | GetLockMask(EXCLUSIVE_LOCK) | GetLockMask(ACCESS_EXCLUSIVE_LOCK),

    # /* EXCLUSIVE_LOCK */
    GetLockMask(ROW_SHARE_LOCK) | GetLockMask(ROW_EXCLUSIVE_LOCK) | GetLockMask(SHARE_UPDATE_EXCLUSIVE_LOCK) |
    GetLockMask(SHARE_LOCK) | GetLockMask(SHARE_ROW_EXCLUSIVE_LOCK) | GetLockMask(EXCLUSIVE_LOCK) |
    GetLockMask(ACCESS_EXCLUSIVE_LOCK),

    # /* ACCESS_EXCLUSIVE_LOCK */
    GetLockMask(ACCESS_SHARE_LOCK) | GetLockMask(ROW_SHARE_LOCK) | GetLockMask(ROW_EXCLUSIVE_LOCK) |
    GetLockMask(SHARE_UPDATE_EXCLUSIVE_LOCK) | GetLockMask(SHARE_LOCK) | GetLockMask(SHARE_ROW_EXCLUSIVE_LOCK) |
    GetLockMask(EXCLUSIVE_LOCK) | GetLockMask(ACCESS_EXCLUSIVE_LOCK)
]

def hasConflict(mode1: int, mode2: int):
    return lockConflictMap[mode1] & GetLockMask(mode2) != 0
