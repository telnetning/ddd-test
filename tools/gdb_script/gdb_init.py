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
import gdb
import sys
if not sys.version.startswith("3."):
    raise gdb.error("GDB's python version is\n" + str(sys.version) + "\nExpected version for dstore script is 3.7.0+")
from os import path
sys.path.append(path.expanduser(path.dirname(__file__)))
import gdb_util
import python_workers
import buffer_bufferpool
import buffer_pd
import lock_locking
import lock_lwlock
import general_dump