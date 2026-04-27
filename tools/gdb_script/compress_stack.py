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
"""
    please make this python script and your input gstack file together.
"""
import re
import sys

HANG_FILE=""
if len(sys.argv)<1:
    raise Exception("need input gstack hang file and place it to the same dir with this file")
HANG_FILE=sys.argv[1]

class GstackFile(object):
    def __init__(self, gstack_file):
        self.file = gstack_file
        self.threads = []
        self.threads_only_func_stacks = []

    def GetThreads(self):
        if self.threads != None and len(self.threads) != 0:
            return
        data = ""
        with open(self.file, "r") as f:
            data = f.read()
        self.threads = re.compile("Thread \d+ .*?(?=Thread \d+ )", re.S).findall(data)

    def GetMappedFuncStacksBaseThreads(self):
        if self.threads_only_func_stacks != None and len(self.threads_only_func_stacks) != 0:
            return
        self.GetThreads()
        for thread in self.threads:
            self.threads_only_func_stacks.append(" ".join(re.compile("in (.*?)\(").findall(thread)))

    def PrintCompressStackBaseFunc(self):
        self.GetMappedFuncStacksBaseThreads()
        threads_only_func_stacks_set = set(self.threads_only_func_stacks)
        for e in threads_only_func_stacks_set:
            print(f"===repeat {self.threads_only_func_stacks.count(e)}===")
            print(self.threads[self.threads_only_func_stacks.index(e)])
            print("--------")

GstackFile(HANG_FILE).PrintCompressStackBaseFunc()
