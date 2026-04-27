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
from gdb_util import *

class LWLockIter:
    def __init__(self, lwlockContext):
        self.heldLocks = lwlockContext["held_lwlocks"]
        self.numHeldLocks = lwlockContext["num_held_lwlocks"]
        self.curIdx = 0

    def __iter__(self):
        return self

    def __next__(self) ->gdb.Value:
        if self.curIdx < self.numHeldLocks:
            res = self.heldLocks[self.curIdx]
            self.curIdx += 1
            return res
        else:
            raise StopIteration


def scan_thread_for_info(num_threads=None):
    orig_thread = gdb.selected_thread()
    threads = getThreads(num_threads)
    print("number of threads:", len(threads))
    print("Traversing through all threads to preload info...\n")
    for thread in threads:
        thread.switch()
    info = []
    for thread in threads:
        try:
            thrd = getThrdCtx(thread)
            if int(thrd) != 0:
                thread_info = []
                thread_info.append(thread.num)
                thread_info.append(thrd["lwlockContext"]["num_held_lwlocks"])
                lock_info = []
                for lwlock in LWLockIter(thrd["lwlockContext"]):
                    lock = lwlock["lock"]
                    mode = lwlock["mode"]
                    lock_info.append((str(lock), int(mode)))
                thread_info.append(lock_info)
                info.append(thread_info)
        except Exception as e:
            pass
    orig_thread.switch()
    return info

class PrintLwLockInfo(DstoreGdbCommand):
    """Prints which lwlocks are held by which threads - Usage: print_lwlock_info"""
    def __init__(self):
        super().__init__("print_lwlock_info")

    @errorDecorator
    def invoke(self, args, from_tty):
        gdb.execute("set print pretty on")
        gdb.execute("set pagination off")

        info = scan_thread_for_info()
        print("==== PrintLwLockInfo Report Starts Here ====\n")
        for thread_id, numlock, lockinfo in info:
            if numlock != 0:
                print("Thread Id: {} Num Locks: {}".format(thread_id, numlock))
                for lock, mode in lockinfo:
                    print("\tlwlock address: {}, lock mode: {}".format(lock, mode))
        print("")

class FindLwlock(DstoreGdbCommand):
    """Scans through all threads to see if anyone is holding the lwlock - Usage: find_lwlock <lwlock_address>"""
    def __init__(self):
        super().__init__("find_lwlock")

    @errorDecorator
    def invoke(self, args, from_tty):
        if len(args) == 0:
            raise UsageError(self.__doc__)
        val = gdb.parse_and_eval(args)
        lwlock_type = gdb.lookup_type("DSTORE::LWLock")
        lwlock_address = None
        if val.type in [gdb.lookup_type("int"), gdb.lookup_type("long"), gdb.lookup_type("void").pointer()]:
            lwlock_address = val.cast(lwlock_type.pointer())
        elif val.type == lwlock_type:
            lwlock_address = val.address
        if lwlock_address == None:
            raise UsageError(self.__doc__)
        assertType(lwlock_type.pointer(), lwlock_address)
        lwlock_address = str(lwlock_address)
        gdb.execute("set print pretty on")
        gdb.execute("set pagination off")

        info = scan_thread_for_info()
        holders = 0
        print("==== FindLwlock Report Starts Here ====\n")
        for thread_id, numlock, lockinfo in info:
            if numlock != 0:
                for lock, mode in lockinfo:
                    if lock == lwlock_address:
                        print("\tThread Id: {}, lwlock address: {}, lock mode: {}"
                              .format(thread_id, lock, mode))
                        holders += 1
        if holders == 0:
            print("Not found!\n")
        else:
            print("\nFound {} holder{}\n".format(holders, "" if holders == 1 else "s"))

PrintLwLockInfo()
FindLwlock()
