#!/usr/bin/env python
# coding=utf-8
# description: Python script for buffer dirty page queue.
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
# date: 2025-05-15

import gdb

class DumpDirtyPageQueue(gdb.Command):
    """Dump the dirty page queue starting from a given address. Usage: dump_dirty_page_queue <start_address>"""

    def __init__(self):
        super().__init__("dump_dirty_page_queue", gdb.COMMAND_USER)

    @staticmethod
    def print_buffer_desc(addr):
        try:
            print(f"bufferDesc: {addr}")
            gdb.execute(f"p *(BufferDesc *){addr}")
        except gdb.error as e:
            print(f"Error printing BufferDesc at {addr}: {e}")

    def invoke(self, args, from_tty):
        arg_list = gdb.string_to_argv(args)
        if len(arg_list) != 1:
            raise gdb.GdbError("Usage: dump_dirty_page_queue <start_address>")
        start_addr = arg_list[0]
        if not start_addr.startswith("0x"):
            raise gdb.GdbError("Address must be in hexadecimal format (e.g., 0x7ffff7a2b010)")

        addr = start_addr
        while addr != "0x0":
            self.print_buffer_desc(addr)
            try:
                next_addr = gdb.parse_and_eval(f"((BufferDesc *){addr})->nextDirtyPagePtr._M_b._M_p")
                addr = str(next_addr)
            except gdb.error as e:
                print(f"Error accessing next node at {addr}: {e}")
                break

DumpDirtyPageQueue()