#!/usr/bin/env python
# coding=utf-8
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
# description: Python script for gdb print function.
# date: 2025-05-15

import gdb

def set_pretty_print():
    pretty = gdb.parameter("print pretty")
    if not pretty:
    gdb.execute("set print pretty on")

### argument page is the address of heap page ###
def print_heap_page_item(page):
    set_pretty_print()
    page_str = "((DSTORE::HeapPage *)" + str(page) + ")"
    cmd_ptr = "p (char *)(" + page_str + ")->m_data" 
    cmd_ptr += " + sizeof(DSTORE::TD) * " + page_str + "->dataHeader.tdCount"

    result = gdb.execute(cmd_ptr, to_string=True)
    varlist = result.split()
    item_id_ptr = varlist[2]

    print("ItemId address is {}".format(item_id_ptr))


### argument page is the address of heap page ###
def print_btr_page_item(page):
    set_pretty_print()
    page_str = "((DSTORE::BtrPage *)" + str(page) + ")"
    cmd_ptr = "p (char *)(" + page_str + ")->m_data" 
    cmd_ptr += " + sizeof(DSTORE::TD) * " + page_str + "->dataHeader.tdCount"

    result = gdb.execute(cmd_ptr, to_string=True)
    varlist = result.split()
    item_id_ptr = varlist[2]

    print("ItemId address is {}".format(item_id_ptr))
