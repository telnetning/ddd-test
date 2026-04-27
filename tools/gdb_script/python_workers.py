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


class PythonWorkers(gdb.Parameter):
    def __init__(self):
        super(PythonWorkers, self).__init__(
            "python-workers", gdb.COMMAND_DATA, gdb.PARAM_UINTEGER)
        self.value = 10
        self.show_doc = "Number of workers in the pool used in python scripts. The value unlimited means num_cpu_cores() is used."

    def get_set_string(self):
        return "{} {}".format(self.show_doc, "unlimited" if self.value == None else self.value)

class PythonChunkSize(gdb.Parameter):
    def __init__(self):
        super(PythonChunkSize, self).__init__(
            "python-chunk-size", gdb.COMMAND_DATA, gdb.PARAM_UINTEGER)
        self.value = 100
        self.show_doc = "Approximate chunk size for each worker. Unlimited mean we aim for workload/workers."

    def get_set_string(self):
        return "{} {}".format(self.show_doc, "unlimited" if self.value == None else self.value)

class PythonWorkerLive(gdb.Parameter):
    def __init__(self):
        super(PythonWorkerLive, self).__init__(
            "python-instance-type", gdb.COMMAND_DATA, gdb.PARAM_AUTO_BOOLEAN)
        self.value = None
        self.show_doc = "Controls internal script logic for if the gdb instance is on a live process or a core file."

    def get_set_string(self):
        alive = gdb.selected_inferior().was_attached
        return "{} {}{}".format(self.show_doc, "(auto) " if self.value is None else "",
                                "Live" if self.value is True or (self.value is None and alive) else "Core")

PythonWorkers()
PythonChunkSize()
