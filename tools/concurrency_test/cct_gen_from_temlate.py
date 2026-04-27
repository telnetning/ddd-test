#!/usr/bin/env python
# coding: utf-8
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
""" concurrent tests auto-generator tool 

Provide a concurrent tests auto-generator tool to Speeds up the development of concurrent test cases 
and facilitates the maintenance of concurrent test cases.

Please use below command to show the usage：
dstore/tools/concurrency_test/cct_gen_from_temlate.py -h

Suggestion:
1.Add cct directory under unitest source code directory of each module to store concurrent test case templates 
and automatically generated concurrent test cases.
2.Create the template directory in the cct directory for storing test case templates. 
It is recommended that the template name contain the _template suffix,
3.Newly generated concurrent test cases are stored in the cct directory.
"""

import json
import logging
import os
import stat
import re
import sys
import argparse
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def refresh_thread_time_slot(thread_time_slot, _line4parse):
    cct_thread = re.search("thread-(\d+):\s*\{(.+)\}", _line4parse)
    if cct_thread:
        thread_id = int(cct_thread.group(1))
        time_slot_str = cct_thread.group(2)
        _time_s = time_slot_str.split(",")

        if thread_id in thread_time_slot:
            raise Exception("Error: the thread id %d is existing", thread_id)
        thread_time_slot[thread_id] = [_t.strip() for _t in _time_s]


def refresh_func_blocks(func_blocks, _line4parse, switch, _i):
    cct_f = re.search(".*(b|e)_(f\d+).*", _line4parse)
    if cct_f:
        b_e = cct_f.group(1)
        func = cct_f.group(2)
        if b_e == "b":
            switch[func] += 1
            func_blocks[func].append(_i)
        elif b_e == "e":
            switch[func] -= 1
            func_blocks[func].append(_i + 1)
            if switch[func] != 0:
                raise Exception("Error: the func begin and end marked incorrect")
        else:
            raise Exception("Error: format error, func block should between b_f<int> and e_f<int>")


def parse_file(source_code_f):
    thread_time_slot = {}
    func_blocks = defaultdict(lambda: [])

    with open(source_code_f) as source_code:
        _lines = source_code.readlines()
        switch = defaultdict(lambda: 0)
        for _i, _line4parse in enumerate(_lines):
            cct_mark = re.search("(\*|//)\s*CCT::", _line4parse)
            if cct_mark:
                logger.debug(_i, cct_mark, _line4parse)

                refresh_thread_time_slot(thread_time_slot, _line4parse)

                refresh_func_blocks(func_blocks, _line4parse, switch, _i)

            # change the test name to add _CCT in case duplicated testname error
            testname_define = re.search("^TEST.{0,2}\((.+),\s*(.+)\)", _line4parse)

            if testname_define:
                _lines[_i] = _line4parse.replace(testname_define.group(2), testname_define.group(2) + "_CCT")

    return thread_time_slot, func_blocks, _lines


def get_indent():
    indent = " " * 3
    indent2 = (" " + indent) * 2
    return indent, indent2


def wait_blk(indent, wrf, thr_id, time_, f_):
    if SYNC_POINT == "Y":
        wrf.write("\n%s printf(\"thread:%s, time_slot:%s, func:%s\\n\");\n" % (indent, thr_id, time_, f_))
        wrf.write("%s syncPointGroup.SyncPoint(%s);\n" % (indent, thr_id))
    else:
        wrf.write("%s step_progress_%d[++tid].set_value(tid);\n" % (indent, thr_id))
        wrf.write("%s while (step_await[tid]) {\n" % indent)
        wrf.write("%s    std::this_thread::sleep_for(std::chrono::milliseconds(5));\n" % indent)
        wrf.write("%s }\n" % indent)


def concurrent_ctl_common(wrf, thread_time_slot):
    indent, indent2 = get_indent()

    time_slot_num = 0
    for _, time_fs in thread_time_slot.items():
        time_slot_num = max(time_slot_num, len(time_fs))

    wrf.write("%s int thread_num=%d;\n" % (indent, len(thread_time_slot)))
    wrf.write("%s int time_slot_num=%d;\n" % (indent, time_slot_num))

    if SYNC_POINT == "Y":
        wrf.write("\n%s SyncPointGroup syncPointGroup{thread_num};\n" % (indent))
    else:
        for thr_id, _ in thread_time_slot.items():
            wrf.write("\n%s // For thread:%d\n" % (indent, thr_id))
            wrf.write("%s std::promise<int> step_progress_%d[time_slot_num];\n" % (indent, thr_id))
            wrf.write("%s std::future<int> coordinator_%d[time_slot_num];\n" % (indent, thr_id))
            wrf.write("%s for (size_t i = 0; i < time_slot_num; i++) {\n" % (indent))
            wrf.write("%s coordinator_%d[i] = step_progress_%d[i].get_future();\n" % (indent2, thr_id, thr_id))
            wrf.write("%s } // end thread:%d\n" % (indent, thr_id))

        wrf.write("\n%s // For time slot go on\n" % (indent))
        wrf.write("%s int step_await[time_slot_num]{1};\n" % (indent))
        wrf.write("%s const int timeout = 20;\n" % (indent))


def thread_definition_write_func_blk(write_blk_func_args):
    func_blocks, f_, wrf, thr_id, time_, has_written_to = write_blk_func_args
    indent, indent2 = get_indent()

    line_blk = func_blocks.get(f_, [])

    logger.info("begion to write func block %s %s", f_, line_blk)

    # when f_ == ni
    if len(line_blk) == 0:
        wait_blk(indent2, wrf, thr_id, time_, f_)
    elif len(line_blk) != 2:
        raise Exception("Error for func_blocks[%s]" % f_)
    else:
        # should ignore the CCT::b_f/e_f in the block between in [has_written_to, line_blk[0]]
        for _ckt, _ckf in func_blocks.items():
            if has_written_to < _ckf[0] and _ckf[1] < line_blk[0]:
                contents = [indent + _ for _ in _raw_content[has_written_to: _ckf[0]]]
                wrf.write(indent.join(contents))
                has_written_to = _ckf[0]
            if _ckf[1] < line_blk[0]:
                has_written_to = _ckf[1]

        contents = [indent + _ for _ in _raw_content[has_written_to: line_blk[1]]]
        wrf.write(indent.join(contents))

        wait_blk(indent2, wrf, thr_id, time_, f_)
        has_written_to = line_blk[1]

    return has_written_to


def thread_definition(wrf, first_blk, thread_time_slot, func_blocks):
    indent, indent2 = get_indent()

    has_written_to = None
    for thr_id, time_fs in thread_time_slot.items():
        # thread definition
        wrf.write("\n%s // auto-generated thread %d func\n" % (indent, thr_id))
        wrf.write("%s auto thread_%d_func = [&]() {\n" % (indent, thr_id))
        if SYNC_POINT != "Y":
            wrf.write("%s int tid = -1;\n" % indent2)

        if REG_THREAD == "Y":
            wrf.write("%s create_thread_and_register();\n" % indent2)
        if INIT_TXN == "Y":
            wrf.write("%s ut_init_transaction_runtime();\n" % indent2)

        logger.info("")
        logger.info("For thread %s, %s", thr_id, time_fs)

        # func configuration specified
        has_written_to = first_blk[0]
        for time_, f_ in [_.split(":") for _ in time_fs]:
            _write_blk_func_args = (func_blocks, f_, wrf, thr_id, time_, has_written_to)
            has_written_to = thread_definition_write_func_blk(_write_blk_func_args)

        wrf.write("%s };  // auto-generated for thread %d done\n\n" % (indent, thr_id))

    if not has_written_to:
        raise Exception("Please check the source code, the CCT:: should be begin with /* or # ")
    return has_written_to


def chk_wait_status(wrf, indent, indent2):
    wrf.write("\n%s // wait for the asyn op and the results\n" % indent)
    wrf.write("%s for (size_t i = 0; i < time_slot_num; i++) {\n" % indent)

    for thr_id, _ in _thread_time_slot.items():
        wrf.write("%s std::cout << \"thread-%d, time_slot[\" << i << \"] \";\n" % (indent2, thr_id))
        wrf.write("%s while (coordinator_%d[i].wait_for(std::chrono::milliseconds(50)) "
                  "!= std::future_status::ready) {std::cout << \".\";}\n" % (indent2, thr_id))

        wrf.write("%s int r%d = coordinator_%d[i].get();\n" % (indent2, thr_id, thr_id))
        wrf.write("%s std::cout << \"reached:\" << r%d << std::endl;\n" % (indent2, thr_id))
        wrf.write("\n")

    wrf.write("%s if (step_await[i] == 1)\n" % indent2)
    wrf.write("%s%s step_await[i] = 0;\n" % (indent2, indent2))

    close_loc = "%s }\n" % indent
    wrf.write(close_loc)

    wrf.write("%s int await_sum = 0;\n" % indent)
    wrf.write("%s for (size_t i = 0; i < time_slot_num; i++) {\n" % indent)
    wrf.write("%s await_sum += step_await[i];\n" % indent2)
    wrf.write(close_loc)

    wrf.write("%s if (await_sum == 0) {\n" % indent)
    wrf.write("%s std::cout << \"All time_slot reached!\" << std::endl;\n" % indent2)
    wrf.write("%s } else {\n" % indent)
    wrf.write("%s std::cerr << \"Please check why some steps are still awaiting!, "
              "await_sum=\" << await_sum << std::endl;\n" % indent2)
    wrf.write(close_loc)

    wrf.write("\n")


def thread_call(wrf, thread_time_slot):
    indent, indent2 = get_indent()
    for thr_id, _ in thread_time_slot.items():
        wrf.write("%s std::thread thread_%d(thread_%d_func);\n" % (indent, thr_id, thr_id))

    if SYNC_POINT != "Y":
        chk_wait_status(wrf, indent, indent2)

    for thr_id, _ in thread_time_slot.items():
        wrf.write("%s thread_%d.join();\n" % (indent, thr_id))


def write_concurrent_test(wr_file, thread_time_slot, func_blocks, raw_content):
    flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
    modes = stat.S_IWUSR | stat.S_IRUSR

    with os.fdopen(os.open(wr_file, flags, modes), 'w') as wrf:
        if SYNC_POINT == "Y":
            wrf.write("#include \"ut_utilities/ut_sync_point_group.h\"\n")
        else:
            wrf.write("#include <future>\n")

        wrf.write("#include <thread>\n")

        func_list = list(func_blocks.keys())

        # before the first one of func_blocks, write to new file directly
        first_blk = func_blocks.get(func_list[0], [])
        if not first_blk:
            raise Exception("ERROR: because no func list configured")

        wrf.write("".join(raw_content[: first_blk[0]]))

        # for concurrent control common block
        concurrent_ctl_common(wrf, thread_time_slot)

        # for threads definition
        has_written_to = thread_definition(wrf, first_blk, thread_time_slot, func_blocks)

        # call threads and join them
        thread_call(wrf, thread_time_slot)

        # write the remain contents
        wrf.write("".join(raw_content[has_written_to:]))

        # write comments
        wrf.write("\n/*This test was generated from template: %s */\n" % template_file)


if __name__ == '__main__':
    aparse = argparse.ArgumentParser(
        prog="cct_gen_from_template",
        description="concurrent tests generator from template"
    )

    aparse.add_argument('-d', '--dir', required=True, help="template file directory")
    aparse.add_argument('-t', '--template', required=True, help="template file name")
    aparse.add_argument('-o', '--ouffile_prefix', default="cct_", help="the new test name generated")

    aparse.add_argument('-s', '--sync_point', default="Y",
                        help="use sync_point or promise to control concurrent behaviors, default:Y")
    aparse.add_argument('-i', '--init_transaction', default="Y", help="init transaction for each thread, default:Y")
    aparse.add_argument(
        '-r', '--register_thread', default="Y",
        help="create thread and register, will call create_thread_and_register(), default:Y")

    args = aparse.parse_args()

    template_dir = os.path.normpath(args.dir)
    template_file = args.template

    if "temp" not in template_dir:
        logger.warning("suggest to put template files under the dir template")

    if "template" in template_file:
        new_file = args.ouffile_prefix + template_file.replace("template", "")
    elif "temp" in template_file:
        new_file = args.ouffile_prefix + template_file.replace("temp", "")
    else:
        new_file = args.ouffile_prefix + template_file

    # the variable defined in __main__ will be global
    SYNC_POINT = args.sync_point
    INIT_TXN = args.init_transaction
    REG_THREAD = args.register_thread

    _thread_time_slot, _func_blocks, _raw_content = parse_file(template_dir + os.sep + template_file)

    if "template" in template_dir:
        _nfile = os.path.dirname(template_dir) + os.sep + new_file
    else:
        _nfile = template_dir + os.sep + new_file

    write_concurrent_test(_nfile, _thread_time_slot, _func_blocks, _raw_content)
    logger.info("created new test source code: %s", _nfile)
