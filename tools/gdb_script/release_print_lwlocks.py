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
Use case:
      In running release binary, dstore loads system`s release libpthread.so that has been stripped,
    and it cannot print the Thread Local variable (such as 'thrd').
    this script can resolve the problem above.
Problems & Notes:
    some dstore background threads still can not print thrd(also lwlocks) at release binary.
    it only can print those User-worker threads which do not go out DSTORE namespace.
"""
import gdb
import re

"""
    Gdb Python Function - ReleasePrintCurThreadThrd.
    Usage Example:
        1. source this python gdb script
        2. python ReleasePrintCurThreadThrd()
            print thrd variable on current thread
"""
def ReleasePrintCurThreadThrd():
    def OnlyPrintThrdVar(thrd_var, thread, frame):
        p_thrd_var = "p *" + thrd_var
        res = gdb.execute(p_thrd_var, to_string = True)
        print(f"Thread {thread.num}, LWP {thread.ptid[1]}:")
        print(f"thrd: {res}")
    ReleaseDoWithCurThrdVar(OnlyPrintThrdVar)

"""
    Gdb Python Function - ReleasePrintCurThreadLwlock.
    Usage Example:
        1. source this python gdb script
        2. python ReleasePrintCurThreadLwlock()
            print the lwlocks held by current thread
"""
def ReleasePrintCurThreadLwlock():
    def OnlyPrintLwlock(thrd_var, thread, frame):
        print(f"Thread {thread.num}, LWP {thread.ptid[1]}:")
        lwlock_num_var = thrd_var + "->lwlockContext.num_held_lwlocks"
        p_lwlock_num_var = "p " + lwlock_num_var
        lwlock_num = gdb.execute(p_lwlock_num_var, to_string = True)
        if lwlock_num == None or lwlock_num == "":
            print("Exception case in OnlyPrintLwlock")
            return -1
        lwlock_num = lwlock_num.split("=")[1].strip()
        if int(lwlock_num) == 0:
            print("HOLD 0 lwlock")
            return 0
        print(f"HOLD {lwlock_num} lwlocks")
        res = ""
        lwlock_ptr_base = thrd_var + "->lwlockContext.held_lwlocks["
        for i in range(0, int(lwlock_num)):
            lwlock_ptr = lwlock_ptr_base + str(i) + "]"
            p_lwlock_var = "p " + lwlock_ptr
            res += gdb.execute(p_lwlock_var, to_string = True) + "; "
        print(f"lwlocks: {res}")
        return 0
    ReleaseDoWithCurThrdVar(OnlyPrintLwlock)

"""
    Gdb Python Function - ReleasePrintAllLwlocks.
    Usage Example:
        1. source this python gdb script
        2. python ReleasePrintAllLwlocks()
            print the lwlocks held by all business threads.
"""
def ReleasePrintAllLwlocks():
    for inferior in gdb.inferiors():
        threads = inferior.threads()
        print(f"reading total {len(threads)} num threads...")
        for thread in threads:
            thread.switch()
            ReleasePrintCurThreadLwlock()

"""
    Gdb Python Function - PrintThreadNumberByLWP.
    Usage Example:
        1. source this python gdb script
        2. python PrintThreadNumberByLWP("212333")
            input lwp number(such as "212333") and print the mapped thread number
"""
def PrintThreadNumberByLWP(lwp):
    for i in gdb.inferiors():
        for t in i.threads():
            if int(t.ptid[1]) == int(lwp):
                print(t.num)
                return t.num
    print("no found")
    return -1

"""
    Gdb Python Function - PrintCurThreadId.
    print current thread`s thread id
"""
def PrintCurThreadId():
    cur_thread = gdb.selected_thread()
    stack_string = gdb.execute("bt", to_string = True)
    thrd_entry_addr = FindThrdEntryAddrFromStack(stack_string)
    if thrd_entry_addr == None or thrd_entry_addr == "":
        return 0
    if thrd_entry_addr.find("optimize") != -1:
        print(f"Thread {cur_thread.num}, LWP {cur_thread.ptid[1]}, thrd entry addr has optimized.")
        return -1
    frame = gdb.newest_frame()
    while frame != None:
        frame_func = frame.name()
        if frame_func == None or frame_func == "":
            frame = frame.older()
            continue
        procId_var = '((((knl_thrd_context*)(((knl_thread_arg*)%s)->t_thrd)).proc_cxt.MyProcPid))' % (thrd_entry_addr)
        procId = ""
        try:
            procId = gdb.execute("p {}".format(procId_var), to_string = True)
        except:
            pass
        if procId != None and procId != "":
            print(f"thread id:{procId}")
            break
        frame = frame.older()

def PrintAllThreadId():
    for inferior in gdb.inferiors():
        threads = inferior.threads()
        print(f"reading total {len(threads)} num threads...")
        for thread in threads:
            thread.switch()
            print(f"Thread {thread.num}, LWP {thread.ptid[1]}:")
            PrintCurThreadId()

def ReleasePrintPrivateRefIfCurThreadPin(bufferDesc):
    def OnlyPrintPrivateRef(thrd_var, thread, frame):
        trx_var = thrd_var + "->m_transactionList->m_activeTransaction"
        p_trx_var = "p " + trx_var
        trx = gdb.execute(p_trx_var, to_string = True)
        if trx == None or trx == "":
            print("Exception case in OnlyPrintPrivateRef")
            return -1
        trx = trx.split("=")[1].strip().split(" ")[-1]
        if trx == "0x0":
            privateRefOut = gdb.execute("p " + thrd_var + "->bufferPrivateRefCount->m_private_refcount_array[0]@7", \
                to_string = True)
            if privateRefOut.find("{buffer = " + str(bufferDesc)) != -1 and \
                privateRefOut.find("{buffer = " + str(bufferDesc) + ", refcount = 0}") == -1:
                print(f"Thread {thread.num}, LWP {thread.ptid[1]}:")
                print(privateRefOut)
        else:
            privateRefOut = gdb.execute("p " + trx_var + "->m_bufferPrivateRefCount->m_private_refcount_array[0]@7", \
                to_string = True)
            if privateRefOut.find("{buffer = " + str(bufferDesc)) != -1 and \
                privateRefOut.find("{buffer = " + str(bufferDesc) + ", refcount = 0}") == -1:
                print(f"Thread {thread.num}, LWP {thread.ptid[1]}:")
                print(privateRefOut)
    ReleaseDoWithCurThrdVar(OnlyPrintPrivateRef)

def WhoPinTheBufferAtRelease(bufferDesc):
    for inferior in gdb.inferiors():
        threads = inferior.threads()
        print(f"reading total {len(threads)} num threads...")
        for thread in threads:
            thread.switch()
            ReleasePrintPrivateRefIfCurThreadPin(bufferDesc)

def WhoPinTheBufferAtDebug(bufferDesc):
    for inferior in gdb.inferiors():
        threads = inferior.threads()
        print(f"reading total {len(threads)} num threads...")
        for thread in threads:
            thread.switch()
            DebugPrintPrivateRefIfCurThreadPin(bufferDesc)

def DebugPrintPrivateRefIfCurThreadPin(bufferDesc):
    def DebugPrintPrivateRef(thread):
        p_trx_var = "p thrd->m_transactionList->m_activeTransaction"
        trx = gdb.execute(p_trx_var, to_string = True)
        if trx == None or trx == "":
            print("Exception case in OnlyPrintPrivateRef")
            return -1
        trx = trx.split("=")[1].strip().split(" ")[-1]
        if trx == "0x0":
            privateRefOut = gdb.execute("p thrd->bufferPrivateRefCount->m_private_refcount_array[0]@7", \
                to_string = True)
            target = re.compile(bufferDesc).findall(privateRefOut)
            if len(target) != 0:
                print(f"Thread {thread.num}, LWP {thread.ptid[1]}:")
                print(privateRefOut)
        else:
            privateRefOut = gdb.execute(p_trx_var + "->m_bufferPrivateRefCount->m_private_refcount_array[0]@7", \
                to_string = True)
            target = re.compile(bufferDesc).findall(privateRefOut)
            if len(target) != 0:
                print(f"Thread {thread.num}, LWP {thread.ptid[1]}:")
                print(privateRefOut)
    DebugDoWithCurThrdVar(DebugPrintPrivateRef)

"""
    Utils func.
"""
def DebugDoWithCurThrdVar(callback):
    cur_thread = gdb.selected_thread()
    frame = gdb.newest_frame()
    while frame != None:
        frame_func = frame.name()
        if frame_func == None or frame_func == "":
            frame = frame.older()
            continue
        if frame_func.find("DSTORE") != -1:
            try:
                frame.select()
                return callback(cur_thread)
            except:
                print(f"Thread {cur_thread.num}, LWP {cur_thread.ptid[1]}, exception at thrd.")
        frame = frame.older()

def FindThrdEntryAddrFromStack(stack):
    res_list = re.compile("internal_thread_func.*?args=(.*?)\)").findall(stack)
    if len(res_list) == 0:
        return ""
    elif len(res_list) > 1:
        raise Exception("FindThrdEntryAddrFromStack meet an unexpected case!")
    else:
        return res_list[0]

def ReleaseDoWithCurThrdVar(callback):
    cur_thread = gdb.selected_thread()
    stack_string = gdb.execute("bt", to_string = True)
    thrd_entry_addr = FindThrdEntryAddrFromStack(stack_string)
    if thrd_entry_addr == None or thrd_entry_addr == "":
        return 0
    if thrd_entry_addr.find("optimize") != -1:
        print(f"Thread {cur_thread.num}, LWP {cur_thread.ptid[1]}, thrd entry addr has optimized.")
        return -1
    frame = gdb.newest_frame()
    while frame != None:
        frame_func = frame.name()
        if frame_func == None or frame_func == "":
            frame = frame.older()
            continue
        if frame_func.find("DSTORE") != -1:
            try:
                frame.select()
                # @gaochenyu
                thrd_var = "((ThreadContext*)(((knl_thrd_context*)(((knl_thread_arg*)%s)->t_thrd)).storage_thread))" % (thrd_entry_addr)
                return callback(thrd_var, cur_thread, frame)
            except:
                print(f"Thread {cur_thread.num}, LWP {cur_thread.ptid[1]}, exception at thrd.")
        frame = frame.older()
