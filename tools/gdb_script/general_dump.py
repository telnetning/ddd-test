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
import sys
import gdb
import json
import subprocess
from collections import deque
from gdb_util import *
from rpc import *
from lock_locking import *
from lock_lwlock import *
from lock_conflict import hasConflict

def threadDumpInfo(thread):
    thread.switch()
    print("\n========== Thread number {} ==========".format(thread.num))
    try:
        thrdCtx = getThrdCtx(thread)
    except:
        thrdCtx = 0
    if int(thrdCtx) != 0:
        if int(thrdCtx["threadCore"]["core"]) != 0:
            print("Thread ID {}\n".format(thrdCtx["threadCore"]["core"]["pid"]))
        lwLocks = getHeldLWLocks(thrdCtx)
        print("Holding {} lw locks".format(len(lwLocks)))
        for address, mode in lwLocks:
            print("\tlwlock address: {}, lock mode: {}".format(address, mode))
        print("")
    frames = getThreadFrames()
    for i, frame in enumerate(frames):
        frameName = frame.name()
        values = {}
        if not frameName:
            continue
        if frameName == "DSTORE::DistributedBufferMgr::Read":
            values = getValues(frame, ["pageId", "mode", "bufferDesc", "pdbId"], ["Page Id", "Mode", "Buffer Desc"])
            pageId = values["Page Id"]
            pdbId = int(values["pdbId"].referenced_value())
            values["Page Id"] = "({}, {}, {})".format(pdbId, pageId["m_fileId"], pageId["m_blockId"])
            del values["pdbId"]
            if not values["Buffer Desc"].is_optimized_out and int(values["Buffer Desc"]):
                buffer = gdb.execute("p *(BufferDesc*){}".format(str(values["Buffer Desc"])), False, True)
                values["Buffer Desc"] = buffer[buffer.find("= ") + 2 : -1]
        elif frameName == "DSTORE::BufferCommunicator::RpcSend":
            values = getValues(frame, ["destNodeId", "destNodeRole", "bufferRequest"], ["Dest Node", "Role", "Buffer"])
            buffer = gdb.execute("p *(BufferPoolRPCRequestMsg*){}".format(str(values["Buffer"])), False, True)
            values["Buffer"] = buffer[buffer.find("= ") + 2 : -1]
        elif frameName == "DSTORE::LWLockAcquire":
            values = getValues(frame, ["lock", "mode"], ["Lock", "Mode"])
        elif frameName.startswith("DSTORE::BufferCommunicator::RpcReceive"):
            values = getValues(frame, ["rpcRequest->communicateInfo.dest.nodeId", "bufferRequest"], ["Dest Node Id", "Buffer"])
            try:
                buffer = gdb.execute("p *(BufferPoolRPCRequestMsg*){}".format(str(values["Buffer"])), False, True)
                values["Buffer"] = buffer[buffer.find("= ") + 2 : -1]
            except gdb.error:
                values["Buffer"] = "<optimized out>"
        elif frameName == "DSTORE::ThreadContext::Sleep":
            values = {"": "Thread is sleeping"}
        elif any(frameName.startswith(syncLock) for syncLock in ["DSTORE::CrossThreadRWLock::SyncLock", "DSTORE::CrossThreadRWLock::TrySyncLock"]):
            print("{} @ frame {}".format(frameName, i))
            frame.select()
            try:
                lock = gdb.parse_and_eval("*this")
                dumpCrossThreadRWLock(lock, "CrossThreadRWLock")
            except gdb.error:
                print("No symbol table for CrossThreadRWLock::SyncLock")
            print("")
            continue
        else:
            continue
        print("{} @ frame {}".format(frameName, i))
        printDict(values, "\t")
        print("")

def getHeldLWLocks(thrdCtx):
    lwLockContext = thrdCtx["lwlockContext"]
    numLocks = int(lwLockContext["num_held_lwlocks"])
    locks = []
    for i in range(numLocks):
        lock = lwLockContext["held_lwlocks"][i]["lock"]
        mode = lwLockContext["held_lwlocks"][i]["mode"]
        locks.append((str(lock), int(mode)))
    return locks


def getThreadFrames():
    frames = []
    frame = gdb.newest_frame()
    while frame != None:
        if frame.is_valid():
            frames.append(frame)
        frame = frame.older()
    return frames


def getValues(frame, variables, name):
    frame.select()
    res = {}
    for i, variable in enumerate(variables):
        res[name[i] if name and len(name) > i and name[i] else variable] = gdb.parse_and_eval(variable)
    return res

class DstoreGetThreadId(DstoreGdbCommand):
    """Prints out the current thread's id if obtainable - Usage: dstore_get_thrd_id"""
    def __init__(self):
        super().__init__("dstore_get_thrd_id")

    @errorDecorator
    def invoke(self, args, from_tty):
        try:
            print(f"ThreadId: {getThreadId()}")
        except:
            print("Failed to get current thread id")
class DstoreThreadIdToLWP(DstoreGdbCommand):
    """Prints out which LWPs map to which thread ids - Usage: dstore_thrd_to_lwp [threadId | threadCore]"""
    def __init__(self):
        super().__init__("dstore_thrd_to_lwp")

    @errorDecorator
    def invoke(self, args, from_tty):
        filter = None
        if args:
            try:
                filter = gdb.parse_and_eval(args)
                if filter.type in [gdb.lookup_type("int"), gdb.lookup_type("long")]:
                    filter = int(filter)
                elif filter.type in [gdb.lookup_type("DSTORE::ThreadContext"), gdb.lookup_type("DSTORE::ThreadContext").pointer()]:
                    filter = getThreadId(filter)
                else:
                    raise UsageError(self.__doc__)
            except:
                raise UsageError(self.__doc__)
        found = False
        for thread in gdb.selected_inferior().threads():
            try:
                thrdCtx = getThrdCtx(thread)
                thrdId = getThreadId(thrdCtx)
                found = True
            except:
                thrdId = None
            if filter is None or (thrdId == filter):
                print(f"Thread Num: {thread.num} - LWP: {thread.ptid[1]}{'' if thrdId is None else f' - ThreadId: {thrdId}'}")
        if not found:
            print("Did not find any thread information")

class DstoreThrdCtx(DstoreGdbCommand):
    """Tries to obtain the ThreadContext pointer for the current thread - Usage: dstore_thrd_ctx"""
    def __init__(self):
        super().__init__("dstore_thrd_ctx")

    @errorDecorator
    def invoke(self, args, from_tty):
        try:
            argList = gdb.string_to_argv(args)
            if len(argList) > 0:
                var = argList[0]
            else:
                var = "thrdCtx"
            thrdCtx = getThrdCtx()
            gdb.set_convenience_variable(var, thrdCtx)
            print(f"${var} = ((::DSTORE::ThreadContext *){str(thrdCtx)})")
        except:
            print("Unable to get ThreadContext variable")

class DumpDstoreInfo(DstoreGdbCommand):
    """Dumps all information on a gaussdb process - Usage: dump_dstore_info"""
    def __init__(self):
        super().__init__("dump_dstore_info")

    @errorDecorator
    def invoke(self, args, from_tty):
        orig_thread = gdb.selected_thread()
        threads = getThreads()
        for thread in threads:
            threadDumpInfo(thread)
        orig_thread.switch()

class DumpDstoreInfoCurrentThread(DstoreGdbCommand):
    """Dumps all information on the current thread - Usage: dump_dstore_current_thread_info"""
    def __init__(self):
        super().__init__("dump_dstore_current_thread_info")

    @errorDecorator
    def invoke(self, args, from_tty):
        threadDumpInfo(gdb.selected_thread())

class DumpDstoreDeadlockEdges(DstoreGdbCommand):

    """Dumps all edges that the current node knows about to try and determine if there is a deadlock - Usage: dump_dstore_deadlock_edges"""
    def __init__(self):
        super().__init__("dump_dstore_deadlock_edges")
        self.voidPtr = gdb.lookup_type("void").pointer()
        self.lockNodeInfo = ["nodeId", "threadId", "threadCoreIdx"]

    @errorDecorator
    def invoke(self, args, from_tty):
        if not sys.version.startswith("3."):
            exit(1)
        # General info
        self.lockRequestLinkerType = gdb.lookup_type("DSTORE::LockRequestLinker")
        self.distLockRequestPtrType = gdb.lookup_type("DSTORE::DistLockRequest").pointer()
        self.lockRequestPtrType = gdb.lookup_type("DSTORE::LockRequest").pointer()

        # Gather Node info
        self.nodeId = getNodeId()
        self.edges = deque()

        # Generate the edges from LockMgr locks
        # LockMgr
        print("Getting Lock Mgr Info")
        self.getLockMgrLocks()
        # XactLockMgr
        print("Getting Xact Lock Mgr Info")
        self.getXactLockMgrLocks()
        # TableLockMgr
        print("Getting Table Lock Mgr Info")
        self.getTableLockMgrLocks()

        # Generate the edges from LWLocks
        print("Getting LWLock Info")
        self.getLWLocks()

        # Generate the edges from CrossThreadRWLock
        print("Getting Cross Thread RWLock Info")
        self.getCrossThreadRWLocks()

        # Generate the edges from RPCs
        print("Getting RPC Info")
        self.getRpcEdges()

        # Report the edges found
        print("EDGES START")
        for edge in self.edges:
            print(json.dumps(edge))
        print("EDGES END")

    def getLockEntryConflicts(self, entry: gdb.Value, mgrType: str):
        entryInfo = {
            "tag": getLockTagStr(entry["lockEntryCore"]["m_lockTag"]),
            "grantedTotal": int(entry["lockEntryCore"]["m_grantedTotal"]),
            "waitingTotal": int(entry["lockEntryCore"]["m_waitingTotal"])
        }
        # Record each holder
        holders = []
        for holder in SkipListIter(entry["grantedQueue"], self.distLockRequestPtrType if mgrType != "XACT_LOCK_MGR" else self.lockRequestPtrType):
            holderInfo = {**self.getDistLockRequestInfo(holder), **entryInfo}
            holders.append(holderInfo)
        # Record each waiter
        prevWaiter = None
        for waiter in DListItr(entry["waitingQueue"], "defaultNodesSpace", self.lockRequestLinkerType.pointer()):
            # the type of waiter is still dlist_node* so we need to convert it to the request type
            request = gdb.parse_and_eval(f"(('{self.lockRequestLinkerType.tag}' *){int(waiter)})->ownerLockRequest").cast(self.distLockRequestPtrType)
            waiterInfo = {**self.getDistLockRequestInfo(request), **entryInfo}
            if prevWaiter == None:
                # The first waiter is the only one who's conflicts matter
                for holder in holders:
                    # Figure out if there is a conflict using the mask
                    if hasConflict(holder["modeInt"], waiterInfo["modeInt"]):
                        # Add edge waiter -> holder
                        self.edges.append({
                            "src": {k : waiterInfo[k] for k in self.lockNodeInfo},
                            "dest": {k : holder[k] for k in self.lockNodeInfo},
                            "type": mgrType,
                            "info": {
                                "conflict":"WAITER_AND_HOLDER",
                                "waitMode": waiterInfo["mode"],
                                "holdMode": holder["mode"],
                                **entryInfo
                            }
                        })
            else:
                # All other waiters are just dependant on the entry before them
                # Add edge waiter -> prevWaiter
                self.edges.append({
                    "src": {k :  waiterInfo[k] for k in self.lockNodeInfo},
                    "dest": {k : prevWaiter[k] for k in self.lockNodeInfo},
                    "type": mgrType,
                    "info": {
                        "conflict":"WAITER_AND_WAITER",
                        "waitMode": waiterInfo["mode"],
                        **entryInfo
                    }
                })
            prevWaiter = waiterInfo

    def getLockMgrLocks(self):
        lockMgr = getLockMgr(True)
        # We have to scan through each bucket in chash
        for bucket in CHashTableBucketIter(lockMgr["m_consistentHashTable"]):
            # Ignore invalid buckets
            if not isBucketValid(bucket):
                continue
            # Go through each entry
            for entry in LockEntryItr(bucket["m_element_list_head"]):
                self.getLockEntryConflicts(entry, "LOCK_MGR")

    def getXactLockMgrLocks(self):
        lockMgr = getXactLockMgr(True)
        htab = lockMgr["m_lockTable"].cast(gdb.lookup_type("DSTORE::LockHashTable").pointer())["m_lockTable"]
        # We have to scan through the entire lock hash table
        for entry in HTABIter(htab, gdb.lookup_type("DSTORE::LockEntry").pointer()):
            self.getLockEntryConflicts(entry, "XACT_LOCK_MGR")

    def getTableLockMgrLocks(self):
        lockMgr = getTableLockMgr(True)
        htab = lockMgr["m_lockTable"].cast(gdb.lookup_type("DSTORE::LockHashTable").pointer())["m_lockTable"]
        # We have to scan through the entire lock hash table
        for entry in HTABIter(htab, gdb.lookup_type("DSTORE::LockEntry").pointer()):
            self.getLockEntryConflicts(entry, "TABLE_LOCK_MGR")

    def getLWLocks(self):
        waiterTypePtr = gdb.lookup_type("DSTORE::LWLockWaiter").pointer()
        lockModeType = gdb.lookup_type("DSTORE::LWLockMode")
        waiterAddrToThrd = {}
        edges = deque()
        # We need to go through each thread to see if it is 1) waiting for a LWLock 2) holding a LWLock
        for thrdCtx in ThreadCtxIter():
            if thrdCtx["threadCore"]["core"] == 0:
                continue
            thrdInfo = {"nodeId": self.nodeId, "threadId": getThreadId(thrdCtx), "threadCoreIdx": getThreadCoreIdx(thrdCtx)}
            # If we are waiting for a LWLock, we need to add our address the the list
            if bool(thrdCtx["threadCore"]["core"]["lockWaiter"]["waiting"]):
                waiterAddrToThrd[int(thrdCtx["threadCore"]["core"]["lockWaiter"].address)] = thrdInfo
            # For each LWLock we hold, we go through it to record the waiters
            for lwlock in LWLockIter(thrdCtx["lwlockContext"]):
                prevWaiter = None
                for waiter in DListItr(lwlock["lock"]["waiters"], "next_waiter", waiterTypePtr):
                    if prevWaiter == None:
                        # The first waiter is blocked by the holder
                        edges.append({
                            "src": int(waiter),
                            "dest": thrdInfo,
                            "type": "LW_LOCK",
                            "info": {
                                "conflict": "WAITER_AND_HOLDER",
                                "holdMode": str(lwlock["mode"]),
                                "waitMode": str(waiter["mode"].cast(lockModeType)),
                                "groupId": int(lwlock["lock"]["groupId"]),
                                "address": hex(int(lwlock.address))
                            }
                        })
                    else:
                        # Every other waiter is blocked by the waiter before them
                        edges.append({
                            "src": int(waiter),
                            "dest": prevWaiter,
                            "type": "LW_LOCK",
                            "info": {
                                "conflict": "WAITER_AND_WAITER",
                                "waitMode": str(waiter["mode"].cast(lockModeType)),
                                "groupId": int(lwlock["lock"]["groupId"]),
                                "address": hex(int(lwlock.address))
                            }
                        })
                    prevWaiter = int(waiter)
        # We convert the LWLockWaiter addresses to thread info
        for edge in edges:
            for key in ["src", "dest"]:
                if not isinstance(edge[key], dict) and edge[key] in waiterAddrToThrd:
                    edge[key] = waiterAddrToThrd[edge[key]]
            self.edges.append(edge)

    def getCrossThreadRWLocks(self):
        visitedLocks = set()
        lockRequestType = gdb.lookup_type("DSTORE::CrossThreadRWLock::Request")
        for thread in gdb.selected_inferior().threads():
            # See if we can find a call to any of the CrossThreadLWLock functions.
            thread.switch()
            frame = findFrame("CrossThreadRWLock::", "CrossThreadRWLock::Request::")
            if not frame:
                continue
            frame.select()
            # If we found a frame with a CrossThreadLWLock, we read the holder and waiters list
            lock = gdb.parse_and_eval("this")
            if lock.type.target().unqualified() != gdb.lookup_type("DSTORE::CrossThreadRWLock") or \
                int(lock) in visitedLocks:
                continue
            visitedLocks.add(int(lock))
            prevWaiter = None
            for waiter in DListItr(lock["m_waitQueue"], "m_listEntry", lockRequestType.pointer()):
                waiterInfo = {"nodeId": int(waiter["m_nodeId"]["nodeId"]), "threadId": int(waiter["m_threadId"])}
                if prevWaiter == None:
                    # First waiter conflicts with all the holders
                    for holder in DListItr(lock["m_grantQueue"], "m_listEntry", lockRequestType.pointer()):
                        self.edges.append({
                            "src": waiterInfo,
                            "dest": {"nodeId": int(holder["m_nodeId"]["nodeId"]), "threadId": int(holder["m_threadId"])},
                            "type": "CROSS_THREAD_RW_LOCK",
                            "info": {
                                "conflict": "WAITER_AND_HOLDER",
                                "waitMode": str(waiter["m_mode"]),
                                "holdMode": str(waiter["m_mode"]),
                                "address": hex(int(lock))
                            }
                        })
                else:
                    # Every other waiter is blocked by the waiter before them
                    self.edges.append({
                        "src": waiterInfo,
                        "dest": prevWaiter,
                        "type": "CROSS_THREAD_RW_LOCK",
                        "info": {
                            "conflict": "WAITER_AND_WAITER",
                            "waitMode": str(waiter["m_mode"]),
                            "address": hex(int(lock))
                        }
                    })
                prevWaiter = waiterInfo

    def getRpcEdges(self):
        LoadLibComm()
        commMessageType = gdb.lookup_type("CommMessage")
        seqField = GetRpcSeqField()
        for thrdCtx in ThreadCtxIter():
            # Look for RPC received by this thread
            for rpcMessage in FindReceivedRpcMessages():
                try:
                    commMessage = rpcMessage["data"]["memContext"].cast(commMessageType.pointer())
                    self.edges.append({
                        "src": {"nodeId": int(commMessage["msgTransInfo"]["srcNid"])},
                        "dest": {"nodeId": self.nodeId, "threadId": getThreadId(thrdCtx), "threadCoreIdx": getThreadCoreIdx(thrdCtx)},
                        "type": "RPC_REQUEST",
                        "info": {
                            "seqId": int(commMessage["msgTransInfo"][seqField])
                        }
                    })
                except:
                    pass
                break
            # Look for RPC requests sent by this thread
            for rpcMessage in FindSentRpcMessages():
                try:
                    commMessage = rpcMessage["data"]["memContext"].cast(commMessageType.pointer())
                    self.edges.append({
                        "src": {"nodeId": self.nodeId, "threadId": getThreadId(thrdCtx), "threadCoreIdx": getThreadCoreIdx(thrdCtx)},
                        "dest": {"nodeId": int(commMessage["msgTransInfo"]["destNid"])},
                        "type": "RPC_REQUEST",
                        "info": {
                            "seqId": int(commMessage["msgTransInfo"][seqField])
                        }
                    })
                except:
                    pass


    def getDistLockRequestInfo(self, request: gdb.Value) -> dict:
        return {
            "nodeId": str(request["nodeId"]["nodeId"]) if request.type != self.lockRequestPtrType else self.nodeId,
            "mode": str(request["m_lockMode"]),
            "modeInt": int(request["m_lockMode"]),
            "threadId": int(request["threadId"]),
            "threadCoreIdx": int(request["threadCoreIdx"])
        }

DstoreGetThreadId()
DstoreThreadIdToLWP()
DstoreThrdCtx()
DumpDstoreInfo()
DumpDstoreInfoCurrentThread()
DumpDstoreDeadlockEdges()
