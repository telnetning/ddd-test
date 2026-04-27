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
import os
import re
import gdb
import traceback
import subprocess
from typing import List, Tuple

class UsageError(gdb.GdbError):
    pass

def _getType(var):
    if isinstance(var, gdb.Value):
        var = var.type
    if isinstance(var, gdb.Type):
        var = var.tag if var.tag != None else str(var)
    if not isinstance(var, str):
        gdb.GdbError(f"Could not convert {type(var)} into str")
    return var.replace("struct ", "").replace("class ", "")

def compareType(a, b):
    """Compares the given types and makes sure that they match"""
    return _getType(a) == _getType(b)


def assertType(expectType, actualType, info=""):
    """Compares the given types and makes sure that they match. Raises an exception if they do not"""
    if not compareType(expectType, actualType):
        raise gdb.GdbError("Error: {} Expected type {}, instead got {}".format(
            info, _getType(expectType), _getType(actualType)))

def printDict(dict, prefix=""):
    print("{}{}".format(prefix,
            ", ".join(["{} = {}".format(field, dict[field]) if field else dict[field] for field in dict])))

def printFilter(filter):
    """Given a filter to match entries, print out the fields and their values"""
    if filter:
        print("Filter: {}".format(
            ", ".join(["{} = {}".format(field, filter[field]) for field in filter])))


def checkFilter(filter, values):
    """Checks to see if the values match the filter"""
    return all([values[field] == filter[field] for field in filter])


def errorDecorator(func):
    """Decorator to wrap functions with try except with traceback"""
    def errLogFunc(*args, **kwargs):
        try:
            func(*args, **kwargs)
        except KeyboardInterrupt:
            raise
        except UsageError as e:
            print(e)
        except Exception as e:
            print(traceback.print_exc())
            raise
    return errLogFunc

def getThreads(num_threads = None):
    """Returns an array of all the thread in the current inferior, sorted by thread num"""
    threads = [t for t in gdb.selected_inferior().threads() if (num_threads is None or t.num <= num_threads)]
    return sorted(threads, key = lambda thread: thread.num)

class ThreadCtxIter:
    """Iterates over all thrdCtx variables across all threads"""
    def __init__(self):
        self.threads = iter(gdb.selected_inferior().threads())

    def __iter__(self):
        return self

    def __next__(self) -> gdb.Value:
        while 1:
            # Iterate over each thread
            thread = next(self.threads)
            try:
                # Try to get the thrdCtx from the current thread
                thrdCtx = getThrdCtx(thread)
                return thrdCtx
            except:
                # If we fail to geth the thrdCtx, move on to the next thread
                continue

def getFieldOffset(type:gdb.Type, fieldName:str) -> int:
    """Given a type and field name, calculated the byte offset of the field in the type"""
    for field in type.fields():
        if field.name == fieldName:
            return field.bitpos // 8
    raise gdb.GdbError(f"Did not find field with name {fieldName} in type {str(type)}")

def getStorageInstance(isDistributed = False) -> gdb.Value:
    cast = f"('::DSTORE::StorageDistributedComputeInstance' *)" if isDistributed else ""
    return gdb.parse_and_eval(f"{cast}'::DSTORE::g_storageInstance'")

def getGucs() -> gdb.Value:
    return getStorageInstance()["m_guc"]

def getNodeId() -> int:
    return int(getGucs()["selfNodeId"])

def saveNodeId() -> gdb.Value:
    gdb.set_convenience_variable("nodeId", getGucs()["selfNodeId"])

def getThrdCtx(thread: gdb.InferiorThread = None) -> gdb.Value:
    """This function tries to get the thread context by looking for any references to it throughout the stack"""
    if thread:
        thread.switch()
    else:
        thread = gdb.selected_thread()
    thrdCtx = 0
    thrdCtxType = gdb.lookup_type("DSTORE::ThreadContext").pointer()
    try:
        # First, try to see if we can just the the ctx through the thread local variable thrd
        thrdCtx = gdb.parse_and_eval("'::DSTORE::thrd'")
        assertType(thrdCtxType, thrdCtx)
        if int(thrdCtx) != 0 and not thrdCtx.is_optimized_out:
            return thrdCtx
    except Exception as e:
        pass
    # Try to get the thrd variable by looking at gauss_db_thread_main() or internal_thread_func()
    knlThreadArgType = gdb.lookup_type("knl_thread_arg").pointer()
    frameItr = FrameItr(pattern=re.compile("gauss_db_thread_main|internal_thread_func"))
    for var, frame in VariableItr(frameItr = frameItr, pattern=re.compile("args?"), types=[gdb.lookup_type("void").pointer()]):
        try:
            value = var.value(frame)
            thrdCtx = value.cast(knlThreadArgType)["t_thrd"].cast(gdb.lookup_type("knl_thrd_context").pointer())["storage_thread"].cast(thrdCtxType)
            if int(thrdCtx) != 0 and not thrdCtx.is_optimized_out:
                return thrdCtx
        except Exception as e:
            print(e)
            continue

    # We will go through each frame in the thread and see if there is a reference to thrd
    types = [thrdCtxType, knlThreadArgType]
    for var, frame in VariableItr(types = types):
        try:
            value = var.value(frame)
            if value.type == knlThreadArgType:
                # If we found knlThreadArgType, we need to find the storage_thread member and cast it to the correct type
                thrdCtx = value["t_thrd"].cast(gdb.lookup_type("knl_thrd_context").pointer())["storage_thread"].cast(thrdCtxType)
            elif value.type == thrdCtxType:
                thrdCtx = value
            # Make sure the variable has a valid address and is not optimized out
            if int(thrdCtx) != 0 and not thrdCtx.is_optimized_out:
                return thrdCtx
        except Exception as e:
            continue
    raise gdb.error("Could not get thrd ctx")

gdbThreadRe = re.compile("^.+Thread\s+(0x[0-9a-f]+?)\s+.+$")
def gdbGetThreadId(thread: gdb.InferiorThread = None) -> int:
    """Try to get the thread id from gdb"""
    if thread:
        thread.switch()
    match = gdbThreadRe.match(gdb.execute("thread", from_tty = True, to_string = True))
    assert match
    return int(match.group(1), 16)

def getThreadId(thrdCtx: gdb.Value = None) -> int:
    if not thrdCtx:
        thrdCtx = getThrdCtx()
    return int(thrdCtx["threadCore"]["core"]["pid"])

def getThreadCoreIdx(thrdCtx: gdb.Value = None) -> int:
    if not thrdCtx:
        thrdCtx = getThrdCtx()
    return int(thrdCtx["threadCore"]["core"]["selfIdx"])

def dumpDList(dlist, type, max = None):
    """Dumps dlist elements"""
    for node in DListItr(dlist, max = max):
        print("@{}".format(str(node)))
        gdb.execute("p *({}){}".format(type, str(node)))

def dumpCHashBucket(bucket, dumpQueue = False):
    """Util function to pretty print CHashBucket"""
    assertType(gdb.lookup_type("DSTORE::ConsistentHashBucket").pointer(), bucket)
    pretty = gdb.parameter("print pretty")
    if not pretty:
        gdb.execute("set print pretty on")
    gdb.execute("p *(ConsistentHashBucket *){}".format(int(bucket)))
    if dumpQueue:
        dumpCrossThreadRWLock(bucket["m_bucket_latch"], "Bucket")
    if not pretty:
        gdb.execute("set print pretty off")

def dumpCrossThreadRWLock(crossThreadRWLock, name=""):
    assertType(gdb.lookup_type("DSTORE::CrossThreadRWLock"), crossThreadRWLock)
    print("\n{} GrantedQueue ({} holders):".format(name, int(crossThreadRWLock["m_readerCount"])))
    dumpDList(crossThreadRWLock["m_grantQueue"], "'::DSTORE::CrossThreadRWLock::Request' *", int(crossThreadRWLock["m_readerCount"]))
    print("\n{} WaitingQueue ({} waiters):".format(name, int(crossThreadRWLock["m_waiterCount"])))
    dumpDList(crossThreadRWLock["m_waitQueue"], "'::DSTORE::CrossThreadRWLock::Request' *", int(crossThreadRWLock["m_waiterCount"]))

def formatBufTag(bufTag):
    return "({}, {}, {})".format(int(bufTag["pdbId"]), bufTag["pageId"]["m_fileId"], bufTag["pageId"]["m_blockId"])

class FrameItr:
    """Iterates over all frames that match the given regex pattern"""
    def __init__(self, frame: gdb.Frame = None, pattern: re.Pattern = None):
        if frame:
            self.frame = frame
        else:
            self.frame = gdb.newest_frame()
        self.pattern = pattern
        pass

    def __iter__(self):
        return self

    def __next__(self) -> gdb.Frame:
        while self.frame:
            if self.frame.is_valid() and (not self.pattern or (self.frame.name() and self.pattern.search(self.frame.name()))):
                res = self.frame
                self.frame = self.frame.older()
                return res
            self.frame = self.frame.older()
        raise StopIteration

class VariableItr:
    """Iterates over all variables in each frame in the FrameItr that match any of the given types and match the regex pattern"""
    def __init__(self, frameItr: FrameItr = None, types: List[gdb.Type] = None, pattern: re.Pattern = None, checkOptimized = False):
        if frameItr:
            self.frameItr = iter(frameItr)
        else:
            self.frameItr = iter(FrameItr())
        self.blockItr = None
        self.types = types
        self.pattern = pattern
        self.checkOptimized = checkOptimized
    def __iter__(self):
        return self

    def __next__(self) -> Tuple[gdb.Symbol, gdb.Frame]:
        while 1:
            try:
                if self.blockItr == None:
                    raise StopIteration
                while 1:
                    symbol = next(self.blockItr)
                    if (not self.types or str(symbol.type) in [str(t) for t in self.types]) and \
                        (not self.pattern or self.pattern.search(symbol.name)) and \
                        (self.checkOptimized or not symbol.value(self.frame).is_optimized_out):
                        return symbol, self.frame
            except StopIteration:
                self.frame = next(self.frameItr)
                try:
                    self.blockItr = iter(self.frame.block())
                except RuntimeError:
                    continue


def findFrame(match, ignore: str = None, startFrame: gdb.Frame = None) -> gdb.Frame:
    if isinstance(match, str):
        match = [match]
    frame = startFrame if startFrame else gdb.newest_frame()
    while frame != None:
        if frame.is_valid() and frame.name():
            if all([m in frame.name() for m in match]) and (not ignore or ignore not in frame.name()):
                return frame
        frame = frame.older()
    return None


def findDstoreFrame():
    orig = (gdb.selected_thread (), gdb.selected_frame())
    for thread in getThreads():
        thread.switch()
        for f in FrameItr(pattern = re.compile("DSTORE::")):
            f.select()
            return orig
    raise gdb.error("Could not find DSTORE function")


def restoreOriginalFrame(orig):
    orig[0].switch()
    orig[1].select()


def isReleaseMode():
    info = gdb.execute("info shared", to_string=True).split("\n")
    for line in info:
        if line.strip().endswith("libdstore.so"):
            return "(*)" in line
    raise gdb.error("Unable to find libdstore.so in \"info shared\"")


def parseBufTag(argList, filterArgs):
    # Figure out what we need to filter by
    filterArgs = {}
    val = None
    if len(argList) == 3:
        try:
            for i, name in enumerate(['pdbId', 'fileId', 'blockId']):
                filterArgs[name] = int(argList[i])
        except ValueError:
            filterArgs = {}
            pass
    if not filterArgs:
        try:
            print(" ".join(argList))
            val = gdb.parse_and_eval(" ".join(argList))
            if val.type in [gdb.lookup_type('DSTORE::BufferDesc').pointer(), gdb.lookup_type('DSTORE::BufferDesc')]:
                val = val["bufTag"]
            if val.type == gdb.lookup_type('DSTORE::BufferTag').pointer():
                val = val.dereference()
            if val.type == gdb.lookup_type('DSTORE::BufferTag'):
                filterArgs = {
                    "pdbId": int(val["pdbId"]),
                    "fileId": int(val["pageId"]['m_fileId']),
                    "blockId": int(val["pageId"]['m_blockId'])
                }
            else:
                raise Exception("Expecting type DSTORE::BufferDesc or DSTORE::BufferTag got {}".format(
                    str(val.type)))
        except:
            raise gdb.error()
    return {"filterArgs":filterArgs, "val": val}

def isBucketValid(bucket: gdb.Value):
    bucketType = gdb.lookup_type("DSTORE::ConsistentHashBucket")
    argType = bucket.type.unqualified()
    if not (compareType(argType, bucketType) or compareType(argType, bucketType.pointer())):
        raise gdb.error(f"Expected type ConsistentHashBucket, got {str(argType)}")
    state = bucket["m_bucketValidityState"]
    return str(state) == "DSTORE::BUCKET_STATE_VALID"


def htabLookup(bufHash, filterArgs, compare):
    # Get the hash of the bufTag
    res = subprocess.run(["htablookup", \
        "-p", str(filterArgs["pdbId"]), "-f", str(filterArgs["fileId"]), "-b", str(filterArgs["blockId"]), \
        "-m", str(int(bufHash["hctl"]["max_bucket"])), "-H", str(int(bufHash["hctl"]["high_mask"])), \
        "-l", str(int(bufHash["hctl"]["low_mask"])), "-s", str(int(bufHash["sshift"])), \
        "-S", str(int(bufHash["ssize"])), "-M", "release" if isReleaseMode() else "debug"], capture_output=True)
    if res.returncode != 0:
        raise gdb.error("Call to htablookup failed with ret {}".format(res.returncode))
    info = {}
    for line in res.stdout.decode().strip().split("\n"):
        print(line)
        key, _, value = line.strip().split(" ")
        info[key] = int(value)
    hashBucket = bufHash["dir"][info["segNum"]][info["segNdx"]]
    while int(hashBucket) != 0:
        if int(hashBucket["hashvalue"]) == info["hash"]:
            item = hashBucket.cast(gdb.lookup_type("char").pointer()) + info["align"]
            if compare(item, filterArgs):
                return item
        print("Collision found, following link")
        hashBucket = hashBucket["link"]
    return None


class DstoreGdbCommand(gdb.Command):
    def __init__(self, commandPrefix):
        super().__init__(commandPrefix, gdb.COMMAND_USER)

    def complete(self, text, word):
        return gdb.COMPLETE_SYMBOL

    def invoke(self, argument, from_tty):
        raise NotImplementedError


class DListItr:
    def __init__(self, dlist: gdb.Value, linkField: str = None, type: gdb.Type = None, max: int = None):
        if compareType(gdb.lookup_type("DSTORE::dlist_head"), dlist):
            dlist = dlist.address
        assertType(gdb.lookup_type("DSTORE::dlist_head").pointer(), dlist)
        self.voidPtr = gdb.lookup_type("void").pointer()
        self.type = type if type and linkField else self.voidPtr
        self.offset = getFieldOffset(type.target(), linkField) if type and linkField else 0
        self.head = dlist["head"].address
        self.curNode = dlist["head"]["next"]
        self.max = max
        self.curCount = 0

    def __iter__(self):
        return self

    def __next__(self) -> gdb.Value:
        if self.curNode != self.head and (self.max == None or self.curCount < self.max) :
            res = self.curNode.cast(self.voidPtr) - self.offset
            self.curNode = self.curNode["next"]
            self.curCount += 1
            return res.cast(self.type)
        else:
            raise StopIteration


class BufferpoolMemChunkIter(DListItr):
    def __init__(self, memChunkList):
        assertType(gdb.lookup_type(
            "DSTORE::BufferMemChunkList").pointer(), memChunkList)
        super().__init__(memChunkList["m_head"])
        self.numMemChunk = int(memChunkList["m_numOfMemChunk"])
        self.curMemChunkIdx = 0

    def __next__(self):
        if self.curMemChunkIdx < self.numMemChunk:
            # Get the next dlist element
            node = super().__next__()
            # Since BufferMemChunkWrapper is a template class, we hae trouble getting the member as we can't get the correct type from gdb
            # The offset of *memChunk in memChunkWrapper is 24
            # We basically do $res = *(BufferMemChunk **) ((void *)$curMemChunk + 24)
            res = (node.cast(gdb.lookup_type("void").pointer()) + 24).cast(
                gdb.lookup_type("DSTORE::BufferMemChunk").pointer().pointer()).dereference()
            self.curMemChunkIdx += 1
            # Since res is a pointer to BufferMemChunk, we need to actually get it's address
            return res
        else:
            raise StopIteration


class BufferDescIter:
    def __init__(self, bufferMemChunk, toPointer = True):
        assertType(gdb.lookup_type(
            "DSTORE::BufferMemChunk").pointer(), bufferMemChunk)
        self.numBufferDesc = int(bufferMemChunk["m_numOfBuf"])
        self.bufferDescList = bufferMemChunk["mBufferDesc"]
        self.curBufferDesc = 0
        self.toPointer = toPointer

    def __iter__(self):
        return self

    def __next__(self):
        if self.curBufferDesc < self.numBufferDesc:
            res = self.bufferDescList[self.curBufferDesc]
            self.curBufferDesc += 1
            return int(res.address) if self.toPointer else res
        else:
            raise StopIteration


class CHashTableBucketIter:
    def __init__(self, consistenHashTable):
        assertType(gdb.lookup_type(
            "DSTORE::ConsistentHashTable").pointer(), consistenHashTable)
        self.bucketArray = consistenHashTable["m_bucket_array"]
        self.numBuckets = consistenHashTable["m_bucket_num"]
        self.curBucketIdx = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self.curBucketIdx < self.numBuckets:
            res = self.bucketArray[self.curBucketIdx]
            self.curBucketIdx += 1
            return res
        else:
            raise StopIteration


class CHashBucketElementIter:
    def __init__(self, consistentHashBucket):
        assertType(gdb.lookup_type("DSTORE::ConsistentHashBucket"),
                   consistentHashBucket)
        self.curElement = consistentHashBucket["m_element_list_head"]["m_next"]
        self.endElement = consistentHashBucket["m_element_list_head"]

    def __iter__(self):
        return self

    def __next__(self):
        if self.curElement != self.endElement:
            res = self.curElement["m_elementPtr"]
            self.curElement = self.curElement["m_next"]
            return res
        else:
            raise StopIteration

class HTABIter:
    """Iterates over all elements in a HTAB"""
    def __init__(self, htab: gdb.Value, type: gdb.Type):
        assertType(gdb.lookup_type("DSTORE::HTAB").pointer(), htab)
        hashElem = gdb.lookup_type("DSTORE::HASHELEMENT")
        # Alignment formula for powers of 2
        self.align = (hashElem.sizeof + hashElem.alignof - 1) & ~(hashElem.alignof - 1)
        self.voidPtr = gdb.lookup_type("void").pointer()
        self.htab = htab
        self.hctl = htab["hctl"]
        self.type = type
        self.ssize = self.hctl["ssize"]
        self.sshift = self.hctl["sshift"]
        self.curBucket = 0
        self.maxBucket = self.hctl["max_bucket"]
        self.curEntry = gdb.Value(0).cast(self.voidPtr)

    def __iter__(self):
        return self

    def __next__(self):
        curElem = self.curEntry
        if int(curElem) != 0:
            self.curEntry = curElem["link"]
            if int(self.curEntry) == 0:
                self.curBucket += 1
            return self.elementKey(curElem)
        if self.curBucket > self.maxBucket:
            raise StopIteration

        segment_num = self.curBucket >> self.sshift
        # MOD for powers of 2
        segment_ndx = self.curBucket & (self.ssize - 1)

        segp = self.htab["dir"][segment_num]

        # Find the first item in bucket chain and check if bucket is empty
        curElem = segp[segment_ndx]
        while int(curElem) == 0:
            # Empty bucket advance to nex
            self.curBucket += 1
            if self.curBucket > self.maxBucket:
                # Search is done
                raise StopIteration
            segment_ndx += 1
            if segment_ndx >= self.ssize:
                segment_num += 1
                segment_ndx = 0
                segp = self.htab["dir"][segment_num]
            curElem = segp[segment_ndx]

        self.curEntry = curElem["link"]
        if int(self.curEntry) == 0:
            self.curBucket += 1
        return self.elementKey(curElem)

    def elementKey(self, key: gdb.Value):
        return (key.cast(self.voidPtr) + self.align).cast(self.type)
