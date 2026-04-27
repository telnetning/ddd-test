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
import gdb
import math
import traceback
import multiprocessing
from gdb_util import *


def initPool(parseLock, isLive):
    global lock
    lock = parseLock
    global live
    live = isLive


def bufferpoolGetMemChunkItter(is_distributed):
    origFrame = findDstoreFrame()
    buffer_mgr_type = 'DistributedBufferMgr' if is_distributed else 'BufMgr'
    # Try to get the buffer manager
    buf_mgr = gdb.parse_and_eval(
        '({} *)g_storageInstance->m_bufMgr'.format(buffer_mgr_type))
    # Make sure it is the right type
    assertType('DSTORE::{} *'.format(buffer_mgr_type),
               buf_mgr, "g_storageInstance->m_bufMgr")
    restoreOriginalFrame(origFrame)
    # Here we start going through the memory chunk lists
    return BufferpoolMemChunkIter(buf_mgr["m_bufferMemChunkList"])


def dumpBufferDesc(addr):
    pretty = gdb.parameter("print pretty")
    if not pretty:
        gdb.execute("set print pretty on")
    gdb.execute(f"p *('::DSTORE::BufferDesc' *){addr}")
    if not pretty:
        gdb.execute("set print pretty off")


def scanBufferDesc(bufferDescAddress):
    """
    Goes through the mem_chunk at the given address and searches for BufferDesc that matches the filter
    """
    with lock:
        bufferDesc = gdb.Value(bufferDescAddress).cast(
            gdb.lookup_type("DSTORE::BufferDesc").pointer())
        if live:
            str(bufferDesc)
    tag = bufferDesc["bufTag"]
    fileId = int(tag["pageId"]["m_fileId"])
    blockId = int(tag["pageId"]["m_blockId"])
    pdbId = int(tag["pdbId"])
    return {"fileId": fileId, "blockId": blockId, "pdbId": pdbId, "address": bufferDescAddress}

def findSingleBufferDesc(is_distributed, isLive, filterArgs):
    chunkItter = bufferpoolGetMemChunkItter(is_distributed)
    nWorkers = gdb.parameter("python-workers")
    maxChunkSize = gdb.parameter("python-chunk-size")
    lock = multiprocessing.Lock()
    with multiprocessing.Pool(nWorkers, initializer=initPool, initargs=(lock, isLive)) as pool:
        try:
            print("m_numOfMemChunk = {}".format(chunkItter.numMemChunk))
            print("number of python-workers = {}".format(
                nWorkers if nWorkers is not None else "Unlimited"))
            if nWorkers is None:
                nWorkers = os.cpu_count()

            for idx, memChunk in enumerate(chunkItter):
                bufferDescIter = BufferDescIter(memChunk)
                chunkSize = math.ceil(bufferDescIter.numBufferDesc / nWorkers)
                if maxChunkSize is not None:
                    chunkSize = min(chunkSize, maxChunkSize)
                descriptors = pool.imap(
                    scanBufferDesc, bufferDescIter, chunkSize)
                for bufferDesc in descriptors:
                    if checkFilter(filterArgs, bufferDesc):
                        pool.close()
                        pool.join()
                        gdb.execute(
                            "p (BufferDesc *) {}".format(bufferDesc['address']))
                        dumpBufferDesc(bufferDesc['address'])
                        return
                print("Checked memchunk {}".format(idx))
            print("Failed to find buffer desc")
            pool.close()
            pool.join()
        except Exception as e:
            print(e)
            print(traceback.print_exc())


class BufferpoolDumpBufferDescByAddress(DstoreGdbCommand):
    """Dumps a single buffer desc by address or gdb variable - Usage bufferpool_dump_bufferdesc_by_address <bufferdesc>"""

    def __init__(self):
        super().__init__("bufferpool_dump_bufferdesc_by_address")

    def invoke(self, args, from_tty):
        bufferDescType = gdb.lookup_type('DSTORE::BufferDesc')
        parsedArgs = gdb.parse_and_eval(args)
        if parsedArgs.type in [gdb.lookup_type("int"), gdb.lookup_type("long"), gdb.lookup_type("void").pointer()]:
            parsedArgs = parsedArgs.cast(bufferDescType.pointer())
        if parsedArgs.type == bufferDescType.pointer():
            parsedArgs = parsedArgs.dereference()
        assertType(bufferDescType, parsedArgs)
        dumpBufferDesc(str(parsedArgs.address))


class BufferpoolDumpBufferDescScan(DstoreGdbCommand):
    """Scans all of Bufferpool's BufferDesc based on filters provided - Usage: bufferpool_dump_bufferdesc_through_scan <distributed|single> <pdbId> <fileId> <blockId>"""

    def __init__(self):
        super().__init__("bufferpool_dump_bufferdesc_through_scan")

    @errorDecorator
    def invoke(self, args, from_tty):
        argList = gdb.string_to_argv(args)
        if len(argList) < 2:
            raise UsageError(self.__doc__)
        isLive = gdb.selected_inferior().was_attached
        # Is the system distributed or single node
        is_distributed = 'distributed'.startswith(argList[0])
        argList = argList[1:]

        # Figure out what we need to filter by
        try:
            res = parseBufTag(argList, {})
            filterArgs = res["filterArgs"]
            val = res["val"]
            printFilter(filterArgs)
        except gdb.error:
            raise UsageError(self.__doc__)
        findSingleBufferDesc(is_distributed, isLive, filterArgs)


class BufferpoolDumpBufferDesc(DstoreGdbCommand):
    """Dumps Bufferpool's BufferDesc based on filters provided - Usage: bufferpool_dump_bufferdesc <distributed|single> <pdbId> <fileId> <blockId>"""

    def __init__(self):
        super().__init__("bufferpool_dump_bufferdesc")

    @errorDecorator
    def invoke(self, args, from_tty):
        argList = gdb.string_to_argv(args)
        if len(argList) < 2:
            raise UsageError(self.__doc__)
        # Is the system distributed or single node
        is_distributed = 'distributed'.startswith(argList[0])
        argList = argList[1:]

        # Figure out what we need to filter by
        try:
            res = parseBufTag(argList, {})
            filterArgs = res["filterArgs"]
            val = res["val"]
            printFilter(filterArgs)
        except gdb.error:
            raise UsageError(self.__doc__)
        if gdb.selected_inferior().was_attached:
            succ = False
            try:
                origFrame = findDstoreFrame()
                if val == None:
                    gdb.set_convenience_variable("bufTag", gdb.parse_and_eval("(BufferTag *)malloc(sizeof(BufferTag))"))
                    gdb.execute("set $bufTag->pdbId = {}".format(filterArgs["pdbId"]))
                    gdb.execute("set $bufTag->pageId.m_fileId = {}".format(filterArgs["fileId"]))
                    gdb.execute("set $bufTag->pageId.m_blockId = {}".format(filterArgs["blockId"]))
                else:
                    gdb.set_convenience_variable("bufTag", val.address)
                bufTable = gdb.parse_and_eval("(({}*)(g_storageInstance->m_bufMgr))->m_buftable".format('DistributedBufferMgr' if is_distributed else 'BufMgr'))
                gdb.set_convenience_variable("bufTable", bufTable)
                gdb.set_convenience_variable("bufHash", bufTable["m_bufHash"])
                hashCode = gdb.parse_and_eval("$bufTable->GetHashCode($bufTag)")
                print("Hash of bufTag {} = {}".format(formatBufTag(gdb.convenience_variable("bufTag")), int(hashCode)))
                gdb.set_convenience_variable("hashCode", hashCode)
                hashFind = gdb.parse_and_eval("'BufLookUp<(DSTORE::HASHACTION)0>(DSTORE::HTAB*, DSTORE::BufferTag const*, unsigned int, bool*)'"
                                                "($bufHash, $bufTag, $hashCode, (bool*)0)").cast(gdb.lookup_type("DSTORE::BufferLookupEnt").pointer())
                print("Found entry:", str(hashFind.dereference()))
                dumpBufferDesc(int(hashFind["buffer"]))
                succ = True
            except:
                print("Failed, using backup method")
                pass
            finally:
                restoreOriginalFrame(origFrame)
                if val == None:
                    gdb.parse_and_eval("free($bufTag)")
            if succ:
                return
        origFrame = findDstoreFrame()
        bufHash = gdb.parse_and_eval("(({}*)(g_storageInstance->m_bufMgr))->m_buftable->m_bufHash".format('DistributedBufferMgr' if is_distributed else 'BufMgr'))
        restoreOriginalFrame(origFrame)
        bufferDesc = htabLookup(bufHash, filterArgs, lambda item, filter: \
            formatBufTag(item.cast(gdb.lookup_type("DSTORE::BufferDesc").pointer())["bufTag"]) == \
            "({}, {}, {})".format(filter["pdbId"], filter["fileId"], filter["blockId"]))
        if bufferDesc:
            dumpBufferDesc(int(bufferDesc))
        else:
            print("Could not find buffer")


class BufferpoolDumpAllBufferdesc(DstoreGdbCommand):
    """Dumps all of Bufferpool's BufferDesc - Usage: bufferpool_dump_all_bufferdesc <distributed|single>"""

    def __init__(self):
        super().__init__("bufferpool_dump_all_bufferdesc")

    @errorDecorator
    def invoke(self, args, from_tty):
        arg_list = gdb.string_to_argv(args)
        if len(arg_list) != 1:
            raise UsageError(self.__doc__)
        # Is the system distributed or single node
        is_distributed = 'distributed'.startswith(arg_list[0])
        chunkItter = bufferpoolGetMemChunkItter(is_distributed)
        nWorkers = gdb.parameter("python-workers")
        maxChunkSize = gdb.parameter("python-chunk-size")
        lock = multiprocessing.Lock()
        isLive = gdb.selected_inferior().was_attached
        with multiprocessing.Pool(nWorkers, initializer=initPool, initargs=(lock, isLive)) as pool:
            print("m_numOfMemChunk = {}".format(chunkItter.numMemChunk))
            print("number of python-workers = {}".format(
                nWorkers if nWorkers is not None else "Unlimited"))
            if nWorkers is None:
                nWorkers = os.cpu_count()

            for idx, memChunk in enumerate(chunkItter):
                bufferDescIter = BufferDescIter(memChunk)
                chunkSize = math.ceil(bufferDescIter.numBufferDesc / nWorkers)
                if maxChunkSize is not None:
                    chunkSize = min(chunkSize, maxChunkSize)
                descriptors = pool.imap(
                    scanBufferDesc, bufferDescIter, chunkSize)
                print("memchunk {}:".format(idx))
                for bufferDesc in descriptors:
                    print("\t({}, {}, {}): {}".format(
                        *[bufferDesc[i] for i in ["pdbId", "fileId", "blockId"]], hex(bufferDesc["address"])))
            pool.close()
            pool.join()


class BufferpoolDumpAllBufferdescST(DstoreGdbCommand):
    """Dumps all of Bufferpool's BufferDesc with a single thread - Usage: bufferpool_dump_all_bufferdesc_st <distributed|single>"""

    def __init__(self):
        super().__init__("bufferpool_dump_all_bufferdesc_st")

    @errorDecorator
    def invoke(self, args, from_tty):
        arg_list = gdb.string_to_argv(args)
        if len(arg_list) != 1:
            raise UsageError(self.__doc__)
        # Is the system distributed or single node
        is_distributed = 'distributed'.startswith(arg_list[0])
        chunkItter = bufferpoolGetMemChunkItter(is_distributed)
        for idx, memChunk in enumerate(chunkItter):
            print("memchunk {}:".format(idx))
            for bufferDesc in BufferDescIter(memChunk, False):
                print("\t{}: {}".format(formatBufTag(bufferDesc["bufTag"]), hex(bufferDesc.address)))

BufferpoolDumpBufferDescByAddress()
BufferpoolDumpBufferDescScan()
BufferpoolDumpBufferDesc()
BufferpoolDumpAllBufferdesc()
BufferpoolDumpAllBufferdescST()
