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

def getLockMgr(isDistributed):
    return getStorageInstance(isDistributed)["m_distLockMgr" if isDistributed else "m_lockMgr"]

def getXactLockMgr(isDistributed):
    return getStorageInstance(isDistributed)["m_distXactLockMgr" if isDistributed else "m_xactLockMgr"]

def getTableLockMgr(isDistributed):
    return getStorageInstance(isDistributed)["m_distTableLockMgr" if isDistributed else "m_tableLockMgr"]

def getLockTagStr(lockTag: gdb.Value) -> str:
    lockTagType = gdb.lookup_type("DSTORE::LockTag")
    if compareType(lockTagType.pointer(), lockTag):
        lockTag = lockTag.dereference()
    return " ".join(str(lockTag[field.name]) for field in lockTagType.fields())

def lockGetLockEntryItr(bucket_addr):
    lockEntryList = gdb.parse_and_eval(f"(('::DSTORE::ConsistentHashBucket' *){bucket_addr})->m_element_list_head")
    assertType('DSTORE::CHashBucketElement *', lockEntryList.type)
    return LockEntryItr(lockEntryList)

class LockEntryItr:
    def __init__(self, lockEntryList):
        assertType(gdb.lookup_type("DSTORE::CHashBucketElement").pointer(), lockEntryList.type)
        self.type = gdb.lookup_type("DSTORE::LockEntry").pointer()
        self.curNode = lockEntryList["m_next"]
        self.curEntry = self.curNode["m_elementPtr"]

    def __iter__(self):
        return self

    def __next__(self):
        if int(self.curEntry) != 0:
            res = self.curEntry
            self.curNode = self.curNode["m_next"]
            self.curEntry = self.curNode["m_elementPtr"]
            return res.cast(self.type)
        else:
            raise StopIteration
class SkipListIter(DListItr):
    def __init__(self, skipList: gdb.Value, type: gdb.Type):
        assertType(gdb.lookup_type("DSTORE::LockRequestSkipList"), skipList.type)
        self.linkType = gdb.lookup_type("DSTORE::LockRequestLinker")
        # Go through the bottom of the skip list since that layer should have every request
        super().__init__(skipList["m_indexHeads"][0], "defaultNodesSpace", self.linkType.pointer())
        self.skipListType = type

    def __iter__(self):
        return self

    def __next__(self) -> gdb.Value:
        link = super().__next__()
        # Get the actual address
        return link["ownerLockRequest"].cast(self.skipListType)

def dumpSkipList(skipList: gdb.Value, type: str):
    for i, request in enumerate(SkipListIter(skipList, gdb.lookup_type("void").pointer())):
        print("Request", i, ":", f"({type}){str(request)}")
        gdb.execute(f"p *({type}){int(request)}")


"""
LockDumpAllEntriesByBucketAddr: [address in hex format including 0x] -dump all lock entries in given bucket
"""
class LockDumpAllEntriesByBucketAddr(gdb.Command):
    """Usage: lock_dump_all_entries_by_bucket_address <bucket_addr>"""
    def __init__(self):
        super(LockDumpAllEntriesByBucketAddr, self) \
            .__init__("lock_dump_all_entries_by_bucket_address", gdb.COMMAND_USER)

    def complete(self, text, word):
        return gdb.COMPLETE_SYMBOL

    @errorDecorator
    def invoke(self, args, from_tty):
        arg_list = gdb.string_to_argv(args)
        if len(arg_list) != 1:
            raise UsageError(self.__doc__)
        bucket_addr = arg_list[0]
        num_entries = gdb.parse_and_eval("(('::DSTORE::ConsistentHashBucket' *){})->m_numEntries".format(bucket_addr))
        if num_entries == 0:
            print("This bucket has no entries.")
            return
        for entry in lockGetLockEntryItr(bucket_addr):
            print(str(entry))

def dumpLockEntry(lockEntry):
    pretty = gdb.parameter("print pretty")
    if not pretty:
        gdb.execute("set print pretty on")
    print("\nLock Tag:")
    gdb.execute(f"p (('::DSTORE::LockEntry' *){lockEntry.address})->lockEntryCore.m_lockTag")
    print("\nLockEntry Content:")
    gdb.execute(f"p *('::DSTORE::LockEntry' *){lockEntry.address}")
    print("\nGrantedQueue:")
    dumpSkipList(lockEntry["grantedQueue"], "'::DSTORE::DistLockRequestInterface' *")
    print("\nWaitingQueue:")
    for i, linker in enumerate(DListItr(lockEntry["waitingQueue"], "defaultNodesSpace", gdb.lookup_type("DSTORE::LockRequestLinker").pointer())):
        print("Waiter", i, ":", f"('::DSTORE::DistLockRequestInterface' *){str(linker['ownerLockRequest'])}")
        gdb.execute(f"p *('::DSTORE::DistLockRequestInterface' *){int(linker['ownerLockRequest'])}")
    if not pretty:
        gdb.execute("set print pretty off")

"""
LockDumpEntryByAddress: [address in hex format including 0x] - given address of an entry,
dump it's tag, content, granted queue and waiting queue
"""
class LockDumpEntryByAddress(DstoreGdbCommand):
    """Usage: lock_dump_entry_by_address <entry_addr>"""
    def __init__(self):
        super().__init__("lock_dump_entry_by_address")

    @errorDecorator
    def invoke(self, args, from_tty):
        lock_entry_type = gdb.lookup_type('DSTORE::LockEntry')
        parsed_args = gdb.parse_and_eval(args)
        if parsed_args.type in [gdb.lookup_type("int"), gdb.lookup_type("long")]:
            parsed_args = parsed_args.cast(lock_entry_type.pointer())
        if parsed_args.type == lock_entry_type.pointer():
            parsed_args = parsed_args.dereference()
        assertType(lock_entry_type, parsed_args.type)
        dumpLockEntry(parsed_args)

def scanLockRequest(dListNode):
    # Hard coded value of offsetof(LockRequestLinker, link)
    offset = 0x0
    linker = (dListNode.cast(gdb.lookup_type("void").pointer()) + offset).cast(gdb.lookup_type("DSTORE::LockRequestLinker").pointer())
    return linker["owner"]

def dumpGrantedWaitingQueue(lock_entry_addr, queueType):
    granted = True if queueType == "grantedQueue" else False
    queue_num_type = "m_grantedTotal" if granted else "m_waitingTotal"
    queue = gdb.parse_and_eval('&(((LockEntry *){})->{})'.format(lock_entry_addr, queueType))
    queue_num = gdb.parse_and_eval('((LockEntry *){})->lockEntryCore->{}'.format(lock_entry_addr, queue_num_type))
    if queue_num == 0:
        print("The {} queue is empty.".format("granted" if granted else "waiting"))
        return
    print("There are total number of {} request(s) in the {} queue:"
        .format(queue_num, "granted" if granted else "waiting"))
    for node in DListItr(queue):
        res = scanLockRequest(node)
        print("@{}:".format(res.dereference().address))
        gdb.execute("p *(DistLockRequestInterface *){}".format(int(res)))

def dumpLockBucket(addr):
    pretty = gdb.parameter("print pretty")
    if not pretty:
        gdb.execute("set print pretty on")
    gdb.execute("p *('::DSTORE::ConsistentHashBucket' *){}".format(addr))
    if not pretty:
        gdb.execute("set print pretty off")

def lockGetBucketItr(is_distributed: bool):
    lock_mgr_type = 'DistributedLockMgr' if is_distributed else 'LockMgr'
    # Try to get the lock manager
    lock_mgr = getLockMgr(is_distributed)
    assertType('DSTORE::{} *'.format(lock_mgr_type), lock_mgr.type, "(Distributed)LockMgr")
    ch_table = lock_mgr["m_consistentHashTable"]
    bucket_num = ch_table["m_bucket_num"]
    bucket_array = ch_table["m_bucket_array"]
    bucket_info = []
    for i in range (bucket_num):
        bucket_id = bucket_array[i]["m_bucket_id"]
        bucket_info.append((str(bucket_array[i].address), int(bucket_id)))
    return bucket_info

def scan_lock_bucket_list(bucket_info):
    bucket_addr, bucket_id = bucket_info
    bucket = gdb.parse_and_eval("(('::DSTORE::ConsistentHashBucket' *){})".format(bucket_addr))
    num_entries = gdb.parse_and_eval("(('::DSTORE::ConsistentHashBucket' *){})->m_numEntries".format(bucket_addr))
    res_str = "({})Bucket id: {}\tBucket address: {}\tNumEntries: {}".format("Valid" if isBucketValid(bucket) else "Invalid",
        bucket_id, bucket_addr, num_entries)
    return res_str

def scan_lock_bucket_list_2(bucket_info):
    bucket_addr, bucket_id = bucket_info
    res = []
    bucket = gdb.parse_and_eval("(('::DSTORE::ConsistentHashBucket' *){})".format(bucket_addr))
    num_entries = gdb.parse_and_eval("(('::DSTORE::ConsistentHashBucket' *){})->m_numEntries".format(bucket_addr))
    if isBucketValid(bucket) and num_entries > 0:
        for entry in lockGetLockEntryItr(bucket_addr):
            lockTag = entry["lockEntryCore"]["m_lockTag"]
            field1 = int(lockTag["field1"])
            field2 = int(lockTag["field2"])
            field3 = int(lockTag["field3"])
            lockTagType = lockTag["lockTagType"]
            res.append((bucket_id, str(entry), field1, field2, field3, int(lockTagType)))
        return res

class LockDumpAllBuckets(gdb.Command):
    """Usage: lock_dump_all_buckets <distributed|single>"""
    def __init__(self):
        super(LockDumpAllBuckets, self).__init__("lock_dump_all_buckets", gdb.COMMAND_USER)

    def complete(self, text, word):
        return gdb.COMPLETE_SYMBOL

    @errorDecorator
    def invoke(self, args, from_tty):
        arg_list = gdb.string_to_argv(args)
        if len(arg_list) != 1:
            raise UsageError(self.__doc__)
        is_distributed = 'distributed'.startswith(arg_list[0])
        bucket_info = lockGetBucketItr(is_distributed)
        for bucket in bucket_info:
            res = scan_lock_bucket_list(bucket)
            print(res)

"""
LockDumpBucketByBucketAddress: given bucket address, dump the content in it
"""
class LockDumpBucketByBucketAddress(gdb.Command):
    """Usage: lock_dump_bucket_by_bucket_address <bucket_addr>"""
    def __init__(self):
        super(LockDumpBucketByBucketAddress, self).__init__("lock_dump_bucket_by_bucket_address", gdb.COMMAND_USER)

    def complete(self, text, word):
        return gdb.COMPLETE_SYMBOL

    def invoke(self, args, from_tty):
        try:
            lock_bucket_type = gdb.lookup_type('DSTORE::ConsistentHashBucket')
            parsed_args = gdb.parse_and_eval(args)
            if parsed_args.type in [gdb.lookup_type("int"), gdb.lookup_type("long")]:
                parsed_args = parsed_args.cast(lock_bucket_type.pointer())
            if parsed_args.type == lock_bucket_type.pointer():
                parsed_args = parsed_args.dereference()
            assertType(lock_bucket_type, parsed_args.type)
            dumpLockBucket(str(parsed_args.address))
        except Exception as e:
            print(e)

"""
LockDumpAllLockEntries: dump all entries in all buckets.
"""
class LockDumpAllEntries(DstoreGdbCommand):
    """Usage: lock_dump_all_entries <distributed|single>"""
    def __init__(self):
        super().__init__("lock_dump_all_entries")

    @errorDecorator
    def invoke(self, args, from_tty):
        arg_list = gdb.string_to_argv(args)
        if len(arg_list) != 1:
            raise UsageError(self.__doc__)
        is_distributed = 'distributed'.startswith(arg_list[0])
        origFrame = findDstoreFrame()
        bucket_info = lockGetBucketItr(is_distributed)
        for bucket in bucket_info:
            entries = scan_lock_bucket_list_2(bucket)
            if entries is not None:
                for entry in entries:
                    bucket_id, entry_addr, field1, field2, field3, lockTagType = entry
                    print("\nLockTagType = {}, field1 = {}, field2 = {}"
                        ", field3 = {}\nbucket_id {}, lock_entry_addr @{}\n"
                        .format(lockTagType, field1, field2, field3, bucket_id, entry_addr))
        restoreOriginalFrame(origFrame)


def getLockTagType(lockTagType):
    types = ["LOCKTAG_TABLE",   #0
    "LOCKTAG_TABLE_EXTEND"      #1
    "LOCKTAG_TBS_EXTEND",       #2
    "LOCKTAG_TRANSACTION",      #3
    "LOCKTAG_CSN",              #4
    "LOCKTAG_ZONE",
    "LOCKTAG_CONTROL_FILE",
    "LOCKTAG_CONTROL_FILE_WAL_STREAM_ASSIGN",
    "LOCKTAG_PARTITION",
    "LOCKTAG_DEADLOCK_DETECT",
    "LOCKTAG_MAX_NUM"]
    return "DSTORE::" + types[lockTagType]

"""
LockDumpEntry: dump the entry given lock tag and fieldIds.
"""
class LockDumpEntry(gdb.Command):
    """Usage: lock_dump_entry <distributed|single> <lockTagType> <field1> <field2> <field3> (lockTagType is integer)"""
    # 0: LOCKTAG_TABLE
    # 1: LOCKTAG_TABLE_EXTEND
    # 2: LOCKTAG_TBS_EXTEND
    # 3: LOCKTAG_TRANSACTION
    # 4: LOCKTAG_CSN
    # 5: LOCKTAG_ZONE
    # 6: LOCKTAG_CONTROL_FILE
    # 7: LOCKTAG_CONTROL_FILE_WAL_STREAM_ASSIGN
    # 8: LOCKTAG_PARTITION
    # 9: LOCKTAG_DEADLOCK_DETECT
    # 10: LOCKTAG_MAX_NUM
    def __init__(self):
        super(LockDumpEntry, self).__init__("lock_dump_entry", gdb.COMMAND_USER)

    def complete(self, text, word):
        return gdb.COMPLETE_SYMBOL

    def invoke(self, args, from_tty):
        arg_list = gdb.string_to_argv(args)
        if len(arg_list) != 5:
            raise UsageError(self.__doc__)
        is_distributed = 'distributed'.startswith(arg_list[0])
        find_lock_tag_type = int(arg_list[1])
        find_field1 = int(arg_list[2])
        find_field2 = int(arg_list[3])
        find_field3 = int(arg_list[4])

        bucket_info = lockGetBucketItr(is_distributed)
        for bucket in bucket_info:
            entries = scan_lock_bucket_list_2(bucket)
            if entries is not None:
                for entry in entries:
                    bucket_id, entry_addr, field1, field2, field3, lockTagType = entry
                    if field1 == find_field1 and field2 == find_field2 and field3 == find_field3 \
                        and lockTagType == find_lock_tag_type:
                        print("\nFound:\nLockTagType = {}, field1 = {}, field2 = {}"
                            ", field3 = {}\nbucket_id {}, lock_entry_addr @{}\n"
                            .format(lockTagType, field1, field2, field3, bucket_id, entry_addr))
                        return
        print("Not found!")

"""
TableLockDumpAllEntries: Dumps all table lock entries in the current node
"""
class TableLockDumpAllEntries(DstoreGdbCommand):
    """Usage: table_lock_dump_all_entries <distributed>"""
    def __init__(self):
        super().__init__("table_lock_dump_all_entries")

    @errorDecorator
    def invoke(self, args, from_tty):
        arg_list = gdb.string_to_argv(args)
        if len(arg_list) != 1:
            raise UsageError(self.__doc__)
        is_distributed = 'distributed'.startswith(arg_list[0])
        table_lock_mgr = getTableLockMgr(is_distributed)

        htab = table_lock_mgr["m_lockTable"].cast(gdb.lookup_type("DSTORE::LockHashTable").pointer())["m_lockTable"]
        # We have to scan through the entire lock hash table
        for entry in HTABIter(htab, gdb.lookup_type("DSTORE::LockEntry").pointer()):
            tag = entry["lockEntryCore"]["m_lockTag"]
            fields = ["lockTagType", "field1", "field2", "field3"]
            print("Type: {} Tag: ({}, {}, {})".format(*[tag[field] for field in fields]), "Address: {}".format(str(entry)))

"""
TableLockDumpAllEntries: Dumps all table lock entries in the current node
"""
class XactLockDumpAllEntries(DstoreGdbCommand):
    """Usage: xact_lock_dump_all_entries <distributed>"""
    def __init__(self):
        super().__init__("xact_lock_dump_all_entries")

    @errorDecorator
    def invoke(self, args, from_tty):
        arg_list = gdb.string_to_argv(args)
        if len(arg_list) != 1:
            raise UsageError(self.__doc__)
        is_distributed = 'distributed'.startswith(arg_list[0])
        table_lock_mgr = getXactLockMgr(is_distributed)

        htab = table_lock_mgr["m_lockTable"].cast(gdb.lookup_type("DSTORE::LockHashTable").pointer())["m_lockTable"]
        # We have to scan through the entire lock hash table
        for entry in HTABIter(htab, gdb.lookup_type("DSTORE::LockEntry").pointer()):
            tag = entry["lockEntryCore"]["m_lockTag"]
            fields = ["lockTagType", "field1", "field2", "field3"]
            print("Type: {} Tag: ({}, {}, {})".format(*[tag[field] for field in fields]), "Address: {}".format(str(entry)))

LockDumpAllBuckets()
LockDumpAllEntriesByBucketAddr()
LockDumpBucketByBucketAddress()
LockDumpEntryByAddress()
LockDumpAllEntries()
LockDumpEntry()
TableLockDumpAllEntries()
XactLockDumpAllEntries()
