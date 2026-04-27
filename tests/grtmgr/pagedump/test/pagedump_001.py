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
############################################################################v01#
# TESTCASE NAME : pagedump_001.py
# COMPONENT(S)  : Page dump
# DESCRIPTION   : Tests the data dump output of HeapDiskTuples.
# MODIFIED BY   : WHO            WHEN          COMMENT
#               : -------------- ------------- ---------------------------------
#               : s84299475      2023-09-19    Created this testcase
################################################################################

from fvt_basic import *
from fvt_util import  *
from testXXX import *
from string import Template
import re

testcase = 'pagedump_001'
tableName = testcase + "_t1"
minWaitTime = 3
num_datanodes = 0
ports = list()

def setup ():
#-----------------------------------------------------------------------------
# Description : Setup
# Exp Results : success
#-----------------------------------------------------------------------------
  rc = 0

  global num_datanodes
  num_datanodes = get_num_datanodes()

  global ports 
  ports = get_port_numbers()

  rc = 0

  rc |= fvt_connectDB(DB_NAME = dbname, PGPORT = port_no)
  if 0 != rc:
    fvt_print_to_err_and_log("connect to database " + dbname + " failed.")
    return -1

  rc |= testSQL(
    DESC          = "",
    SQL           = "DROP TABLE " + tableName,
    PASS_CRITERIA = "IGNORE"
  )

  fvt_set_disabled_testunits()

  return rc

# Parses the CTID to isolate the blockId
def getBlockId(tableName):
  resultSet = list()
  rc = testSQL(
    DESC          = "get the ctid",
    SQL           = "SELECT ctid FROM " + tableName,
    PASS_CRITERIA = "NONE",
    EXPECTED      = "NONE",
    GET_ROWSET    = resultSet
  )
  return resultSet[0][0].split(",")[1]

# For linked tuples, gets the blockId of the next chunk in the sequence
def getNextLinkChunkCtid(curBlockId):
  dump = get_command_output("%s/tmp_build/bin/pagedump -f \"%s/dstore/PDB_2/file6_8\" -b %s -d"%(code_path, data_dir, curBlockId))
  pattern = "Next Linked Chunk's CTID \(ItemPointerData\): \(\d+, (\d+), \d+\)"
  match = re.search(pattern, dump)
  if match:
    return match.group(1)
  else:
    return None

def testunit_1():
#--------------------------------------------------------------------------------------------------------
# Description : Tests for a table with one int column.  
#               Includes 0, 1, 2, -1, -2, 2147483647, and -2147483648 (INT_MAX and INT_MIN).
#               Values are inputted using 2 transactions.
#--------------------------------------------------------------------------------------------------------
  rc = 0
 
  rc |= testSQL(
    DESC          = "create main table",
    SQL           = Template("""
                    CREATE TABLE $t1 (
                      a1 int
                      );
                    """).substitute({'t1': tableName}),
    PASS_CRITERIA = "SQLSTATE",
    SQLSTATE      = "00000"
  )

  rc |= testSQL(
        DESC          = "insert a row",
        SQL           = "insert into %s values(0);"%(tableName),
        PASS_CRITERIA = "SQLSTATE",
        SQLSTATE      = "00000"
    )
  
  blockId = getBlockId(tableName)

  rc |= testSQL(
    DESC          = "insert many rows",
    SQL           = "insert into %s values(1), (2), (-1), (-2), (2147483647), (-2147483648);"%(tableName),
    PASS_CRITERIA = "SQLSTATE",
    SQLSTATE      = "00000"
  )

  rc |= run_checkpoint()
  rc |= testCMD(
    DESC          = "data dump",
    COMMAND       = "%s/tmp_build/bin/pagedump -f \"%s/dstore/PDB_2/file6_8\" -b %s -d"%(code_path, data_dir, blockId),
    PASS_CRITERIA = "GREP",
    PATTERN       = """
ItemId 1: \(([0-9]+), 12\)
HeapDiskTuple
    m_tdId: 0
    m_lockerTdId: INVALID_TD_SLOT
    m_size: 12
    m_info:
        m_hasNull: 0
        m_hasVarwidth: 0
        m_hasExternal: 0
        m_hasOid: 0
        m_tdStatus: attached as new owner    
        m_liveMode: TUPLE_BY_NORMAL_INSERT
        m_linkInfo: 0
        m_numColumn: 1
        m_isNullReserve: 0
    Data \(Length = 4 Bytes\):
        00000000

ItemId 2: \(([0-9]+), 12\)
HeapDiskTuple
    m_tdId: 1
    m_lockerTdId: INVALID_TD_SLOT
    m_size: 12
    m_info:
        m_hasNull: 0
        m_hasVarwidth: 0
        m_hasExternal: 0
        m_hasOid: 0
        m_tdStatus: attached as new owner    
        m_liveMode: TUPLE_BY_NORMAL_INSERT
        m_linkInfo: 0
        m_numColumn: 1
        m_isNullReserve: 0
    Data \(Length = 4 Bytes\):
        01000000

ItemId 3: \(([0-9]+), 12\)
HeapDiskTuple
    m_tdId: 1
    m_lockerTdId: INVALID_TD_SLOT
    m_size: 12
    m_info:
        m_hasNull: 0
        m_hasVarwidth: 0
        m_hasExternal: 0
        m_hasOid: 0
        m_tdStatus: attached as new owner    
        m_liveMode: TUPLE_BY_NORMAL_INSERT
        m_linkInfo: 0
        m_numColumn: 1
        m_isNullReserve: 0
    Data \(Length = 4 Bytes\):
        02000000

ItemId 4: \(([0-9]+), 12\)
HeapDiskTuple
    m_tdId: 1
    m_lockerTdId: INVALID_TD_SLOT
    m_size: 12
    m_info:
        m_hasNull: 0
        m_hasVarwidth: 0
        m_hasExternal: 0
        m_hasOid: 0
        m_tdStatus: attached as new owner    
        m_liveMode: TUPLE_BY_NORMAL_INSERT
        m_linkInfo: 0
        m_numColumn: 1
        m_isNullReserve: 0
    Data \(Length = 4 Bytes\):
        FFFFFFFF

ItemId 5: \(([0-9]+), 12\)
HeapDiskTuple
    m_tdId: 1
    m_lockerTdId: INVALID_TD_SLOT
    m_size: 12
    m_info:
        m_hasNull: 0
        m_hasVarwidth: 0
        m_hasExternal: 0
        m_hasOid: 0
        m_tdStatus: attached as new owner    
        m_liveMode: TUPLE_BY_NORMAL_INSERT
        m_linkInfo: 0
        m_numColumn: 1
        m_isNullReserve: 0
    Data \(Length = 4 Bytes\):
        FEFFFFFF

ItemId 6: \(([0-9]+), 12\)
HeapDiskTuple
    m_tdId: 1
    m_lockerTdId: INVALID_TD_SLOT
    m_size: 12
    m_info:
        m_hasNull: 0
        m_hasVarwidth: 0
        m_hasExternal: 0
        m_hasOid: 0
        m_tdStatus: attached as new owner    
        m_liveMode: TUPLE_BY_NORMAL_INSERT
        m_linkInfo: 0
        m_numColumn: 1
        m_isNullReserve: 0
    Data \(Length = 4 Bytes\):
        FFFFFF7F

ItemId 7: \(([0-9]+), 12\)
HeapDiskTuple
    m_tdId: 1
    m_lockerTdId: INVALID_TD_SLOT
    m_size: 12
    m_info:
        m_hasNull: 0
        m_hasVarwidth: 0
        m_hasExternal: 0
        m_hasOid: 0
        m_tdStatus: attached as new owner    
        m_liveMode: TUPLE_BY_NORMAL_INSERT
        m_linkInfo: 0
        m_numColumn: 1
        m_isNullReserve: 0
    Data \(Length = 4 Bytes\):
        00000080
"""
  )

  rc |= testSQL(
      DESC          = "",
      SQL           = "DROP TABLE " + tableName,
      PASS_CRITERIA = "IGNORE"
    )

  return rc

def testunit_2():
#-----------------------------------------------------------------------------------------------------------------------------------------
# Description : Tests for a table with two columns.  One is of type char(30) and the other is of type boolean.  
#               This data will take up EXACTLY one row of hex.
#-----------------------------------------------------------------------------------------------------------------------------------------
  rc = 0
 
  rc |= testSQL(
    DESC          = "create main table",
    SQL           = Template("""
                    CREATE TABLE $t1 (
                      a1 char(30),
                      a2 BOOLEAN
                      );
                    """).substitute({'t1': tableName}),
    PASS_CRITERIA = "SQLSTATE",
    SQLSTATE      = "00000"
  )

  myStr = "w"*30
  rc |= testSQL(
        DESC          = "insert a row",
        SQL           = "insert into %s values('%s',TRUE);"%(tableName, myStr),
        PASS_CRITERIA = "SQLSTATE",
        SQLSTATE      = "00000"
    )
  
  blockId = getBlockId(tableName)

  rc |= run_checkpoint()
  rc |= testCMD(
    DESC          = "data dump",
    COMMAND       = "%s/tmp_build/bin/pagedump -f \"%s/dstore/PDB_2/file6_8\" -b %s -d"%(code_path, data_dir, blockId),
    PASS_CRITERIA = "GREP",
    PATTERN       = """
ItemId 1: \(([0-9]+), 40\)
HeapDiskTuple
    m_tdId: 0
    m_lockerTdId: INVALID_TD_SLOT
    m_size: 40
    m_info:
        m_hasNull: 0
        m_hasVarwidth: 1
        m_hasExternal: 0
        m_hasOid: 0
        m_tdStatus: attached as new owner    
        m_liveMode: TUPLE_BY_NORMAL_INSERT
        m_linkInfo: 0
        m_numColumn: 2
        m_isNullReserve: 0
    Data \(Length = 32 Bytes\):
        3F77777777777777 7777777777777777 7777777777777777 7777777777777701
"""
  )

  rc |= testSQL(
      DESC          = "",
      SQL           = "DROP TABLE " + tableName,
      PASS_CRITERIA = "IGNORE"
    )

  return rc

def testunit_3():
#----------------------------------------------------------------------------------------------------------------------
# Description : Tests for a table with one column of type char(25).  
#               This data will take up LESS than one row of hex.
#----------------------------------------------------------------------------------------------------------------------
  rc = 0
 
  rc |= testSQL(
    DESC          = "create main table",
    SQL           = Template("""
                    CREATE TABLE $t1 (
                      a1 char(25)
                      );
                    """).substitute({'t1': tableName}),
    PASS_CRITERIA = "SQLSTATE",
    SQLSTATE      = "00000"
  )

  myStr = "w"*25
  rc |= testSQL(
        DESC          = "insert a row",
        SQL           = "insert into %s values('%s');"%(tableName, myStr),
        PASS_CRITERIA = "SQLSTATE",
        SQLSTATE      = "00000"
    )
  
  blockId = getBlockId(tableName)

  rc |= run_checkpoint()
  rc |= testCMD(
    DESC          = "data dump",
    COMMAND       = "%s/tmp_build/bin/pagedump -f \"%s/dstore/PDB_2/file6_8\" -b %s -d"%(code_path, data_dir, blockId),
    PASS_CRITERIA = "GREP",
    PATTERN       = """
ItemId 1: \(([0-9]+), 34\)
HeapDiskTuple
    m_tdId: 0
    m_lockerTdId: INVALID_TD_SLOT
    m_size: 34
    m_info:
        m_hasNull: 0
        m_hasVarwidth: 1
        m_hasExternal: 0
        m_hasOid: 0
        m_tdStatus: attached as new owner    
        m_liveMode: TUPLE_BY_NORMAL_INSERT
        m_linkInfo: 0
        m_numColumn: 1
        m_isNullReserve: 0
    Data \(Length = 26 Bytes\):
        3577777777777777 7777777777777777 7777777777777777 7777
"""
  )

  rc |= testSQL(
      DESC          = "",
      SQL           = "DROP TABLE " + tableName,
      PASS_CRITERIA = "IGNORE"
    )

  return rc

def testunit_4():
#----------------------------------------------------------------------------------------------------------------------
# Description : Tests for a table with one column of type char(40).  
#               This data will take up MORE than one row of hex.
#----------------------------------------------------------------------------------------------------------------------
  rc = 0
 
  rc |= testSQL(
    DESC          = "create main table",
    SQL           = Template("""
                    CREATE TABLE $t1 (
                      a1 char(40)
                      );
                    """).substitute({'t1': tableName}),
    PASS_CRITERIA = "SQLSTATE",
    SQLSTATE      = "00000"
  )

  myStr = "a"*40
  rc |= testSQL(
        DESC          = "insert a row",
        SQL           = "insert into %s values('%s');"%(tableName, myStr),
        PASS_CRITERIA = "SQLSTATE",
        SQLSTATE      = "00000"
    )
  
  blockId = getBlockId(tableName)

  rc |= run_checkpoint()
  rc |= testCMD(
    DESC          = "data dump",
    COMMAND       = "%s/tmp_build/bin/pagedump -f \"%s/dstore/PDB_2/file6_8\" -b %s -d"%(code_path, data_dir, blockId),
    PASS_CRITERIA = "GREP",
    PATTERN       = """
ItemId 1: \(([0-9]+), 49\)
HeapDiskTuple
    m_tdId: 0
    m_lockerTdId: INVALID_TD_SLOT
    m_size: 49
    m_info:
        m_hasNull: 0
        m_hasVarwidth: 1
        m_hasExternal: 0
        m_hasOid: 0
        m_tdStatus: attached as new owner    
        m_liveMode: TUPLE_BY_NORMAL_INSERT
        m_linkInfo: 0
        m_numColumn: 1
        m_isNullReserve: 0
    Data \(Length = 41 Bytes\):
        5361616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 61
"""
  )

  rc |= testSQL(
      DESC          = "",
      SQL           = "DROP TABLE " + tableName,
      PASS_CRITERIA = "IGNORE"
    )

  return rc

def testunit_5():
#------------------------------------------------------------------------------------------------------------------------
# Description : Table with one column of type text.  The inserted row includes many types of ASCII characters.  The data 
#               will span multiple lines of hex.
#------------------------------------------------------------------------------------------------------------------------
  rc = 0
 
  rc |= testSQL(
    DESC          = "create main table",
    SQL           = Template("""
                    CREATE TABLE $t1 (
                      a1 text
                      );
                    """).substitute({'t1': tableName}),
    PASS_CRITERIA = "SQLSTATE",
    SQLSTATE      = "00000"
  )

  myStr = " !#*+$;{]|~`@!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
  rc |= testSQL(
        DESC          = "insert a row",
        SQL           = "insert into %s values('%s');"%(tableName, myStr),
        PASS_CRITERIA = "SQLSTATE",
        SQLSTATE      = "00000"
    )
  
  blockId = getBlockId(tableName)

  rc |= run_checkpoint()
  rc |= testCMD(
    DESC          = "data dump",
    COMMAND       = "%s/tmp_build/bin/pagedump -f \"%s/dstore/PDB_2/file6_8\" -b %s -d"%(code_path, data_dir, blockId),
    PASS_CRITERIA = "GREP",
    PATTERN       = """
ItemId 1: \(([0-9]+), 101\)
HeapDiskTuple
    m_tdId: 0
    m_lockerTdId: INVALID_TD_SLOT
    m_size: 101
    m_info:
        m_hasNull: 0
        m_hasVarwidth: 1
        m_hasExternal: 0
        m_hasOid: 0
        m_tdStatus: attached as new owner    
        m_liveMode: TUPLE_BY_NORMAL_INSERT
        m_linkInfo: 0
        m_numColumn: 1
        m_isNullReserve: 0
    Data \(Length = 93 Bytes\):
        BB2021232A2B243B 7B5D7C7E60402121 2121212121212121 2121212121212121
        2121212121212121 2121212121212121 2121212121212121 2121212121212121
        2121212121212121 2121212121212121 2121212121212121 2121212121
"""
  )

  rc |= testSQL(
      DESC          = "",
      SQL           = "DROP TABLE " + tableName,
      PASS_CRITERIA = "IGNORE"
    )

  return rc

def testunit_6():
#----------------------------------------------------------------------------------------------------------------------
# Description : Table with one column of type text.  The inserted row is an empty string (same as NULL).
#----------------------------------------------------------------------------------------------------------------------
  rc = 0
 
  rc |= testSQL(
    DESC          = "create main table",
    SQL           = Template("""
                    CREATE TABLE $t1 (
                      a1 text
                      );
                    """).substitute({'t1': tableName}),
    PASS_CRITERIA = "SQLSTATE",
    SQLSTATE      = "00000"
  )

  myStr = ""
  rc |= testSQL(
        DESC          = "insert a row",
        SQL           = "insert into %s values('%s');"%(tableName, myStr),
        PASS_CRITERIA = "SQLSTATE",
        SQLSTATE      = "00000"
    )
  
  blockId = getBlockId(tableName)

  rc |= run_checkpoint()
  rc |= testCMD(
    DESC          = "data dump",
    COMMAND       = "%s/tmp_build/bin/pagedump -f \"%s/dstore/PDB_2/file6_8\" -b %s -d"%(code_path, data_dir, blockId),
    PASS_CRITERIA = "GREP",
    PATTERN       = """
ItemId 1: \(([0-9]+), 16\)
HeapDiskTuple
    m_tdId: 0
    m_lockerTdId: INVALID_TD_SLOT
    m_size: 16
    m_info:
        m_hasNull: 1
        m_hasVarwidth: 0
        m_hasExternal: 0
        m_hasOid: 0
        m_tdStatus: attached as new owner    
        m_liveMode: TUPLE_BY_NORMAL_INSERT
        m_linkInfo: 0
        m_numColumn: 1
        m_isNullReserve: 1
    Nullbitmap \(Hex\): 00
    Data \(Length = 0 Bytes\):
"""
  )

  rc |= testSQL(
      DESC          = "",
      SQL           = "DROP TABLE " + tableName,
      PASS_CRITERIA = "IGNORE"
    )

  return rc

def testunit_7():
#--------------------------------------------------------------------------------------------------------
# Description : Tests when the showTupleData option is NOT present on a small tuple.
#--------------------------------------------------------------------------------------------------------
  rc = 0
 
  rc |= testSQL(
    DESC          = "create main table",
    SQL           = Template("""
                    CREATE TABLE $t1 (
                      a1 int
                      );
                    """).substitute({'t1': tableName}),
    PASS_CRITERIA = "SQLSTATE",
    SQLSTATE      = "00000"
  )

  rc |= testSQL(
        DESC          = "insert a row",
        SQL           = "insert into %s values(3);"%(tableName),
        PASS_CRITERIA = "SQLSTATE",
        SQLSTATE      = "00000"
    )
  
  blockId = getBlockId(tableName)

  rc |= run_checkpoint()
  rc |= testCMD(
    DESC          = "NO hex data dump (no -d option)",
    COMMAND       = "%s/tmp_build/bin/pagedump -f \"%s/dstore/PDB_2/file6_8\" -b %s"%(code_path, data_dir, blockId),
    PASS_CRITERIA = "GREP",
    PATTERN       = """
ItemId 1: \(([0-9]+), 12\)
HeapDiskTuple
    m_tdId: 0
    m_lockerTdId: INVALID_TD_SLOT
    m_size: 12
    m_info:
        m_hasNull: 0
        m_hasVarwidth: 0
        m_hasExternal: 0
        m_hasOid: 0
        m_tdStatus: attached as new owner    
        m_liveMode: TUPLE_BY_NORMAL_INSERT
        m_linkInfo: 0
        m_numColumn: 1
        m_isNullReserve: 0



-------------------
"""
  )

  rc |= testSQL(
      DESC          = "",
      SQL           = "DROP TABLE " + tableName,
      PASS_CRITERIA = "IGNORE"
    )

  return rc

def testunit_8():
#-------------------------------------------------------------------------------------------------------------------------
# Description : Table with two columns of type text.  Each inserted string is of length 256.
#               Each string will have 4 bytes of overhead (due to being over 126 characters long) for a total size of 260.
#               The size of the overhead of the string is included in the string size.
#               This is reflected in the data dump.
#-------------------------------------------------------------------------------------------------------------------------
  rc = 0
 
  rc |= testSQL(
    DESC          = "create main table",
    SQL           = Template("""
                    CREATE TABLE $t1 (
                      a1 text,
                      a2 text
                      );
                    """).substitute({'t1': tableName}),
    PASS_CRITERIA = "SQLSTATE",
    SQLSTATE      = "00000"
  )

  myStr = "a" * 256
  myStr2 = "b" * 256
  rc |= testSQL(
        DESC          = "insert a row",
        SQL           = "insert into %s values('%s', '%s');"%(tableName, myStr, myStr2),
        PASS_CRITERIA = "SQLSTATE",
        SQLSTATE      = "00000"
    )
  
  blockId = getBlockId(tableName)

  rc |= run_checkpoint()
  rc |= testCMD(
    DESC          = "data dump",
    COMMAND       = "%s/tmp_build/bin/pagedump -f \"%s/dstore/PDB_2/file6_8\" -b %s -d"%(code_path, data_dir, blockId),
    PASS_CRITERIA = "GREP",
    PATTERN       = """
ItemId 1: \(([0-9]+), 528\)
HeapDiskTuple
    m_tdId: 0
    m_lockerTdId: INVALID_TD_SLOT
    m_size: 528
    m_info:
        m_hasNull: 0
        m_hasVarwidth: 1
        m_hasExternal: 0
        m_hasOid: 0
        m_tdStatus: attached as new owner    
        m_liveMode: TUPLE_BY_NORMAL_INSERT
        m_linkInfo: 0
        m_numColumn: 2
        m_isNullReserve: 0
    Data \(Length = 520 Bytes\):
        1004000061616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616110040000 6262626262626262 6262626262626262 6262626262626262
        6262626262626262 6262626262626262 6262626262626262 6262626262626262
        6262626262626262 6262626262626262 6262626262626262 6262626262626262
        6262626262626262 6262626262626262 6262626262626262 6262626262626262
        6262626262626262 6262626262626262 6262626262626262 6262626262626262
        6262626262626262 6262626262626262 6262626262626262 6262626262626262
        6262626262626262 6262626262626262 6262626262626262 6262626262626262
        6262626262626262 6262626262626262 6262626262626262 6262626262626262
        6262626262626262
"""
  )

  rc |= testSQL(
      DESC          = "",
      SQL           = "DROP TABLE " + tableName,
      PASS_CRITERIA = "IGNORE"
    )

  return rc

def testunit_9():
#-------------------------------------------------------------------------------------------------------------------------
# Description : The table contains a big tuple, with nulls, and 249 columns of text. 
#               249 columns results in EXACTLY one row of nullbitmap hex.
#               249 was derived using the following formula found in GetBitmapLen().  (249+8-1)/8=32
#-------------------------------------------------------------------------------------------------------------------------
  rc = 0
 
  rc |= testSQL(
    DESC          = "create main table",
    SQL           = Template("""
                    CREATE TABLE $t1 (a1 TEXT, a2 TEXT, a3 TEXT, a4 TEXT, a5 TEXT, a6 TEXT, a7 TEXT, a8 TEXT, a9 TEXT, a10 TEXT, a11 TEXT, 
                             a12 TEXT, a13 TEXT, a14 TEXT, a15 TEXT, a16 TEXT, a17 TEXT, a18 TEXT, a19 TEXT, a20 TEXT, a21 TEXT, a22 TEXT, 
                             a23 TEXT, a24 TEXT, a25 TEXT, a26 TEXT, a27 TEXT, a28 TEXT, a29 TEXT, a30 TEXT, a31 TEXT, a32 TEXT, a33 TEXT, 
                             a34 TEXT, a35 TEXT, a36 TEXT, a37 TEXT, a38 TEXT, a39 TEXT, a40 TEXT, a41 TEXT, a42 TEXT, a43 TEXT, a44 TEXT, 
                             a45 TEXT, a46 TEXT, a47 TEXT, a48 TEXT, a49 TEXT, a50 TEXT, a51 TEXT, a52 TEXT, a53 TEXT, a54 TEXT, a55 TEXT, 
                             a56 TEXT, a57 TEXT, a58 TEXT, a59 TEXT, a60 TEXT, a61 TEXT, a62 TEXT, a63 TEXT, a64 TEXT, a65 TEXT, a66 TEXT, 
                             a67 TEXT, a68 TEXT, a69 TEXT, a70 TEXT, a71 TEXT, a72 TEXT, a73 TEXT, a74 TEXT, a75 TEXT, a76 TEXT, a77 TEXT, 
                             a78 TEXT, a79 TEXT, a80 TEXT, a81 TEXT, a82 TEXT, a83 TEXT, a84 TEXT, a85 TEXT, a86 TEXT, a87 TEXT, a88 TEXT, 
                             a89 TEXT, a90 TEXT, a91 TEXT, a92 TEXT, a93 TEXT, a94 TEXT, a95 TEXT, a96 TEXT, a97 TEXT, a98 TEXT, a99 TEXT, 
                             a100 TEXT, a101 TEXT, a102 TEXT, a103 TEXT, a104 TEXT, a105 TEXT, a106 TEXT, a107 TEXT, a108 TEXT, a109 TEXT, 
                             a110 TEXT, a111 TEXT, a112 TEXT, a113 TEXT, a114 TEXT, a115 TEXT, a116 TEXT, a117 TEXT, a118 TEXT, a119 TEXT, 
                             a120 TEXT, a121 TEXT, a122 TEXT, a123 TEXT, a124 TEXT, a125 TEXT, a126 TEXT, a127 TEXT, a128 TEXT, a129 TEXT, 
                             a130 TEXT, a131 TEXT, a132 TEXT, a133 TEXT, a134 TEXT, a135 TEXT, a136 TEXT, a137 TEXT, a138 TEXT, a139 TEXT, 
                             a140 TEXT, a141 TEXT, a142 TEXT, a143 TEXT, a144 TEXT, a145 TEXT, a146 TEXT, a147 TEXT, a148 TEXT, a149 TEXT, 
                             a150 TEXT, a151 TEXT, a152 TEXT, a153 TEXT, a154 TEXT, a155 TEXT, a156 TEXT, a157 TEXT, a158 TEXT, a159 TEXT, 
                             a160 TEXT, a161 TEXT, a162 TEXT, a163 TEXT, a164 TEXT, a165 TEXT, a166 TEXT, a167 TEXT, a168 TEXT, a169 TEXT, 
                             a170 TEXT, a171 TEXT, a172 TEXT, a173 TEXT, a174 TEXT, a175 TEXT, a176 TEXT, a177 TEXT, a178 TEXT, a179 TEXT, 
                             a180 TEXT, a181 TEXT, a182 TEXT, a183 TEXT, a184 TEXT, a185 TEXT, a186 TEXT, a187 TEXT, a188 TEXT, a189 TEXT, 
                             a190 TEXT, a191 TEXT, a192 TEXT, a193 TEXT, a194 TEXT, a195 TEXT, a196 TEXT, a197 TEXT, a198 TEXT, a199 TEXT, 
                             a200 TEXT, a201 TEXT, a202 TEXT, a203 TEXT, a204 TEXT, a205 TEXT, a206 TEXT, a207 TEXT, a208 TEXT, a209 TEXT, 
                             a210 TEXT, a211 TEXT, a212 TEXT, a213 TEXT, a214 TEXT, a215 TEXT, a216 TEXT, a217 TEXT, a218 TEXT, a219 TEXT, 
                             a220 TEXT, a221 TEXT, a222 TEXT, a223 TEXT, a224 TEXT, a225 TEXT, a226 TEXT, a227 TEXT, a228 TEXT, a229 TEXT, 
                             a230 TEXT, a231 TEXT, a232 TEXT, a233 TEXT, a234 TEXT, a235 TEXT, a236 TEXT, a237 TEXT, a238 TEXT, a239 TEXT, 
                             a240 TEXT, a241 TEXT, a242 TEXT, a243 TEXT, a244 TEXT, a245 TEXT, a246 TEXT, a247 TEXT, a248 TEXT, a249 TEXT);
                    """).substitute({'t1': tableName}),
    PASS_CRITERIA = "SQLSTATE",
    SQLSTATE      = "00000"
  )

  rc |= testSQL(
        DESC          = "insert a row",
        SQL           = "INSERT INTO %s (a1, a2, a3, a4) VALUES (repeat('a', 18000), '', 'c', 'd');"%(tableName),
        PASS_CRITERIA = "SQLSTATE",
        SQLSTATE      = "00000"
    )
  
  blockId = getBlockId(tableName)

  rc |= run_checkpoint()
  rc |= testCMD(
    DESC          = "data dump",
    COMMAND       = "%s/tmp_build/bin/pagedump -f \"%s/dstore/PDB_2/file6_8\" -b %s -d"%(code_path, data_dir, blockId),
    PASS_CRITERIA = "GREP",
    PATTERN       = """
ItemId 1: \(([0-9]+), 7960\)
HeapDiskTuple \(BigTuple First Linked Chunk\)
    m_tdId: 0
    m_lockerTdId: INVALID_TD_SLOT
    m_size: 7960
    m_info:
        m_hasNull: 1
        m_hasVarwidth: 1
        m_hasExternal: 0
        m_hasOid: 0
        m_tdStatus: attached as new owner    
        m_liveMode: TUPLE_BY_NORMAL_INSERT
        m_linkInfo: 1
        m_numColumn: 249
        m_isNullReserve: 1
    Next Linked Chunk's CTID \(ItemPointerData\): \(([0-9]+), ([0-9]+), ([0-9]+)\)
    NumChunks \(Uint8\): 3
    Nullbitmap \(Hex\): 0D00000000000000 0000000000000000 0000000000000000 0000000000000000
    Data \(Length = 7911 Bytes\):
        5019010061616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        61616161616161
"""
  )

  rc |= testSQL(
      DESC          = "",
      SQL           = "DROP TABLE " + tableName,
      PASS_CRITERIA = "IGNORE"
    )

  return rc

def testunit_10():
#-------------------------------------------------------------------------------------------------------------------------
# Description : The table contains a small tuple, with nulls, with OID, and 300 columns of ints.
#               300 columns results in MORE than one row of nullbitmap hex.
#               Nulls alternate between null and an incrementing int.
#-------------------------------------------------------------------------------------------------------------------------
  rc = 0
 
  rc |= testSQL(
    DESC          = "create main table",
    SQL           = Template("""
                    CREATE TABLE $t1 (a1 INT, a2 INT, a3 INT, a4 INT, a5 INT, a6 INT, a7 INT, a8 INT, a9 INT, a10 INT, a11 INT, 
                             a12 INT, a13 INT, a14 INT, a15 INT, a16 INT, a17 INT, a18 INT, a19 INT, a20 INT, a21 INT, a22 INT, 
                             a23 INT, a24 INT, a25 INT, a26 INT, a27 INT, a28 INT, a29 INT, a30 INT, a31 INT, a32 INT, a33 INT, 
                             a34 INT, a35 INT, a36 INT, a37 INT, a38 INT, a39 INT, a40 INT, a41 INT, a42 INT, a43 INT, a44 INT, 
                             a45 INT, a46 INT, a47 INT, a48 INT, a49 INT, a50 INT, a51 INT, a52 INT, a53 INT, a54 INT, a55 INT, 
                             a56 INT, a57 INT, a58 INT, a59 INT, a60 INT, a61 INT, a62 INT, a63 INT, a64 INT, a65 INT, a66 INT, 
                             a67 INT, a68 INT, a69 INT, a70 INT, a71 INT, a72 INT, a73 INT, a74 INT, a75 INT, a76 INT, a77 INT, 
                             a78 INT, a79 INT, a80 INT, a81 INT, a82 INT, a83 INT, a84 INT, a85 INT, a86 INT, a87 INT, a88 INT, 
                             a89 INT, a90 INT, a91 INT, a92 INT, a93 INT, a94 INT, a95 INT, a96 INT, a97 INT, a98 INT, a99 INT, 
                             a100 INT, a101 INT, a102 INT, a103 INT, a104 INT, a105 INT, a106 INT, a107 INT, a108 INT, a109 INT, 
                             a110 INT, a111 INT, a112 INT, a113 INT, a114 INT, a115 INT, a116 INT, a117 INT, a118 INT, a119 INT, 
                             a120 INT, a121 INT, a122 INT, a123 INT, a124 INT, a125 INT, a126 INT, a127 INT, a128 INT, a129 INT, 
                             a130 INT, a131 INT, a132 INT, a133 INT, a134 INT, a135 INT, a136 INT, a137 INT, a138 INT, a139 INT, 
                             a140 INT, a141 INT, a142 INT, a143 INT, a144 INT, a145 INT, a146 INT, a147 INT, a148 INT, a149 INT, 
                             a150 INT, a151 INT, a152 INT, a153 INT, a154 INT, a155 INT, a156 INT, a157 INT, a158 INT, a159 INT, 
                             a160 INT, a161 INT, a162 INT, a163 INT, a164 INT, a165 INT, a166 INT, a167 INT, a168 INT, a169 INT, 
                             a170 INT, a171 INT, a172 INT, a173 INT, a174 INT, a175 INT, a176 INT, a177 INT, a178 INT, a179 INT, 
                             a180 INT, a181 INT, a182 INT, a183 INT, a184 INT, a185 INT, a186 INT, a187 INT, a188 INT, a189 INT, 
                             a190 INT, a191 INT, a192 INT, a193 INT, a194 INT, a195 INT, a196 INT, a197 INT, a198 INT, a199 INT, 
                             a200 INT, a201 INT, a202 INT, a203 INT, a204 INT, a205 INT, a206 INT, a207 INT, a208 INT, a209 INT, 
                             a210 INT, a211 INT, a212 INT, a213 INT, a214 INT, a215 INT, a216 INT, a217 INT, a218 INT, a219 INT, 
                             a220 INT, a221 INT, a222 INT, a223 INT, a224 INT, a225 INT, a226 INT, a227 INT, a228 INT, a229 INT, 
                             a230 INT, a231 INT, a232 INT, a233 INT, a234 INT, a235 INT, a236 INT, a237 INT, a238 INT, a239 INT, 
                             a240 INT, a241 INT, a242 INT, a243 INT, a244 INT, a245 INT, a246 INT, a247 INT, a248 INT, a249 INT,
                             a250 INT, a251 INT, a252 INT, a253 INT, a254 INT, a255 INT, a256 INT, a257 INT, a258 INT, a259 INT,
                             a260 INT, a261 INT, a262 INT, a263 INT, a264 INT, a265 INT, a266 INT, a267 INT, a268 INT, a269 INT,
                             a270 INT, a271 INT, a272 INT, a273 INT, a274 INT, a275 INT, a276 INT, a277 INT, a278 INT, a279 INT, 
                             a280 INT, a281 INT, a282 INT, a283 INT, a284 INT, a285 INT, a286 INT, a287 INT, a288 INT, a289 INT, 
                             a290 INT, a291 INT, a292 INT, a293 INT, a294 INT, a295 INT, a296 INT, a297 INT, a298 INT, a299 INT, 
                             a300 INT) with (OIDS=true);
                    """).substitute({'t1': tableName}),
    PASS_CRITERIA = "SQLSTATE",
    SQLSTATE      = "00000"
  )

  rc |= testSQL(
        DESC          = "insert a row",
        SQL           = 
            """INSERT INTO %s VALUES (1, NULL, 3, NULL, 5, NULL, 7, NULL, 9, NULL, 11, NULL, 13, NULL, 15, NULL, 17, 
            NULL, 19, NULL, 21, NULL, 23, NULL, 25, NULL, 27, NULL, 29, NULL, 31, NULL, 33, NULL, 35, NULL, 37, NULL, 39, NULL, 41, 
            NULL, 43, NULL, 45, NULL, 47, NULL, 49, NULL, 51, NULL, 53, NULL, 55, NULL, 57, NULL, 59, NULL, 61, NULL, 63, NULL, 65, 
            NULL, 67, NULL, 69, NULL, 71, NULL, 73, NULL, 75, NULL, 77, NULL, 79, NULL, 81, NULL, 83, NULL, 85, NULL, 87, NULL, 89, 
            NULL, 91, NULL, 93, NULL, 95, NULL, 97, NULL, 99, NULL, 101, NULL, 103, NULL, 105, NULL, 107, NULL, 109, NULL, 111, 
            NULL, 113, NULL, 115, NULL, 117, NULL, 119, NULL, 121, NULL, 123, NULL, 125, NULL, 127, NULL, 129, NULL, 131, NULL, 
            133, NULL, 135, NULL, 137, NULL, 139, NULL, 141, NULL, 143, NULL, 145, NULL, 147, NULL, 149, NULL, 151, NULL, 153, 
            NULL, 155, NULL, 157, NULL, 159, NULL, 161, NULL, 163, NULL, 165, NULL, 167, NULL, 169, NULL, 171, NULL, 173, NULL, 
            175, NULL, 177, NULL, 179, NULL, 181, NULL, 183, NULL, 185, NULL, 187, NULL, 189, NULL, 191, NULL, 193, NULL, 195, 
            NULL, 197, NULL, 199, NULL, 201, NULL, 203, NULL, 205, NULL, 207, NULL, 209, NULL, 211, NULL, 213, NULL, 215, NULL, 
            217, NULL, 219, NULL, 221, NULL, 223, NULL, 225, NULL, 227, NULL, 229, NULL, 231, NULL, 233, NULL, 235, NULL, 237, 
            NULL, 239, NULL, 241, NULL, 243, NULL, 245, NULL, 247, NULL, 249, NULL, 251, NULL, 253, NULL, 255, NULL, 257, NULL, 
            259, NULL, 261, NULL, 263, NULL, 265, NULL, 267, NULL, 269, NULL, 271, NULL, 273, NULL, 275, NULL, 277, NULL, 279, 
            NULL, 281, NULL, 283, NULL, 285, NULL, 287, NULL, 289, NULL, 291, NULL, 293, NULL, 295, NULL, 297, NULL, 299, NULL);"""%(tableName),
        PASS_CRITERIA = "SQLSTATE",
        SQLSTATE      = "00000"
    )
  
  blockId = getBlockId(tableName)

  rc |= run_checkpoint()
  rc |= testCMD(
    DESC          = "data dump",
    COMMAND       = "%s/tmp_build/bin/pagedump -f \"%s/dstore/PDB_2/file6_8\" -b %s -d"%(code_path, data_dir, blockId),
    PASS_CRITERIA = "GREP",
    PATTERN       = """
ItemId 1: \(([0-9]+), 1256\)
HeapDiskTuple
    m_tdId: 0
    m_lockerTdId: INVALID_TD_SLOT
    m_size: 1256
    m_info:
        m_hasNull: 1
        m_hasVarwidth: 0
        m_hasExternal: 0
        m_hasOid: 1
        m_tdStatus: attached as new owner    
        m_liveMode: TUPLE_BY_NORMAL_INSERT
        m_linkInfo: 0
        m_numColumn: 300
        m_isNullReserve: 1
    Nullbitmap \(Hex\):
        5555555555555555 5555555555555555 5555555555555555 5555555555555555
        555555555505
    OID \(Uint32\): ([0-9]+)
    Data \(Length = 1200 Bytes\):
        0100000000000000 0300000000000000 0500000000000000 0700000000000000
        0900000000000000 0B00000000000000 0D00000000000000 0F00000000000000
        1100000000000000 1300000000000000 1500000000000000 1700000000000000
        1900000000000000 1B00000000000000 1D00000000000000 1F00000000000000
        2100000000000000 2300000000000000 2500000000000000 2700000000000000
        2900000000000000 2B00000000000000 2D00000000000000 2F00000000000000
        3100000000000000 3300000000000000 3500000000000000 3700000000000000
        3900000000000000 3B00000000000000 3D00000000000000 3F00000000000000
        4100000000000000 4300000000000000 4500000000000000 4700000000000000
        4900000000000000 4B00000000000000 4D00000000000000 4F00000000000000
        5100000000000000 5300000000000000 5500000000000000 5700000000000000
        5900000000000000 5B00000000000000 5D00000000000000 5F00000000000000
        6100000000000000 6300000000000000 6500000000000000 6700000000000000
        6900000000000000 6B00000000000000 6D00000000000000 6F00000000000000
        7100000000000000 7300000000000000 7500000000000000 7700000000000000
        7900000000000000 7B00000000000000 7D00000000000000 7F00000000000000
        8100000000000000 8300000000000000 8500000000000000 8700000000000000
        8900000000000000 8B00000000000000 8D00000000000000 8F00000000000000
        9100000000000000 9300000000000000 9500000000000000 9700000000000000
        9900000000000000 9B00000000000000 9D00000000000000 9F00000000000000
        A100000000000000 A300000000000000 A500000000000000 A700000000000000
        A900000000000000 AB00000000000000 AD00000000000000 AF00000000000000
        B100000000000000 B300000000000000 B500000000000000 B700000000000000
        B900000000000000 BB00000000000000 BD00000000000000 BF00000000000000
        C100000000000000 C300000000000000 C500000000000000 C700000000000000
        C900000000000000 CB00000000000000 CD00000000000000 CF00000000000000
        D100000000000000 D300000000000000 D500000000000000 D700000000000000
        D900000000000000 DB00000000000000 DD00000000000000 DF00000000000000
        E100000000000000 E300000000000000 E500000000000000 E700000000000000
        E900000000000000 EB00000000000000 ED00000000000000 EF00000000000000
        F100000000000000 F300000000000000 F500000000000000 F700000000000000
        F900000000000000 FB00000000000000 FD00000000000000 FF00000000000000
        0101000000000000 0301000000000000 0501000000000000 0701000000000000
        0901000000000000 0B01000000000000 0D01000000000000 0F01000000000000
        1101000000000000 1301000000000000 1501000000000000 1701000000000000
        1901000000000000 1B01000000000000 1D01000000000000 1F01000000000000
        2101000000000000 2301000000000000 2501000000000000 2701000000000000
        2901000000000000 2B01000000000000                   
    
"""
  )

  rc |= testSQL(
      DESC          = "",
      SQL           = "DROP TABLE " + tableName,
      PASS_CRITERIA = "IGNORE"
    )

  return rc

def testunit_11():
#-------------------------------------------------------------------------------------------------------------------------
# Description : Dumping a big tuple with no showTupleData option.
#-------------------------------------------------------------------------------------------------------------------------
  rc = 0
 
  rc |= testSQL(
    DESC          = "create main table",
    SQL           = Template("""
                    CREATE TABLE $t1 (a1 text)  with (OIDS=false);
                    """).substitute({'t1': tableName}),
    PASS_CRITERIA = "SQLSTATE",
    SQLSTATE      = "00000"
  )

  rc |= testSQL(
        DESC          = "insert a row",
        SQL           = "INSERT INTO %s VALUES (repeat('a', 18000));"%(tableName),
        PASS_CRITERIA = "SQLSTATE",
        SQLSTATE      = "00000"
    )
  
  blockId = getBlockId(tableName)

  rc |= run_checkpoint()
  rc |= testCMD(
    DESC          = "data dump",
    COMMAND       = "%s/tmp_build/bin/pagedump -f \"%s/dstore/PDB_2/file6_8\" -b %s"%(code_path, data_dir, blockId),
    PASS_CRITERIA = "GREP",
    PATTERN       = """
ItemId 1: \(([0-9]+), 7960\)
HeapDiskTuple \(BigTuple First Linked Chunk\)
    m_tdId: 0
    m_lockerTdId: INVALID_TD_SLOT
    m_size: 7960
    m_info:
        m_hasNull: 0
        m_hasVarwidth: 1
        m_hasExternal: 0
        m_hasOid: 0
        m_tdStatus: attached as new owner    
        m_liveMode: TUPLE_BY_NORMAL_INSERT
        m_linkInfo: 1
        m_numColumn: 1
        m_isNullReserve: 0
"""
  )

  rc |= testSQL(
      DESC          = "",
      SQL           = "DROP TABLE " + tableName,
      PASS_CRITERIA = "IGNORE"
    )

  return rc

def testunit_12():
#-------------------------------------------------------------------------------------------------------------------------
# Description : Table contains a big tuple, with nulls, with OID, and 16 columns of text.
#-------------------------------------------------------------------------------------------------------------------------
  rc = 0
 
  rc |= testSQL(
    DESC          = "create main table",
    SQL           = Template("""
                    CREATE TABLE $t1 (a1 TEXT, a2 TEXT, a3 TEXT, a4 TEXT, a5 TEXT, a6 TEXT, a7 TEXT, a8 TEXT, 
                             a9 TEXT, a10 TEXT, a11 TEXT, a12 TEXT, a13 TEXT, a14 TEXT, a15 TEXT, a16 TEXT)  with (OIDS=true);
                    """).substitute({'t1': tableName}),
    PASS_CRITERIA = "SQLSTATE",
    SQLSTATE      = "00000"
  )

  rc |= testSQL(
        DESC          = "insert a row",
        SQL           = "INSERT INTO %s VALUES (repeat('a', 18000), '', 'c', 'd', '', '', '', '', '', '', '', '', '', '', '', '');"%(tableName),
        PASS_CRITERIA = "SQLSTATE",
        SQLSTATE      = "00000"
    )
  
  blockId = getBlockId(tableName)

  rc |= run_checkpoint()
  rc |= testCMD(
    DESC          = "data dump",
    COMMAND       = "%s/tmp_build/bin/pagedump -f \"%s/dstore/PDB_2/file6_8\" -b %s -d"%(code_path, data_dir, blockId),
    PASS_CRITERIA = "GREP",
    PATTERN       = """
ItemId 1: \(([0-9]+), 7960\)
HeapDiskTuple \(BigTuple First Linked Chunk\)
    m_tdId: 0
    m_lockerTdId: INVALID_TD_SLOT
    m_size: 7960
    m_info:
        m_hasNull: 1
        m_hasVarwidth: 1
        m_hasExternal: 0
        m_hasOid: 1
        m_tdStatus: attached as new owner    
        m_liveMode: TUPLE_BY_NORMAL_INSERT
        m_linkInfo: 1
        m_numColumn: 16
        m_isNullReserve: 1
    Next Linked Chunk's CTID \(ItemPointerData\): \(([0-9]+), ([0-9]+), ([0-9]+)\)
    NumChunks \(Uint8\): 3
    Nullbitmap \(Hex\): 0D00
    OID \(Uint32\): ([0-9]+)
    Data \(Length = 7935 Bytes\):
        5019010061616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 61616161616161
"""
  )

  rc |= testSQL(
      DESC          = "",
      SQL           = "DROP TABLE " + tableName,
      PASS_CRITERIA = "IGNORE"
    )

  return rc

def testunit_13():
#-------------------------------------------------------------------------------------------------------------------------
# Description : Dumping a bigTuple's non-first linked tuple chunk.  
#               This means m_linkInfo == HeapDiskTupLinkInfoType::TUP_LINK_NOT_FIRST_CHUNK_TYPE.
#-------------------------------------------------------------------------------------------------------------------------
  rc = 0
 
  rc |= testSQL(
    DESC          = "create main table",
    SQL           = Template("""
                    CREATE TABLE $t1 (a1 text)  with (OIDS=false);
                    """).substitute({'t1': tableName}),
    PASS_CRITERIA = "SQLSTATE",
    SQLSTATE      = "00000"
  )

  rc |= testSQL(
        DESC          = "insert a row",
        SQL           = "INSERT INTO %s VALUES (repeat('a', 18000));"%(tableName),
        PASS_CRITERIA = "SQLSTATE",
        SQLSTATE      = "00000"
    )
  rc |= run_checkpoint()

  blockId = getBlockId(tableName)
  nextBlockId = getNextLinkChunkCtid(blockId)
  rc |= testCMD(
    DESC          = "data dump",
    COMMAND       = "%s/tmp_build/bin/pagedump -f \"%s/dstore/PDB_2/file6_8\" -b %s -d"%(code_path, data_dir, nextBlockId),
    PASS_CRITERIA = "GREP",
    PATTERN       = """
ItemId 1: \(([0-9]+), 7960\)
HeapDiskTuple \(BigTuple Non-First Linked Chunk\)
    m_tdId: 0
    m_lockerTdId: INVALID_TD_SLOT
    m_size: 7960
    Next Linked Chunk's CTID \(ItemPointerData\): \(([0-9]+), ([0-9]+), ([0-9]+)\)
    Data \(Length = 7943 Bytes\):
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        61616161616161
"""
    )

  rc |= testSQL(
      DESC          = "",
      SQL           = "DROP TABLE " + tableName,
      PASS_CRITERIA = "IGNORE"
    )

  return rc

def testunit_14():
#-------------------------------------------------------------------------------------------------------------------------
# Description : A bigTuple's non-first linked chunk dump, but with no showTupleData option.
#-------------------------------------------------------------------------------------------------------------------------
  rc = 0
 
  rc |= testSQL(
    DESC          = "create main table",
    SQL           = Template("""
                    CREATE TABLE $t1 (a1 text)  with (OIDS=false);
                    """).substitute({'t1': tableName}),
    PASS_CRITERIA = "SQLSTATE",
    SQLSTATE      = "00000"
  )

  rc |= testSQL(
        DESC          = "insert a row",
        SQL           = "INSERT INTO %s VALUES (repeat('a', 18000));"%(tableName),
        PASS_CRITERIA = "SQLSTATE",
        SQLSTATE      = "00000"
    )
  rc |= run_checkpoint()

  blockId = getBlockId(tableName)
  nextBlockId = getNextLinkChunkCtid(blockId)
  rc |= testCMD(
    DESC          = "data dump",
    COMMAND       = "%s/tmp_build/bin/pagedump -f \"%s/dstore/PDB_2/file6_8\" -b %s"%(code_path, data_dir, nextBlockId),
    PASS_CRITERIA = "GREP",
    PATTERN       = """
ItemId 1: \(([0-9]+), 7960\)
HeapDiskTuple \(BigTuple Non-First Linked Chunk\)
    m_tdId: 0
    m_lockerTdId: INVALID_TD_SLOT
    m_size: 7960
"""
    )

  rc |= testSQL(
      DESC          = "",
      SQL           = "DROP TABLE " + tableName,
      PASS_CRITERIA = "IGNORE"
    )

  return rc

def testunit_15():
#-------------------------------------------------------------------------------------------------------------------------
# Description : Dumps a big tuple's last linked tuple chunk.  "next linked chunk's ctid" points to 
#               INVALID_ITEM_POINTER due to being last.
#-------------------------------------------------------------------------------------------------------------------------
  rc = 0
 
  rc |= testSQL(
    DESC          = "create main table",
    SQL           = Template("""
                    CREATE TABLE $t1 (a1 text)  with (OIDS=false);
                    """).substitute({'t1': tableName}),
    PASS_CRITERIA = "SQLSTATE",
    SQLSTATE      = "00000"
  )

  rc |= testSQL(
        DESC          = "insert a row",
        SQL           = "INSERT INTO %s VALUES (repeat('a', 18000));"%(tableName),
        PASS_CRITERIA = "SQLSTATE",
        SQLSTATE      = "00000"
    )
  rc |= run_checkpoint()

  blockId = getBlockId(tableName)
  nextBlockId = getNextLinkChunkCtid(blockId)
  lastBlockId = getNextLinkChunkCtid(nextBlockId)

  rc |= testCMD(
    DESC          = "data dump",
    COMMAND       = "%s/tmp_build/bin/pagedump -f \"%s/dstore/PDB_2/file6_8\" -b %s -d"%(code_path, data_dir, lastBlockId),
    PASS_CRITERIA = "GREP",
    PATTERN       = """
ItemId 1: \(([0-9]+), 2135\)
HeapDiskTuple \(BigTuple Non-First Linked Chunk\)
    m_tdId: 0
    m_lockerTdId: INVALID_TD_SLOT
    m_size: 2135
    Next Linked Chunk's CTID \(ItemPointerData\): INVALID_ITEM_POINTER
    Data \(Length = 2118 Bytes\):
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        6161616161616161 6161616161616161 6161616161616161 6161616161616161
        616161616161
"""
    )

  rc |= testSQL(
      DESC          = "",
      SQL           = "DROP TABLE " + tableName,
      PASS_CRITERIA = "IGNORE"
    )

  return rc

def cleanup ():
#-----------------------------------------------------------------------------
# Description : cleanup
# Exp Results : success
#-----------------------------------------------------------------------------
  rc = 0

  rc |= fvt_disconnect("")

  return rc
