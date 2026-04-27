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
import sys
import json
import string
import random
import subprocess
from collections import deque

path = os.path.dirname(os.path.abspath(__file__))
relPath = os.path.relpath(path, os.path.expanduser("~"))
files = [f for f in os.listdir(path) if f.endswith(".py") and os.path.isfile(os.path.join(path, f))]
hash = {}
verbose = False
getBacktrace = True

def log(*args):
    if verbose:
        print(*args)

def randStr():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k = 10))

def runCommand(command: str, connection:subprocess.Popen = None):
    log(connection.args[2] if connection else "localhost", "<", command)
    res = []
    if connection == None:
        connection = subprocess.run(command, shell = True, capture_output = True, text = True)
        if connection.returncode != 0:
            print("STDERRR >", connection.stderr)
            raise Exception(f"Call to '{command}' returned with non-zero status code {connection.returncode}")
        for line in connection.stdout.splitlines():
            log(">", line)
        return connection.stdout.splitlines()
    else:
        end = randStr()
        connection.stdin.write(f"{command}\n")
        connection.stdin.write(f"echo -e \"{end}\\n$?\"\n")
        connection.stdin.flush()
        line = connection.stdout.readline().strip()
        while line != end:
            log(connection.args[2], ">", line)
            res.append(line)
            line = connection.stdout.readline().strip()
        log(connection.args[2], ">END", line)
        ret = int(connection.stdout.readline().strip())
        if ret != 0:
            print(connection.args[2], "STDERRR >", connection.stderr.read())
            raise Exception(f"Call to '{command}' returned with non-zero status code {ret}")
    return res

def scanNode(edgeList: deque, address: str = None):
    try:
        tempDir = None
        ssh = None
        if address:
            ssh = subprocess.Popen(['ssh', '-ttT', address], \
                stdin = subprocess.PIPE, \
                stdout = subprocess.PIPE, \
                universal_newlines=True, \
                bufsize = 0)
            runCommand("echo", ssh)
        # If we are debugging on a remote node, we need to make sure we have an updated version of the script on the machine
        if ssh:
            scriptPath = None
            scriptPaths = [dir.strip().split() for dir in runCommand(f"find ~ -type f \( {' -or '.join([f'-name {f}' for f in files])} \) -printf '%h\\n' | sort | uniq -cd", ssh)]
            scriptPaths = [dir[1] for dir in scriptPaths if int(dir[0]) == len(files)]
            log("Found scripts in", scriptPaths)
            for dir in scriptPaths:
                match = True
                log("Checking dir", dir)
                for file in files:
                    h = runCommand(f"sha256sum {os.path.join(dir, file)}", ssh)[0].split()[0]
                    if h != hash[file]:
                        log(file, "did not match hash")
                        match = False
                        break
                if match:
                    log("Selected", dir)
                    scriptPath = dir
                    break
            if not scriptPath:
                log("Creating TMP folder")
                # No viable copy of scripts found. We will copy it ourselves.
                tempDir = runCommand("mktemp -d", ssh)[0]
                scriptPath = tempDir
                runCommand(f"scp {' '.join([os.path.join(path, f) for f in files])} {address}:{scriptPath}")
        else:
            scriptPath = path


        pids = runCommand("pgrep -u $USER gaussdb", ssh)
        print(f"Found {len(pids)} gaussdb processes", f"on {address}" if address else "")
        # For each of the pids, we need to run the command
        gdbPreLoadCommands = [
            "set auto-load safe-path /",
            "set build-id-verbose 0",
            "set print thread-events off"
        ]
        gdbPreLoadString = " ".join([f"-iex '{cmd}'" for cmd in gdbPreLoadCommands])
        gdbCommands =  [
            f"source {scriptPath}/gdb_init.py",
            "dump_dstore_deadlock_edges",
        ]
        if getBacktrace:
            if ssh and not tempDir:
                tempDir = runCommand("mktemp -d", ssh)[0]
                traceDir = tempDir
            else:
                traceDir = "."
            gdbCommands.extend([
                "python gdb_util.saveNodeId()",
                f"eval \"set logging file {traceDir}/node_%d.trace\", $nodeId",
                "set logging redirect on",
                "set logging on",
                "thread apply all bt"
            ])
            gdbCommandString = " ".join([f"-ex '{cmd}'" for cmd in gdbCommands])
        for pid in pids:
            print("Running gdb script on", address if address else "localhost", "process", pid)
            output = runCommand(f"gdb -p {pid} -n {gdbPreLoadString} {gdbCommandString} --batch --quiet", ssh)
            save = False
            for line in output:
                line = line.strip()
                if "EDGES START" in line:
                    save = True
                elif "EDGES END" in line:
                    save = False
                elif save:
                    edgeList.append(json.loads(line))
        if ssh and getBacktrace:
            print("Copying traces from remote")
            output = runCommand(f"scp {address}:{traceDir}/*.trace ./")
    except:
        raise
    finally:
        if tempDir:
            runCommand(f"rm -r {tempDir}", ssh)
        if ssh:
            print("Closing ssh connection to", address)
            ssh.stdin.write("exit\n")
            ssh.stdin.flush()
        else:
            print("Finished local scan")

class Edge:
    def __init__(self, src, dest, type, info):
        self.src: Vertex = src
        self.dest: Vertex = dest
        self.type: str = type
        self.info: dict[str, str] = info

    def __str__(self):
        return f"""NodeId = {self.src.nodeId} ThreadId = {hex(self.src.threadId)} --- Waiting For ---> NodeId = {self.dest.nodeId} ThreadId = {hex(self.dest.threadId)}
        \tType = {self.type}
        \tInfo = {str(self.info)}\n"""

class Vertex:
    def __init__(self, nodeId, threadId, threadCoreIdx):
        self.nodeId = nodeId
        self.threadId = threadId
        self.threadCoreIdx = threadCoreIdx
        self.inEdges: dict[str, Edge] = {}
        self.outEdges: dict[str, Edge] = {}

    def __str__(self):
        return f"({self.nodeId}, {self.threadId})"

    def addEdge(self, edge: Edge):
        if edge.src == self:
            assert str(edge.dest) not in self.outEdges
            self.outEdges[str(edge.dest)] = edge
        elif edge.dest == self:
            assert str(edge.dest) not in self.inEdges
            self.inEdges[str(edge.src)] = edge
        else:
            assert False

class DeadlockDetector:
    def __init__(self):
        self.rawEdgeList = deque()
        self.partialRpcEdges: dict[str, dict] = {}
        self.vertexMap: dict[str, Vertex] = {}

    def constructEdges(self):
        for edgeValues in self.rawEdgeList:
            if edgeValues["type"] == "RPC_REQUEST":
                # We are missing info from one half of the request
                seqId = edgeValues["info"]["seqId"]
                key = f"({edgeValues['src']['nodeId']}, {edgeValues['dest']['nodeId']}, {seqId})"
                if key in self.partialRpcEdges:
                    missingValue = "src" if "threadId" not in edgeValues["src"] else "dest"
                    if self.partialRpcEdges[key][missingValue].get("nodeId"):
                        assert edgeValues[missingValue]["nodeId"] == self.partialRpcEdges[key][missingValue]["nodeId"]
                    edgeValues[missingValue] = self.partialRpcEdges[key][missingValue]
                    del self.partialRpcEdges[key]
                else:
                    self.partialRpcEdges[key] = edgeValues
                    continue
            # Create the edgeValues
            src = self.getVertex(edgeValues["src"])
            dest = self.getVertex(edgeValues["dest"])
            edge = Edge(src, dest, edgeValues["type"], edgeValues["info"])
            log(str(edge))
            src.addEdge(edge)
            dest.addEdge(edge)
        if len(self.partialRpcEdges) != 0:
            print("Warning: Not all RPC threads matched between nodes")

    def getVertex(self, values: dict) -> Vertex:
        log("Get Vertex", values)
        key = f"({values['nodeId']}, {values['threadId']})"
        threadCoreIdx = values.get("threadCoreIdx")
        if key not in self.vertexMap:
            self.vertexMap[key] = Vertex(values["nodeId"], values["threadId"], threadCoreIdx)
        vertex = self.vertexMap[key]
        if threadCoreIdx and not vertex.threadCoreIdx:
            vertex.threadCoreIdx = threadCoreIdx
        elif threadCoreIdx and vertex.threadCoreIdx:
            assert threadCoreIdx == vertex.threadCoreIdx
        return vertex

    def topoSort(self):
        queue = deque(self.vertexMap.values())
        while queue:
            vertex: Vertex = queue.pop()
            if str(vertex) not in self.vertexMap:
                continue
            # If the vertex is not waiting or no one is depending on it, then it cannot be part of a cycle.
            if len(vertex.inEdges) == 0 or len(vertex.outEdges) == 0:
                for edge in vertex.inEdges.values():
                    del edge.src.outEdges[str(vertex)]
                    queue.append(edge.src)
                for edge in vertex.outEdges.values():
                    del edge.dest.inEdges[str(vertex)]
                    queue.append(edge.dest)
                vertex.inEdges.clear()
                del self.vertexMap[str(vertex)]

    def reportCycles(self):
        if len(self.vertexMap) == 0:
            print("No Deadlocks Found")
            return
        visited = set()
        curVertex = None
        print("\n======= Cycles =======\n")
        for vertex in self.vertexMap.values():
            curVertex = vertex
            hasCycle = False
            while 1:
                if str(curVertex) in visited:
                    break
                visited.add(str(curVertex))
                outEdge = list(curVertex.outEdges.values())[0]
                print(str(outEdge))
                curVertex = outEdge.dest
                hasCycle = True
            if hasCycle:
                print("Deadlock Detected\n")


scanLocalNode = False
manual = False
remoteNodes = sys.argv[1:]
if "--scanLocal" in remoteNodes:
    scanLocalNode = True
    remoteNodes.remove("--scanLocal")
if "--verbose" in remoteNodes:
    verbose = True
    remoteNodes.remove("--verbose")
if "--manual" in remoteNodes:
    manual = True
    remoteNodes.remove("--manual")
if "--skipTrace" in remoteNodes:
    getBacktrace = False
    remoteNodes.remove("--skipTrace")
if "--help" in remoteNodes or "-h" in remoteNodes or (len(remoteNodes) == 0 and not scanLocalNode and not manual):
    print(
f"""{sys.argv[0]} [--scanLocal] [--verbose] [--manual] [--skipTrace] [--help] <Remote1> <Remote2> ...

\t--scanLocal\tScans the gaussdb processes on the local machine
\t--verbose\tVerbose output of all the commands
\t--manual\tAdd extra edges though stdin
\t--skipTrace\tSkips acquiring the backtrace of each node
\t--help\t\tShows the usage of {sys.argv[0]}

The remotes included should be of the formate user@address. This script will check if a copy of itself
already exists on the remote  before trying to copy itself over to use for debugging. All edges are
then collected back here and are checked to see if any deadlocks are present.
""")
    exit()
print("Checking gaussdb processes on ", "$USER@localhost " if scanLocalNode else "", " ".join(remoteNodes), sep="")
if len(remoteNodes) > 0:
    # If we need to debug on remote nodes, we need to make sure they have an updated version of the script
    for f in files:
        hash[f] = runCommand(f"sha256sum {os.path.join(path, f)}")[0].split()[0]

dd = DeadlockDetector()
if scanLocalNode:
    scanNode(dd.rawEdgeList)
for node in remoteNodes:
    scanNode(dd.rawEdgeList, node)
if manual:
    print("Additional Edges:")
    input = sys.stdin.read().split("\n")
    for line in input:
        line = line.strip()
        if line:
            dd.rawEdgeList.append(json.loads(line))
dd.constructEdges()
dd.topoSort()
dd.reportCycles()
