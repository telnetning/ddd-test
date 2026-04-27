/*
 * Copyright (C) 2026 Huawei Technologies Co.,Ltd.
 *
 * dstore is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * dstore is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. if not, see <https://www.gnu.org/licenses/>.
 */

#ifndef DSTORE_WAIT_FOR_GRAPH_H
#define DSTORE_WAIT_FOR_GRAPH_H

#include "securec.h"
#include "common/algorithm/dstore_hsearch.h"
#include "common/algorithm/dstore_string_info.h"
#include "lock/dstore_lock_datatype.h"

namespace DSTORE {

struct VertexTag {
    uint64 field1;               /* a 64-bit ID field */
    uint64 field2;               /* a 64-bit ID field */
    uint64 field3;               /* a 64-bit ID field */
    uint32 field4;               /* a 32-bit ID field */
    uint32 field5;               /* a 32-bit ID field */

    void Dump(StringInfo str) const;

    bool operator==(const VertexTag &tag) const
    {
        return memcmp(this, &tag, sizeof(VertexTag)) == 0;
    }
};

static_assert(sizeof(VertexTag) == sizeof(uint64) + sizeof(uint64) + sizeof(uint64) + sizeof(uint32) + sizeof(uint32),
    "make sure vertex tag is a tight structure");

class Vertex;
struct VertexHashEntry {
    VertexTag tag;
    Vertex *vertex;
};

class Edge : public BaseObject {
public:
    Edge();
    virtual ~Edge();
    DISALLOW_COPY_AND_MOVE(Edge);

    virtual Edge *Duplicate(DstoreMemoryContext ctx);

    void SetPeerVertex(Vertex *peerVertex);
    Vertex *GetPeerVertex();

    void SetReverseEdge(Edge *reverseEdge);
    Edge *GetReverseEdge();

    void LinkAfter(Edge *edge);
    void RemoveLinks();
    Edge *GetNext();

protected:
    Vertex *m_peerVertex;
    Edge *m_reverseEdge;
    Edge *m_prevEdge;
    Edge *m_nextEdge;
};

class Vertex : public BaseObject {
public:
    explicit Vertex(bool checkAllEdgeInCycle, int initOutEdgeCnt = 0);
    virtual ~Vertex();
    DISALLOW_COPY_AND_MOVE(Vertex);

    virtual Vertex *Duplicate(DstoreMemoryContext ctx);

    void SetVertexTag(VertexTag *tag);
    VertexTag *GetVertexTag();

    void SetToBeRemoved();
    bool IsToBeRemoved();

    void AddInEdge(Edge *inEdge);
    void AddOutEdge(Edge *outEdge);

    void RemoveInEdge(Edge *inEdge);
    void RemoveOutEdge(Edge *outEdge);

    bool IsVertexInCycle();

    void AppendAfter(Vertex *vertex);
    void ClearNext();
    Vertex *GetNext();

    class EdgeIterator {
    public:
        explicit EdgeIterator(Vertex *vertex, bool isInEdge);
        ~EdgeIterator() = default;
        DISALLOW_COPY_AND_MOVE(EdgeIterator);

        Edge *GetNextEdge();
    private:
        Vertex *m_vertex;
        Edge *m_startEdge;
        Edge *m_nextEdge;
    };

protected:
    int m_inEdgeCnt;
    int m_outEdgeCnt;
    int m_initOutEdgeCnt;
    bool m_checkAllEdgeInCycle;
    Edge m_inEdgeList;
    Edge m_outEdgeList;
    Vertex *m_next;
    VertexTag *m_tag;
    bool m_isToBeRemoved;
};

class WaitForGraph : public BaseObject {
public:
    explicit WaitForGraph();
    ~WaitForGraph();

    RetStatus BuildWaitForGraph(DstoreMemoryContext mctx);
    void DestroyWaitForGraph();

    Vertex *FindVertex(VertexTag &tag);
    RetStatus AddVertex(VertexTag &tag, Vertex *vertex);
    uint32 GetVertexNumber();

    RetStatus AddEdge(Edge *oriEdge, Vertex *startVertex, Vertex *endVertex);

    void ScanVerticesNotInCycleAndPushIntoDeleteQueue();
    RetStatus DeleteAllVerticesNotInCycle();
    void PushIntoDeleteQueue(Vertex *vertex);

    class CycleIterator {
    public:
        explicit CycleIterator(WaitForGraph *waitForGraph);
        ~CycleIterator() = default;
        DISALLOW_COPY_AND_MOVE(CycleIterator);

        Vertex *GetNextVertex();
    private:
        WaitForGraph *m_waitForGraph;
        HASH_SEQ_STATUS m_hashStatus;
    };

#ifdef UT
    int GetQueueLength()
    {
        return m_vertexQueueLength;
    }
#endif

protected:
    Vertex *PopFromQueue();
    bool IsQueueEmpty();

    DstoreMemoryContext m_ctx;
    HTAB *m_vertexs;
    Vertex m_vertexQueue;
    int m_vertexQueueLength;
};

}
#endif