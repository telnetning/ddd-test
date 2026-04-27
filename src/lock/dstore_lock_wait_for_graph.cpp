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

#include "lock/dstore_lock_wait_for_graph.h"
#include "errorcode/dstore_lock_error_code.h"

namespace DSTORE {

Edge::Edge()
    : m_peerVertex(nullptr),
      m_reverseEdge(nullptr),
      m_prevEdge(this),
      m_nextEdge(this)
{
}

Edge::~Edge()
{
    m_peerVertex = nullptr;
    m_reverseEdge = nullptr;
    m_prevEdge = nullptr;
    m_nextEdge = nullptr;
}

Edge *Edge::Duplicate(DstoreMemoryContext ctx)
{
    Edge *edge = DstoreNew(ctx) Edge();
    if (STORAGE_VAR_NULL(edge)) {
        return nullptr;
    }
    StorageAssert(m_reverseEdge == nullptr);
    return edge;
}

void Edge::SetPeerVertex(Vertex *peerVertex)
{
    m_peerVertex = peerVertex;
}

Vertex *Edge::GetPeerVertex()
{
    return m_peerVertex;
}

void Edge::SetReverseEdge(Edge *reverseEdge)
{
    m_reverseEdge = reverseEdge;
}

Edge *Edge::GetReverseEdge()
{
    return m_reverseEdge;
}

void Edge::LinkAfter(Edge *edge)
{
    StorageAssert(edge != nullptr);
    m_prevEdge = edge;
    m_nextEdge = edge->m_nextEdge;
    m_prevEdge->m_nextEdge = this;
    m_nextEdge->m_prevEdge = this;
}

void Edge::RemoveLinks()
{
    StorageAssert(m_prevEdge != nullptr);
    StorageAssert(m_nextEdge != nullptr);
    m_prevEdge->m_nextEdge = m_nextEdge;
    m_nextEdge->m_prevEdge = m_prevEdge;
    m_prevEdge = this;
    m_nextEdge = this;
}

Edge *Edge::GetNext()
{
    return m_nextEdge;
}

Vertex::Vertex(bool checkAllEdgeInCycle, int initOutEdgeCnt)
    : m_inEdgeCnt(0),
      m_outEdgeCnt(0),
      m_initOutEdgeCnt(initOutEdgeCnt),
      m_checkAllEdgeInCycle(checkAllEdgeInCycle),
      m_inEdgeList(),
      m_outEdgeList(),
      m_next(nullptr),
      m_tag(nullptr),
      m_isToBeRemoved(false)
{
}

Vertex::~Vertex()
{
    Edge *edge = nullptr;
    Vertex::EdgeIterator iterIn(this, true);
    while ((edge = iterIn.GetNextEdge()) != nullptr) {
        RemoveInEdge(edge);
        delete edge;
    }

    Vertex::EdgeIterator iterOut(this, false);
    while ((edge = iterOut.GetNextEdge()) != nullptr) {
        RemoveInEdge(edge);
        delete edge;
    }
}

Vertex *Vertex::Duplicate(DstoreMemoryContext ctx)
{
    Vertex *vertex = DstoreNew(ctx) Vertex(m_checkAllEdgeInCycle, m_initOutEdgeCnt);
    if (STORAGE_VAR_NULL(vertex)) {
        return nullptr;
    }

    StorageAssert(m_inEdgeCnt == 0);
    StorageAssert(m_outEdgeCnt == 0);
    return vertex;
}

void Vertex::SetVertexTag(VertexTag *tag)
{
    m_tag = tag;
}

VertexTag *Vertex::GetVertexTag()
{
    return m_tag;
}

void Vertex::SetToBeRemoved()
{
    m_isToBeRemoved = true;
}

bool Vertex::IsToBeRemoved()
{
    return m_isToBeRemoved;
}

void Vertex::AddInEdge(Edge *inEdge)
{
    inEdge->LinkAfter(&m_inEdgeList);
    m_inEdgeCnt++;
}

void Vertex::AddOutEdge(Edge *outEdge)
{
    outEdge->LinkAfter(&m_outEdgeList);
    m_outEdgeCnt++;
}

void Vertex::RemoveInEdge(Edge *inEdge)
{
    inEdge->RemoveLinks();
    m_inEdgeCnt--;
}

void Vertex::RemoveOutEdge(Edge *outEdge)
{
    outEdge->RemoveLinks();
    m_outEdgeCnt--;
}

bool Vertex::IsVertexInCycle()
{
    if ((m_inEdgeCnt == 0) || (m_outEdgeCnt == 0) || (m_checkAllEdgeInCycle && (m_outEdgeCnt != m_initOutEdgeCnt))) {
        return false;
    }
    return true;
}

void Vertex::AppendAfter(Vertex *vertex)
{
    vertex->m_next = this;
}

void Vertex::ClearNext()
{
    m_next = nullptr;
}

Vertex *Vertex::GetNext()
{
    return m_next;
}

Vertex::EdgeIterator::EdgeIterator(Vertex *vertex, bool isInEdge)
    : m_vertex(vertex),
      m_startEdge(isInEdge ? &vertex->m_inEdgeList : &vertex->m_outEdgeList),
      m_nextEdge(m_startEdge->GetNext())
{
    StorageAssert(m_nextEdge != nullptr);
}

Edge *Vertex::EdgeIterator::GetNextEdge()
{
    if (m_nextEdge == m_startEdge) {
        return nullptr;
    }
    Edge *edge = m_nextEdge;
    StorageAssert(m_nextEdge != nullptr);
    m_nextEdge = m_nextEdge->GetNext();
    return edge;
}

WaitForGraph::WaitForGraph()
    : m_ctx(nullptr),
      m_vertexs(nullptr),
      m_vertexQueue(false),
      m_vertexQueueLength(0)
{
}

WaitForGraph::~WaitForGraph()
{
    m_ctx = nullptr;
    m_vertexs = nullptr;
}

RetStatus WaitForGraph::BuildWaitForGraph(DstoreMemoryContext mctx)
{
    m_ctx = mctx;

    HASHCTL info;
    info.keysize = sizeof(VertexTag);
    info.entrysize = sizeof(VertexHashEntry);
    info.hash = tag_hash;
    info.hcxt = m_ctx;
    int hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_SHRCTX);
    long estimateSize = 215;

    m_vertexs = hash_create("Wait-For Graph Vertex hash", estimateSize, &info, hashFlags);
    if (unlikely(m_vertexs == nullptr)) {
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

void WaitForGraph::DestroyWaitForGraph()
{
    if (likely(m_vertexs != nullptr)) {
        HASH_SEQ_STATUS hashStatus;
        VertexHashEntry *vertexHashEntry = nullptr;
        hash_seq_init(&hashStatus, m_vertexs);
        while ((vertexHashEntry = static_cast<VertexHashEntry *>(hash_seq_search(&hashStatus))) != nullptr) {
            delete vertexHashEntry->vertex;
        }
        hash_destroy(m_vertexs);
        m_vertexs = nullptr;
    }
}

void WaitForGraph::PushIntoDeleteQueue(Vertex *vertex)
{
    vertex->SetToBeRemoved();
    Vertex *oldNext = m_vertexQueue.GetNext();
    vertex->AppendAfter(&m_vertexQueue);
    if (oldNext != nullptr) {
        oldNext->AppendAfter(vertex);
    }
    m_vertexQueueLength++;
}

Vertex *WaitForGraph::PopFromQueue()
{
    Vertex *vertex = m_vertexQueue.GetNext();
    if ((vertex != nullptr) && (vertex->GetNext() != nullptr)) {
        Vertex *next = vertex->GetNext();
        next->AppendAfter(&m_vertexQueue);
        vertex->ClearNext();
    } else {
        m_vertexQueue.ClearNext();
    }

    if (vertex != nullptr) {
        m_vertexQueueLength--;
    }
    return vertex;
}

bool WaitForGraph::IsQueueEmpty()
{
    return (m_vertexQueue.GetNext() == nullptr);
}

Vertex *WaitForGraph::FindVertex(VertexTag &tag)
{
    bool found = false;
    VertexHashEntry *vertexHashEntry = static_cast<VertexHashEntry *>(hash_search(m_vertexs,
        static_cast<void *>(&tag), HASH_FIND, &found));
    if (vertexHashEntry == nullptr) {
        return nullptr;
    }
    return vertexHashEntry->vertex;
}

RetStatus WaitForGraph::AddVertex(VertexTag &tag, Vertex *vertex)
{
    bool found = false;
    VertexHashEntry *vertexHashEntry = static_cast<VertexHashEntry *>(hash_search(m_vertexs,
        static_cast<void *>(&tag), HASH_ENTER, &found));
    if (STORAGE_VAR_NULL(vertexHashEntry)) {
        storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
        return DSTORE_FAIL;
    }

    if (found) {
        return DSTORE_SUCC;
    }

    vertexHashEntry->tag = tag;
    vertexHashEntry->vertex = vertex->Duplicate(m_ctx);
    if (STORAGE_VAR_NULL(vertexHashEntry->vertex)) {
        (void)hash_search(m_vertexs, static_cast<void *>(&tag), HASH_REMOVE, &found);
        storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
        return DSTORE_FAIL;
    }

    vertexHashEntry->vertex->SetVertexTag(&(vertexHashEntry->tag));
    return DSTORE_SUCC;
}

uint32 WaitForGraph::GetVertexNumber()
{
    if (STORAGE_VAR_NULL(m_vertexs)) {
        return 0;
    }
    return static_cast<uint32>(hash_get_num_entries(m_vertexs));
}

RetStatus WaitForGraph::AddEdge(Edge *oriEdge, Vertex *startVertex, Vertex *endVertex)
{
    Edge *edge = oriEdge->Duplicate(m_ctx);
    if (STORAGE_VAR_NULL(edge)) {
        storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
        return DSTORE_FAIL;
    }

    Edge *reverseEdge = oriEdge->Duplicate(m_ctx);
    if (STORAGE_VAR_NULL(reverseEdge)) {
        delete edge;
        storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
        return DSTORE_FAIL;
    }

    edge->SetReverseEdge(reverseEdge);
    edge->SetPeerVertex(endVertex);

    reverseEdge->SetReverseEdge(edge);
    reverseEdge->SetPeerVertex(startVertex);

    startVertex->AddOutEdge(edge);
    endVertex->AddInEdge(reverseEdge);
    return DSTORE_SUCC;
}

void WaitForGraph::ScanVerticesNotInCycleAndPushIntoDeleteQueue()
{
    /* Scan all vertices to find out the vertices that are not in cycle. */
    HASH_SEQ_STATUS hashStatus;
    VertexHashEntry *vertexHashEntry = nullptr;
    hash_seq_init(&hashStatus, m_vertexs);
    while ((vertexHashEntry = static_cast<VertexHashEntry *>(hash_seq_search(&hashStatus))) != nullptr) {
        if (!vertexHashEntry->vertex->IsVertexInCycle() && !vertexHashEntry->vertex->IsToBeRemoved()) {
            PushIntoDeleteQueue(vertexHashEntry->vertex);
        }
    }

    ErrLog(DSTORE_LOG, MODULE_LOCK,
        ErrMsg("Scan all vertices and put %d into queue to be removed.", m_vertexQueueLength));
}

RetStatus WaitForGraph::DeleteAllVerticesNotInCycle()
{
    /* Step1: Skip deletion if there is no removable vertex. */
    if (IsQueueEmpty()) {
        return DSTORE_SUCC;
    }

    /* Step2: Traverse through the queue and remove the vertices. */
    Vertex *toBeRemoved = PopFromQueue();
    while (toBeRemoved != nullptr) {
        Vertex *peerVertex = nullptr;
        Edge *edge = nullptr;

        /* Step2.1: Remove in edges, if peer vertex is not in cycle then push it into queue. */
        Vertex::EdgeIterator iterIn(toBeRemoved, true);
        while ((edge = iterIn.GetNextEdge()) != nullptr) {
            peerVertex = edge->GetPeerVertex();
            peerVertex->RemoveOutEdge(edge->GetReverseEdge());
            delete edge->GetReverseEdge();
            if (!peerVertex->IsVertexInCycle() && !peerVertex->IsToBeRemoved()) {
                PushIntoDeleteQueue(peerVertex);
            }

            toBeRemoved->RemoveInEdge(edge);
            delete edge;
        }

        /* Step2.2: Remove out edges, if peer vertex is not in cycle then push it into queue. */
        Vertex::EdgeIterator iterOut(toBeRemoved, false);
        while ((edge = iterOut.GetNextEdge()) != nullptr) {
            peerVertex = edge->GetPeerVertex();
            peerVertex->RemoveInEdge(edge->GetReverseEdge());
            delete edge->GetReverseEdge();
            if (!peerVertex->IsVertexInCycle() && !peerVertex->IsToBeRemoved()) {
                PushIntoDeleteQueue(peerVertex);
            }

            toBeRemoved->RemoveOutEdge(edge);
            delete edge;
        }

        /* Step2.3: Remove the vertex itself. */
        bool found = false;
        (void)hash_search(m_vertexs, static_cast<void *>(toBeRemoved->GetVertexTag()), HASH_REMOVE, &found);
        StorageAssert(found);
        delete toBeRemoved;

        /* Step2.4: Get new vertex to be removed. */
        toBeRemoved = PopFromQueue();
    }

    ErrLog(DSTORE_LOG, MODULE_LOCK,
        ErrMsg("After delete all vertices not in cycle, there are %u vertices left.", GetVertexNumber()));
    return DSTORE_SUCC;
}

WaitForGraph::CycleIterator::CycleIterator(WaitForGraph *waitForGraph)
    : m_waitForGraph(waitForGraph),
      m_hashStatus{}
{
    hash_seq_init(&m_hashStatus, m_waitForGraph->m_vertexs);
}

Vertex *WaitForGraph::CycleIterator::GetNextVertex()
{
    VertexHashEntry *vertexHashEntry = static_cast<VertexHashEntry *>(hash_seq_search(&m_hashStatus));
    if (vertexHashEntry == nullptr) {
        return nullptr;
    }
    return vertexHashEntry->vertex;
}

}