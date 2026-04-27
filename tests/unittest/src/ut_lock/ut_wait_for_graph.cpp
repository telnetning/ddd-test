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
#include "ut_utilities/ut_dstore_framework.h"

using namespace DSTORE;
class LockWaitForGraphTest : public DSTORETEST {
protected:
    void SetUp() override
    {
        DSTORETEST::SetUp();
        UtInstance *instance = DstoreNew(m_ut_memory_context)UtInstance();
        instance->Install(&DSTORETEST::m_guc, m_ut_memory_context);
        instance->Startup(&DSTORETEST::m_guc);
    }

    void TearDown() override
    {
        UtInstance *instance = (UtInstance *)g_storageInstance;
        instance->Shutdown();
        delete instance;
        DSTORETEST::TearDown();
    }
};

typedef std::pair<unsigned, unsigned> EdgeIndexPair;

/*
 * TEST: Test a simple case for cycle detection algorithm for graph used in deadlock detection.
 * In this case, all nodes are NOT in cycle.
 *               
 * ┌───┐   ┌───┐ 
 * │ 0 ├───► 1 │ 
 * └───┘   └───┘ 
 *               
*/
TEST_F(LockWaitForGraphTest, FindNoCycleTestSimple_level0)
{
    // input
    VertexTag vertices[] = {
        VertexTag{0, 0, 0, 0, 0},
        VertexTag{1, 0, 0, 0, 0}
    };
    EdgeIndexPair edges[] = {EdgeIndexPair{0, 1}};
    int verticesNum = sizeof(vertices) / sizeof(VertexTag);
    int edgesNum = sizeof(edges) / sizeof(EdgeIndexPair);
    int expectedQueueLength = 2;
    int expectedLastVerticesNum = 0;

    // run
    WaitForGraph waitForGraph;
    RetStatus ret = waitForGraph.BuildWaitForGraph(m_ut_memory_context);
    ASSERT_EQ(ret, DSTORE_SUCC);

    for (int i = 0; i < verticesNum; ++i) {
        EXPECT_EQ(waitForGraph.FindVertex(vertices[i]), nullptr);
    }
    for (int i = 0; i < verticesNum; ++i) {
        Vertex FakeVertex(false);
        ASSERT_EQ(waitForGraph.AddVertex(vertices[i], &FakeVertex), DSTORE_SUCC);
    }
    for (int i = 0; i < verticesNum; ++i) {
        Vertex *vertex_found = waitForGraph.FindVertex(vertices[i]);
        ASSERT_NE(vertex_found, nullptr);
        ASSERT_NE(vertex_found->GetVertexTag(), nullptr);
        EXPECT_EQ(*(vertex_found->GetVertexTag()), vertices[i]);
    }
    EXPECT_EQ(waitForGraph.GetVertexNumber(), verticesNum);
    for (int i = 0; i < edgesNum; ++i) {
        Edge fakeEdge;
        Vertex *startVertex = waitForGraph.FindVertex(vertices[edges[i].first]);
        Vertex *endVertex = waitForGraph.FindVertex(vertices[edges[i].second]);
        ASSERT_NE(startVertex, nullptr);
        ASSERT_NE(endVertex, nullptr);
        ASSERT_EQ(waitForGraph.AddEdge(&fakeEdge, startVertex, endVertex), DSTORE_SUCC);
    }
    waitForGraph.ScanVerticesNotInCycleAndPushIntoDeleteQueue();
    EXPECT_EQ(waitForGraph.GetQueueLength(), expectedQueueLength);
    waitForGraph.DeleteAllVerticesNotInCycle();
    EXPECT_EQ(waitForGraph.GetQueueLength(), 0);
    EXPECT_EQ(waitForGraph.GetVertexNumber(), expectedLastVerticesNum);
    waitForGraph.DestroyWaitForGraph();
}

/*
 * TEST: Test a simple case for cycle detection algorithm for graph used in deadlock detection.
 * In this case, all nodes are in cycle.
 *
 *          ┌───┐  
 *    ┌─────► 1 │  
 *    │     └─┬─┘  
 *  ┌─┴─┐     │    
 *  │ 0 │     │    
 *  └─▲─┘     │    
 *    │     ┌─▼─┐  
 *    └─────┤ 2 │  
 *          └───┘  
*/
TEST_F(LockWaitForGraphTest, FindCycleTestSimple_level0)
{
    // input
    VertexTag vertices[] = {
        VertexTag{0, 0, 0, 0, 0},
        VertexTag{1, 0, 0, 0, 0},
        VertexTag{2, 0, 0, 0, 0}
    };
    EdgeIndexPair edges[] = {
        EdgeIndexPair{0, 1},
        EdgeIndexPair{1, 2},
        EdgeIndexPair{2, 0},
    };
    int verticesNum = sizeof(vertices) / sizeof(VertexTag);
    int edgesNum = sizeof(edges) / sizeof(EdgeIndexPair);
    int expectedQueueLength = 0;
    int expectedLastVerticesNum = 3;

    // run
    WaitForGraph waitForGraph;
    RetStatus ret = waitForGraph.BuildWaitForGraph(m_ut_memory_context);
    ASSERT_EQ(ret, DSTORE_SUCC);

    for (int i = 0; i < verticesNum; ++i) {
        EXPECT_EQ(waitForGraph.FindVertex(vertices[i]), nullptr);
    }
    for (int i = 0; i < verticesNum; ++i) {
        Vertex FakeVertex(false);
        ASSERT_EQ(waitForGraph.AddVertex(vertices[i], &FakeVertex), DSTORE_SUCC);
    }
    for (int i = 0; i < verticesNum; ++i) {
        Vertex *vertex_found = waitForGraph.FindVertex(vertices[i]);
        ASSERT_NE(vertex_found, nullptr);
        ASSERT_NE(vertex_found->GetVertexTag(), nullptr);
        EXPECT_EQ(*(vertex_found->GetVertexTag()), vertices[i]);
    }
    EXPECT_EQ(waitForGraph.GetVertexNumber(), verticesNum);
    for (int i = 0; i < edgesNum; ++i) {
        Edge fakeEdge;
        Vertex *startVertex = waitForGraph.FindVertex(vertices[edges[i].first]);
        Vertex *endVertex = waitForGraph.FindVertex(vertices[edges[i].second]);
        ASSERT_NE(startVertex, nullptr);
        ASSERT_NE(endVertex, nullptr);
        ASSERT_EQ(waitForGraph.AddEdge(&fakeEdge, startVertex, endVertex), DSTORE_SUCC);
    }
    waitForGraph.ScanVerticesNotInCycleAndPushIntoDeleteQueue();
    EXPECT_EQ(waitForGraph.GetQueueLength(), expectedQueueLength);
    waitForGraph.DeleteAllVerticesNotInCycle();
    EXPECT_EQ(waitForGraph.GetQueueLength(), 0);
    EXPECT_EQ(waitForGraph.GetVertexNumber(), expectedLastVerticesNum);
    waitForGraph.DestroyWaitForGraph();
}

/*
 * TEST: Test a complex case for cycle detection algorithm for graph used in deadlock detection.
 * In this case, node 0~4, 7 and 8 are in cycle, 4, 5, 9 and 10 are not.
 * 
 *          ┌───┐    ┌───┐                        
 *    ┌─────► 1 ◄────┤ 4 ◄────┐                   
 *    │     └─┬─┘    └─▲─┘    │                   
 *  ┌─┴─┐     │        │    ┌─┴─┐                 
 *  │ 0 │     │        │    │ 5 │                 
 *  └─▲─┘     │        │    └─┬─┘    ┌───┐  ┌───┐ 
 *    │     ┌─▼─┐    ┌─┴─┐    │      │ 7 ├──► 9 │ 
 *    └─────┤ 2 ┼────► 3 ◄────┘      └▲─┬┘  └───┘ 
 *          └─┬─┘    └───┘            │ │         
 *            │                       │ │         
 *          ┌─▼─┐                    ┌┴─▼┐  ┌────┐
 *          │ 6 │                    │ 8 ◄──┤ 10 │
 *          └───┘                    └───┘  └────┘
 */
TEST_F(LockWaitForGraphTest, FindCycleTestComplex_level0)
{
    // input
    VertexTag vertices[] = {
        VertexTag{0, 0, 0, 0, 0},
        VertexTag{1, 0, 0, 0, 0},
        VertexTag{2, 0, 0, 0, 0},
        VertexTag{3, 0, 0, 0, 0},
        VertexTag{4, 0, 0, 0, 0},
        VertexTag{0, 1, 0, 0, 0},
        VertexTag{0, 0, 1, 0, 0},
        VertexTag{0, 0, 0, 1, 0},
        VertexTag{0, 0, 0, 0, 1},
        VertexTag{0, 1, 1, 0, 0},
        VertexTag{0, 0, 1, 1, 1},
    };
    EdgeIndexPair edges[] = {
        EdgeIndexPair{0, 1},
        EdgeIndexPair{1, 2},
        EdgeIndexPair{2, 0},
        EdgeIndexPair{2, 3},
        EdgeIndexPair{3, 4},
        EdgeIndexPair{4, 1},
        EdgeIndexPair{5, 4},
        EdgeIndexPair{5, 3},
        EdgeIndexPair{2, 6},
        EdgeIndexPair{7, 8},
        EdgeIndexPair{8, 7},
        EdgeIndexPair{7, 9},
        EdgeIndexPair{10, 8},
    };
    int verticesNum = sizeof(vertices) / sizeof(VertexTag);
    int edgesNum = sizeof(edges) / sizeof(EdgeIndexPair);
    int expectedQueueLength = 4;
    int expectedLastVerticesNum = 7;

    //run
    WaitForGraph waitForGraph;
    RetStatus ret = waitForGraph.BuildWaitForGraph(m_ut_memory_context);
    ASSERT_EQ(ret, DSTORE_SUCC);

    for (int i = 0; i < verticesNum; ++i) {
        EXPECT_EQ(waitForGraph.FindVertex(vertices[i]), nullptr);
    }
    for (int i = 0; i < verticesNum; ++i) {
        Vertex FakeVertex(false);
        ASSERT_EQ(waitForGraph.AddVertex(vertices[i], &FakeVertex), DSTORE_SUCC);
    }
    for (int i = 0; i < verticesNum; ++i) {
        Vertex *vertex_found = waitForGraph.FindVertex(vertices[i]);
        ASSERT_NE(vertex_found, nullptr);
        ASSERT_NE(vertex_found->GetVertexTag(), nullptr);
        EXPECT_EQ(*(vertex_found->GetVertexTag()), vertices[i]);
    }
    EXPECT_EQ(waitForGraph.GetVertexNumber(), verticesNum);
    for (int i = 0; i < edgesNum; ++i) {
        Edge fakeEdge;
        Vertex *startVertex = waitForGraph.FindVertex(vertices[edges[i].first]);
        Vertex *endVertex = waitForGraph.FindVertex(vertices[edges[i].second]);
        ASSERT_NE(startVertex, nullptr);
        ASSERT_NE(endVertex, nullptr);
        ASSERT_EQ(waitForGraph.AddEdge(&fakeEdge, startVertex, endVertex), DSTORE_SUCC);
    }
    waitForGraph.ScanVerticesNotInCycleAndPushIntoDeleteQueue();
    EXPECT_EQ(waitForGraph.GetQueueLength(), expectedQueueLength);
    waitForGraph.DeleteAllVerticesNotInCycle();
    EXPECT_EQ(waitForGraph.GetQueueLength(), 0);
    EXPECT_EQ(waitForGraph.GetVertexNumber(), expectedLastVerticesNum);
    waitForGraph.DestroyWaitForGraph();
}