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
 *
 * ---------------------------------------------------------------------------------
 * 
 * ut_double_linked_list.cpp
 * 
 * Description:
 * 1. test double linked list
 *
 * ---------------------------------------------------------------------------------
 */
#include "gtest/gtest.h"
#include "container/linked_list.h"
#include "memory/memory_ctx.h"


typedef DListHead TestDListHead;
typedef struct TestDListNode TestDListNode;
struct TestDListNode {
    int value;
    DListNode node;
};


TestDListNode *CreateTestDListNode(int val)
{
    TestDListNode *node = (TestDListNode *)MemAlloc(sizeof(TestDListNode));
    if (node == NULL) {
        return NULL;
    }
    node->value = val;
    node->node.next = NULL;
    return node;
}

void DestroyTestDListNode(TestDListNode *node)
{
    if (node) {
        MemFree(node);
    }
}

TestDListHead *CreateTestDList()
{
    TestDListHead *head = (TestDListHead*)MemAlloc(sizeof(TestDListHead));
    if (head == NULL) {
        return NULL;
    }
    DListInit(head);
    return head;
}

void ClearTestDList(TestDListHead *head)
{
    if (DListIsEmpty(head)) {
        return;
    }
    DListMutableIter iter;
    DLIST_MODIFY_FOR_EACH(iter, head) {
        DListNode *cur = iter.cur;
        /* unlink cur node from linked list  */
        DListDelete(cur);
        /* get test data */
        TestDListNode *testData = DLIST_CONTAINER(TestDListNode, node, cur);
        DestroyTestDListNode(testData);
    }
}

void DestroyTestDList(TestDListHead *head)
{
    if (!DListIsEmpty(head)) {
        ClearTestDList(head);
    }
    MemFree(head);
}

bool TestDListPushHead(TestDListHead *head, int val)
{
    TestDListNode *node = CreateTestDListNode(val);
    if (node == NULL) {
        return false;
    }
    DListPushHead(head, &node->node);

    return true;
}

void TestDListPushTail(DListHead *head, int val)
{
    TestDListNode *node = CreateTestDListNode(val);
    ASSERT_NE(node, nullptr);
    DListPushTail(head, &node->node);
}

TestDListNode *TestDListPopHead(TestDListHead *head)
{
    DListNode *node = DListPopHeadNode(head);
    return DLIST_CONTAINER(TestDListNode, node, node);
}

/* test double linked list */
class DLinkedListTest : public testing::Test {
public:
    void SetUp() override {
        MemoryContextSwitchTo(MemoryContextCreate(NULL, MEM_CXT_TYPE_GENERIC, "ut_dlinklist_mctx",
                                                  0, DEFAULT_UNLIMITED_SIZE, DEFAULT_UNLIMITED_SIZE));
    };

    void TearDown() override {
        MemoryContextDelete(MemoryContextSwitchTo(NULL));
    };
};

TEST_F(DLinkedListTest, DListCreateTest)
{
    TestDListHead *head = CreateTestDList();
    ASSERT_NE(head, (TestDListHead *)NULL);

    /* destroy list */
    DestroyTestDList(head);
}

TEST_F(DLinkedListTest, DListPushHeadTest)
{
    TestDListHead *head = CreateTestDList();
    ASSERT_NE(head, (TestDListHead *)NULL);

    /* insert node in head */
    for (int i = 0; i < 10; ++i) {
        bool ret = TestDListPushHead(head,i);
        ASSERT_EQ(ret, true);
    }

    /* destroy list */
    DestroyTestDList(head);
}

TEST_F(DLinkedListTest, DListPopHeadTest)
{
    TestDListHead *head = CreateTestDList();
    ASSERT_NE(head, (TestDListHead *)NULL);

    /* insert node in head */
    for (int i = 0; i < 10; ++i) {
        bool ret = TestDListPushHead(head,i);
        ASSERT_EQ(ret, true);
    }

    /* remove node from head */
    for (int i = 9; i >= 0 && !DListIsEmpty(head); --i) {
        TestDListNode *node = TestDListPopHead(head);
        ASSERT_NE(node, (TestDListNode *)NULL);
        ASSERT_EQ(node->value, i);
        DestroyTestDListNode(node);
    }

    /* destroy list */
    DestroyTestDList(head);
}

TEST_F(DLinkedListTest, DListTraverseTest)
{
    TestDListHead *head = CreateTestDList();
    ASSERT_NE(head, (TestDListHead *)NULL);

    /* insert node in head */
    for (int i = 0; i < 10; ++i) {
        bool ret = TestDListPushHead(head,i);
        ASSERT_EQ(ret, true);
    }

    /* find node of which value equal 9 */
    DListIter it;
    bool found = false;
    DLIST_FOR_EACH(it, head) {
        TestDListNode *curNode = DLIST_CONTAINER(TestDListNode, node, it.cur);
        if (curNode->value == 9) {
            found = true;
        }
    }
    ASSERT_EQ(found, true);

    /* destroy list */
    DestroyTestDList(head);
}

TEST_F(DLinkedListTest, DListRemoveTest)
{
    TestDListHead *head = CreateTestDList();
    ASSERT_NE(head, (TestDListHead *)NULL);

    /* insert node in head */
    for (int i = 0; i < 10; ++i) {
        bool ret = TestDListPushHead(head,i);
        ASSERT_EQ(ret, true);
    }

    /* find and remove node of which value equal 9 */
    bool success = false;
    DListMutableIter iter;
    DLIST_MODIFY_FOR_EACH(iter, head) {
        DListNode *cur = iter.cur;
        TestDListNode *testData = DLIST_CONTAINER(TestDListNode, node, cur);
        if (testData->value == 9) {
            DListDelete(cur);
            DestroyTestDListNode(testData);
            success = true;
        }
    }
    ASSERT_EQ(success, true);

    /* destroy list */
    DestroyTestDList(head);
}

TEST_F(DLinkedListTest, DListAppendMoveTest001)
{
    DListHead *src = CreateTestDList();
    ASSERT_NE(src, nullptr);
    DListHead *dst = CreateTestDList();
    ASSERT_NE(dst, nullptr);
    for (int i = 0; i < 10; ++i) {
        TestDListPushTail(src, i);
    }
    DListIter it;
    int expectNum = 0;
    DLIST_FOR_EACH(it, src) {
        TestDListNode *curNode = DLIST_CONTAINER(TestDListNode, node, it.cur);
        ASSERT_EQ(curNode->value, expectNum);
        expectNum++;
    }
    /* Move src to dst */
    DListAppendMove(dst, src);
    ASSERT_TRUE(DListIsEmpty(src));
    ASSERT_FALSE(DListIsEmpty(dst));
    expectNum = 0;
    DLIST_FOR_EACH(it, dst) {
        TestDListNode *curNode = DLIST_CONTAINER(TestDListNode, node, it.cur);
        ASSERT_EQ(curNode->value, expectNum);
        expectNum++;
    }
    DestroyTestDList(src);
    DestroyTestDList(dst);
}

TEST_F(DLinkedListTest, DListAppendMoveTest002)
{
    DListHead *src = CreateTestDList();
    ASSERT_NE(src, nullptr);
    DListHead *dst = CreateTestDList();
    ASSERT_NE(dst, nullptr);
    for (int i = 0; i < 10; ++i) {
        TestDListPushTail(dst, i);
        TestDListPushTail(src, i + 10);
    }
    ASSERT_FALSE(DListIsEmpty(src));
    DListAppendMove(dst, src);
    ASSERT_TRUE(DListIsEmpty(src));
    DListIter it;
    int expectNum = 0;
    DLIST_FOR_EACH(it, dst) {
        TestDListNode *curNode = DLIST_CONTAINER(TestDListNode, node, it.cur);
        ASSERT_EQ(curNode->value, expectNum);
        expectNum++;
    }
    ASSERT_EQ(expectNum, 20);
    DestroyTestDList(src);
    DestroyTestDList(dst);
}
