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
 * ut_single_linked_list.cpp
 * 
 * Description:
 * 1. test single linked list
 *
 * ---------------------------------------------------------------------------------
 */
#include "gtest/gtest.h"
#include "container/linked_list.h"
#include "memory/memory_ctx.h"

typedef SListHead TestSListHead;
typedef struct TestSListNode TestSListNode;
struct TestSListNode {
    int value;
    SListNode node;
};


TestSListNode *CreateTestSListNode(int val)
{
    TestSListNode *node = (TestSListNode *)MemAlloc(sizeof(TestSListNode));
    if (node == NULL) {
        return NULL;
    }
    node->value = val;
    node->node.next = NULL;
    return node;
}

void DestroyTestSListNode(TestSListNode *node)
{
    if (node) {
        MemFree(node);
    }
}

TestSListHead *CreateTestSList()
{
    TestSListHead *head = (TestSListHead*)MemAlloc(sizeof(TestSListHead));
    if (head == NULL) {
        return NULL;
    }
    SListInit(head);
    return head;
}

void ClearTestSList(TestSListHead *head)
{
    if (SListIsEmpty(head)) {
        return;
    }
    SListMutableIter iter;
    SLIST_MODIFY_FOR_EACH(iter, head) {
        SListNode *cur = iter.cur;
        SListDeleteCurrent(&iter);
        /* Converts the address of the SListNode into the address of the object (TestSListNode)
         * that contains the SListNode.
         */
        TestSListNode *curNode = SLIST_CONTAINER(TestSListNode, node, cur);
        DestroyTestSListNode(curNode);
    }
}

void DestroyTestSList(TestSListHead *head)
{
    if (!SListIsEmpty(head)) {
    	ClearTestSList(head);
    }
    MemFree(head);
}

bool TestSListPush(TestSListHead *head, int val)
{
    TestSListNode *node = CreateTestSListNode(val);
    if (node == NULL) {
        return false;
    }
    SListPushHead(head, &node->node);

    return true;
}

TestSListNode *TestSListPop(TestSListHead *head)
{
    SListNode *node = SListPopHeadNode(head);
    return SLIST_CONTAINER(TestSListNode, node, node);
}

bool TestFindNodeWithValue(TestSListHead *head, int value, SListNode **node)
{
    SListNode *testNode = head->head.next;
    while (testNode != NULL) {
        TestSListNode *curNode = SLIST_CONTAINER(TestSListNode, node, testNode);
        if (curNode->value == value) {
            *node = &curNode->node;
            return true;
        }
        testNode = testNode->next;
    }
    *node = NULL;
    return false;
}

class SLinkedListTest : public testing::Test {
public:
    void SetUp() override {
        MemoryContextSwitchTo(MemoryContextCreate(NULL, MEM_CXT_TYPE_GENERIC, "ut_slinklist_mctx",
                                                  0, DEFAULT_UNLIMITED_SIZE, DEFAULT_UNLIMITED_SIZE));
    };

    void TearDown() override {
        MemoryContextDelete(MemoryContextSwitchTo(NULL));
    };
};


TEST_F(SLinkedListTest, SListCreateTest)
{
    TestSListHead *head = CreateTestSList();
    ASSERT_NE(head, (TestSListHead *)NULL);

    /* destroy list */
    DestroyTestSList(head);
}

TEST_F(SLinkedListTest, SListPushTest)
{
    TestSListHead *head = CreateTestSList();
    ASSERT_NE(head, (TestSListHead *)NULL);

    /* insert node in head */
    for (int i = 0; i < 10; ++i) {
        bool ret = TestSListPush(head,i);
        ASSERT_EQ(ret, true);
    }

    /* destroy list */
    DestroyTestSList(head);
}

TEST_F(SLinkedListTest, SListPopTest)
{
    TestSListHead *head = CreateTestSList();
    ASSERT_NE(head, (TestSListHead *)NULL);

    /* insert node in head */
    for (int i = 0; i < 10; ++i) {
        bool ret = TestSListPush(head,i);
        ASSERT_EQ(ret, true);
    }

    /* remove node from head */
    for (int i = 9; i >= 0 && !SListIsEmpty(head); --i) {
        TestSListNode *node = TestSListPop(head);
        ASSERT_NE(node, (TestSListNode *)NULL);
        ASSERT_EQ(node->value, i);
        DestroyTestSListNode(node);
    }

    /* destroy list */
    DestroyTestSList(head);
}

TEST_F(SLinkedListTest, SListTraverseTest)
{
    TestSListHead *head = CreateTestSList();
    ASSERT_NE(head, (TestSListHead *)NULL);

    /* insert node in head */
    for (int i = 0; i < 10; ++i) {
        bool ret = TestSListPush(head,i);
        ASSERT_EQ(ret, true);
    }

    /* find node of which value equal 9 */
    SListIter it;
    bool found = false;
    SLIST_FOR_EACH(it, head) {
        TestSListNode *curNode = SLIST_CONTAINER(TestSListNode, node, it.cur);
        if (curNode->value == 9) {
            found = true;
        }
    }
    ASSERT_EQ(found, true);

    /* destroy list */
    DestroyTestSList(head);
}

TEST_F(SLinkedListTest, SListRemoveTest)
{
    TestSListHead *head = CreateTestSList();
    ASSERT_NE(head, (TestSListHead *)NULL);

    /* insert node in head */
    for (int i = 0; i < 10; ++i) {
        bool ret = TestSListPush(head,i);
        ASSERT_EQ(ret, true);
    }

    /* find and remove node of which value equal 9 */
    bool success = false;
    SListMutableIter iter;
    SLIST_MODIFY_FOR_EACH(iter, head) {
        SListNode *cur = iter.cur;
        TestSListNode *curNode = SLIST_CONTAINER(TestSListNode, node, cur);
        if (curNode->value == 9) {
            SListDeleteCurrent(&iter);
            DestroyTestSListNode(curNode);
            success = true;
        }
    }
    ASSERT_EQ(success, true);

    /* destroy list */
    DestroyTestSList(head);
}

TEST_F(SLinkedListTest, SListDeleteFirstNodeTest)
{
    TestSListHead *head = CreateTestSList();
    ASSERT_NE(head, (TestSListHead *)NULL);

    /* insert node in head */
    for (int i = 0; i < 10; ++i) {
        bool ret = TestSListPush(head,i);
        ASSERT_EQ(ret, true);
    }

    /* find and remove first node of which value equal 9 */
    SListNode *cur = NULL;

    bool result = TestFindNodeWithValue(head, 9, &cur);
    ASSERT_TRUE(result);

    SListDelete((SListHead *)head, cur);

    result = TestFindNodeWithValue(head, 9, &cur);
    ASSERT_FALSE(result);
    /* destroy list */
    DestroyTestSList(head);
}

TEST_F(SLinkedListTest, SListDeleteLastNodeTest)
{
    TestSListHead *head = CreateTestSList();
    ASSERT_NE(head, (TestSListHead *)NULL);

    /* insert node in head */
    for (int i = 0; i < 10; ++i) {
        bool ret = TestSListPush(head,i);
        ASSERT_EQ(ret, true);
    }

    /* find and remove last node of which value equal 0 */
    SListNode *cur = NULL;

    bool result = TestFindNodeWithValue(head, 0, &cur);
    ASSERT_TRUE(result);

    SListDelete((SListHead *)head, cur);

    result = TestFindNodeWithValue(head, 0, &cur);
    ASSERT_FALSE(result);
    /* destroy list */
    DestroyTestSList(head);
}


