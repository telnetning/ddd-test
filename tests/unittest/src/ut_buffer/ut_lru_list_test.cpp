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
//
// Created by c00428156 on 2022/4/16.
//
#include "ut_buffer/ut_lru_test.h"

class LruListTest : public LruTest {
protected:

};

TEST_F(LruListTest, PullEmptyListTest)
{
    LruList list{};
    list.Initialize();
    LruNode* node = list.PopTail();
    ASSERT_EQ(node, nullptr);

    ASSERT_EQ(list.Length(), 0);
}

TEST_F(LruListTest, AddTailTest)
{
    LruList list{};
    list.Initialize();
    TestEntry entrys[10]{};
    for (int i = 0; i < 10; i++) {
        entrys[i].value = i;
        list.AddTail(&entrys[i].node);
    }

    ASSERT_EQ(list.Length(), 10);

    auto iter = list.RBegin();
    int value = 9;
    while (iter != list.REnd()) {
        TestEntry* entry = BufLruList::GetNode(*iter)->GetValue<TestEntry>();
        ASSERT_EQ(entry->value, value);
        value--;
        ++iter;
    }
    ASSERT_EQ(value, -1);
}

TEST_F(LruListTest, AddHeadTest)
{
    LruList list{};
    list.Initialize();
    TestEntry entrys[10]{};
    for (int i = 0; i < 10; i++) {
        entrys[i].value = i;
        list.AddHead(&entrys[i].node);
    }

    ASSERT_EQ(list.Length(), 10);

    auto iter = list.RBegin();
    int value = 0;
    while (iter != list.REnd()) {
        TestEntry* entry = BufLruList::GetNode(*iter)->GetValue<TestEntry>();
        ASSERT_EQ(entry->value, value);
        value++;
        ++iter;
    }
    ASSERT_EQ(value, 10);
}

TEST_F(LruListTest, MoveHeadTest)
{
    LruList list{};
    list.Initialize();
    TestEntry entrys[10]{};
    for (int i = 0; i < 10; i++) {
        entrys[i].value = i;
        list.AddHead(&entrys[i].node);
    }

    ASSERT_EQ(list.Length(), 10);

    list.MoveHead(&entrys[0].node);
    ASSERT_EQ(list.Length(), 10);
    LruNode* node = list.PopTail();
    TestEntry* entry = node->GetValue<TestEntry>();
    ASSERT_EQ(entry->value, 1);

    ASSERT_EQ(list.Length(), 9);
}

TEST_F(LruListTest, RemoveTest)
{
    LruList list{};
    list.Initialize();
    TestEntry entrys[10]{};
    for (int i = 0; i < 10; i++) {
        entrys[i].value = i;
        list.AddHead(&entrys[i].node);
    }

    ASSERT_EQ(list.Length(), 10);

    for (int i = 1; i < 9; i++) {
        list.Remove(&entrys[i].node);
    }

    ASSERT_EQ(list.Length(), 2);

    LruNode* node = list.PopTail();
    TestEntry* entry = node->GetValue<TestEntry>();
    ASSERT_EQ(entry->value, 0);
    ASSERT_EQ(list.Length(), 1);

    node = list.PopTail();
    entry = node->GetValue<TestEntry>();
    ASSERT_EQ(entry->value, 9);
    ASSERT_EQ(list.Length(), 0);
}