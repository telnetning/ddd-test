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

#include <algorithm>
#include "ut_timer_wheel.h"

constexpr int TEST_EVENT_COUNT = 3;

using namespace std;

bool TimerWheelTest::CheckTwoSetEqual(const set<uint32_t> &set1, const set<uint32_t> &set2)
{
    if (set1.size() != set2.size()) {
        return false;
    }
    if (any_of(set1.begin(), set1.end(), [&](uint32_t i) {return set2.find(i) == set2.end();})) {
        return false;
    }
    return true;
}

TEST_F(TimerWheelTest, AddEventTest001)
{
    /* Add 3 event with timeout 10,20,30 */
    Event eventList[TEST_EVENT_COUNT];
    for (int i = 0; i < TEST_EVENT_COUNT; ++i) {
        InitTestEvent(&eventList[i], (i + 1) * TEST_JIFFY_TIME, i);
        AddEvent(timerWheel, &eventList[i]);
    }
    /* Call GetMultipleJiffyEventList 3 times */
    DListHead jiffyEventList;
    for (int i = 0; i < TEST_EVENT_COUNT; ++i) {
        DListInit(&jiffyEventList);
        GetMultipleJiffyEventList(timerWheel, 1, &jiffyEventList);
        ASSERT_EQ(GetTestEventId(DListPopHeadNode(&jiffyEventList)), i);
        ASSERT_TRUE(DListIsEmpty(&jiffyEventList));
    }
    /* Timer wheel has no event */
    DListInit(&jiffyEventList);
    GetMultipleJiffyEventList(timerWheel, 1, &jiffyEventList);
    ASSERT_TRUE(DListIsEmpty(&jiffyEventList));

    /* Add 3 event with timeout 20,30,40 */
    for (int i = 0; i < TEST_EVENT_COUNT; ++i) {
        InitTestEvent(&eventList[i], (i + 2) * TEST_JIFFY_TIME, i + 10);
        AddEvent(timerWheel, &eventList[i]);
    }
    /* First call GetMultipleJiffyEventList return empty event list */
    DListInit(&jiffyEventList);
    GetMultipleJiffyEventList(timerWheel, 1, &jiffyEventList);
    ASSERT_TRUE(DListIsEmpty(&jiffyEventList));
    /* Call GetMultipleJiffyEventList 3 times */
    for (int i = 0; i < TEST_EVENT_COUNT; ++i) {
        DListInit(&jiffyEventList);
        GetMultipleJiffyEventList(timerWheel, 1, &jiffyEventList);
        ASSERT_EQ(GetTestEventId(DListPopHeadNode(&jiffyEventList)), i + 10);
        ASSERT_TRUE(DListIsEmpty(&jiffyEventList));
    }
    /* Timer wheel has no event */
    DListInit(&jiffyEventList);
    GetMultipleJiffyEventList(timerWheel, 1, &jiffyEventList);
    ASSERT_TRUE(DListIsEmpty(&jiffyEventList));
}

TEST_F(TimerWheelTest, AddEventTest002)
{
    /* Add 3 event with timeout 20 */
    Event eventList1[TEST_EVENT_COUNT];
    set<uint32_t> eventIdSet1;
    for (int i = 0; i < TEST_EVENT_COUNT; ++i) {
        InitTestEvent(&eventList1[i], 2 * TEST_JIFFY_TIME, i);
        eventIdSet1.insert(i);
        AddEvent(timerWheel, &eventList1[i]);
    }
    /* Add 3 event with timeout 40 */
    Event eventList2[TEST_EVENT_COUNT];
    set<uint32_t> eventIdSet2;
    for (int i = 0; i < TEST_EVENT_COUNT; ++i) {
        InitTestEvent(&eventList2[i], 4 * TEST_JIFFY_TIME, i + 10);
        eventIdSet2.insert(i + 10);
        AddEvent(timerWheel, &eventList2[i]);
    }
    /* Time 10 has no event */
    DListHead jiffyEventList;
    DListInit(&jiffyEventList);
    GetMultipleJiffyEventList(timerWheel, 1, &jiffyEventList);
    ASSERT_TRUE(DListIsEmpty(&jiffyEventList));
    /* Time 20 has 3 events of id 0,1,2 */
    DListInit(&jiffyEventList);
    GetMultipleJiffyEventList(timerWheel, 1, &jiffyEventList);
    set<uint32_t> resEventIdSet;
    for (int i = 0; i < TEST_EVENT_COUNT; ++i) {
        ASSERT_FALSE(DListIsEmpty(&jiffyEventList));
        uint32_t curEventId = GetTestEventId(DListPopHeadNode(&jiffyEventList));
        resEventIdSet.insert(curEventId);
    }
    ASSERT_TRUE(CheckTwoSetEqual(eventIdSet1, resEventIdSet));
    ASSERT_TRUE(DListIsEmpty(&jiffyEventList));
    /* Time 30 has no event */
    DListInit(&jiffyEventList);
    GetMultipleJiffyEventList(timerWheel, 1, &jiffyEventList);
    ASSERT_TRUE(DListIsEmpty(&jiffyEventList));
    /* Time 40 has 3 events of id 10,11,12 */
    DListInit(&jiffyEventList);
    GetMultipleJiffyEventList(timerWheel, 1, &jiffyEventList);
    resEventIdSet.clear();
    for (int i = 0; i < TEST_EVENT_COUNT; ++i) {
        ASSERT_FALSE(DListIsEmpty(&jiffyEventList));
        uint32_t curEventId = GetTestEventId(DListPopHeadNode(&jiffyEventList));
        resEventIdSet.insert(curEventId);
    }
    ASSERT_TRUE(CheckTwoSetEqual(eventIdSet2, resEventIdSet));
    ASSERT_TRUE(DListIsEmpty(&jiffyEventList));
    /* Time 50 has no event */
    DListInit(&jiffyEventList);
    GetMultipleJiffyEventList(timerWheel, 1, &jiffyEventList);
    ASSERT_TRUE(DListIsEmpty(&jiffyEventList));
}

TEST_F(TimerWheelTest, AddEventTest003)
{
    /* Add 3 event with timeout 2560,2580,2600 */
    Event eventList[TEST_EVENT_COUNT];
    for (int i = 0; i < TEST_EVENT_COUNT; ++i) {
        InitTestEvent(&eventList[i], (i * 2 + 256) * TEST_JIFFY_TIME, i);
        AddEvent(timerWheel, &eventList[i]);
    }
    /* First 255 jiffy has no event */
    DListHead jiffyEventList;
    DListInit(&jiffyEventList);
    GetMultipleJiffyEventList(timerWheel, 255, &jiffyEventList);
    ASSERT_TRUE(DListIsEmpty(&jiffyEventList));
    /* Time 2560 get event id 0 */
    DListInit(&jiffyEventList);
    GetMultipleJiffyEventList(timerWheel, 1, &jiffyEventList);
    ASSERT_FALSE(DListIsEmpty(&jiffyEventList));
    ASSERT_EQ(GetTestEventId(DListPopHeadNode(&jiffyEventList)), 0);
    ASSERT_TRUE(DListIsEmpty(&jiffyEventList));
    /* Time 2570 has no event */
    DListInit(&jiffyEventList);
    GetMultipleJiffyEventList(timerWheel, 1, &jiffyEventList);
    ASSERT_TRUE(DListIsEmpty(&jiffyEventList));
    /* Time 2580 get event id 1 */
    DListInit(&jiffyEventList);
    GetMultipleJiffyEventList(timerWheel, 1, &jiffyEventList);
    ASSERT_FALSE(DListIsEmpty(&jiffyEventList));
    ASSERT_EQ(GetTestEventId(DListPopHeadNode(&jiffyEventList)), 1);
    ASSERT_TRUE(DListIsEmpty(&jiffyEventList));
    /* Time 2590 has no event */
    DListInit(&jiffyEventList);
    GetMultipleJiffyEventList(timerWheel, 1, &jiffyEventList);
    ASSERT_TRUE(DListIsEmpty(&jiffyEventList));
    /* Time 2600 get event id 1 */
    DListInit(&jiffyEventList);
    GetMultipleJiffyEventList(timerWheel, 1, &jiffyEventList);
    ASSERT_FALSE(DListIsEmpty(&jiffyEventList));
    ASSERT_EQ(GetTestEventId(DListPopHeadNode(&jiffyEventList)), 2);
    ASSERT_TRUE(DListIsEmpty(&jiffyEventList));
    /* Time 2610 has no event */
    DListInit(&jiffyEventList);
    GetMultipleJiffyEventList(timerWheel, 1, &jiffyEventList);
    ASSERT_TRUE(DListIsEmpty(&jiffyEventList));
}

TEST_F(TimerWheelTest, AddEventTest004)
{
    /* Add 1 event with timeout 300 jiffy */
    uint64_t targetTimeoutJiffy = 300;
    Event event;
    InitTestEvent(&event, targetTimeoutJiffy * TEST_JIFFY_TIME, 1);
    int testLoop = 3;
    DListHead jiffyEventList;
    while (testLoop--) {
        AddEvent(timerWheel, &event);
        /* First 299 jiffy has no event */
        DListInit(&jiffyEventList);
        GetMultipleJiffyEventList(timerWheel, targetTimeoutJiffy - 1, &jiffyEventList);
        ASSERT_TRUE(DListIsEmpty(&jiffyEventList));
        /* Time 300 jiffy get event id 1 */
        DListInit(&jiffyEventList);
        GetMultipleJiffyEventList(timerWheel, 1, &jiffyEventList);
        ASSERT_FALSE(DListIsEmpty(&jiffyEventList));
        ASSERT_EQ(GetTestEventId(DListPopHeadNode(&jiffyEventList)), 1);
        ASSERT_TRUE(DListIsEmpty(&jiffyEventList));
        /* Next jiffy has no event */
        DListInit(&jiffyEventList);
        GetMultipleJiffyEventList(timerWheel, 1, &jiffyEventList);
        ASSERT_TRUE(DListIsEmpty(&jiffyEventList));
    }
}

TEST_F(TimerWheelTest, AddEventTest005)
{
    /* Add 3 event with timeout 655360*1,655360*2,655360*3 */
    Event eventList[TEST_EVENT_COUNT];
    DListHead jiffyEventList;
    int testLoop = 3;
    while (testLoop--) {
        for (int i = 0; i < TEST_EVENT_COUNT; ++i) {
            InitTestEvent(&eventList[i], ((i + 1) * 256 * 256) * TEST_JIFFY_TIME, i);
            AddEvent(timerWheel, &eventList[i]);
        }
        for (int i = 0; i < TEST_EVENT_COUNT; ++i) {
            /* First 65535 jiffy has no event */
            DListInit(&jiffyEventList);
            GetMultipleJiffyEventList(timerWheel, 256 * 256 - 1, &jiffyEventList);
            ASSERT_TRUE(DListIsEmpty(&jiffyEventList));
            /* 65536 jiffy get 1 event of id i */
            DListInit(&jiffyEventList);
            GetMultipleJiffyEventList(timerWheel, 1, &jiffyEventList);
            ASSERT_FALSE(DListIsEmpty(&jiffyEventList));
            ASSERT_EQ(GetTestEventId(DListPopHeadNode(&jiffyEventList)), i);
            ASSERT_TRUE(DListIsEmpty(&jiffyEventList));
        }
        DListInit(&jiffyEventList);
        GetMultipleJiffyEventList(timerWheel, 1, &jiffyEventList);
        ASSERT_TRUE(DListIsEmpty(&jiffyEventList));
    }
}

TEST_F(TimerWheelTest, GetAndClearAllEvent001)
{
    /* Add 3 event with timeout 10,20,30 */
    Event eventList[TEST_EVENT_COUNT];
    for (int i = 0; i < TEST_EVENT_COUNT; ++i) {
        InitTestEvent(&eventList[i], (i + 1) * TEST_JIFFY_TIME, i);
        AddEvent(timerWheel, &eventList[i]);
    }
    /* First jiffy get event id 0 */
    DListHead jiffyEventList;
    DListInit(&jiffyEventList);
    GetMultipleJiffyEventList(timerWheel, 1, &jiffyEventList);
    ASSERT_EQ(GetTestEventId(DListPopHeadNode(&jiffyEventList)), 0);
    ASSERT_TRUE(DListIsEmpty(&jiffyEventList));
    /* Get and clear all remaining event, get event id 1,2 */
    DListHead remainEventList;
    DListInit(&remainEventList);
    GetAndClearAllEvent(timerWheel, &remainEventList);
    set<uint32_t> expectEventIdSet, actualEventIdSet;
    expectEventIdSet.insert(1);
    expectEventIdSet.insert(2);
    while (!DListIsEmpty(&remainEventList)) {
        uint32_t curEventId = GetTestEventId(DListPopHeadNode(&remainEventList));
        actualEventIdSet.insert(curEventId);
    }
    ASSERT_TRUE(CheckTwoSetEqual(expectEventIdSet, actualEventIdSet));
}
