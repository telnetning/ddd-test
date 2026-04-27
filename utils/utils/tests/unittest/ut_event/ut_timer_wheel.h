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

#ifndef UTILS_COMMON_UT_TIMER_WHEEL_H
#define UTILS_COMMON_UT_TIMER_WHEEL_H

#include <vector>
#include <set>

#include <gtest/gtest.h>
#include "event/event_base.h"
#include "event/timer_wheel.h"

constexpr uint16_t TEST_JIFFY_TIME = 10;

class TimerWheelTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        timerWheel = &timerWheelInstance;
        InitTimerWheel(timerWheel, TEST_JIFFY_TIME);
    }

    void TearDown() override
    {
        timerWheel = nullptr;
    }

    static void InitTestEvent(Event *event, uint32_t timeout, uint32_t eventId)
    {
        DListNodeInit(&event->attachedNode);
        event->flags = eventId; /* Using this variable to store event id in unittest */
        event->timeout = timeout;
    }

    static uint32_t GetTestEventId(DListNode *eventNode)
    {
        Event *curEvent = DLIST_CONTAINER(Event, attachedNode, eventNode);
        return curEvent->flags;
    }

    static bool CheckTwoSetEqual(const std::set<uint32_t> &set1, const std::set<uint32_t> &set2);

    TimerWheel *timerWheel = nullptr;
private:
    TimerWheel timerWheelInstance;
};

#endif /* UTILS_COMMON_UT_TIMER_WHEEL_H */
