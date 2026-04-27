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

#include "event/timer_wheel.h"
#include "event/event_base.h"

#define BITS_PER_BYTE 8

static const uint32_t WHEEL_LIST_JIFFY_COUNT[WHEEL_MAX_LIST_COUNT] = {1, 1 << 8, 1 << 16, 1 << 24};
static const uint32_t WHEEL_LIST_INSERT_MASK[WHEEL_MAX_LIST_COUNT] = {0xFFU, 0xFFU << 8, 0xFFU << 16, 0xFFU << 24};
static const uint32_t WHEEL_LIST_DECREASE_MASK[WHEEL_MAX_LIST_COUNT] = {0, 0xFFU, 0xFFFFU, 0xFFFFFFU};

static inline DListHead *GetCurTimeoutEventList(TimerWheel *timerWheel)
{
    return &timerWheel->tv[0][timerWheel->curIndexTv[0]];
}

static void GetListAndIndexByTimeout(uint32_t timeout, uint32_t *listId, uint32_t *index)
{
    ASSERT(listId != NULL);
    ASSERT(index != NULL);
    for (uint32_t i = WHEEL_MAX_LIST_COUNT; i > 0; --i) {
        uint32_t curList = i - 1;
        if ((timeout & WHEEL_LIST_INSERT_MASK[curList]) != 0) {
            *listId = curList;
            *index = (timeout >> (curList * BITS_PER_BYTE)) % TIMER_ARRAY_LEN;
            return;
        }
    }
    *listId = *index = 0;
}

static void AddEventInternal(TimerWheel *timerWheel, Event *event)
{
    uint32_t listId, index;
    GetListAndIndexByTimeout(event->timeoutJiffy, &listId, &index);
    ASSERT(listId < WHEEL_MAX_LIST_COUNT);
    ASSERT(index < TIMER_ARRAY_LEN);
    for (uint32_t i = 0; i < WHEEL_MAX_LIST_COUNT; ++i) {
        event->addWheelIndex[i] = timerWheel->curIndexTv[i];
    }
    uint32_t targetIndex = (index + timerWheel->curIndexTv[listId]) % TIMER_ARRAY_LEN;
    DListPushTail(&timerWheel->tv[listId][targetIndex], &event->attachedNode);
}

static void PushForwardTimerWheel(TimerWheel *timerWheel, uint32_t level)
{
    ASSERT(level >= 1);
    ASSERT(level < WHEEL_MAX_LIST_COUNT);
    timerWheel->curIndexTv[level] = (timerWheel->curIndexTv[level] + 1) % TIMER_ARRAY_LEN;
    DListHead *curEventList = &timerWheel->tv[level][timerWheel->curIndexTv[level]];
    while (!DListIsEmpty(curEventList)) {
        DListNode *curNode = DListPopHeadNode(curEventList);
        Event *curEvent = DLIST_CONTAINER(Event, attachedNode, curNode);
        curEvent->timeoutJiffy &= WHEEL_LIST_DECREASE_MASK[level];
        for (uint32_t i = 0; i < level; ++i) {
            curEvent->timeoutJiffy = curEvent->timeoutJiffy + curEvent->addWheelIndex[i] * WHEEL_LIST_JIFFY_COUNT[i];
        }
        AddEventInternal(timerWheel, curEvent);
    }
    if (level < WHEEL_MAX_LIST_COUNT - 1 && timerWheel->curIndexTv[level] == 0) {
        PushForwardTimerWheel(timerWheel, level + 1);
    }
}

void InitTimerWheel(TimerWheel *timerWheel, uint16_t jiffyTime)
{
    ASSERT(timerWheel != NULL);
    ASSERT(jiffyTime > 0);
    for (int i = 0; i < WHEEL_MAX_LIST_COUNT; ++i) {
        for (int j = 0; j < TIMER_ARRAY_LEN; ++j) {
            DListInit(&timerWheel->tv[i][j]);
        }
        timerWheel->curIndexTv[i] = 0;
    }
    timerWheel->jiffyTime = jiffyTime;
}

void AddEvent(TimerWheel *timerWheel, Event *event)
{
    ASSERT(timerWheel != NULL);
    ASSERT(event != NULL);
    if (event->timeout == 0) {
        event->timeoutJiffy = MAX_JIFFY_THRESHOLD;
    } else if (event->timeout < timerWheel->jiffyTime) {
        event->timeoutJiffy = 1;
    } else {
        event->timeoutJiffy = event->timeout / timerWheel->jiffyTime;
    }
    AddEventInternal(timerWheel, event);
}

void PopEvent(__attribute__((unused)) TimerWheel *timerWheel, Event *event)
{
    ASSERT(event != NULL);
    DListDelete(&event->attachedNode);
}

static void GetNextJiffyEventList(TimerWheel *timerWheel, DListHead *eventList)
{
    timerWheel->curIndexTv[0] = (timerWheel->curIndexTv[0] + 1) % TIMER_ARRAY_LEN;
    if (timerWheel->curIndexTv[0] == 0) {
        /* push forward next level wheel */
        PushForwardTimerWheel(timerWheel, 1);
    }
    DListHead *curEventList = GetCurTimeoutEventList(timerWheel);
    DListAppendMove(eventList, curEventList);
}

void GetMultipleJiffyEventList(TimerWheel *timerWheel, uint64_t jiffyCount, DListHead *eventList)
{
    ASSERT(timerWheel != NULL);
    ASSERT(eventList != NULL);
    ASSERT(DListIsEmpty(eventList));
    ASSERT(jiffyCount > 0);
    for (uint64_t i = 0; i < jiffyCount; ++i) {
        GetNextJiffyEventList(timerWheel, eventList);
    }
}

void GetAndClearAllEvent(TimerWheel *timerWheel, DListHead *eventList)
{
    ASSERT(timerWheel != NULL);
    ASSERT(eventList != NULL);
    ASSERT(DListIsEmpty(eventList));
    for (int i = 0; i < WHEEL_MAX_LIST_COUNT; ++i) {
        for (int j = 0; j < TIMER_ARRAY_LEN; ++j) {
            DListHead *curList = &timerWheel->tv[i][j];
            while (!DListIsEmpty(curList)) {
                DListNode *curNode = DListPopHeadNode(curList);
                DListPushTail(eventList, curNode);
            }
        }
    }
}
