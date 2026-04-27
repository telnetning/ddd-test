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
 * timer_wheel.h
 *
 * Description:
 * This file defines timer wheel operations for loop event
 *
 * ---------------------------------------------------------------------------------
 */
#ifndef UTILS_COMMON_TIMER_WHEEL_H
#define UTILS_COMMON_TIMER_WHEEL_H

#include "container/linked_list.h"
#include "defines/common.h"
#include "types/atomic_type.h"
#include "event/event_loop.h"

GSDB_BEGIN_C_CODE_DECLS

#define TIMER_ARRAY_LEN      256        /* 4 array can represent 2^32 time interval */
#define MAX_JIFFY_THRESHOLD  UINT32_MAX /* 2 ^ 32 - 1 */
#define WHEEL_MAX_LIST_COUNT 4

typedef struct TimerWheel TimerWheel;
struct TimerWheel {
    DListHead tv[WHEEL_MAX_LIST_COUNT][TIMER_ARRAY_LEN];
    uint32_t curIndexTv[WHEEL_MAX_LIST_COUNT];
    uint16_t jiffyTime;
};

/**
 * Initialize timer wheel instance
 * @param[in] timerWheel timer wheel instance
 * @param[in] jiffyTime time period in ms of each jiffy
 */
void InitTimerWheel(TimerWheel *timerWheel, uint16_t jiffyTime);

void AddEvent(TimerWheel *timerWheel, Event *event);

void PopEvent(__attribute__((unused)) TimerWheel *timerWheel, Event *event);

/**
 * Get next multiple jiffy event from timer wheel
 * @param[in] timerWheel timer wheel instance
 * @param[in] jiffyCount expect jiffy count for this call
 * @param[in,out] eventList DList use to store event, must be empty
 */
void GetMultipleJiffyEventList(TimerWheel *timerWheel, uint64_t jiffyCount, DListHead *eventList);

void GetAndClearAllEvent(TimerWheel *timerWheel, DListHead *eventList);

GSDB_END_C_CODE_DECLS

#endif /* UTILS_COMMON_TIMER_WHEEL_H */
