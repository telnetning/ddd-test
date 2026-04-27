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
 * -----------------------------------------------------------------------------------
 *
 * event_timer.c
 *
 * Description:
 * 1. implementation of event for timer.
 *
 * -----------------------------------------------------------------------------------
 */

#include "event/event_timer.h"

static ErrorCode TimerEventInit(SYMBOL_UNUSED TimerEvent *self, SYMBOL_UNUSED TypeInitParams *initData)
{
    ASSERT(self != NULL);
    return ERROR_SYS_OK;
}

static void TimerEventFinalize(TimerEvent *self)
{
    ASSERT(self != NULL);
    CallEventCallBackFinalize(UP_TYPE_CAST(self, Event));
}

static void TimerEventOpsInit(SYMBOL_UNUSED TimerEventOps *self)
{
    ASSERT(self != NULL);
}

DEFINE_NEW_TYPED_CLASS(TimerEvent, Event)
