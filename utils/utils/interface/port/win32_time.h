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
 * win32_time.h
 *
 * Description:Defines the io external interfaces wrapper for windows platform file io.
 *
 * ---------------------------------------------------------------------------------
 */

#ifndef UTILS_WIN32_TIME_H
#define UTILS_WIN32_TIME_H

#include <fcntl.h>
#include <io.h>
#include <synchapi.h>

GSDB_BEGIN_C_CODE_DECLS

/* Time  wrapper. */
typedef struct TimesSecondsSinceEpoch TimesSecondsSinceEpoch;
struct TimesSecondsSinceEpoch {
    time_t time;
};

typedef struct TimeFormatStructure TimeFormatStructure;
struct TimeFormatStructure {
    struct tm formatTime;
    long useconds;
};

/* Number of seconds since epoch. */
void Time(TimesSecondsSinceEpoch *time);
/* Converts seconds to local time format. */
void LocalTime(TimesSecondsSinceEpoch *sourceTime, TimeFormatStructure *destFormatTime);
/* Converts seconds to the GTM time format. */
void GmtTime(TimesSecondsSinceEpoch *sourceTime, TimeFormatStructure *destFormatTime);

/* Format the time as a string. */
Size FormatTime(char *strDest, size_t maxSize, const char *format, TimeFormatStructure *time);

typedef struct TimeValue TimeValue;
struct TimeValue {
    long seconds;
    long useconds;
};

/* Get the current time.Time precision is microseconds. */
TimeValue GetCurrentTimeValue(void);
/* Converts high precision time to local time format */
void LocalHighPrecisionTime(TimeValue *sourceTime, TimeFormatStructure *destFormatTime);

/* Sleep microseconds. */
void Usleep(long microseconds);

GSDB_END_C_CODE_DECLS
#endif /* UTILS_WIN32_TIME_H */
