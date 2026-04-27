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
 * win32_io.c
 *
 * Description:
 * 1. Implementation of the windows io interface wrapper
 *
 * ---------------------------------------------------------------------------------
 */

#include "port/win32_time.h"

/* Number of seconds since epoch. */
void Time(TimesSecondsSinceEpoch *time)
{
    (void)time(&(time->time));
}

/* Converts seconds to local time format. */
void LocalTime(TimesSecondsSinceEpoch *sourceTime, TimeFormatStructure *destFormatTime)
{
    (void)localtime_s(&(destFormatTime->formatTime), &(sourceTime->time));
    destFormatTime->useconds = 0;
}

/* Converts seconds to the GTM time format. */
void GmtTime(TimesSecondsSinceEpoch *sourceTime, TimeFormatStructure *destFormatTime)
{
    (void)gmtime_s(&(destFormatTime->formatTime), &(sourceTime->time));
    destFormatTime->useconds = 0;
}

/* Format the time as a string. */
Size FormatTime(char *strDest, size_t maxSize, const char *format, TimeFormatStructure *time)
{
    return strftime(strDest, maxSize, format, &(time->formatTime));
}

/**
 * Note: some broken versions only have 8 trailing zero's, the correct epoch has 9 trailing zero's.
 * This magic number is the number of 100 nanosecond intervals since January 1, 1601 (UTC)
 * until 00:00:00 January 1, 1970
 */
static const uint64_t EPOCH = ((uint64_t)116444736000000000ULL);
/*
 * FILETIME represents the number of 100-nanosecond intervals since
 * January 1, 1601 (UTC).
 */
#define FILETIME_UNITS_PER_SEC  10000000L
#define FILETIME_UNITS_PER_USEC 10

/* Get the current time.Time precision is microseconds. */
TimeValue GetCurrentTimeValue(void)
{
    TimeValue result;
    FILETIME file_time;
    ULARGE_INTEGER ularge;
    GetSystemTimeAsFileTime(&file_time);
    ularge.LowPart = file_time.dwLowDateTime;
    ularge.HighPart = file_time.dwHighDateTime;
    result.seconds = (long)((ularge.QuadPart - EPOCH) / FILETIME_UNITS_PER_SEC);
    result.useconds = (long)(((ularge.QuadPart - EPOCH) % FILETIME_UNITS_PER_SEC) / FILETIME_UNITS_PER_USEC);
    return result;
}

void LocalHighPrecisionTime(TimeValue *sourceTime, TimeFormatStructure *destFormatTime)
{
    (void)localtime_s(&(destFormatTime->formatTime), &(sourceTime->seconds));
    destFormatTime->useconds = sourceTime->useconds;
}

/* SleepEx sleep time precision is milliseconds. */
#define MICRO_PER_MILLI      1000
#define HALF_MICRO_PER_MILLI 500
void Usleep(long microseconds)
{
    SleepEx((microseconds < HALF_MICRO_PER_MILLI ? 1 : (microseconds + HALF_MICRO_PER_MILLI) / MICRO_PER_MILLI), FALSE);
}
