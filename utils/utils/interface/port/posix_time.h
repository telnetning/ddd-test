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
 * posix_time.h
 *
 * Description:Defines the time external interfaces wrapper for linux platform time.
 *
 * ---------------------------------------------------------------------------------
 */

#ifndef UTILS_POSIX_TIME_H
#define UTILS_POSIX_TIME_H

#include <stdio.h>
#include <time.h>
#include "securec.h"
#include "defines/common.h"
#include "port/posix_errcode.h"
#include "types/data_types.h"

GSDB_BEGIN_C_CODE_DECLS

#define SECONDS_TO_MILLISECONDS     1000
#define MICROSECONDS_TO_NANOSECONDS 1000
#define MILLISECONDS_TO_NANOSECONDS 1000000
#define SECONDS_TO_NANOSECONDS      1000000000

typedef enum ClockType {
    /* Clock that cannot be set and represents monotonic time since some unspecified starting point.
     * This clock is not affected by discontinuous jumps in the system time,
     * but it does not increase when the system is suspended. */
    CLOCKTYPE_MONOTONIC = 0,
    /* Similar to CLOCK_MONOTONIC, but provides access to a raw hardware-based time
     * that is not subject to NTP adjustments. */
    CLOCKTYPE_MONOTONIC_RAW,
    /* Identical to CLOCK_MONOTONIC, except it also includes any time that the system is suspended. */
    CLOCKTYPE_BOOTTIME
} ClockType;

/* Time  wrapper. */
typedef struct TimesSecondsSinceEpoch TimesSecondsSinceEpoch;
struct TimesSecondsSinceEpoch {
    time_t timeSeconds;
};

typedef struct TimeFormatStructure TimeFormatStructure;
struct TimeFormatStructure {
    struct tm formatTime;
    long useconds;
};

/* Number of seconds since epoch. */
void Time(TimesSecondsSinceEpoch *timeSeconds);
/* Converts seconds to local time format. */
void LocalTime(TimesSecondsSinceEpoch *sourceTime, TimeFormatStructure *destFormatTime);
/* Converts seconds to the GTM time format. */
void GmtTime(TimesSecondsSinceEpoch *sourceTime, TimeFormatStructure *destFormatTime);
/* Format the time as a string. */
Size FormatTime(char *strDest, size_t maxSize, const char *format, TimeFormatStructure *time);

typedef struct TimeValue TimeValue;
struct TimeValue {
    time_t seconds;
    long useconds;
};
/* Get the current time.Time precision is microseconds. */
TimeValue GetCurrentTimeValue(void);
/* Converts high precision time to local time format */
void LocalHighPrecisionTime(TimeValue *sourceTime, TimeFormatStructure *destFormatTime);
/* Sleep microseconds. */
void Usleep(long microseconds);

/*
 * Convert milliseconds to absolute time.
 */
void ConvertMillisecondsToAbsoluteTime(time_t milliseconds, struct timespec *abstime);
/**
 * GetClockTimeValue - Get the time value of the time type specified by the input parameter.
 *
 * @param[in] timeType: specifies the time type.
 * @param[out] errorCode: ERROR_SYS_OK if the time is obtained successfully, or other result code if fail.
 * @return the time value.
 */
TimeValue GetClockValue(ClockType clockType, ErrorCode *errorCode);

#define BIGGEST(a, b) (((a) > (b)) ? (a) : (b))

struct Ttinfo {     /* time type information */
    long ttGmtoff; /* UTC offset in seconds */
    int ttIsdst;   /* used to set tm_isdst */
    int ttAbbrind; /* abbreviation list index */
    int ttTtisstd; /* TRUE if transition is std time */
    int ttTtisgmt; /* TRUE if transition is UTC */
};

struct Lsinfo {         /* leap second information */
    time_t lsTrans; /* transition time */
    long lsCorr;       /* correction to apply */
};

#define TZ_MAX_TIMES  1200
#define TZ_MAX_TYPES  256 /* Limited by what (unsigned char)'s can hold */
#define TZ_MAX_CHARS  50 /* Maximum number of abbreviation characters (limited by what unsigned chars can hold) */
#define TZ_MAX_LEAPS  50 /* Maximum number of leap second corrections */
#define TZ_STRLEN_MAX 255 /* Maximum length of a timezone name (not including trailing null) */

#define SECSPERMIN 60
#define MINSPERHOUR 60
#define HOURSPERDAY 24
#define DAYSPERWEEK 7
#define DAYSPERNYEAR 365
#define DAYSPERLYEAR 366
#define SECSPERHOUR (SECSPERMIN * MINSPERHOUR)
#define SECSPERDAY ((long)SECSPERHOUR * HOURSPERDAY)
#define MONSPERYEAR 12

#define TM_SUNDAY 0
#define TM_MONDAY 1
#define TM_TUESDAY 2
#define TM_WEDNESDAY 3
#define TM_THURSDAY 4
#define TM_FRIDAY 5
#define TM_SATURDAY 6

#define TM_JANUARY 0
#define TM_FEBRUARY 1
#define TM_MARCH 2
#define TM_APRIL 3
#define TM_MAY 4
#define TM_JUNE 5
#define TM_JULY 6
#define TM_AUGUST 7
#define TM_SEPTEMBER 8
#define TM_OCTOBER 9
#define TM_NOVEMBER 10
#define TM_DECEMBER 11

#define TM_YEAR_BASE 1900

#define EPOCH_YEAR 1970
#define EPOCH_WDAY TM_THURSDAY

struct State {
    int leapcnt;
    int timecnt;
    int typecnt;
    int charcnt;
    int goback;
    int goahead;
    time_t ats[TZ_MAX_TIMES];
    unsigned char types[TZ_MAX_TIMES];
    struct Ttinfo ttis[TZ_MAX_TYPES];
    char chars[BIGGEST(BIGGEST(TZ_MAX_CHARS + 1, 3 /* sizeof gmt */), (2 * (TZ_STRLEN_MAX + 1)))];
    struct Lsinfo lsis[TZ_MAX_LEAPS];
};

struct pg_tz {
    /* tZname contains the canonically-cased name of the timezone */
    char tZname[TZ_STRLEN_MAX + 1];
    struct State state;
};

#define FOUR_HUNDRED_YEAR_CYCLE_LENGTH 400
#define CENTURY_LENGTH 100
#define FOUR_YEAR_CYCLE_LENGTH 4
#define ISLEAP(y) (((y) % FOUR_YEAR_CYCLE_LENGTH) == 0 && \
                   (((y) % CENTURY_LENGTH) != 0 || ((y) % FOUR_HUNDRED_YEAR_CYCLE_LENGTH) == 0))

#ifndef YEARSPERREPEAT
#define YEARSPERREPEAT 400 /* years before a Gregorian repeat */
#endif                     /* !defined YEARSPERREPEAT */

/*
** The Gregorian year averages 365.2425 days, which is 31556952 seconds.
*/
#ifndef AVGSECSPERYEAR
#define AVGSECSPERYEAR 31556952L
#endif /* !defined AVGSECSPERYEAR */

struct tm* GetLocaltime(const time_t* timeptr, const struct pg_tz* tz);

extern THR_LOCAL struct pg_tz* log_timezone;

/* Fault injection related definitions */
enum FaultInjectionPosixTimePoint {
    MOCK_GET_TIME_ERROR,
};

GSDB_END_C_CODE_DECLS
#endif /* UTILS_POSIX_TIME_H */
