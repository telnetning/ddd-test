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
 * posix_time.c
 *
 * Description:
 * 1. Implementation of the linux time interface wrapper.
 *
 * ---------------------------------------------------------------------------------
 */

#include <sys/time.h>
#include "fault_injection/fault_injection.h"
#include "port/posix_time.h"

/* Number of seconds since epoch. */
UTILS_EXPORT void Time(TimesSecondsSinceEpoch *timeSeconds)
{
    (void)time(&(timeSeconds->timeSeconds));
}

/* Converts seconds to local time format. */
UTILS_EXPORT void LocalTime(TimesSecondsSinceEpoch *sourceTime, TimeFormatStructure *destFormatTime)
{
    (void)localtime_r(&(sourceTime->timeSeconds), &(destFormatTime->formatTime));
    destFormatTime->useconds = 0;
}

/* Converts seconds to the GTM time format. */
UTILS_EXPORT void GmtTime(TimesSecondsSinceEpoch *sourceTime, TimeFormatStructure *destFormatTime)
{
    (void)gmtime_r(&(sourceTime->timeSeconds), &(destFormatTime->formatTime));
    destFormatTime->useconds = 0;
}

/* Format the time as a string. */
UTILS_EXPORT Size FormatTime(char *strDest, size_t maxSize, const char *format, TimeFormatStructure *time)
{
    return strftime(strDest, maxSize, format, &(time->formatTime));
}

/* Get the current time.Time precision is microseconds. */
UTILS_EXPORT TimeValue GetCurrentTimeValue(void)
{
    TimeValue result;
    struct timeval tv;
    /*
     * This is required on alpha, there the timeval structs are int's not longs and a
     * cast only would fail horribly.
     */
    if (unlikely(gettimeofday(&tv, NULL) != 0)) {
        result.seconds = result.useconds = 0;
    } else {
        result.seconds = tv.tv_sec;
        result.useconds = tv.tv_usec;
    }
    return result;
}

UTILS_EXPORT void LocalHighPrecisionTime(TimeValue *sourceTime, TimeFormatStructure *destFormatTime)
{
    (void)localtime_r(&(sourceTime->seconds), &(destFormatTime->formatTime));
    destFormatTime->useconds = sourceTime->useconds;
}

/*
 * Sleep microseconds.
 * 4.3BSD, POSIX.1-2001.POSIX.1-2001 declares this function obsolete; use nanosleep(2) instead.
 * POSIX.1-2008 removes the specification of usleep().
 */
#define USECOND_PER_SECOND           1000000
#define NANOSECONDS_PER_MICROSECONDS 1000

UTILS_EXPORT void Usleep(long microseconds)
{
    struct timespec request;
    struct timespec remaining;
    request.tv_sec = microseconds / USECOND_PER_SECOND;
    request.tv_nsec = NANOSECONDS_PER_MICROSECONDS * (microseconds % USECOND_PER_SECOND);
    while (nanosleep(&request, &remaining) == -1 && errno == EINTR) {
        request = remaining;
    }
}

UTILS_EXPORT TimeValue GetClockValue(ClockType clockType, ErrorCode *errorCode)
{
    int rc;
    TimeValue result = {0};
    struct timespec tp;
    switch (clockType) {
        case CLOCKTYPE_BOOTTIME:
            rc = clock_gettime(CLOCK_BOOTTIME, &tp);
            FAULT_INJECTION_ACTION(MOCK_GET_TIME_ERROR, (rc = -1));
            if (rc != 0) {
                if (errorCode != NULL) {
                    PosixErrorCode2PortErrorCode(rc, errorCode);
                }
                return result;
            }
            break;
        case CLOCKTYPE_MONOTONIC:
            rc = clock_gettime(CLOCK_MONOTONIC, &tp);
            FAULT_INJECTION_ACTION(MOCK_GET_TIME_ERROR, (rc = -1));
            if (rc != 0) {
                if (errorCode != NULL) {
                    PosixErrorCode2PortErrorCode(rc, errorCode);
                }
                return result;
            }
            break;
        case CLOCKTYPE_MONOTONIC_RAW:
            rc = clock_gettime(CLOCK_MONOTONIC_RAW, &tp);
            FAULT_INJECTION_ACTION(MOCK_GET_TIME_ERROR, (rc = -1));
            if (rc != 0) {
                if (errorCode != NULL) {
                    PosixErrorCode2PortErrorCode(rc, errorCode);
                }
                return result;
            }
            break;
        default:
            if (errorCode != NULL) {
                *errorCode = ERROR_UTILS_PORT_UNKNOWN_CLOCKTYPE;
            }
            return result;
    }
    result.seconds = tp.tv_sec;
    result.useconds = tp.tv_nsec / NANOSECONDS_PER_MICROSECONDS;
    if (errorCode != NULL) {
        *errorCode = ERROR_SYS_OK;
    }
    return result;
}

/*
 * Convert milliseconds to absolute time.
 */
void ConvertMillisecondsToAbsoluteTime(time_t milliseconds, struct timespec *abstime)
{
#ifdef PTHREAD_GET_EXPIRATION_NP
    struct timespec reltime;
    reltime.tv_sec = milliseconds / SECONDS_TO_MILLISECONDS;
    reltime.tv_nsec = milliseconds % SECONDS_TO_MILLISECONDS * MILLISECONDS_TO_NANOSECONDS;
    pthread_get_expiration_np(&reltime, abstime);
#else
    struct timeval curtime;
    (void)gettimeofday(&curtime, NULL);
    abstime->tv_sec = curtime.tv_sec + milliseconds / SECONDS_TO_MILLISECONDS;
    abstime->tv_nsec = curtime.tv_usec * MICROSECONDS_TO_NANOSECONDS +
                       milliseconds % SECONDS_TO_MILLISECONDS * MILLISECONDS_TO_NANOSECONDS;
    if (abstime->tv_nsec >= SECONDS_TO_NANOSECONDS) {
        abstime->tv_nsec -= SECONDS_TO_NANOSECONDS;
        abstime->tv_sec += 1;
    }
#endif
}

/*
 * Return the number of leap years through the end of the given year
 * where, to make the math easy, the answer for year zero is defined as zero.
 */
static int leaps_thru_end_of(const int y)
{
    return (y >= 0) ? (y / FOUR_YEAR_CYCLE_LENGTH - y / CENTURY_LENGTH + y / FOUR_HUNDRED_YEAR_CYCLE_LENGTH) :
                      -(leaps_thru_end_of(-(y + 1)) + 1);
}

/*
 * Simplified normalize logic courtesy Paul Eggert.
 */
static int increment_overflow(int* number, int delta)
{
    int number0;

    number0 = *number;
    *number += delta;
    return (*number < number0) != (delta < 0);
}

static const int mon_lengths[2][MONSPERYEAR] = {
    {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}, {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}};

static const int YEAR_LENGTHS[2] = {DAYSPERNYEAR, DAYSPERLYEAR};

static struct tm* Timesub(const time_t* timep, long offset, const struct State* sp, struct tm* tmp)
{
    const struct Lsinfo* lp = NULL;
    time_t tdays;
    int idays; /* unsigned would be so 2003 */
    long rem;
    int y;
    const int* ip = NULL;
    long corr;
    int hit;
    int i;

    corr = 0;
    hit = 0;
    i = sp->leapcnt;
    while (--i >= 0) {
        lp = &sp->lsis[i];
        if (*timep >= lp->lsTrans) {
            if (*timep == lp->lsTrans) {
                if (i == 0) {
                    hit = lp->lsCorr > 0;
                } else {
                    hit = lp->lsCorr > sp->lsis[i - 1].lsCorr;
                }

                if (hit) {
                    while (i > 0 && sp->lsis[i].lsTrans == sp->lsis[i - 1].lsTrans + 1 &&
                           sp->lsis[i].lsCorr == sp->lsis[i - 1].lsCorr + 1) {
                        ++hit;
                        --i;
                    }
                }
            }
            corr = lp->lsCorr;
            break;
        }
    }
    y = EPOCH_YEAR;
    tdays = *timep / SECSPERDAY;
    rem = *timep - tdays * SECSPERDAY;
    while (tdays < 0 || tdays >= YEAR_LENGTHS[ISLEAP(y)]) {
        int newy;
        time_t tdelta;
        int idelta;
        int leapdays;

        tdelta = tdays / DAYSPERLYEAR;
        idelta = (int)tdelta;
        if (tdelta - idelta >= 1 || idelta - tdelta >= 1) {
            return NULL;
        }
        if (idelta == 0) {
            idelta = (tdays < 0) ? -1 : 1;
        }
        newy = y;
        if (increment_overflow(&newy, idelta)) {
            return NULL;
        }
        leapdays = leaps_thru_end_of(newy - 1) - leaps_thru_end_of(y - 1);
        tdays -= ((time_t)newy - y) * DAYSPERNYEAR;
        tdays -= leapdays;
        y = newy;
    }

    /*
     * Given the range, we can now fearlessly cast...
     */
    idays = (int)tdays;
    rem += offset - corr;
    while (rem < 0) {
        rem += SECSPERDAY;
        --idays;
    }
    while (rem >= SECSPERDAY) {
        rem -= SECSPERDAY;
        ++idays;
    }
    while (idays < 0) {
        if (increment_overflow(&y, -1)) {
            return NULL;
        }
        idays += YEAR_LENGTHS[ISLEAP(y)];
    }
    while (idays >= YEAR_LENGTHS[ISLEAP(y)]) {
        idays -= YEAR_LENGTHS[ISLEAP(y)];
        if (increment_overflow(&y, 1)) {
            return NULL;
        }
    }
    tmp->tm_year = y;
    if (increment_overflow(&tmp->tm_year, -TM_YEAR_BASE)) {
        return NULL;
    }
    tmp->tm_yday = idays;

    /*
     * The "extra" mods below avoid overflow problems.
     */
    tmp->tm_wday = EPOCH_WDAY + ((y - EPOCH_YEAR) % DAYSPERWEEK) * (DAYSPERNYEAR % DAYSPERWEEK) +
                   leaps_thru_end_of(y - 1) - leaps_thru_end_of(EPOCH_YEAR - 1) + idays;
    tmp->tm_wday %= DAYSPERWEEK;
    if (tmp->tm_wday < 0) {
        tmp->tm_wday += DAYSPERWEEK;
    }
    tmp->tm_hour = (int)(rem / SECSPERHOUR);
    rem %= SECSPERHOUR;
    tmp->tm_min = (int)(rem / SECSPERMIN);

    /*
     * A positive leap second requires a special representation. This uses
     * "... ??:59:60" et seq.
     */
    tmp->tm_sec = (int)(rem % SECSPERMIN) + hit;
    ip = mon_lengths[ISLEAP(y)];
    for (tmp->tm_mon = 0; idays >= ip[tmp->tm_mon]; ++(tmp->tm_mon)) {
        idays -= ip[tmp->tm_mon];
    }
    tmp->tm_mday = (int)(idays + 1);
    tmp->tm_isdst = 0;
    tmp->tm_gmtoff = offset;
    return tmp;
}

static struct tm* Localsub(const time_t* timep, long offset, struct tm* tmp, const struct pg_tz* tz)
{
    const struct State* sp = NULL;
    const struct Ttinfo* ttisp = NULL;
    int i;
    struct tm* result = NULL;
    const time_t t = *timep;

    sp = &tz->state;
    if ((sp->goback && t < sp->ats[0]) || (sp->goahead && t > sp->ats[sp->timecnt - 1])) {
        time_t newt = t;
        time_t seconds;
        time_t tcycles;
        int64_t icycles;

        if (t < sp->ats[0]) {
            seconds = sp->ats[0] - t;
        } else {
            seconds = t - sp->ats[sp->timecnt - 1];
        }
        --seconds;
        tcycles = seconds / YEARSPERREPEAT / AVGSECSPERYEAR;
        ++tcycles;
        icycles = tcycles;
        if (tcycles - icycles >= 1 || icycles - tcycles >= 1) {
            return NULL;
        }
        seconds = icycles;
        seconds *= YEARSPERREPEAT;
        seconds *= AVGSECSPERYEAR;
        if (t < sp->ats[0]) {
            newt += seconds;
        } else {
            newt -= seconds;
        }
        if (newt < sp->ats[0] || newt > sp->ats[sp->timecnt - 1]) {
            return NULL; /* "cannot happen" */
        }
        result = Localsub(&newt, offset, tmp, tz);
        if (result == tmp) {
            time_t newy;

            newy = tmp->tm_year;
            if (t < sp->ats[0]) {
                newy -= icycles * YEARSPERREPEAT;
            } else {
                newy += icycles * YEARSPERREPEAT;
            }
            tmp->tm_year = (int)newy;
            if (tmp->tm_year != newy) {
                return NULL;
            }
        }
        return result;
    }
    if (sp->timecnt == 0 || t < sp->ats[0]) {
        i = 0;
        while (sp->ttis[i].ttIsdst) {
            if (++i >= sp->typecnt) {
                i = 0;
                break;
            }
        }
    } else {
        int lo = 1;
        int hi = sp->timecnt;

        while (lo < hi) {
            int mid = (lo + hi) >> 1;

            if (t < sp->ats[mid]) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        i = (int)sp->types[lo - 1];
    }
    ttisp = &sp->ttis[i];

    result = Timesub(&t, ttisp->ttGmtoff, sp, tmp);
    tmp->tm_isdst = ttisp->ttIsdst;
    tmp->tm_zone = &sp->chars[ttisp->ttAbbrind];
    return result;
}

THR_LOCAL struct pg_tz* log_timezone = NULL;

static THR_LOCAL struct tm tm;

struct tm* GetLocaltime(const time_t* timeptr, const struct pg_tz* tz)
{
    return Localsub(timeptr, 0L, &tm, tz);
}