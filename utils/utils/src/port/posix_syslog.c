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
 * posix_syslog.c
 *
 * Description:
 * 1. Implementation of the POSIX syslog interface wrapper.
 *
 * ---------------------------------------------------------------------------------
 */
#include <syslog.h>
#include <stdbool.h>
#include <string.h>
#include <stdarg.h>
#include "port/posix_syslog.h"

/* Facility string to linux facility value mapping. */
typedef struct SyslogFacility SyslogFacility;
struct SyslogFacility {
    const char *name;
    int val;
};
static const SyslogFacility SYSLOG_FACILITY_OPTIONS[] = {
    {"local0", LOG_LOCAL0}, {"local1", LOG_LOCAL1}, {"local2", LOG_LOCAL2},
    {"local3", LOG_LOCAL3}, {"local4", LOG_LOCAL4}, {"local5", LOG_LOCAL5},
    {"local6", LOG_LOCAL6}, {"local7", LOG_LOCAL7}, {NULL, 0}};

/**
 * Open the event log.
 * @param ident :Event log source name. Used to distinguish different application logs.
 * @param facility: Specifies the program type that records logs.
 * @param log: Not used on linux.
 */
UTILS_EXPORT void OpenSyslog(const char *ident, const char *facility, Syslog *log)
{
    int syslogFacility = LOG_LOCAL0;
    int i;
    if (log->log) {
        return;
    }
    for (i = 0; SYSLOG_FACILITY_OPTIONS[i].name != NULL; i++) {
        if (strcmp(SYSLOG_FACILITY_OPTIONS[i].name, facility) == 0) {
            syslogFacility = SYSLOG_FACILITY_OPTIONS[i].val;
        }
    }
    openlog(ident, LOG_PID | LOG_NDELAY | LOG_NOWAIT, syslogFacility);
    log->log = true;
    return;
}

/**
 * Sending messages to system logs.The ability to set the maximum message size (maxMessageSize,configuration parameter)
 * for the program Rsyslog in version 6.3.4 of 2011 was added,the default value is set to 8096 bytes and keep the
 * same ever since.The size is adequate unless you are logging additional debug/crash output.
 * @param log : Not used on the linux.
 * @param level : Log Level.
 * @param format : An sprintf-style format string.
 */
UTILS_EXPORT void ReportSyslog(SYMBOL_UNUSED Syslog *log, int level, const char *format, ...)
{
    int eventLevel;
    switch (level) {
        case EVENT_LOG_DEBUG:
            eventLevel = LOG_DEBUG;
            break;
        case EVENT_LOG_INFO:
            eventLevel = LOG_INFO;
            break;
        case EVENT_LOG_NOTICE:
            eventLevel = LOG_NOTICE;
            break;
        case EVENT_LOG_WARNING:
            eventLevel = LOG_WARNING;
            break;
        case EVENT_LOG_ERROR:
            eventLevel = LOG_ERR;
            break;
        case EVENT_LOG_CRIT:
        default:
            eventLevel = LOG_CRIT;
            break;
    }
    va_list args;
    va_start(args, format);
    vsyslog(eventLevel, format, args);
    va_end(args);
}

/**
 * Close the event log.
 * @param log : Not used on the linux.
 */
UTILS_EXPORT void CloseSyslog(Syslog *log)
{
    log->log = false;
    closelog();
}

/**
 * Check whether the log have opened.
 * @param log : Syslog
 * @return
 */
UTILS_EXPORT bool IsSyslogOpen(Syslog *log)
{
    return log->log;
}
