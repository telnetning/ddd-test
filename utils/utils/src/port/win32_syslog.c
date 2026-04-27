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
 * win32_syslog.c
 *
 * Description:
 * 1. Implementation of the windows syslog interface wrapper.
 *
 * ---------------------------------------------------------------------------------
 */

#include "port/win32_syslog.h"

/**
 * Open the event log.
 * @param ident :Event log source name.Used to distinguish different application logs.
 * @param facility: Not used on windows.
 * @param log: Event log handle.
 */
void OpenSyslog(const char *ident, const char *facility, Syslog *log)
{
    log->log = INVALID_HANDLE_VALUE;
    log->log = RegisterEventSource(NULL, ident);
    if (log->log == NULL) {
        log->log = INVALID_HANDLE_VALUE;
    }
    return;
}

#define MAX_SYS_LOG_BUFFER_SIZE 1024

/**
 * Send messages to system logs.
 * @param level : Log Level.
 * @param message : Log messages.
 * @param len : Log messages length.
 */
void ReportSyslog(Syslog *log, int level, const char *format, ...)
{
    int eventLevel = EVENTLOG_ERROR_TYPE;
    switch (level) {
        case EVENT_LOG_DEBUG:
        case EVENT_LOG_INFO:
        case EVENT_LOG_NOTICE:
            eventLevel = EVENTLOG_INFORMATION_TYPE;
            break;
        case EVENT_LOG_WARNING:
            eventLevel = EVENTLOG_WARNING_TYPE;
            break;
        case EVENT_LOG_ERROR:
        case EVENT_LOG_CRIT:
        default:
            eventLevel = EVENTLOG_ERROR_TYPE;
            break;
    }
    va_list args;
    va_start(args, format);
    char message[MAX_SYS_LOG_BUFFER_SIZE];
    int rc = vsprintf_s(message, MAX_SYS_LOG_BUFFER_SIZE, format, args);
    if (rc < 0) {
        va_end(args);
        return;
    }
    va_end(args);
    /* All events are Id 0. */
    ReportEventA(log->log, eventLevel, 0, 0, NULL, 1, 0, &message, NULL);
}

/**
 * Close the event log.
 * @param log : Event log handle.
 */
void CloseSyslog(Syslog *log)
{
    BOOL rc;
    rc = DeregisterEventSource(log->log);
    if (rc) {
        log->log = INVALID_HANDLE_VALUE;
    }
    return;
}

/**
 * Check whether the log have opened.
 * @param log : Syslog
 * @return
 */
bool IsSyslogOpen(Syslog *log)
{
    return log->log != INVALID_HANDLE_VALUE;
}
