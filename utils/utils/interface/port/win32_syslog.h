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
 * win32_syslog.h
 *
 * Description:Defines the system log external interfaces wrapper for windows platform event log.
 *
 * ---------------------------------------------------------------------------------
 */
#ifndef UTILS_WIN32_SYSLOG_H
#define UTILS_WIN32_SYSLOG_H

#include <windows.h>
#include <stdio.h>
#include <stdarg.h>
#include <stdio.h>
#include "win32_errcode.h"

GSDB_BEGIN_C_CODE_DECLS

/* Event log wrapper. */
typedef struct Syslog Syslog;
struct Syslog {
    HANDLE log;
};
/* Syslog static initialization. */
#define SYSLOG_INITIALIZER   \
    {                        \
        INVALID_HANDLE_VALUE \
    }

/**
 * Open the event log.
 * @param ident :Event log source name.Used to distinguish different application logs.
 * @param facility: Not used on windows.
 * @param log: Event log handle.
 */
void OpenSyslog(const char *ident, const char *facility, Syslog *log);
/**
 * Sending messages to system logs.
 * @param log : Event log handle.
 * @param level : Log Level.
 * @param format : An sprintf-style format string.
 */
void ReportSyslog(Syslog *log, int level, const char *format, ...);
/**
 * Close the event log.
 * @param log : Event log handle.
 */
void CloseSyslog(Syslog *log);

/**
 * Check whether the log have opened.
 * @param log : Syslog
 * @return
 */
bool IsSyslogOpen(Syslog *log);

GSDB_END_C_CODE_DECLS
#endif /* UTILS_WIN32_SYSLOG_H */
