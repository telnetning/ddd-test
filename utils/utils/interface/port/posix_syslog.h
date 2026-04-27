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
 * posix_syslog.h
 *
 * Description:Defines the system log external interfaces wrapper for linux platform event log.
 *
 * ---------------------------------------------------------------------------------
 */
#ifndef UTILS_POSIX_SYSLOG_H
#define UTILS_POSIX_SYSLOG_H

#include "defines/common.h"
#include "port/port_syslog.h"

GSDB_BEGIN_C_CODE_DECLS

/* Event log wrapper. */
typedef struct Syslog Syslog;
struct Syslog {
    bool log; /* This field only indicates whether to initialize. */
};
/* Syslog static initialization. */
#define SYSLOG_INITIALIZER \
    {                      \
        false              \
    }

/**
 * Open the event log.
 * @param ident :Event log source name.Used to distinguish different application logs.
 * @param facility: Specifies the program type that records logs.
 * The facility valid values are from "local0" to "local7",the default value is "local0".
 * @param log: Not used on linux.
 */
void OpenSyslog(const char *ident, const char *facility, Syslog *log);
/**
 * Sending messages to system logs.
 * @param log : Not used on the linux.
 * @param level : Log Level.
 * The level valid values are from EVENT_LOG_DEBUG to EVENT_LOG_CRIT,the default value is EVENT_LOG_CRIT.
 * @param format : An sprintf-style format string.
 */
void ReportSyslog(Syslog *log, int level, const char *format, ...);
/**
 * Close the event log.
 * @param log : Not used on the linux.
 */
void CloseSyslog(Syslog *log);

/**
 * Check whether the log have opened.
 * @param log : Syslog
 * @return
 */
bool IsSyslogOpen(Syslog *log);

GSDB_END_C_CODE_DECLS
#endif /* UTILS_POSIX_SYSLOG_H */
