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
 * port_syslog.h
 *
 * Description:Defines the syslog level for the platform portable interface.
 *
 * ---------------------------------------------------------------------------------
 */
#ifndef UTILS_PORT_SYSLOG_H
#define UTILS_PORT_SYSLOG_H

#include "defines/common.h"

GSDB_BEGIN_C_CODE_DECLS

/*
 * Define log message levels.The Linux system supports the following log levels:LOG_DEBUG,LOG_INFO,
 * LOG_NOTICE,LOG_WARNING,LOG_ERR,LOG_CRIT.The Windows system supports the following log levels:
 * EVENTLOG_INFORMATION_TYPE,EVENTLOG_WARNING_TYPE,EVENTLOG_ERROR_TYPE.The Linux definition is used
 * as the unified cross-platform log level definition.
 */

/*
 * Cross-platform log level definition
 */
#define EVENT_LOG_DEBUG   1
#define EVENT_LOG_INFO    2
#define EVENT_LOG_NOTICE  3
#define EVENT_LOG_WARNING 4
#define EVENT_LOG_ERROR   5
#define EVENT_LOG_CRIT    6

GSDB_END_C_CODE_DECLS

#endif /* UTILS_PORT_SYSLOG_H */
