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
 * port_signal.h
 *
 * Description:Defines the signal for the platform portable interface.
 *
 * ---------------------------------------------------------------------------------
 */
#ifndef UTILS_PORT_SIGNAL_H
#define UTILS_PORT_SIGNAL_H

#include "defines/common.h"

GSDB_BEGIN_C_CODE_DECLS

/* Linux supports both unreliable signals and reliable signals. The value range of unreliable
 * signals is 1 to 31, and the value range of reliable signals is 32 to 64.
 * The definition below includes only unreliable signals.
 * To facilitate signal value conversion, the defined signal value is the same as the Linux signal value.
 */

#define SIG_HUP    1  /* SIGHUP. */
#define SIG_INT    2  /* SIGINT. */
#define SIG_QUIT   3  /* SIGQUIT. */
#define SIG_ILL    4  /* SIGILL. */
#define SIG_TRAP   5  /* SIGTRAP. */
#define SIG_ABRT   6  /* SIGABRT. */
#define SIG_BUS    7  /* SIGBUS. */
#define SIG_FPE    8  /* SIGFPE. */
#define SIG_KILL   9  /* SIGKILL. */
#define SIG_USR1   10 /* SIGUSR1. */
#define SIG_SEGV   11 /* SIGSEGV. */
#define SIG_USR2   12 /* SIGUSR2. */
#define SIG_PIPE   13 /* SIGPIPE. */
#define SIG_ALRM   14 /* SIGALRM. */
#define SIG_TERM   15 /* SIGTERM. */
#define SIG_STKFLT 16 /* SIGSTKFLT. */
#define SIG_CHLD   17 /* SIGCHLD. */
#define SIG_CONT   18 /* SIGCONT. */
#define SIG_STOP   19 /* SIGSTOP. */
#define SIG_TSTP   20 /* SIGTSTP. */
#define SIG_TTIN   21 /* SIGTTIN. */
#define SIG_TTOU   22 /* SIGTTOU. */
#define SIG_URG    23 /* SIGURG. */
#define SIG_XCPU   24 /* SIGXCPU. */
#define SIG_XFSZ   25 /* SIGXFSZ. */
#define SIG_VTALRM 26 /* SIGVTALRM. */
#define SIG_PROF   27 /* SIGPROF. */
#define SIG_WINCH  28 /* SIGWINCH. */
#define SIG_IO     29 /* SIGIO. */
#define SIG_PWR    30 /* SIGPWR. */
#define SIG_SYS    31 /* SIGSYS. */

/* For ease of use, the maximum signal value is defined as 32. */
#define MAX_SIG_NUM     32
#define INVALID_SIG_NUM MAX_SIG_NUM

typedef void (*SignalFunction)(int portSignalNo);
#define SIG_FUNC_DFL ((SignalFunction)0)
#define SIG_FUNC_ERR ((SignalFunction)-1)
#define SIG_FUNC_IGN ((SignalFunction)1)

typedef void (*SignalMessageFunction)(int portSignalNo, void *signalMessage, size_t messageSize);
#define SIG_MESSAGE_FUNC_DFL ((SignalMessageFunction)0)
#define SIG_MESSAGE_FUNC_ERR ((SignalMessageFunction)-1)
#define SIG_MESSAGE_FUNC_IGN ((SignalMessageFunction)1)

GSDB_END_C_CODE_DECLS
#endif /* UTILS_PORT_SIGNAL_H */
