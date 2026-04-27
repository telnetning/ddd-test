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
 * port_errorcode.h
 *
 * Description:Defines the error code for the platform portable interface.
 *
 * ---------------------------------------------------------------------------------
 */
#ifndef UTILS_PORT_ERRORCODE_H
#define UTILS_PORT_ERRORCODE_H

#include "defines/common.h"
#include "defines/err_code.h"
#include "defines/utils_errorcode.h"

GSDB_BEGIN_C_CODE_DECLS

// clang-format off
/* Unknown OS error. */
#define ERROR_UTILS_PORT_UNKNOWN \
    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_PORT_MODULE_ID, 0x00000001)

/* Insufficient memory exists to initialize the os object. */
#define ERROR_UTILS_PORT_ENOMEM \
    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_PORT_MODULE_ID, 0x00000002)

/* The value specified by input parameter is invalid or the value specified for the parameter is outside the
 * range of legal values for that parameter */
#define ERROR_UTILS_PORT_EINVAL \
    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_PORT_MODULE_ID, 0x00000003)

/* The system lacked the necessary resources (other than memory) to initialize another os object. */
#define ERROR_UTILS_PORT_EAGAIN \
    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_PORT_MODULE_ID, 0x00000004)

/* The caller does not have the privilege to perform the operation. */
#define ERROR_UTILS_PORT_EPERM \
    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_PORT_MODULE_ID, 0x00000005)

/* The implementation has detected an attempt to reinitialize the object referenced by the input parameter (a
 * previously initialized, but not yet destroyed) or the implementation has detected an attempt to destroy
 * the object referenced by the parameter while it is locked or referenced by another thread. */
#define ERROR_UTILS_PORT_EBUSY \
    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_PORT_MODULE_ID, 0x00000006)

/* The current thread already owns the mutex. */
#define ERROR_UTILS_PORT_EDEADLK \
    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_PORT_MODULE_ID, 0x00000007)

/* The time specified by the parameter to GaussEventTimedWait() has passed. */
#define ERROR_UTILS_PORT_ETIMEDOUT \
    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_PORT_MODULE_ID, 0x00000008)

/* The option {_POSIX_THREAD_ATTR_STACKSIZE} is not defined and the stacksize attribute for threads is not supported. */
#define ERROR_UTILS_PORT_ENOSYS \
    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_PORT_MODULE_ID, 0x00000009)

/* No thread could be found corresponding to that specified by the given thread ID. */
#define ERROR_UTILS_PORT_ESRCH \
    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_PORT_MODULE_ID, 0x00000010)

/* Operation not support on the current platform. */
#define ERROR_UTILS_PORT_ENOTSUP \
    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_PORT_MODULE_ID, 0x00000011)

/* The length of the string exceeds the allowed limit or is too small. */
#define ERROR_UTILS_PORT_ERANGE \
    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_PORT_MODULE_ID, 0x00000012)

/* Directory does not exist, or name is an empty string. */
#define ERROR_UTILS_PORT_ENOENT \
    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_PORT_MODULE_ID, 0x00000013)

/* Name is not a directory. */
#define ERROR_UTILS_PORT_ENOTDIR \
    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_PORT_MODULE_ID, 0x00000014)

/* The tp points outside the accessible address space. */
#define ERROR_UTILS_PORT_EFAULT \
    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_PORT_MODULE_ID, 0x00000015)

/* The hot-pluggable device (like USB for example) represented by a dynamic clk_id has disappeared
 * after its character device was opened. */
#define ERROR_UTILS_PORT_ENODEV \
    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_PORT_MODULE_ID, 0x00000016)

/* Unknown time type. */
#define ERROR_UTILS_PORT_UNKNOWN_CLOCKTYPE \
    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_PORT_MODULE_ID, 0x00000017)

/* The object have already existed . */
#define ERROR_UTILS_PORT_EEXIST \
    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_PORT_MODULE_ID, 0x00000018)

/* The pipe or file no data to read . */
#define ERROR_UTILS_PORT_ENODATA \
    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_PORT_MODULE_ID, 0x00000019)

/* The error code is true. */
#define ERROR_UTILS_PORT_ETRUE \
    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_PORT_MODULE_ID, 0x00000020)

/* The error code is false. */
#define ERROR_UTILS_PORT_EFALSE \
    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_PORT_MODULE_ID, 0x00000021)

/* The error code is interrupted system call. */
#define ERROR_UTILS_PORT_EINTR \
    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_PORT_MODULE_ID, 0x00000022)

/* The error code is I/O error. */
#define ERROR_UTILS_PORT_EIO \
    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_PORT_MODULE_ID, 0x00000023)

/* The error code is no such device or address. */
#define ERROR_UTILS_PORT_ENXIO \
    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_PORT_MODULE_ID, 0x00000024)

/* The error code is argument list too long. */
#define ERROR_UTILS_PORT_E2BIG \
    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_PORT_MODULE_ID, 0x00000025)

/* The error code is exec format error. */
#define ERROR_UTILS_PORT_ENOEXEC \
    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_PORT_MODULE_ID, 0x00000026)

/* The error code is bad file number. */
#define ERROR_UTILS_PORT_EBADF \
    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_PORT_MODULE_ID, 0x00000027)

/* The error code is no child processes. */
#define ERROR_UTILS_PORT_ECHILD \
    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_PORT_MODULE_ID, 0x00000028)

/* The error code is permission denied. */
#define ERROR_UTILS_PORT_EACCES \
    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_PORT_MODULE_ID, 0x00000029)

/* The error code is block device required. */
#define ERROR_UTILS_PORT_ENOTBLK \
    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_PORT_MODULE_ID, 0x00000030)

/* The error code is cross-device link. */
#define ERROR_UTILS_PORT_EXDEV \
    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_PORT_MODULE_ID, 0x00000031)

/* The error code is is a directory. */
#define ERROR_UTILS_PORT_EISDIR \
    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_PORT_MODULE_ID, 0x00000032)

/* The error code is file table overflow. */
#define ERROR_UTILS_PORT_ENFILE \
    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_PORT_MODULE_ID, 0x00000033)

/* The error code is too many open files. */
#define ERROR_UTILS_PORT_EMFILE \
    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_PORT_MODULE_ID, 0x00000034)

/* The error code is not a typewriter. */
#define ERROR_UTILS_PORT_ENOTTY \
    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_PORT_MODULE_ID, 0x00000035)

/* The error code is text file busy. */
#define ERROR_UTILS_PORT_ETXTBSY \
    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_PORT_MODULE_ID, 0x00000036)

/* The error code is file too large. */
#define ERROR_UTILS_PORT_EFBIG \
    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_PORT_MODULE_ID, 0x00000037)

/* The error code is no space left on device. */
#define ERROR_UTILS_PORT_ENOSPC \
    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_PORT_MODULE_ID, 0x00000038)

/* The error code is illegal seek. */
#define ERROR_UTILS_PORT_ESPIPE \
    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_PORT_MODULE_ID, 0x00000039)

/* The error code is read-only file system. */
#define ERROR_UTILS_PORT_EROFS \
    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_PORT_MODULE_ID, 0x00000040)

/* The error code is too many links. */
#define ERROR_UTILS_PORT_EMLINK \
    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_PORT_MODULE_ID, 0x00000041)

/* The error code is broken pipe. */
#define ERROR_UTILS_PORT_EPIPE \
    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_PORT_MODULE_ID, 0x00000042)

/* The error code is math argument out of domain of func. */
#define ERROR_UTILS_PORT_EDOM \
    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_PORT_MODULE_ID, 0x00000043)

// clang-format on

GSDB_END_C_CODE_DECLS

#endif /* UTILS_PORT_ERRORCODE_H */
