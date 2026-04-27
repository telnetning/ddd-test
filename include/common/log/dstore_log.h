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
 * ---------------------------------------------------------------------------------------
 *
 * dstore_log.h
 *
 *
 * IDENTIFICATION
 *        include/common/log/dstore_log.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_GAUSSKERNEL_DSTORE_INCLUDE_COMMON_LOG_DSTORE_LOG_H_
#define SRC_GAUSSKERNEL_DSTORE_INCLUDE_COMMON_LOG_DSTORE_LOG_H_

#include <cstdarg>
#include "framework/dstore_modules.h"
#include "syslog/err_log.h"

namespace DSTORE {

/* Error level codes */
typedef enum ErrLevel {
    DSTORE_DEBUG5    = DEBUG,      /* Debugging messages, in categories of decreasing detail. */
    DSTORE_DEBUG4    = DEBUG,
    DSTORE_DEBUG3    = DEBUG,
    DSTORE_DEBUG2    = DEBUG,
    DSTORE_DEBUG1    = DEBUG,      /* used by GUC debug_* variables */
    DSTORE_LOG       = LOG,      /* Server operational messages; sent only to server log by default. */
    DSTORE_COMMERROR = LOG,      /* Client communication problems; same as LOG \
                                 * for server reporting, but never sent to client. */
    DSTORE_INFO      = INFO,      /* Messages specifically requested by user (eg VACUUM VERBOSE output); \
                                   * always sent to client regardless of client_min_messages, \
                                   * but by default not sent to server log. */
    DSTORE_NOTICE    = NOTICE,      /* Helpful messages to users about query    \
                                 * operation; sent to client and server log by default. */
    DSTORE_WARNING   = WARNING,      /* Warnings.  NOTICE is for expected messages \
                                 * like implicit sequence creation by SERIAL. \
                                 * WARNING is for unexpected messages. */
    DSTORE_ERROR     = ERROR,      /* user error - abort transaction; return to \
                            * known state */
    DSTORE_ERROR2    = ERROR,      /* user error - only send error message to the client */
    DSTORE_PANIC     = PANIC       /* take down the other backends with me */
} ErrLevel;

/* PANIC_DEBUG_ERROR_RELEASE will allow ErrLog() to panic on debug builds only. */
#ifdef DSTORE_USE_ASSERT_CHECKING
const ErrLevel PANIC_DEBUG_ERROR_RELEASE = ErrLevel::DSTORE_PANIC;
#else
const ErrLevel PANIC_DEBUG_ERROR_RELEASE = ErrLevel::DSTORE_ERROR;
#endif

/* max length of module name */
static constexpr int DSTORE_MODULE_NAME_MAXLEN = 32;

/* 1 bit <--> 1 module, not include MODULE_MAX. its size is (MODULE_MAX + 7) / 8 */
static constexpr int DSTORE_MODULE_BITMAP_SIZE = (MODULE_MAX + 7) / 8;

/* 1 byte --> 8 bit, so byte position is (_m/8) */
inline unsigned int GetModuleBitmapPos(ModuleId id)
{
    return ((unsigned int)id) >> 3;
}

/* mask is 2^x where x is in [0, 7] */
inline unsigned char GetModuleMask(ModuleId id)
{
    return (unsigned char)(1 << (((unsigned int)id) & 0x07));
}

/* map about module id and its name */
typedef struct ModuleData {
    ModuleId m_modId;
    const char m_modName[DSTORE_MODULE_NAME_MAXLEN];
} ModuleData;

#undef  LOCAL_COMPONENT_NAME
#define LOCAL_COMPONENT_NAME "DSTORE"

#undef ErrLog
#define ErrLog(elevel, moduleId, ...) \
    do { \
        if (IS_ERR_LEVEL_PASSED(static_cast<int>(elevel))) { \
            if (ErrStart(static_cast<int>(elevel), ErrStartPosition(__FILE__, __LINE__, FUNCNAME_MACRO), \
                ErrStartModule(LOCAL_COMPONENT_NAME, GetValidModuleName(moduleId), \
                               LOCAL_COMPONENT_ID, static_cast<int>(moduleId), static_cast<int>(elevel)))) { \
                __VA_ARGS__, ErrFinish(); \
            } \
        } \
    } while (0)

bool IsLogStarted();
void SetLogLevelAndFoldParam(int logLevel, int foldPeriod, int foldThreshold, int foldLevel);
void InitLogAdapterInstance(int logLevel, const char* logDir, int foldPeriod, int foldThreshold, int foldLevel);
void StopLogAdapterInstance();
void OpenLoggerThread();
void CloseLoggerThread();

const char *GetValidModuleName(ModuleId moduleId);
const char *GetValidModuleNameForTool(ModuleId moduleId);
void ModuleLoggingInit(bool turnOn);
void EnableModuleLogging(ModuleId moduleId);
void DisableModuleLogging(ModuleId moduleId);
bool ModuleLoggingIsOn(ModuleId moduleId);
}

#endif /* SRC_GAUSSKERNEL_DSTORE_INCLUDE_COMMON_LOG_STORAGE_LOG_H_ */
