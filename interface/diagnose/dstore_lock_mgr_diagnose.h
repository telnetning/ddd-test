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
 */

#ifndef DSTORE_LOCK_MGR_DIAGNOSE_H
#define DSTORE_LOCK_MGR_DIAGNOSE_H
#include <cstdint>
#include "common/dstore_common_utils.h"
#include "lock/dstore_lock_struct.h"

namespace DSTORE {
#pragma GCC visibility push(default)

constexpr int LOCKDESCLEN = 128;
constexpr int LOCKTAG_DIAGNOSE_MAX = 20;
struct LockTagContext {
    uint32_t lockTagType;
    uint32_t field1;
    uint32_t field2;
    uint32_t field3;
    uint32_t field4;
    uint32_t field5;
};

struct LockStatus {
    ThreadId tid;
    uint16_t lockTagType;
    uint32_t field1;
    uint32_t field2;
    uint32_t field3;
    uint32_t field4;
    uint32_t field5;
    unsigned char lockMode;
    bool isWaiting;
    uint32_t grantedCnt;
    char lockTagDescription[LOCKDESCLEN];
};

struct LockStatusContext {
    ThreadId tid;
    LockMode lockMode;
    unsigned int granted;
    bool isWaiting;
};


class LockMgrDiagnose {
public:
    /**
     * Get Lock waiting and holding information for each thread.
     */
    static LockStatus** GetLocksByThread(int& lockNum, ThreadId pid = 0);

    /**
     * free lock status array
     */
    static void FreeLockStatusArr(LockStatus **lockStatus, int lockNum);

    /*
     * Get Table Lock's weak lock and strong lock statistics.
     */
    static char *GetTableLockStatistics(void);

    /*
     * Reset Table Lock's weak lock and strong lock statistics.
     */
    static char *ResetTableLockStatistics(void);

    /*
     * Get lock queuing information through lock tag.
     */
    static char *GetLockByLockTag(const LockTagContext &context);

    /*
     * Get table lock queuing information through lock tag.
     */
    static char *GetTableLockByLockTag(const LockTagContext &context);

    /*
     * Get transaction lock information in lock mgr through xid.
     */
    static char *GetTrxInfoFromLockMgr(PdbId pdbId, uint64_t xid);

    /**
     * Get Lock manager status.
     */
    static char *GetLockMgrStatus(void);

    /**
     * Get lock tag description.
     */
    static char *DecodeLockTag(const LockTagContext &context);
    /**
     * Get Lock tag name by lock tag type.
    */
    static const char *GetLockTagName(uint16_t lock_tag);
};

#pragma GCC visibility pop
}  /* namespace DSTORE */
#endif  /* STORAGE_LOCK_MGR_DIAGNOSE_H */
