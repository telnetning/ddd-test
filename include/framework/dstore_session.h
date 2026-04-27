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
 * dstore_session.h
 *
 * IDENTIFICATION
 *        include/framework/dstore_session.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_SESSION_H
#define DSTORE_SESSION_H
#include "common/memory/dstore_mctx.h"

namespace DSTORE {
class StorageSession {
public:
    inline DstoreMemoryContext GetRoot()
    {
        return topMemCtx;
    }

    DstoreMemoryContext topMemCtx;
    uint64_t sessionId;
    class BufMgrInterface *tmpLocalBufMgr;
    class LockResource *lockRes;
};
}  /* namespace DSTORE */
#endif /* DSTORE_SESSION_H */