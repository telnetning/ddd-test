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
 * dstore_session_interface.cpp
 *
 * IDENTIFICATION
 *        src/framework/dstore_session_interface.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "framework/dstore_session_interface.h"
#include "framework/dstore_session.h"
#include "framework/dstore_pdb.h"
#include "framework/dstore_instance.h"
#include "common/memory/dstore_mctx.h"
#include "framework/dstore_modules.h"
#include "common/log/dstore_log.h"
#include "buffer/dstore_buf_mgr_temporary.h"
#include "tablespace/dstore_tablespace.h"
#include "transaction/dstore_resowner.h"

namespace DSTORE {

StorageSession *CreateStorageSession(uint64_t sessionId, int32 bufNums)
{
    RetStatus ret = DSTORE_SUCC;
    StorageSession *session = static_cast<StorageSession *>(calloc(1, sizeof(StorageSession)));
    if (STORAGE_VAR_NULL(session)) {
        goto CREATE_SESSION_FAILED;
    }
    session->topMemCtx =
        DstoreAllocSetContextCreate(nullptr,
                                    "dstoresession", ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE,
                                    ALLOCSET_DEFAULT_INITSIZE, MemoryContextType::SESSION_CONTEXT, 0);
    if (STORAGE_VAR_NULL(session->topMemCtx)) {
        goto CREATE_SESSION_FAILED;
    }
    session->topMemCtx->session_id = sessionId;
    session->sessionId = sessionId;
    session->tmpLocalBufMgr = nullptr;
    if (bufNums > 0) {
        session->tmpLocalBufMgr = DstoreNew(session->topMemCtx) TmpLocalBufMgr(bufNums);
        if (STORAGE_VAR_NULL(session->tmpLocalBufMgr)) {
            goto CREATE_SESSION_FAILED;
        }
    }
    session->lockRes = static_cast<LockResource *>(DstoreNew(session->topMemCtx) LockResource());
    if (STORAGE_VAR_NULL(session->lockRes)) {
        goto CREATE_SESSION_FAILED;
    }
    ret = session->lockRes->Initialize(session->topMemCtx);
    if (STORAGE_FUNC_FAIL(ret)) {
        goto CREATE_SESSION_FAILED;
    }
    return session;

CREATE_SESSION_FAILED:
    ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("CreateStorageSession failed!"));
    CleanUpSession(session);
    session = nullptr;
    return nullptr;
}

void CleanUpSession(StorageSession *session)
{
    if (session != nullptr) {
        if (session->lockRes != nullptr) {
            session->lockRes->Destroy();
            delete session->lockRes;
            session->lockRes = nullptr;
        }

        BufMgrInterface *bufMgr = session->tmpLocalBufMgr;
        if (bufMgr != nullptr) {
            bufMgr->Destroy();
            delete bufMgr;
        }
        session->tmpLocalBufMgr = nullptr;
        if (session->topMemCtx != nullptr) {
            DstoreMemoryContextDestroyTop(session->topMemCtx);
            session->topMemCtx = nullptr;
        }
        free(session);
    }
}

} /* namespace DSTORE */