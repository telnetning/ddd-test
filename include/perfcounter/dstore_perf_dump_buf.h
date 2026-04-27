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
 * Description: The dump buffer of performance monitor.
 */

#ifndef DSTORE_PERF_DUMP_BUF_H
#define DSTORE_PERF_DUMP_BUF_H

#include "dstore_perf_internal.h"
#include "common/dstore_common_utils.h"
#include "framework/dstore_instance.h"

namespace DSTORE {
class PerfDumpBuf {
public:
    static PerfDumpBuf &GetInstance()
    {
        static PerfDumpBuf instance;
        return instance;
    }
    ~PerfDumpBuf() = default;

    bool Init(UNUSE_PARAM DstoreMemoryContext parent)
    {
        m_dumpBufMemCtx =
            DstoreAllocSetContextCreate(g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE),
                                        "PerfCounterDumpBufMemory", ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE,
                                        ALLOCSET_DEFAULT_MAXSIZE, MemoryContextType::SHARED_CONTEXT);
        if (m_dumpBufMemCtx == nullptr) {
            ErrLog(DSTORE_PANIC,
                   MODULE_FRAMEWORK, ErrMsg("Create PerfDumpBufMemCtx failed. PerfDumpBuf is unavailable."));
            return false;
        }
        ErrLog(DSTORE_INFO, MODULE_FRAMEWORK, ErrMsg("PerfDumpBuf init success."));
        return true;
    }

    void Destroy()
    {
        FreeDumpBuf();
        DstoreMemoryContextDelete(m_dumpBufMemCtx);
        m_dumpBufMemCtx = nullptr;
        ErrLog(DSTORE_INFO, MODULE_FRAMEWORK, ErrMsg("PerfDumpBuf destory success."));
    }

    char *CreateDumpBuf(uint32 dumpBufSize)
    {
        if (likely(m_dumpBuf != nullptr && m_dumpBufSize >= dumpBufSize)) {
            ErrLog(DSTORE_INFO,
                   MODULE_FRAMEWORK, ErrMsg("Not apply for a new space, dump buffer is sufficient."));
            errno_t rc = memset_s(m_dumpBuf, m_dumpBufSize, '\0', m_dumpBufSize);
            storage_securec_check(rc, "\0", "\0");
            return m_dumpBuf;
        }

        FreeDumpBuf();
        if (unlikely(dumpBufSize > PERF_DUMPBUFFER_MAX_SIZE)) {
            ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
                   ErrMsg("PerfCounter maximum limit dump buffer size is %uByte.", PERF_DUMPBUFFER_MAX_SIZE));
            dumpBufSize = PERF_DUMPBUFFER_MAX_SIZE;
        }

        m_dumpBuf = NEW_ARRAY(m_dumpBufMemCtx) char[dumpBufSize];
        if (unlikely(m_dumpBuf == nullptr)) {
            ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK, ErrMsg("Failed to create dump buffer space."));
            m_dumpBufSize = 0;
            return nullptr;
        }
        m_dumpBufSize = dumpBufSize;
        m_dumpBuf[0] = '\0';
        return m_dumpBuf;
    }

    void FreeDumpBuf()
    {
        m_dumpBufSize = 0;
        DELETE_ARRAY_AND_RESET(m_dumpBuf);
    }

    inline uint32 DumpBufSize() const
    {
        return m_dumpBufSize;
    }

public:
    char *m_dumpBuf{nullptr};
    uint32 m_dumpBufSize{0};
    DstoreMemoryContext m_dumpBufMemCtx;

private:
    PerfDumpBuf() = default;
    DISALLOW_COPY_AND_MOVE(PerfDumpBuf);
};
}  // namespace DSTORE
#endif /* STORAGE_PERF_DUMP_BUF_H */
