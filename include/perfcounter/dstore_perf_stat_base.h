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
 * Description: The perf stat base for performance monitor.
 */

#ifndef DSTORE_PERF_STAT_BASE_H
#define DSTORE_PERF_STAT_BASE_H

#include "dstore_perf_global_level.h"
#include "dstore_perf_internal.h"
#include "dstore_perf_dump_buf.h"

namespace DSTORE {
class PerfStatBase : public BaseObject {
public:
    PerfStatBase() = default;
    DISALLOW_COPY_AND_MOVE(PerfStatBase);
    virtual ~PerfStatBase() noexcept
    {
        /* m_name memory is managed by the external invoking module. */
        m_name = nullptr;
    }

    virtual int32 Dump(char *dumpBuf, int32 bufIndex, const char *parent) const = 0;
    virtual void Reset() = 0;

    inline void SetId(PerfId id)
    {
        m_id = id;
    }

    inline void SetPerfLevel(PerfLevel level)
    {
        m_level = level;
    }

    inline void SetName(const char *name)
    {
        m_name = name;
    }

    inline void SetIndentLevel(uint8 level)
    {
        m_indentLevel = (level < PERF_ITEM_MAX_INDENT_LEVEL ? level : PERF_ITEM_MAX_INDENT_LEVEL);
    }
    bool IsInited() const
    {
        return m_inited;
    }
    const char *GetName() const
    {
        return m_name;
    }

    const char *m_name{nullptr};

protected:
    template <typename... Args>
    inline int32 PerfLogSprintf(char *dumpBuf, int32 bufOffset, const char *format, Args... args) const
    {
        errno_t rc =
            sprintf_s(dumpBuf + bufOffset, PerfDumpBuf::GetInstance().DumpBufSize() - static_cast<uint32>(bufOffset),
                      format, args...);
        if (unlikely((rc) == -1)) {
            ErrLog(DSTORE_WARNING,
                   MODULE_FRAMEWORK, ErrMsg("PerfLog Sprintf error, buffer space may be insufficient."));
            return bufOffset;
        }
        return bufOffset + rc;
    }

    inline int32 PerfLogMemcpy(char *dumpBuf, int32 bufOffset, const char *format, int32 length) const
    {
        errno_t rc = memcpy_s(dumpBuf + bufOffset, PerfDumpBuf::GetInstance().DumpBufSize() -
            static_cast<uint32>(bufOffset), format, static_cast<uint32>(length));
        storage_securec_check(rc, "\0", "\0");
        return bufOffset + length;
    }
    void SetInited()
    {
        m_inited = true;
    }

protected:
    PerfId m_id{PERF_STAT_INVALID_ID};
    PerfLevel m_level{PerfLevel::RELEASE};
    uint8 m_indentLevel{0};
    bool m_inited{false};
};
}  // namespace DSTORE
#endif /* STORAGE_PERF_STAT_BASE_H */
