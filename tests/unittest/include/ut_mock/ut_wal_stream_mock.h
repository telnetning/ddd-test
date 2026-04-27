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
#include <atomic>
#include "port/dstore_port.h"
#include "wal/dstore_wal_logstream.h"

#ifndef DSTORE_UT_WAL_STREAM_MOCK_H
#define DSTORE_UT_WAL_STREAM_MOCK_H

namespace DSTORE
{
    class MockWalStream : public WalStream
    {
    public:
        MockWalStream(WalId walId, DstoreMemoryContext memoryContext, WalFileManager *walFileMgr, PdbId pdbId)
            : WalStream(memoryContext, walId, walFileMgr, g_storageInstance->GetGuc()->walFileSize, pdbId),
                m_flushPLsn{0}, m_walId{walId}
        {}

        void Init(WalStreamBuffer *walStreamBuffer)
        {
            WalStream::Init(walStreamBuffer);
        }

        WalId GetWalId() const
        {
            return m_walId;
        }

        WalGroupLsnInfo Append(uint8 *data, uint32 len) final
        {
            uint64 insertPos = m_flushPLsn.load(std::memory_order_acquire);
            uint64 nextInsertPos;
            do {
                nextInsertPos = insertPos + len;
            } while (!m_flushPLsn.compare_exchange_weak(insertPos, nextInsertPos, std::memory_order_acq_rel));
            WalGroupLsnInfo result = {m_walId, nextInsertPos - len, nextInsertPos};
            return result;
        }

        void WaitTargetPlsnPersist(uint64 targetPlsn, bool forceSync = false)
        {
            while (m_flushPLsn < targetPlsn) {
                GaussUsleep(1000);
            }
            return;
        }

        uint64 GetMaxAppendedPlsn() const
        {
            return m_flushPLsn;
        }

        void IncreaseMaxAppendedPlsn()
        {
            m_flushPLsn.fetch_add(1);
        }

    private:
        std::atomic_uint64_t m_flushPLsn;
        WalId m_walId;
    };
}
#endif
