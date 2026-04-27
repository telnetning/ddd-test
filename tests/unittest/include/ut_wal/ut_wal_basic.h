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
#ifndef DSTORE_UT_WAL_BASIC_H
#define DSTORE_UT_WAL_BASIC_H

#include <list>
#include "gtest/gtest.h"
#include "ut_utilities/ut_dstore_framework.h"
#include "wal/dstore_wal.h"
#include "wal/dstore_wal_write_context.h"
#include "wal/dstore_wal_reader.h"

namespace DSTORE {

constexpr int32 NODE_ID = 0; /* Equal to default node id WalStream::Init */
constexpr int32 TIME_LINE_ID = 0;
constexpr int32 STREAM_COUNT = 1;
constexpr uint32 NODE_ID_WAL_ID_FACTOR = 8;

struct WalRecordRedoInfo {
    uint64 endPlsn;
    /* walRecord must be the last in Struct, because of m_mainData[0] in it */
    WalRecord walRecord;
} PACKED;


class WALBASICTEST : virtual public DSTORETEST {
public:
    void SetUp() override;

    void TearDown() override;

protected:
    void Prepare(uint32 walFileSize = 16 * 1024 * 1024);
    void PrepareControlFileContent(NodeId selfNodeId = NODE_ID);
    RetStatus ReadRecordsAfterPlsn(uint64 plsn, WalType walType, std::vector<WalRecordRedoInfo *> &recordList);
    void Clear();
    void MarkBufferDirty(BufferDesc *bufferDesc);
    void UnmarkBufferDirty(BufferDesc *bufferDesc);
    void ClearDirtyFlags();
    WalRecord *BuildDDLWal(uint16 len);
    static WalRecord *BuildWal(uint16 len);
    WalRecordForPage *BuildWalForPage(WalType type, uint16 len, uint64 fileVersion = 1);
    void CheckWalRecordForPage(const WalRecordForPage *newRecord, const WalRecordForPage *orignalRecord);
    WalRecord *CompressRecord(WalRecord *record);
    WalRecord *DecompressRecord(const WalRecord *record);

    ControlFile *m_walControlFile;
    WalManager *m_walManager;
    WalStreamManager *m_walStreamManager;
    WalStream *m_walStream;
    AtomicWalWriterContext *m_walWriter;
    WalRecordReader *m_walRecordReader;
    WalRedoManager *m_walRedoManager;
    std::vector<BufferDesc *> modifiedBuffers;

    const FileId UT_WAL_FILE0_ID = 6000;
    const char *UT_WAL_FILE_0 = "ut_wal_file_0";
    const FileId UT_MAX_WAL_FILE_ID = UINT16_MAX;
    const char *UT_MAX_WAL_FILE_NAME = "ut_max_wal_file";
    static const uint64 MAX_FILE_SIZE = (uint64)1024 * 1024 * 1024 * 128;
};
}
#endif //DSTORE_UT_WAL_BASIC_H
