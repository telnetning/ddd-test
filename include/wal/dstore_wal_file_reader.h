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
 * dstore_wal_file_reader.h
 *
 * Description:
 * Define abstract class WalStreamBytesReader, used to read wal stream bytes.
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_DSTORE_WAL_FILE_READER_H
#define DSTORE_DSTORE_WAL_FILE_READER_H
#include "common/dstore_datatype.h"
#include "common/memory/dstore_mctx.h"

namespace DSTORE {

class WalStreamBytesReader {
public:
    virtual ~WalStreamBytesReader() = default;
    /*
     * Read data in WalFile target plsn.
     *
     * @param: plsn is read start offset.
     * @param: data:read buffer, used to save read data result
     * @param: readLen is called target len to read.
     * @param: resultLen:output parameter, is actual read data len, equal to read_len if read success.
     * @return: OK if success, detail error info otherwise
     */
    virtual RetStatus Read(uint64 plsn, uint8 *data, uint64 readLen, uint64 *resultLen) = 0;

    /*
     * Get a potentially WalRecord read start plsn before input plsn
     *
     * @param: plsn is end bound of target plsn
     * @return: target plsn if exist, otherwise return INVALID_PLSN
     */
    virtual uint64 GetPrevReadStartPoint(uint64 plsn) = 0;
};
}
#endif
