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
 * Description:
 * The declaration of the function crc_arm, implemented in the storage_crc32c_arm.S.
 *
 * IDENTIFICATION
 *        src/common/algorithm/dstore_crc32c_choose.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_CRC32C_CHOOSE_H
#define DSTORE_CRC32C_CHOOSE_H
namespace DSTORE {

uint32_t CompCrc32c(uint32_t crc, const uint8_t *data, size_t len);

}
#endif