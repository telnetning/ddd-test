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
 * IDENTIFICATION
 *        include/port/dstore_port.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_PG_PORT_H
#define DSTORE_PG_PORT_H

namespace DSTORE {
/* Portable delay handling */
extern void GaussUsleep(long microsec);
}  // namespace DSTORE
#endif /* PG_PORT_H */
