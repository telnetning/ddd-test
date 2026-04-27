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
 * ---------------------------------------------------------------------------------
 *
 * port_time.h
 *
 * Description:Defines the time external interfaces wrapper for platform time.
 *
 * ---------------------------------------------------------------------------------
 */

#ifndef UTILS_PORT_TIME_H
#define UTILS_PORT_TIME_H

GSDB_BEGIN_C_CODE_DECLS

/* Sleep microseconds. */
void Usleep(long microseconds);

GSDB_END_C_CODE_DECLS
#endif /* UTILS_PORT_TIME_H */
