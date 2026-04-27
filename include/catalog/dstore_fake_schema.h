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
 * dstore_fake_schema.h
 *
 * IDENTIFICATION
 *        include/catalog/dstore_fake_schema.h
 *
 * ---------------------------------------------------------------------------------------
 */
 
#ifndef SRC_GAUSSKERNEL_INCLUDE_COMMON_DSTORE_FAKE_SCHEMA_H
#define SRC_GAUSSKERNEL_INCLUDE_COMMON_DSTORE_FAKE_SCHEMA_H
 
namespace DSTORE {
#define SchemaDatabase \
{ 1262, {"oid"}, 26, -1, 4, 1, 0, -1, -1, true, 'p', 'i', true, false, false, true, 0, 0, 0, 0 }, \
{ 1262, {"datname"}, 19, -1, NAME_DATA_LEN, 2, 0, -1, -1, false, 'p', 'c', true, false, false, true, 0, 0, 950, 0 }, \
{ 1262, {"datdba"}, 26, -1, 4, 3, 0, -1, -1, true, 'p', 'i', true, false, false, true, 0, 0, 0, 0 }, \
{ 1262, {"encoding"}, 23, -1, 4, 4, 0, -1, -1, true, 'p', 'i', true, false, false, true, 0, 0, 0, 0 }, \
{ 1262, {"datlocprovider"}, 18, -1, 1, 5, 0, -1, -1, true, 'p', 'c', true, false, false, true, 0, 0, 0, 0 }, \
{ 1262, {"datistemplate"}, 16, -1, 1, 6, 0, -1, -1, true, 'p', 'c', true, false, false, true, 0, 0, 0, 0 }, \
{ 1262, {"datallowconn"}, 16, -1, 1, 7, 0, -1, -1, true, 'p', 'c', true, false, false, true, 0, 0, 0, 0 }, \
{ 1262, {"datconnlimit"}, 23, -1, 4, 8, 0, -1, -1, true, 'p', 'i', true, false, false, true, 0, 0, 0, 0 }, \
{ 1262, {"datfrozenxid"}, 28, -1, 4, 9, 0, -1, -1, true, 'p', 'i', true, false, false, true, 0, 0, 0, 0 }, \
{ 1262, {"datminmxid"}, 28, -1, 4, 10, 0, -1, -1, true, 'p', 'i', true, false, false, true, 0, 0, 0, 0 }, \
{ 1262, {"dattablespace"}, 26, -1, 4, 11, 0, -1, -1, true, 'p', 'i', true, false, false, true, 0, 0, 0, 0 }, \
{ 1262, {"datcollate"}, 25, -1, -1, 12, 0, -1, -1, false, 'x', 'i', true, false, false, true, 0, 0, 950, 0 }, \
{ 1262, {"datctype"}, 25, -1, -1, 13, 0, -1, -1, false, 'x', 'i', true, false, false, true, 0, 0, 950, 0 }, \
{ 1262, {"daticulocale"}, 25, -1, -1, 14, 0, -1, -1, false, 'x', 'i', false, false, false, true, 0, 0, 950, 0 }, \
{ 1262, {"datcollversion"}, 25, -1, -1, 15, 0, -1, -1, false, 'x', 'i', false, false, false, true, 0, 0, 950, 0 }, \
{ 1262, {"datacl"}, 1034, -1, -1, 16, 1, -1, -1, false, 'x', 'i', false, false, false, true, 0, 0, 0, 0 }
 
}
 
#endif /* SRC_GAUSSKERNEL_INCLUDE_COMMON_STORAGE_FAKE_SCHEMA_H */
