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

#ifndef TPCC_TABLE_H
#define TPCC_TABLE_H

#include <cstdio>
#include <cstdint>
#include "catalog/dstore_fake_type.h"
#include "table_data_generator.h"

namespace TPCC_BENCHMARK {
constexpr int TPCC_TRANSACTION_NUM = 5;
constexpr int TPCC_DISTICT_PER_WAREHOUSE = 10;
constexpr int TPCC_MAX_NUM_ITEMS = 15;
constexpr int TPCC_MAX_ITEM_LEN = 24;

#define GetTableName(tableNameType) (char*)#tableNameType
#define GetColDesc(tableNameType) tableNameType##_COL_DESC
#define GetIndexDesc(tableNameType) tableNameType##_INDEX_DESC
#define GetTableColMax(tableNameType) tableNameType##_COL_MAX

enum TableNameType : uint8_t {
    TABLE_WAREHOUSE, /* TPC-C warehouse table */
    TABLE_DISTRICT,  /* TPC-C district table */
    TABLE_STOCK,     /* TPC-C stock table */
    TABLE_ITEM,      /* TPC-C item table */
    TABLE_CUSTOMER,  /* TPC-C customer table */
    TABLE_ORDER,     /* TPC-C order table */
    TABLE_NEWORDER,  /* TPC-C neworder table */
    TABLE_ORDERLINE, /* TPC-C orderline table */
    TABLE_HISTORY,   /* TPC-C history table */
    TABLE_MAX_CNT,
};

struct TableDesc {
    char*      tableName;
    uint32_t    colNum;
    ColDef    *colDefs;
    IndexDesc *indexDes;
};

/* http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-c_v5.11.0.pdf */

/*
 * 1. WAREHOUSE Table Layout 
 */
enum WarehouseTableColNameType : uint8_t {
    W_ID,  /* Primary Key, unique warehouse id */
    W_YTD, /* Year to date balance */
    W_TAX, /* Sales tax */
    W_NAME,
    W_STREET_1,
    W_STREET_2,
    W_CITY,
    W_STATE,
    W_ZIP,
    GetTableColMax(TABLE_WAREHOUSE)
};

static __attribute__((__unused__)) ColDef GetColDesc(TABLE_WAREHOUSE)[GetTableColMax(TABLE_WAREHOUSE)] = {
    {  INT4OID, false,  0, 0, 0},
    {FLOAT8OID,  true,  0, 0, 0},
    {FLOAT8OID,  true,  0, 0, 0},
    {  TEXTOID,  true, 10, 0, 0},
    {  TEXTOID,  true, 20, 0, 0},
    {  TEXTOID,  true, 20, 0, 0},
    {  TEXTOID,  true, 20, 0, 0},
    {  TEXTOID,  true,  2, 0, 0},
    {  TEXTOID,  true,  9, 0, 0},
};

static __attribute__((__unused__)) IndexDesc GetIndexDesc(TABLE_WAREHOUSE)[] = {
    {1, {W_ID, 0, 0, 0}, true},
};

/*
 * 2. DISTRICT Table Layout
 */
enum DistrictTableColNameType : uint8_t {
    D_ID,  /* 10 are populated per warehouse */
    D_W_ID,
    D_YTD,
    D_TAX,
    D_NEXT_O_ID,   /* Next available Order number */
    D_NAME,
    D_STREET_1,
    D_STREET_2,
    D_CITY,
    D_STATE,
    D_ZIP,
    GetTableColMax(TABLE_DISTRICT)
};

static __attribute__((__unused__)) ColDef GetColDesc(TABLE_DISTRICT)[GetTableColMax(TABLE_DISTRICT)] = {
    {   INT4OID, false,  0, 0, 0},
    {   INT4OID, false,  0, 0, 0},
    { FLOAT8OID,  true,  0, 0, 0},
    { FLOAT8OID,  true,  0, 0, 0},
    {   INT4OID,  true, 20, 0, 0},
    {VARCHAROID,  true, 10, 0, 0},
    {VARCHAROID,  true, 20, 0, 0},
    {VARCHAROID,  true, 20, 0, 0},
    {VARCHAROID,  true, 20, 0, 0},
    {   CHAROID,  true,  2, 0, 0},
    {   CHAROID,  true,  9, 0, 0},
};

static __attribute__((__unused__)) IndexDesc GetIndexDesc(TABLE_DISTRICT)[] = {
    {2, {D_W_ID, D_ID, 0, 0}, true},
};

/*
 * 3. STOCK Table Layout
 */
enum StockTableColNameType : uint8_t {
    S_W_ID = 0,
    S_I_ID,
    S_QUANTITY,
    S_YTD,
    S_ORDER_CNT,
    S_REMOTE_CNT,
    S_DATA,
    S_DIST_01,
    S_DIST_02,
    S_DIST_03,
    S_DIST_04,
    S_DIST_05,
    S_DIST_06,
    S_DIST_07,
    S_DIST_08,
    S_DIST_09,
    S_DIST_10,
    GetTableColMax(TABLE_STOCK)
};

static __attribute__((__unused__)) ColDef GetColDesc(TABLE_STOCK)[GetTableColMax(TABLE_STOCK)] = {
    {   INT4OID, false,  0, 0, 0},
    {   INT4OID, false,  0, 0, 0},
    {   INT4OID,  true,  0, 0, 0},
    {   INT4OID,  true,  0, 0, 0},
    {   INT4OID,  true,  0, 0, 0},
    {   INT4OID,  true,  0, 0, 0},
    {VARCHAROID,  true, 50, 0, 0},
    {   CHAROID,  true, 24, 0, 0},
    {   CHAROID,  true, 24, 0, 0},
    {   CHAROID,  true, 24, 0, 0},
    {   CHAROID,  true, 24, 0, 0},
    {   CHAROID,  true, 24, 0, 0},
    {   CHAROID,  true, 24, 0, 0},
    {   CHAROID,  true, 24, 0, 0},
    {   CHAROID,  true, 24, 0, 0},
    {   CHAROID,  true, 24, 0, 0},
    {   CHAROID,  true, 24, 0, 0},
};

static __attribute__((__unused__)) IndexDesc GetIndexDesc(TABLE_STOCK)[] = {
    {2, {S_W_ID, S_I_ID, 0, 0}, true},
};

/*
 * 4. ITEM Table Layout
 */
enum ItemTableColNameType : uint8_t { I_ID, I_IM_ID, I_PRICE, I_NAME, I_DATA, GetTableColMax(TABLE_ITEM) };

static __attribute__((__unused__)) ColDef GetColDesc(TABLE_ITEM)[GetTableColMax(TABLE_ITEM)] = {
    {   INT4OID, false,  0, 0, 0},
    {VARCHAROID,  true, 24, 0, 0},
    { FLOAT8OID,  true,  0, 0, 0},
    {VARCHAROID,  true, 50, 0, 0},
    {   INT4OID,  true,  0, 0, 0},
};

static __attribute__((__unused__)) IndexDesc GetIndexDesc(TABLE_ITEM)[] = {
    {1, {I_ID, 0, 0, 0}, true},
};

/*
 * 5. CUSTOMER Table Layout
 */
enum CustomerTableColNameType : uint8_t {
    C_W_ID,
    C_D_ID,
    C_ID,
    C_DISCOUNT,
    C_CREDIT,
    C_LAST,
    C_FIRST,
    C_CREDIT_LIM,
    C_BALANCE,
    C_YTD_PAYMENT,
    C_PAYMENT_CNT,
    C_DELIVERY_CNT,
    C_STREET_1,
    C_STREET_2,
    C_CITY,
    C_STATE,
    C_ZIP,
    C_PHONE,
    C_SINCE,
    C_MIDDLE,
    C_DATA,
    GetTableColMax(TABLE_CUSTOMER)
};

static __attribute__((__unused__)) ColDef GetColDesc(TABLE_CUSTOMER)[GetTableColMax(TABLE_CUSTOMER)] = {
    {     INT4OID, false,   0, 0, 0},
    {     INT4OID, false,   0, 0, 0},
    {     INT4OID, false,   0, 0, 0},
    {   FLOAT8OID,  true,   0, 0, 0},
    /* FXIME: VARCHAR*/
    {  VARCHAROID,  true,   2, 0, 0},
    {  VARCHAROID,  true,  16, 0, 0},
    {  VARCHAROID,  true,  16, 0, 0},
    {   FLOAT8OID,  true,   0, 0, 0},
    {   FLOAT8OID,  true,   0, 0, 0},
    {   FLOAT8OID,  true,   0, 0, 0},
    {     INT4OID,  true,   0, 0, 0},
    {     INT4OID,  true,   0, 0, 0},
    {  VARCHAROID,  true,  20, 0, 0},
    {  VARCHAROID,  true,  20, 0, 0},
    {  VARCHAROID,  true,  20, 0, 0},
    {     CHAROID,  true,   2, 0, 0},
    {     CHAROID,  true,   9, 0, 0},
    {     CHAROID,  true,  16, 0, 0},
    {TIMESTAMPOID,  true,   0, 0, 0},
    {     CHAROID,  true,   2, 0, 0},
    {  VARCHAROID,  true, 500, 0, 0}
};

static __attribute__((__unused__)) IndexDesc GetIndexDesc(TABLE_CUSTOMER)[] = {
    {3, {C_W_ID, C_D_ID, C_ID, 0}, true},
};

/*
 * 6. ORDER Table Layout
 */
enum OrderTableColNameType : uint8_t {
    O_ID = 0,
    O_D_ID,
    O_W_ID,
    O_C_ID,
    O_CARRIER_ID,
    O_OL_CNT,
    O_ALL_LOCAL,
    O_ENTRY_D,
    GetTableColMax(TABLE_ORDER)
};

static __attribute__((__unused__)) ColDef GetColDesc(TABLE_ORDER)[GetTableColMax(TABLE_ORDER)] = {
    {     INT4OID, false, 0, 0, 0},
    {     INT4OID, false, 0, 0, 0},
    {     INT4OID, false, 0, 0, 0},
    {     INT4OID,  true, 0, 0, 0},
    {     INT4OID,  true, 0, 0, 0},
    {     INT4OID,  true, 0, 0, 0},
    {     INT4OID,  true, 0, 0, 0},
    {TIMESTAMPOID,  true, 0, 0, 0},
};

static __attribute__((__unused__)) IndexDesc GetIndexDesc(TABLE_ORDER)[] = {
    {3, {O_W_ID, O_D_ID, O_ID, 0}, true},
};

/*
 * 7. NEW-ORDER Table Layout
 */
enum NewOrderTableColNameType : uint8_t { NO_O_ID = 0, NO_D_ID, NO_W_ID, GetTableColMax(TABLE_NEWORDER) };

static __attribute__((__unused__)) ColDef GetColDesc(TABLE_NEWORDER)[GetTableColMax(TABLE_NEWORDER)] = {
    {INT4OID, false, 0, 0, 0},
    {INT4OID, false, 0, 0, 0},
    {INT4OID, false, 0, 0, 0},
};

static __attribute__((__unused__)) IndexDesc GetIndexDesc(TABLE_NEWORDER)[] = {
    {3, {NO_W_ID, NO_D_ID, NO_O_ID, 0}, true},
};

/*
 * 8. ORDER-LINE Table Layout
 */
enum OrderLineTableColNameType : uint8_t {
    OL_W_ID = 0,
    OL_D_ID,
    OL_O_ID,
    OL_NUMBER,
    OL_I_ID,
    OL_SUPPLY_W_ID,
    OL_DELIVERY_D,
    OL_QUANTITY,
    OL_AMOUNT,
    OL_DIST_INFO,
    GetTableColMax(TABLE_ORDERLINE)
};

static __attribute__((__unused__)) ColDef GetColDesc(TABLE_ORDERLINE)[GetTableColMax(TABLE_ORDERLINE)] = {
    {     INT4OID, false,  0, 0, 0},
    {     INT4OID, false,  0, 0, 0},
    {     INT4OID, false,  0, 0, 0},
    {     INT4OID, false,  0, 0, 0},
    {     INT4OID, false,  0, 0, 0},
    {     INT4OID,  true,  0, 0, 0},
    {TIMESTAMPOID,  true,  0, 0, 0},
    {     INT4OID,  true,  0, 0, 0},
    {   FLOAT8OID,  true,  0, 0, 0},
    {     CHAROID,  true, 24, 0, 0},
};

static __attribute__((__unused__)) IndexDesc GetIndexDesc(TABLE_ORDERLINE)[] = {
    {4, {OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER}, true},
};

/*
 * 9. HISTORY Table Layout
 */
enum HistoryTableColNameType : uint8_t {
    H_ID,
    H_C_ID,
    H_C_D_ID,
    H_C_W_ID,
    H_D_ID,
    H_W_ID,
    H_DATE,
    H_AMOUNT,
    H_DATA,
    GetTableColMax(TABLE_HISTORY)
};

static __attribute__((__unused__)) ColDef GetColDesc(TABLE_HISTORY)[GetTableColMax(TABLE_HISTORY)] = {
    {     INT4OID, true,  0, 0, 0},
    {     INT4OID, true,  0, 0, 0},
    {     INT4OID, true,  0, 0, 0},
    {     INT4OID, true,  0, 0, 0},
    {     INT4OID, true,  0, 0, 0},
    {     INT4OID, true,  0, 0, 0},
    {TIMESTAMPOID, true,  0, 0, 0},
    {   FLOAT8OID, true,  0, 0, 0},
    {  VARCHAROID, true, 24, 0, 0},
};

static __attribute__((__unused__)) TableDesc TpccTableDesc[TABLE_MAX_CNT] = {
    {(char*)GetTableName(TABLE_WAREHOUSE),GetTableColMax(TABLE_WAREHOUSE),GetColDesc(TABLE_WAREHOUSE), GetIndexDesc(TABLE_WAREHOUSE),
     },
    {(char*)GetTableName(TABLE_DISTRICT),GetTableColMax(TABLE_DISTRICT),GetColDesc(TABLE_DISTRICT), GetIndexDesc(TABLE_DISTRICT)
     },
    {(char*)GetTableName(TABLE_STOCK),GetTableColMax(TABLE_STOCK),GetColDesc(TABLE_STOCK), GetIndexDesc(TABLE_STOCK)
     },
    {(char*)GetTableName(TABLE_ITEM),GetTableColMax(TABLE_ITEM),GetColDesc(TABLE_ITEM), GetIndexDesc(TABLE_ITEM)
     },
    {(char*)GetTableName(TABLE_CUSTOMER),GetTableColMax(TABLE_CUSTOMER),GetColDesc(TABLE_CUSTOMER), GetIndexDesc(TABLE_CUSTOMER),
     },
    {(char*)GetTableName(TABLE_ORDER),GetTableColMax(TABLE_ORDER),GetColDesc(TABLE_ORDER), GetIndexDesc(TABLE_ORDER)},
    {(char*)GetTableName(TABLE_NEWORDER),GetTableColMax(TABLE_NEWORDER),GetColDesc(TABLE_NEWORDER), GetIndexDesc(TABLE_NEWORDER),
     },
    {(char*)GetTableName(TABLE_ORDERLINE),GetTableColMax(TABLE_ORDERLINE),GetColDesc(TABLE_ORDERLINE), GetIndexDesc(TABLE_ORDERLINE)},
    {(char*)GetTableName(TABLE_HISTORY),GetTableColMax(TABLE_HISTORY),GetColDesc(TABLE_HISTORY), nullptr},
};
}
#endif