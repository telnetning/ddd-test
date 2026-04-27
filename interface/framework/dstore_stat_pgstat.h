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
 * dstore_stat_pgStat.h
 * ---------------------------------------------------------------------------------------
 */
enum class GsStatMsgType {
    PGSTAT_MTYPE_BGWRITER
};

/* ----------
 * GsStatMsgHdr				The common message header
 * ----------
 */
typedef struct GsStatMsgHdr {
    GsStatMsgType m_type;
    int m_size;
} GsStatMsgHdr;

/* ----------
 * GsStatMsgBgWriter			Sent by the bgwriter to update statistics.
 * ----------
 */
struct GsStatMsgBgWriter {
    GsStatMsgHdr m_hdr;

    long int m_timedCheckpoints; /* Number of checkpoints that are periodically executed. */
    long int m_requestedCheckpoints; /* Number of checkpoints manually executed. */
    long int m_bufWrittenCheckpoints; /* Number of buffers written by checkpoints. */
    long int m_bufAlloc; /* Number of allocated buffers. */
};