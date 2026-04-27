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
 * Description: The item for performance monitor.
 */

#include "perfcounter/dstore_perf_item.h"

namespace DSTORE {
int32 EventStat::Dump(char *dumpBuf, int32 bufIndex, const char *parent) const
{
    int32 offset = bufIndex;
    offset = PerfLogMemcpy(dumpBuf, offset, PERF_ITEM_INDENT, m_indentLevel);
    if (parent != nullptr) {
        offset = PerfLogSprintf(dumpBuf, offset, "%s id=%u, parent=%s: freq=%.4f/sec cnt=" FMT_U64 "\n", m_name, m_id,
                                parent, Frequency(), m_counter.load());
    } else {
        offset = PerfLogSprintf(dumpBuf, offset, "%s id=%u: freq=%.4f/sec cnt=" FMT_U64 "\n", m_name, m_id, Frequency(),
                                m_counter.load());
    }

    return offset - bufIndex;
}

void EventStat::Init()
{
    m_counter.store(0);
    m_startTime = std::chrono::steady_clock::now();
    SetInited();
}

void EventStat::Reset()
{
    /* store the his statistic info to m_hisCounter */
    uint64 counter = m_counter.load();
    m_counter -= counter;
    m_hisCounter += counter;
    m_startTime = std::chrono::steady_clock::now();
}

using namespace std::chrono;
float EventStat::Frequency() const
{
    milliseconds timeInterval = duration_cast<milliseconds>(steady_clock::now() - m_startTime);
    if (timeInterval.count() == 0) {
        return 0;
    }
    return (static_cast<float>(m_counter.load()) / static_cast<float>(timeInterval.count())) * MILLISECONDS_ONE_SECOND;
}

int32 LatestNumberStat::Dump(char *dumpBuf, int32 bufIndex, const char *parent) const
{
    uint64 minLatestNum = (m_minLatestNum.load() == std::numeric_limits<uint64>::max() ? 0 : m_minLatestNum.load());
    uint64 maxLatestNum = (m_maxLatestNum.load() == std::numeric_limits<uint64>::lowest() ? 0 : m_maxLatestNum.load());

    int32 offset = bufIndex;
    offset = PerfLogMemcpy(dumpBuf, offset, PERF_ITEM_INDENT, m_indentLevel);
    if (parent != nullptr) {
        offset = PerfLogSprintf(
            dumpBuf, offset, "%s id=%u, parent=%s: min/max/last=" FMT_U64 "/" FMT_U64 "/" FMT_U64 " cnt=" FMT_U64 "\n",
            m_name, m_id, parent, minLatestNum, maxLatestNum, m_lastLatestNum.load(), m_counter.load());
    } else {
        offset = PerfLogSprintf(dumpBuf, offset,
                                "%s id=%u: min/max/last=" FMT_U64 "/" FMT_U64 "/" FMT_U64 " cnt=" FMT_U64 "\n", m_name,
                                m_id, minLatestNum, maxLatestNum, m_lastLatestNum.load(), m_counter.load());
    }
    return offset - bufIndex;
}

void LatestNumberStat::Init()
{
    m_counter.store(0);
    m_minLatestNum.store(std::numeric_limits<uint64>::max());
    m_maxLatestNum.store(std::numeric_limits<uint64>::lowest());
    m_lastLatestNum.store(0);
    SetInited();
}

void LatestNumberStat::Reset()
{
    Init();
}

int32 LatencyStat::Dump(char *dumpBuf, int32 bufIndex, const char *parent) const
{
    char unit;
    uint64 factor;
    float avg = 0.0f;

    uint64 counter = m_counter.load();
    if (counter != 0) {
        avg = static_cast<float>(m_sum.load()) / static_cast<float>(counter);
    }

    if (avg > NANOSECONDS_ONE_MILLISECOND) {
        unit = 'm';
        factor = NANOSECONDS_ONE_MILLISECOND;
    } else if (avg > NANOSECONDS_ONE_MICROSECONDS) {
        unit = 'u';
        factor = NANOSECONDS_ONE_MICROSECONDS;
    } else {
        unit = 'n';
        factor = 1;
    }
    uint64 min = (m_min.load() == std::numeric_limits<uint64>::max() ? 0 : m_min.load());
    uint64 max = (m_max.load() == std::numeric_limits<uint64>::lowest() ? 0 : m_max.load());

    int32 offset = bufIndex;
    float tmpFactor = static_cast<float>(factor);
    offset = PerfLogMemcpy(dumpBuf, offset, PERF_ITEM_INDENT, m_indentLevel);
    if (parent != nullptr) {
        offset = PerfLogSprintf(
            dumpBuf, offset, "%s id=%u, parent=%s: avg/min/max/sum=%.2f/%.2f/%.2f/%.2f%cs cnt=" FMT_U64 "\n", m_name,
            m_id, parent, avg / tmpFactor, static_cast<float>(min) / tmpFactor, static_cast<float>(max) / tmpFactor,
            static_cast<float>(m_sum.load()) / tmpFactor, unit, m_counter.load());
    } else {
        offset = PerfLogSprintf(dumpBuf, offset, "%s id=%u: avg/min/max/sum=%.2f/%.2f/%.2f/%.2f%cs cnt=" FMT_U64 "\n",
                                m_name, m_id, avg / tmpFactor, static_cast<float>(min) / tmpFactor,
                                static_cast<float>(max) / tmpFactor, static_cast<float>(m_sum.load()) / tmpFactor, unit,
                                m_counter.load());
    }

    return offset - bufIndex;
}
}
