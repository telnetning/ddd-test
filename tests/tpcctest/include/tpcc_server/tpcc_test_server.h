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

#ifndef TPCC_TABLE_HANDLER_H
#define TPCC_TABLE_HANDLER_H
#include "framework/dstore_instance_interface.h"
#include "common/dstore_common_utils.h"
namespace DSTORE {

class TpccStorageInstance : public StorageInstanceInterface {
public:
    static void Init();
    static void Start(Oid allocMaxRelOid, StorageInstanceType storageType = StorageInstanceType::SINGLE, NodeId selfNodeId = -1);
    static void InitFinished();
    static void Stop(uint32_t nodeNum, bool *state);
    void InitWorkingVersionNum(const uint32_t *workingGrandVersionNum) {};
    uint32_t GetWorkingVersionNum()
    {
        return 0;
    }
private:
    static void DestroyDataSimulationContext();
};
}  // namespace DSTORE

void WaitAllNodeReady(uint32_t nodeNum, bool *state);

constexpr uint32_t GRAND_VERSION_NUM = 97039;

#endif