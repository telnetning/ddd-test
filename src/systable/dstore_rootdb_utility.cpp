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
 * The dstore_rootdb_utility.cpp provides the RootDB creation function.
 */

#include "systable/dstore_rootdb_utility.h"

#include "systable/dstore_systable_struct.h"
#include "systable/dstore_systable_utility.h"
#include "tuple/dstore_memheap_tuple.h"
#include "table/dstore_table_interface.h"
#include "systable/dstore_systable_preset.h"

#include "errorcode/dstore_systable_error_code.h"
#include "common/datatype/dstore_uuid_utils.h"

namespace DSTORE {
void RootDBUtility::PreAllocPdbIdWhenBootstrap(PdbId templatePdbId)
{
    RetStatus ret = DSTORE_SUCC;
    PdbId pdbId = INVALID_PDB_ID;
    ControlPdbInfoPageItemData pdbInfo;
    ControlFile *controlFile = g_storageInstance->GetPdb(templatePdbId)->GetControlFile();
    TenantConfig *config = g_storageInstance->GetGuc()->tenantConfig;
    StorageReleasePanic(config == nullptr, MODULE_SYSTABLE, ErrMsg("error tenant config."));
     /* Step1: Alloc template1 pdb id. */
    ret = controlFile->AllocPdbId(TEMPLATE1_SYS_DATABASE, pdbId, true);
    StorageAssert(ret == DSTORE_SUCC && pdbId == PDB_TEMPLATE1_ID);
    ret = ControlPdbInfoPageItemData::Init(&pdbInfo, PDB_TEMPLATE1_ID, PDB_DEFAULT_UUID_STR,
                                           PdbStatus::PDB_STATUS_OPENED_READ_WRITE,
                                           TEMPLATE1_SYS_DATABASE, config->storageConfig.template1VfsName);
    StorageReleasePanic(ret != DSTORE_SUCC, MODULE_SYSTABLE,
                        ErrMsg("init pdbinfo(%s) failed.", TEMPLATE1_SYS_DATABASE));
    pdbInfo.dbaId = BOOTSTRAP_SUPERUSERID;
    ret = controlFile->UpdatePdbItemData(PDB_TEMPLATE1_ID, &pdbInfo, sizeof(ControlPdbInfoPageItemData));
    StorageReleasePanic(ret != DSTORE_SUCC, MODULE_SYSTABLE, ErrMsg("Update pdb id(%u) failed.", pdbId));
    /* Step2: Alloc template0 pdb id. */
    ret = controlFile->AllocPdbId(TEMPLATE0_SYS_DATABASE, pdbId, true);
    StorageAssert(ret == DSTORE_SUCC && pdbId == PDB_TEMPLATE0_ID);
    ret = ControlPdbInfoPageItemData::Init(&pdbInfo, PDB_TEMPLATE0_ID, PDB_DEFAULT_UUID_STR,
                                           PdbStatus::PDB_STATUS_OPENED_READ_WRITE,
                                           TEMPLATE0_SYS_DATABASE, config->storageConfig.template0VfsName);
    StorageReleasePanic(ret != DSTORE_SUCC, MODULE_SYSTABLE,
                        ErrMsg("init pdbinfo(%s)  failed.", TEMPLATE0_SYS_DATABASE));
    ret = controlFile->UpdatePdbItemData(PDB_TEMPLATE0_ID, &pdbInfo, sizeof(ControlPdbInfoPageItemData));
    StorageReleasePanic(ret != DSTORE_SUCC, MODULE_SYSTABLE, ErrMsg("Update pdb id(%u)  failed.", pdbId));
    /* Step3: Alloc rootpdb pdb id. */
    ret = controlFile->AllocPdbId(DEFAULT_SYS_DATABASE, pdbId, true);
    StorageAssert(ret == DSTORE_SUCC && pdbId == PDB_ROOT_ID);
    ret = ControlPdbInfoPageItemData::Init(&pdbInfo, PDB_ROOT_ID, PDB_DEFAULT_UUID_STR,
                                           PdbStatus::PDB_STATUS_OPENED_READ_WRITE,
                                           DEFAULT_SYS_DATABASE, config->storageConfig.rootpdbVfsName);
    StorageReleasePanic(ret != DSTORE_SUCC, MODULE_SYSTABLE, ErrMsg("init pdbinfo(%s)  failed.", DEFAULT_SYS_DATABASE));
    ret = controlFile->UpdatePdbItemData(PDB_ROOT_ID, &pdbInfo, sizeof(ControlPdbInfoPageItemData));
    StorageReleasePanic(ret != DSTORE_SUCC, MODULE_SYSTABLE, ErrMsg("Update pdb id(%u)  failed.", pdbId));
    /* Step4: Alloc templateA pdb id. */
    ret = controlFile->AllocPdbId(TEMPLATEA_SYS_DATABASE, pdbId, true);
    StorageAssert(ret == DSTORE_SUCC && pdbId == PDB_TEMPLATEA_ID);
    char templateaVfsName[MAX_CONFIG_NAME_LENGTH] = {};
    StoragePdb::GenerateVfsName(PDB_TEMPLATEA_ID, templateaVfsName, MAX_CONFIG_NAME_LENGTH);
    ret = ControlPdbInfoPageItemData::Init(&pdbInfo, PDB_TEMPLATEA_ID, PDB_DEFAULT_UUID_STR,
                                           PdbStatus::PDB_STATUS_OPENED_READ_WRITE,
                                           TEMPLATEA_SYS_DATABASE, templateaVfsName);
    StorageReleasePanic(ret != DSTORE_SUCC, MODULE_SYSTABLE,
                        ErrMsg("init pdbinfo(%s)  failed.", TEMPLATEA_SYS_DATABASE));
    pdbInfo.dbaId = BOOTSTRAP_SUPERUSERID;
    ret = controlFile->UpdatePdbItemData(PDB_TEMPLATEA_ID, &pdbInfo, sizeof(ControlPdbInfoPageItemData));
    StorageReleasePanic(ret != DSTORE_SUCC, MODULE_SYSTABLE, ErrMsg("Update pdb id(%u)  failed.", pdbId));
}
}  // namespace DSTORE
