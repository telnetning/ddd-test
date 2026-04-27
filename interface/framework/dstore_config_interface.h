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
 * dstore_config_interface.h
 *
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/dstore/interface/framework/dstore_config_interface.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_TENANT_CONFIG_INTERFACE_H
#define DSTORE_TENANT_CONFIG_INTERFACE_H

#include "framework/dstore_instance_interface.h"

namespace TenantConfigInterface {
#pragma GCC visibility push(default)

DSTORE::RetStatus GetTenantConfig(const char *configFilePath, DSTORE::TenantConfig *tenantConfig);

#pragma GCC visibility pop

}  // namespace TenantConfigInterface
#endif