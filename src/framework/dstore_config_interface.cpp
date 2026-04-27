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
 * dstore_config_interface.cpp
 *
 * Description: this file defineds the behaviors how does storage engine communicate
 * with the external VFS library.
 *
 * ---------------------------------------------------------------------------------------
 *
 */
#include "config/dstore_config.h"
#include "framework/dstore_config_interface.h"

namespace TenantConfigInterface {
using namespace DSTORE;

RetStatus GetTenantConfig(const char *configFilePath, TenantConfig *tenantConfig)
{
    return ParseTenantConfig(configFilePath, tenantConfig);
}

}  // namespace VfsInterface
