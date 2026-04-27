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
 * dstore_config.h
 *
 * Description: this file defines configuration structures for tenant config
 *
 * ---------------------------------------------------------------------------------------
 *
 */
#ifndef DSTORE_TENANT_CONFIG_H
#define DSTORE_TENANT_CONFIG_H

#include "framework/dstore_instance_interface.h"

namespace DSTORE {

bool CheckParserGet(ErrorCode err, const char *name, RetStatus *ret);
RetStatus ParseTenantConfig(const char *configFilePath, TenantConfig *tenantConfig);

} /* namespace DSTORE */
#endif