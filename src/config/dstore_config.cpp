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
 * dstore_config.cpp
 *
 * Description: this file defineds the behaviors how parse tenant config
 *
 * ---------------------------------------------------------------------------------------
 *
 */
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cstring>
#include <fcntl.h>
#include <cstdio>
#include "config_parser/config_parser.h"
#include "common/log/dstore_log.h"
#include "common/memory/dstore_mctx.h"
#include "config/dstore_config.h"

namespace DSTORE {

static ConfigParserHandle *g_rootHandle{nullptr};

bool CheckParserGet(ErrorCode err, const char *name, RetStatus *ret)
{
    if (unlikely(err != ERROR_SYS_OK)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("could not parse tenant file field name:%s, errcode is %lld.", name, err));
        *ret = DSTORE_FAIL;
        return false;
    }
    return true;
}

static RetStatus ParseTenantStorageConfig(TenantConfig *tenantConfig)
{
    RetStatus ret = DSTORE_SUCC;
    errno_t nRet = 0;

    int32_t storageType = 0;
    if (CheckParserGet(ConfigParserGetIntValue(g_rootHandle, "storageConfig.type", &storageType), "storageConfig.type",
                       &ret)) {
        if (storageType < static_cast<int32_t>(StorageType::TENANT_ISOLATION) ||
            storageType > static_cast<int32_t>(StorageType::LOCAL)) {
            tenantConfig->storageConfig.type = StorageType::INVALID_TYPE;
        } else {
            tenantConfig->storageConfig.type = static_cast<StorageType>(storageType);
        }
    }

    char *clientLibPath;
    if (CheckParserGet(ConfigParserGetStrValue(g_rootHandle, "storageConfig.clientLibPath", &clientLibPath),
                       "storageConfig.clientLibPath", &ret)) {
        nRet = strcpy_s(tenantConfig->storageConfig.clientLibPath, FILE_PATH_MAX_LEN, clientLibPath);
        storage_securec_check(nRet, "\0", "\0");
    }

    char *serverAddresses;
    if (CheckParserGet(ConfigParserGetStrValue(g_rootHandle, "storageConfig.serverAddresses", &serverAddresses),
                       "storageConfig.serverAddresses", &ret)) {
        nRet = strcpy_s(tenantConfig->storageConfig.serverAddresses, VFS_LIB_ATTR_LEN, serverAddresses);
        storage_securec_check(nRet, "\0", "\0");
    }

    char *serverProtocolType;
    if (CheckParserGet(ConfigParserGetStrValue(g_rootHandle, "storageConfig.serverProtocolType", &serverProtocolType),
                       "storageConfig.serverProtocolType", &ret)) {
        nRet = strcpy_s(tenantConfig->storageConfig.serverProtocolType, MAXTYPELEN, serverProtocolType);
        storage_securec_check(nRet, "\0", "\0");
    }

    char *rootpdbVfsName;
    if (CheckParserGet(ConfigParserGetStrValue(g_rootHandle, "storageConfig.rootpdbVfsName", &rootpdbVfsName),
                       "storageConfig.rootpdbVfsName", &ret)) {
        nRet = strcpy_s(tenantConfig->storageConfig.rootpdbVfsName, VFS_NAME_MAX_LEN, rootpdbVfsName);
        storage_securec_check(nRet, "\0", "\0");
    }

    char *template0VfsName;
    if (CheckParserGet(ConfigParserGetStrValue(g_rootHandle, "storageConfig.template0VfsName", &template0VfsName),
                       "storageConfig.template0VfsName", &ret)) {
        nRet = strcpy_s(tenantConfig->storageConfig.template0VfsName, VFS_NAME_MAX_LEN, template0VfsName);
        storage_securec_check(nRet, "\0", "\0");
    }

    char *template1VfsName;
    if (CheckParserGet(ConfigParserGetStrValue(g_rootHandle, "storageConfig.template1VfsName", &template1VfsName),
                       "storageConfig.template1VfsName", &ret)) {
        nRet = strcpy_s(tenantConfig->storageConfig.template1VfsName, VFS_NAME_MAX_LEN, template1VfsName);
        storage_securec_check(nRet, "\0", "\0");
    }

    char *votingVfsName;
    if (CheckParserGet(ConfigParserGetStrValue(g_rootHandle, "storageConfig.votingVfsName", &votingVfsName),
                       "storageConfig.votingVfsName", &ret)) {
        nRet = strcpy_s(tenantConfig->storageConfig.votingVfsName, VFS_NAME_MAX_LEN, votingVfsName);
        storage_securec_check(nRet, "\0", "\0");
    }

    char *runlogVfsName;
    if (CheckParserGet(ConfigParserGetStrValue(g_rootHandle, "storageConfig.runlogVfsName", &runlogVfsName),
                       "storageConfig.runlogVfsName", &ret)) {
        nRet = strcpy_s(tenantConfig->storageConfig.runlogVfsName, VFS_NAME_MAX_LEN, runlogVfsName);
        storage_securec_check(nRet, "\0", "\0");
    }

    return ret;
}

static RetStatus ParseTenantStorageSpaceConfig(TenantConfig *tenantConfig)
{
    ConfigParserHandle *handle = nullptr;
    ErrorCode err = ConfigParserGetHandle(g_rootHandle, "storeSpaceConfig", &handle);
    if (unlikely(err != ERROR_SYS_OK)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("could not get storespace config handle, errcode is %lld.", err));
        return DSTORE_FAIL;
    }

    ConfigParserHandle *ele = nullptr;
    int num = 0;
    errno_t nRet = 0;
    RetStatus ret = DSTORE_SUCC;
    CONFIGPARSER_HANDLE_FOR_EACH(ele, handle)
    {
        char *storeSpaceName;
        if (CheckParserGet(ConfigParserGetStrValue(ele, "storeSpaceName", &storeSpaceName), "storeSpaceName", &ret)) {
            nRet = strcpy_s(tenantConfig->storeSpaces[num].storeSpaceName, VFS_NAME_MAX_LEN, storeSpaceName);
            storage_securec_check(nRet, "\0", "\0");
        }
        err = ConfigParserGetIntValue(ele, "maxSpaceSize", &(tenantConfig->storeSpaces[num].maxSpaceSize));
        (void)CheckParserGet(err, "maxSpaceSize", &ret);
        char *type;
        if (CheckParserGet(ConfigParserGetStrValue(ele, "type", &type), "type", &ret)) {
            nRet = strcpy_s(tenantConfig->storeSpaces[num].type, MAXTYPELEN, type);
            storage_securec_check(nRet, "\0", "\0");
        }
        num++;
    }
    tenantConfig->storeSpaceCnt = num;
    return ret;
}

static RetStatus ParseTenantCommunicationConfig(TenantConfig *tenantConfig)
{
    RetStatus ret = DSTORE_SUCC;
    errno_t nRet = 0;

    (void)CheckParserGet(ConfigParserGetIntValue(g_rootHandle, "communicationConfig.clusterId",
                                                 &tenantConfig->communicationConfig.clusterId),
                         "communicationConfig.clusterId", &ret);
    char *localIp;
    if (ConfigParserGetStrValue(g_rootHandle, "communicationConfig.local.localIp", &localIp) == ERROR_SYS_OK) {
        nRet = strcpy_s(tenantConfig->communicationConfig.localConfig.localIp, VFS_LIB_ATTR_LEN, localIp);
        storage_securec_check(nRet, "\0", "\0");
    }

    (void)ConfigParserGetIntValue(g_rootHandle, "communicationConfig.dstore_comm_thread_min",
                                  &tenantConfig->communicationConfig.dstoreCommThreadMin);
    (void)ConfigParserGetIntValue(g_rootHandle, "communicationConfig.dstore_comm_thread_max",
                                  &tenantConfig->communicationConfig.dstoreCommThreadMax);
    char *commConfigStr;
    if (ConfigParserGetStrValue(g_rootHandle, "communicationConfig.comm_config_str", &commConfigStr) == ERROR_SYS_OK) {
        nRet = strcpy_s(tenantConfig->communicationConfig.commConfigStr, VFS_LIB_ATTR_LEN, commConfigStr);
        storage_securec_check(nRet, "\0", "\0");
    }

    int authType = 0;
    if (CheckParserGet(ConfigParserGetIntValue(g_rootHandle, "communicationConfig.authType", &authType),
                       "communicationConfig.authType", &ret)) {
        tenantConfig->communicationConfig.authType = static_cast<uint32_t>(authType);
    }

    return ret;
}

static RetStatus ParseTenantVotingConfig(TenantConfig *tenantConfig)
{
    RetStatus ret = DSTORE_SUCC;
    char *votingFilePath;
    if (CheckParserGet(ConfigParserGetStrValue(g_rootHandle, "votingConfig.votingFilePath", &votingFilePath),
                       "votingConfig.votingFilePath", &ret)) {
        errno_t nRet = strcpy_s(tenantConfig->votingConfig.votingFilePath, FILE_PATH_MAX_LEN, votingFilePath);
        storage_securec_check(nRet, "\0", "\0");
    }
    return ret;
}

static RetStatus ParseTenantLogConfig(TenantConfig *tenantConfig)
{
    RetStatus ret = DSTORE_SUCC;

    (void)CheckParserGet(
        ConfigParserGetIntValue(g_rootHandle, "logConfig.logLevel", &(tenantConfig->logConfig.logLevel)),
        "logConfig.logLevel", &ret);

    char *logPath;
    if (CheckParserGet(ConfigParserGetStrValue(g_rootHandle, "logConfig.logPath", &logPath), "logConfig.logPath",
                       &ret)) {
        errno_t nRet = strcpy_s(tenantConfig->logConfig.logPath, FILE_PATH_MAX_LEN, logPath);
        storage_securec_check(nRet, "\0", "\0");
    }

    return ret;
}

static RetStatus ParseTenantSecurityConnectSslConfig(TenantConfig *tenantConfig)
{
    RetStatus ret = DSTORE_SUCC;
    errno_t nRet = 0;
 
    char *tmpStr;
    if (CheckParserGet(ConfigParserGetStrValue(g_rootHandle, "securityConfig.connectSsl.caFile", &tmpStr),
                       "securityConfig.connectSsl.caFile", &ret)) {
        nRet = strcpy_s(tenantConfig->securityConfig.connectSsl.caFile, DSTORE_MAX_TLS_NAME_LEN, tmpStr);
        storage_securec_check(nRet, "\0", "\0");
    }
 
    if (CheckParserGet(ConfigParserGetStrValue(g_rootHandle, "securityConfig.connectSsl.keyFile", &tmpStr),
                       "securityConfig.connectSsl.keyFile", &ret)) {
        nRet = strcpy_s(tenantConfig->securityConfig.connectSsl.keyFile, DSTORE_MAX_TLS_NAME_LEN, tmpStr);
        storage_securec_check(nRet, "\0", "\0");
    }
 
    if (CheckParserGet(ConfigParserGetStrValue(g_rootHandle, "securityConfig.connectSsl.crlFile", &tmpStr),
                       "securityConfig.connectSsl.crlFile", &ret)) {
        nRet = strcpy_s(tenantConfig->securityConfig.connectSsl.crlFile, DSTORE_MAX_TLS_NAME_LEN, tmpStr);
        storage_securec_check(nRet, "\0", "\0");
    }
 
    if (CheckParserGet(ConfigParserGetStrValue(g_rootHandle, "securityConfig.connectSsl.certFile", &tmpStr),
                       "securityConfig.connectSsl.certFile", &ret)) {
        nRet = strcpy_s(tenantConfig->securityConfig.connectSsl.certFile, DSTORE_MAX_TLS_NAME_LEN, tmpStr);
        storage_securec_check(nRet, "\0", "\0");
    }

    if (CheckParserGet(ConfigParserGetStrValue(g_rootHandle, "securityConfig.connectSsl.cipher", &tmpStr),
                       "securityConfig.connectSsl.cipher", &ret)) {
        nRet = strcpy_s(tenantConfig->securityConfig.connectSsl.cipher, DSTORE_MAX_TLS_NAME_LEN, tmpStr);
        storage_securec_check(nRet, "\0", "\0");
    }
 
    int certNotifyTime = 0;
    if (CheckParserGet(ConfigParserGetIntValue(g_rootHandle, "securityConfig.connectSsl.certNotifyTime",
        &certNotifyTime), "securityConfig.connectSsl.certNotifyTime", &ret)) {
        tenantConfig->securityConfig.connectSsl.certNotifyTime = static_cast<uint32_t>(certNotifyTime);
    }
 
    return ret;
}
 
static RetStatus ParseTenantSecurityRpcSslConfig(TenantConfig *tenantConfig)
{
    RetStatus ret = DSTORE_SUCC;
    errno_t nRet = 0;
 
    char *tmpStr;
    if (CheckParserGet(ConfigParserGetStrValue(g_rootHandle, "securityConfig.rpcSsl.caFile", &tmpStr),
                       "securityConfig.rpcSsl.caFile", &ret)) {
        nRet = strcpy_s(tenantConfig->securityConfig.rpcSsl.caFile, DSTORE_MAX_TLS_NAME_LEN, tmpStr);
        storage_securec_check(nRet, "\0", "\0");
    }
 
    if (CheckParserGet(ConfigParserGetStrValue(g_rootHandle, "securityConfig.rpcSsl.keyFile", &tmpStr),
                       "securityConfig.rpcSsl.keyFile", &ret)) {
        nRet = strcpy_s(tenantConfig->securityConfig.rpcSsl.keyFile, DSTORE_MAX_TLS_NAME_LEN, tmpStr);
        storage_securec_check(nRet, "\0", "\0");
    }
 
    if (CheckParserGet(ConfigParserGetStrValue(g_rootHandle, "securityConfig.rpcSsl.crlFile", &tmpStr),
                       "securityConfig.rpcSsl.crlFile", &ret)) {
        nRet = strcpy_s(tenantConfig->securityConfig.rpcSsl.crlFile, DSTORE_MAX_TLS_NAME_LEN, tmpStr);
        storage_securec_check(nRet, "\0", "\0");
    }
 
    if (CheckParserGet(ConfigParserGetStrValue(g_rootHandle, "securityConfig.rpcSsl.certFile", &tmpStr),
                       "securityConfig.rpcSsl.certFile", &ret)) {
        nRet = strcpy_s(tenantConfig->securityConfig.rpcSsl.certFile, DSTORE_MAX_TLS_NAME_LEN, tmpStr);
        storage_securec_check(nRet, "\0", "\0");
    }

    if (CheckParserGet(ConfigParserGetStrValue(g_rootHandle, "securityConfig.rpcSsl.cipher", &tmpStr),
                       "securityConfig.rpcSsl.cipher", &ret)) {
        nRet = strcpy_s(tenantConfig->securityConfig.rpcSsl.cipher, DSTORE_MAX_TLS_NAME_LEN, tmpStr);
        storage_securec_check(nRet, "\0", "\0");
    }
 
    int certNotifyTime = 0;
    if (CheckParserGet(ConfigParserGetIntValue(g_rootHandle, "securityConfig.rpcSsl.certNotifyTime",
        &certNotifyTime), "securityConfig.rpcSsl.certNotifyTime", &ret)) {
        tenantConfig->securityConfig.rpcSsl.certNotifyTime = static_cast<uint32_t>(certNotifyTime);
    }
 
    return ret;
}

static RetStatus ParseTenantSecurityConfig(TenantConfig *tenantConfig)
{
    RetStatus ret = DSTORE_SUCC;
    errno_t nRet = 0;

    char *tmpStr;
    if (CheckParserGet(ConfigParserGetStrValue(g_rootHandle, "securityConfig.securityRootKeyPath", &tmpStr),
                       "securityConfig.securityConfig.securityRootKeyPath", &ret)) {
        nRet = strcpy_s(tenantConfig->securityConfig.securityRootKeyPath, DSTORE_LOG_PATH_MAX_LEN, tmpStr);
        storage_securec_check(nRet, "\0", "\0");
    }

    if (CheckParserGet(ConfigParserGetStrValue(g_rootHandle, "securityConfig.cipherKey", &tmpStr),
                       "securityConfig.cipherKey", &ret)) {
        nRet = strcpy_s(tenantConfig->securityConfig.cipherKey, MAXNAMELEN, tmpStr);
        storage_securec_check(nRet, "\0", "\0");
    }

    if (CheckParserGet(ConfigParserGetStrValue(g_rootHandle, "securityConfig.commSharedKey", &tmpStr),
                       "securityConfig.commSharedKey", &ret)) {
        nRet = strcpy_s(tenantConfig->securityConfig.commSharedKey, MAXNAMELEN, tmpStr);
        storage_securec_check(nRet, "\0", "\0");
    }

    if (unlikely(ParseTenantSecurityRpcSslConfig(tenantConfig) != DSTORE_SUCC)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Parse tenant security rpcssl config fail."));
    }

    return ret;
}

RetStatus ParseTenantConfig(const char *configFilePath, TenantConfig *tenantConfig)
{
    RetStatus ret = DSTORE_SUCC;
    errno_t nRet = 0;

    if (!CheckParserGet(ConfigParserLoadFile(configFilePath, &g_rootHandle), "load file", &ret)) {
        return ret;
    }

    char *clusterName;
    if (CheckParserGet(ConfigParserGetStrValue(g_rootHandle, "clusterName", &clusterName), "clusterName", &ret)) {
        nRet = strcpy_s(tenantConfig->clusterName, CLUSTER_NAME_MAX_LEN, clusterName);
        storage_securec_check(nRet, "\0", "\0");
    }

    char *tenantName;
    if (CheckParserGet(ConfigParserGetStrValue(g_rootHandle, "tenantName", &tenantName), "tenantName", &ret)) {
        nRet = strcpy_s(tenantConfig->tenantName, DSTORE_TENANT_NAME_MAX_LEN, tenantName);
        storage_securec_check(nRet, "\0", "\0");
    }

    (void)CheckParserGet(ConfigParserGetIntValue(g_rootHandle, "tenantId", &tenantConfig->tenantId), "tenantId", &ret);

    int nodeId = 0;
    if (CheckParserGet(ConfigParserGetIntValue(g_rootHandle, "nodeId", &nodeId), "nodeId", &ret)) {
        tenantConfig->nodeId = static_cast<uint32_t>(nodeId);
    }

    if (unlikely(ParseTenantStorageConfig(tenantConfig) != DSTORE_SUCC)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Parse tenant storage config fail."));
    }
    if (unlikely(ParseTenantStorageSpaceConfig(tenantConfig) != DSTORE_SUCC)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Parse tenant storage spaces config fail."));
    }
    if (unlikely(ParseTenantCommunicationConfig(tenantConfig) != DSTORE_SUCC)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Parse tenant communication spaces config fail."));
    }
    if (unlikely(ParseTenantVotingConfig(tenantConfig) != DSTORE_SUCC)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Parse tenant voting config fail."));
    }
    if (unlikely(ParseTenantLogConfig(tenantConfig) != DSTORE_SUCC)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Parse tenant log config fail."));
    }
    if (likely(tenantConfig->communicationConfig.authType != 0)) {
        if (unlikely(ParseTenantSecurityConfig(tenantConfig) != DSTORE_SUCC)) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Parse tenant security config fail."));
        } else {
            ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("Parse tenant security config success."));
        }
    }
    if (unlikely(ParseTenantSecurityConnectSslConfig(tenantConfig) != DSTORE_SUCC)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Parse tenant security connectssl config fail."));
    }

    (void)CheckParserGet(ConfigParserDelete(g_rootHandle, ""), "delete parse", &ret);
    g_rootHandle = nullptr;
    return ret;
}

} /* namespace DSTORE */