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
 * dstore_uuid_utils.cpp
 *
 *
 *
 * IDENTIFICATION
 *        src/common/datatype/dstore_uuid_utils.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "common/datatype/dstore_uuid_utils.h"
#include <sys/ioctl.h>
#include <net/if.h>
#include "framework/dstore_instance.h"
#include "transaction/dstore_transaction.h"

namespace DSTORE {

/*
 * transfer char* into hexadecimal style.
 * offset is used to modify the range of source char in case of getting range of signed int8. set to 0 if not needed.
 */
void StringToHex(char *source, char *dest, int srcLen, int offset)
{
    int destIndex = 0;
    if (!std::is_signed<char>::value) {
        offset = 0;
    }
    for (int i = 0; i < srcLen; i++) {
        dest[destIndex++] = HEX_CHARS[(source[i] + offset) / HEX_BASE];
        dest[destIndex++] = HEX_CHARS[(source[i] + offset) % HEX_BASE];
    }
}

void IntToHex(uint64 source, char *dest, int srcLen)
{
    char *destPtr = dest + srcLen;
    for (int i = 0; i < srcLen; i++) {
        *(--destPtr) = HEX_CHARS[source & 0xF];
        source >>= HEX_BYTES;
    }
}

uint64 GetCurNodeId()
{
    return g_storageInstance->GetGuc()->selfNodeId;
}

/*
 * get specific num of character read from /dev/urandom, storing in randBuf.
 */
void PseudoRandRead(char *randBuf, size_t num)
{
    FILE *file = fopen(URANDOM_FILE_PATH, "r");
    if (file == NULL) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("could not open file \"%s\".", URANDOM_FILE_PATH));
        return;
    }
    size_t readBytes = 0;
    readBytes = fread(randBuf, 1, num, file);
    if (readBytes != num) {
        fclose(file);
        ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("could not get random number."));
        return;
    }
    fclose(file);
}

void ParseMacAddr(macaddr *mac, char *origin_info)
{
    if (origin_info == NULL) {
        return;
    }
    errno_t rc = EOK;
    rc = memset_s((void *)mac, sizeof(macaddr), 0, sizeof(macaddr));
    storage_securec_check(rc, "\0", "\0");

    mac->a = (unsigned char)origin_info[MAC_ADDR_A_INDEX];
    mac->b = (unsigned char)origin_info[MAC_ADDR_B_INDEX];
    mac->c = (unsigned char)origin_info[MAC_ADDR_C_INDEX];
    mac->d = (unsigned char)origin_info[MAC_ADDR_D_INDEX];
    mac->e = (unsigned char)origin_info[MAC_ADDR_E_INDEX];
    mac->f = (unsigned char)origin_info[MAC_ADDR_F_INDEX];
}

uint64 GetVirtualMacAddr(void)
{
    macaddr mac;
    char virtualMac[MAC_ADDR_LEN] = {0};
    PseudoRandRead(virtualMac, MAC_ADDR_LEN);
    ParseMacAddr(&mac, virtualMac);
    return MAC_GET_UINT64(mac);
}

#ifndef NO_SOCKET
const int NO_SOCKET = -1;
#endif

uint64 g_uuidTime = 0;
uint64 g_uuidShort = 0;
uint64 g_nodeId = 0;
uint32 g_nanoSeq = 0;
char g_clockSeqWithMac[] = "0000-000000000000";
pthread_mutex_t g_uuidMutexLock = PTHREAD_MUTEX_INITIALIZER;

uint64 GetDeviceMacAddr(void)
{
    macaddr mac;
    int sockFd = NO_SOCKET;
    struct ifconf ifconfInfo;
    struct ifreq ifreqInfo;
    char *buf = NULL;
    errno_t rc = EOK;

    sockFd = socket(AF_INET, SOCK_DGRAM, IPPROTO_IP);
    if (sockFd == NO_SOCKET) {
        return 0;
    }

    buf = (char *)DstorePalloc(MAX_MAC_ADDR_LIST * sizeof(ifreq));
    StorageReleasePanic(buf == nullptr, MODULE_BUFMGR, ErrMsg("alloc memory for ifc_buf fail!"));
    ifconfInfo.ifc_len = MAX_MAC_ADDR_LIST * sizeof(ifreq);
    ifconfInfo.ifc_buf = buf;

    /* Return a list of interface (network layer) addresses, only addresses of the AF_INET (IPv4) family for
     * compatibility. */
    if (ioctl(sockFd, SIOCGIFCONF, &ifconfInfo) != -1) {
        struct ifreq *ifrepTmp = ifconfInfo.ifc_req;
        for (uint32 i = 0; i < ((uint32)ifconfInfo.ifc_len / sizeof(struct ifreq)); i++) {
            rc = strcpy_s(ifreqInfo.ifr_name, strlen(ifrepTmp->ifr_name) + 1, ifrepTmp->ifr_name);
            storage_securec_check(rc, "\0", "\0");
            ifrepTmp++;

            /* Check this is not a loopback interface. */
            if (ioctl(sockFd, SIOCGIFFLAGS, &ifreqInfo) != 0 || (ifreqInfo.ifr_flags & IFF_LOOPBACK)) {
                continue;
            }
            /* Get the hardware address of a device using ifr_hwaddr. */
            if (ioctl(sockFd, SIOCGIFHWADDR, &ifreqInfo) == 0) {
                ParseMacAddr(&mac, ifreqInfo.ifr_hwaddr.sa_data);
                break;
            }
        }
    } else {
        ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("Get device mac addr failed."));
    }
    DstorePfreeExt(buf);
    close(sockFd);
    return MAC_GET_UINT64(mac);
}

uint64 GetLatestCurrTime()
{
    char clockSeq[CLOCK_SEQ_CHAR_NUM] = {0};
    /* init g_uuidTime at first time. get mac address and clock seq first. */
    if (unlikely(!g_uuidTime)) {
        PseudoRandRead(clockSeq, CLOCK_SEQ_CHAR_NUM);
        StringToHex(clockSeq, g_clockSeqWithMac, CLOCK_SEQ_CHAR_NUM, INT_RANGE_REVISE_PARAM);

        uint64 macAddr = GetDeviceMacAddr();
        if (!macAddr) {
            /* no mac obtained. use random string as virtual mac. */
            /* ERR_PROC(GAUSS_02703, ERR_LEVEL_LOG,
                     (err_msg("could not get device mac, use random value as virtual mac."))); */
            macAddr = GetVirtualMacAddr();
        }
        IntToHex(macAddr, g_clockSeqWithMac + UUID_MID_PART_LEN + 1, UUID_LAST_PART_LEN);
    }

    /*
     * this is why g_nanoSeq should be uint but not uint64, cause g_nanoSeq will increase, if it is uint64
     * add increase to UINT64_MAX, the final result of currTime will be overflow.
     */
    uint64 currTime = static_cast<uint64>(GetCurrentTimestampInSecond() * HUNDRED_NANO_SECOND) + g_nanoSeq;
    /* consider current xid to prevent duplicate uuid in multiple nodes */
    if (thrd != NULL && thrd->GetActiveTransaction() != NULL) {
        uint64 currXid = thrd->GetActiveTransaction()->GetCurrentXid().m_placeHolder;
        if (currXid + currTime >= (currXid > currTime ? currXid : currTime)) {
            currTime += thrd->GetActiveTransaction()->GetCurrentXid().m_placeHolder;
        }
    }
    /* give back g_nanoSeq we borrowed before */
    if (currTime > g_uuidTime && g_nanoSeq > 0) {
        uint32 giveBack = (uint32)Min(g_nanoSeq, (currTime - g_uuidTime) - 1);
        currTime -= giveBack;
        g_nanoSeq -= giveBack;
    }
    /* borrow an extra g_nanoSeq if currTime == g_uuidTime to prevent timestamp collision */
    if (currTime == g_uuidTime) {
        /*
         * If nanoseq overflows, we need to start over with a new numberspace, cause in the next loop,
         * the value of currTime may collide with an already generated value. So if g_nanoSeq overflows,
         * we won't increase currTime, then the currTime will equal to g_uuidTime, which will lead to
         * new numberspace.
         */
        if (likely(++g_nanoSeq)) {
            ++currTime;
        }
    }
    /*
     * if first time creating uuid, or sys time changed backward, we reset clockSeq to prevent time stamp duplication.
     * meanwhile, we reset g_nanoSeq.
     */
    if (unlikely(currTime <= g_uuidTime)) {
        PseudoRandRead(clockSeq, CLOCK_SEQ_CHAR_NUM);
        StringToHex(clockSeq, g_clockSeqWithMac, CLOCK_SEQ_CHAR_NUM, INT_RANGE_REVISE_PARAM);
        currTime = static_cast<uint64>(GetCurrentTimestampInSecond() * HUNDRED_NANO_SECOND);
        g_nanoSeq = 0;
    }
    return currTime;
}

void UuidGenerate(char *pdbUuid)
{
    if (pdbUuid == nullptr) {
        return;
    }
    errno_t rc = EOK;
    rc = memset_s(pdbUuid, FORMATTED_UUID_ARR_LEN, 0, FORMATTED_UUID_ARR_LEN);
    storage_securec_check(rc, "\0", "\0");

    uint64 currTime = g_uuidTime;
    /* two extra bytes, one for '-', one for '\0' */
    const int EXTRA_BYTES = 2;
    char clockSeqWithMac[UUID_MID_PART_LEN + UUID_LAST_PART_LEN + EXTRA_BYTES] = { 0 };

    pthread_mutex_lock(&g_uuidMutexLock);
    if (!g_nodeId) {
        g_nodeId = GetCurNodeId();
    }
    currTime = GetLatestCurrTime();
    g_uuidTime = currTime;
    /* we need a local copy during hold the lock, so it won't be changed by other thread. */
    rc = strcpy_s(clockSeqWithMac, UUID_MID_PART_LEN + UUID_LAST_PART_LEN + EXTRA_BYTES, g_clockSeqWithMac);
    storage_securec_check(rc, "\0", "\0");
    pthread_mutex_unlock(&g_uuidMutexLock);

    uint32 timestampLow = (uint32)(currTime & 0xFFFFFFFF);
    uint16 timestampMid = (uint16)((currTime >> UUID_timestampMid_OFFSET) & 0xFFFF);
    uint16 timestampHighAndV = (uint16)((currTime >> UUID_TIMESTAMP_HIGH_OFFSET) | ((g_nodeId & 0xFF) << 8));

    int cursor = 0;

    IntToHex(timestampLow, pdbUuid, UUID_FIRST_PART_LEN);
    cursor += UUID_FIRST_PART_LEN;
    pdbUuid[cursor++] = '-';

    IntToHex(timestampMid, pdbUuid + cursor, UUID_MID_PART_LEN);
    cursor += UUID_MID_PART_LEN;
    pdbUuid[cursor++] = '-';

    IntToHex(timestampHighAndV, pdbUuid + cursor, UUID_MID_PART_LEN);
    cursor += UUID_MID_PART_LEN;
    pdbUuid[cursor++] = '-';

    rc = strcpy_s(pdbUuid + cursor, FORMATTED_UUID_VALID_LEN + 1 - cursor, clockSeqWithMac);
    storage_securec_check(rc, "\0", "\0");
}

}