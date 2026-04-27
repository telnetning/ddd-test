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
 * dstore_inet_utils.h
 *
 * IDENTIFICATION
 *        dstore/include/common/datatype/dstore_inet_utils.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_DSTORE_INET_UTILS_H
#define DSTORE_DSTORE_INET_UTILS_H

#include "common/dstore_datatype.h"

namespace DSTORE {

constexpr uint8 DSTORE_AF_INET = 2;  /* Ip address family, IPv4 */
constexpr uint8 DSTORE_AF_INET6 = 3; /* Ip address family, IPv6 */
constexpr uint32 IPV4_MAX_BITS = 32;
constexpr uint32 IPV6_MAX_BITS = 128;

/*
 *  Internal storage format for IP addresses (both INET and CIDR datatypes):
 */
struct InetData {
    uint8 family;     /* DSTORE_AF_INET or DSTORE_AF_INET6 */
    uint8 bits;       /* number of bits in netmask */
    uint8 ipAddr[16]; /* up to 128 bits of address */
};

/*
 * Both INET and CIDR addresses are represented varlena objects, ie, there is a varlena header in front of the struct
 * type depicted above.	This struct depicts what we actually have in memory in "uncompressed" cases.  Note that since
 * the maximum data size is only 18 bytes, INET/CIDR will invariably be stored into tuples using the 1-byte-header
 * varlena format.  However, we have to be prepared to cope with the 4-byte-header format too, because various code may
 * helpfully try to "decompress" 1-byte-header datums.
 */
struct Inet {
    char vlLen[4]; /* Do not touch this field directly! */
    InetData inetData;
};

inline Inet *DstoreDatumGetInet(Datum datum)
{
    return static_cast<Inet *>(static_cast<void *>(DatumGetPointer(datum)));
}
inline uint8 DstoreInetGetIpFamily(Inet *inetPtr)
{
    char *ptr = VarDataAny(inetPtr);
    return static_cast<InetData *>(static_cast<void *>(ptr))->family;
}
inline uint8 DstoreInetGetIpBits(Inet *inetPtr)
{
    char *ptr = VarDataAny(inetPtr);
    return static_cast<InetData *>(static_cast<void *>(ptr))->bits;
}
inline uint8 *DstoreInetGetIpAddr(Inet *inetPtr)
{
    char *ptr = VarDataAny(inetPtr);
    return static_cast<InetData *>(static_cast<void *>(ptr))->ipAddr;
}
inline uint32 DstoreInetGetIpMaxBits(Inet *inetPtr)
{
    return (DstoreInetGetIpFamily(inetPtr) == DSTORE_AF_INET) ? IPV4_MAX_BITS : IPV6_MAX_BITS;
}

constexpr int MAC_ADDR_FIRST_BYTE_SHIFT = 16;
constexpr int MAC_ADDR_SECOND_BYTE_SHIFT = 8;
/* Internal storage format for MAC addresses */
struct MacAddr {
    uint8 a;
    uint8 b;
    uint8 c;
    uint8 d;
    uint8 e;
    uint8 f;
};

inline MacAddr *DstoreDatumGetMacaddr(Datum datum)
{
    return static_cast<MacAddr *>(static_cast<void *>(DatumGetPointer(datum)));
}
inline unsigned long DstoreMacAddrGetHighBits(MacAddr *macAddr)
{
    return static_cast<unsigned long>((macAddr->a << MAC_ADDR_FIRST_BYTE_SHIFT) |
                                      (macAddr->b << MAC_ADDR_SECOND_BYTE_SHIFT) | (macAddr->c));
}
inline unsigned long DstoreMacAddrGetLowBits(MacAddr *macAddr)
{
    return static_cast<unsigned long>((macAddr->d << MAC_ADDR_FIRST_BYTE_SHIFT) |
                                      (macAddr->e << MAC_ADDR_SECOND_BYTE_SHIFT) | (macAddr->f));
}

/* Comparison function for sorting */
int NetworkCmp(Inet *a, Inet *b);
int MacAddrCmp(MacAddr *a, MacAddr *b);

}  // namespace DSTORE

#endif  // DSTORE_STORAGE_INET_UTILS_H
