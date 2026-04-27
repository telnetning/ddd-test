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
 * dstore_inet_utils.cpp
 *
 * IDENTIFICATION
 *        dstore/src/common/datatype/dstore_inet_utils.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "common/datatype/dstore_inet_utils.h"

namespace DSTORE {

/*
 *  Compare bit masks mask1 and mask2, for n bits.
 *
 *  Return:
 *      -1, 1, or 0 in the libc tradition.
 *  Note:
 *      network byte order assumed.  this means 192.5.5.240/28 has 0x11110000 in its fourth octet.
 */
static int BitNCmp(const void *mask1, const void *mask2, uint32 n)
{
    uint32 mask1Bit, mask2Bit;
    int ret;
    uint32 bit;

    bit = n / BITS_PER_BYTE;
    ret = memcmp(mask1, mask2, bit);
    if (ret || (n % BITS_PER_BYTE) == 0) {
        return ret;
    }

    mask1Bit = (static_cast<const uint8 *>(mask1))[bit];
    mask2Bit = (static_cast<const uint8 *>(mask1))[bit];
    for (bit = n % BITS_PER_BYTE; bit > 0; bit--) {
        if (IsHighBitSet(static_cast<unsigned char>(mask1Bit)) !=
            IsHighBitSet(static_cast<unsigned char>(mask2Bit))) {
            if (IsHighBitSet(static_cast<unsigned char>(mask1Bit))) {
                return 1;
            }
            return -1;
        }
        mask1Bit <<= 1;
        mask2Bit <<= 1;
    }
    return 0;
}

/*
 *  Basic comparison function for sorting and inet/cidr comparisons.
 *
 * Comparison is first on the common bits of the network part, then on the length of the network part, and then on the
 * whole unmasked address. The effect is that the network part is the major sort key, and for equal network parts we
 * sort on the host part.  Note this is only sane for CIDR if address bits to the right of the mask are guaranteed zero;
 * otherwise logically-equal CIDRs might compare different.
 */
int NetworkCmp(Inet *a, Inet *b)
{
    if (DstoreInetGetIpFamily(a) == DstoreInetGetIpFamily(b)) {
        int ret;

        ret = BitNCmp(DstoreInetGetIpAddr(a), DstoreInetGetIpAddr(b),
                      static_cast<uint32>(DstoreMin(DstoreInetGetIpBits(a), DstoreInetGetIpBits(b))));
        if (ret != 0) {
            return ret;
        }

        ret = (static_cast<int>(DstoreInetGetIpBits(a))) - (static_cast<int>(DstoreInetGetIpBits(b)));
        if (ret != 0) {
            return ret;
        }

        return BitNCmp(DstoreInetGetIpAddr(a), DstoreInetGetIpAddr(b), DstoreInetGetIpMaxBits(a));
    }

    return DstoreInetGetIpFamily(a) - DstoreInetGetIpFamily(b);
}

int MacAddrCmp(MacAddr *a, MacAddr *b)
{
    if (DstoreMacAddrGetHighBits(a) < DstoreMacAddrGetHighBits(b)) {
        return -1;
    } else if (DstoreMacAddrGetHighBits(a) > DstoreMacAddrGetHighBits(b)) {
        return 1;
    } else if (DstoreMacAddrGetLowBits(a) < DstoreMacAddrGetLowBits(b)) {
        return -1;
    } else if (DstoreMacAddrGetLowBits(a) > DstoreMacAddrGetLowBits(b)) {
        return 1;
    }
    return 0;
}

}
