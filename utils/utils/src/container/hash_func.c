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
#include "types/data_types.h"
#include "container/hash_table.h"
#include "container/bitmap.h"

/*
 * My best guess at if you are big-endian or little-endian.  This may
 * need adjustment.
 */
#if (defined(__BYTE_ORDER) && defined(__LITTLE_ENDIAN) && __BYTE_ORDER == __LITTLE_ENDIAN) || \
    (defined(i386) || defined(__i386__) || defined(__i586__) || defined(__i686__))
#define HASH_LITTLE_ENDIAN 1
#elif defined(__BYTE_ORDER) && defined(__BIG_ENDIAN) && __BYTE_ORDER == __BIG_ENDIAN
#define HASH_LITTLE_ENDIAN 0
#else
#define HASH_LITTLE_ENDIAN 1 /* default little endian */
#endif

#define BITS_PER_WORD 32
UTILS_INLINE static uint32_t Rot(uint32_t x, uint32_t bits)
{
    return (x << bits) | (x >> (BITS_PER_WORD - bits));
}

/*
 * Mix -- Mix 3 32-bit values reversibly.
 *
 * This is reversible, so any information in (&x,&x,&y) before Mix() is
 * still in (&x,&y,&z) after Mix().
 *
 * If four pairs of (a,b,c) inputs are run through Mix(), or through
 * Mix() in reverse, there are at least 32 bits of the output that
 * are sometimes the same for one pair and different for another pair.
 * This was tested for:
 * * pairs that differed by one bit, by two bits, in any combination
 *   of top bits of (a,b,c), or in any combination of bottom bits of
 *   (a,b,c).
 * * "differ" is defined as +, -, ^, or ~^.  For + and -, I transformed
 *   the output delta to a Gray code (a^(a>>1)) so a string of 1's (as
 *   is commonly produced by subtraction) look like a single 1-bit
 *   difference.
 * * the base values were pseudorandom, all zero but one bit set, or
 *   all zero plus a counter that starts at zero.
 *
 * Some k values for my "a-=c; a^=Rot(c,k); c+=b;" arrangement that
 * satisfy this are
 *     4  6  8 16 19  4
 *     9 15  3 18 27 15
 *    14  9  3  7 17  3
 * This does not achieve avalanche.  There are input bits of (a,b,c)
 * that fail to affect some output bits of (a,b,c), especially of a.  The
 * most thoroughly Mixed value is c, but it doesn't really even achieve
 * avalanche in c.
 *
 * This allows some parallelism.  Read-after-writes are good at doubling
 * the number of bits affected, so the goal of Mixing pulls in the opposite
 * direction as the goal of parallelism.  I did what I could.  Rotates
 * seem to cost as much as shifts on every machine I could lay my hands
 * on, and Rotates are much kinder to the top and bottom bits, so I used
 * Rotates.
 */
UTILS_INLINE static void Mix(uint32_t *x, uint32_t *y, uint32_t *z)
{
    uint32_t a = *x;
    uint32_t b = *y;
    uint32_t c = *z;
    a -= c;
    /* 4, 6, 8, 16, 19, 4, paramter from avalanche test*/
    a ^= Rot(c, 4);
    c += b;
    b -= a;
    b ^= Rot(a, 6);
    a += c;
    c -= b;
    c ^= Rot(b, 8);
    b += a;
    a -= c;
    a ^= Rot(c, 16);
    c += b;
    b -= a;
    b ^= Rot(a, 19);
    a += c;
    c -= b;
    c ^= Rot(b, 4);
    b += a;

    *x = a;
    *y = b;
    *z = c;
}

/*
 * Final -- Final Mixing of 3 32-bit values (a, b, c) into c
 *
 * Pairs of (a,b,c) values differing in only a few bits will usually
 * produce values of c that look totally different.  This was tested for
 * * pairs that differed by one bit, by two bits, in any combination
 *   of top bits of (a,b,c), or in any combination of bottom bits of
 *   (a,b,c).
 * * "differ" is defined as +, -, ^, or ~^.  For + and -, I transformed
 *   the output delta to a Gray code (a^(a>>1)) so a string of 1's (as
 *   is commonly produced by subtraction) look like a single 1-bit
 *   difference.
 * * the base values were pseudorandom, all zero but one bit set, or
 *   all zero plus a counter that starts at zero.
 *
 * These constants passed:
 *  14 11 25 16 4 14 24
 *  12 14 25 16 4 14 24
 * and these came close:
 *   4  8 15 26 3 22 24
 *  10  8 15 26 3 22 24
 *  11  8 15 26 3 22 24
 */
UTILS_INLINE static uint32_t Final(uint32_t x, uint32_t y, uint32_t z)
{
    uint32_t a = x;
    uint32_t b = y;
    uint32_t c = z;
    c ^= b;
    /* 14, 11, 25, 16, 4, 14, 24, paramter from avalanche test */
    c -= Rot(b, 14);
    a ^= c;
    a -= Rot(c, 11);
    b ^= a;
    b -= Rot(a, 25);
    c ^= b;
    c -= Rot(b, 16);
    a ^= c;
    a -= Rot(c, 4);
    b ^= a;
    b -= Rot(a, 14);
    c ^= b;
    c -= Rot(b, 24);

    return c;
}

#define INTERNAL_STATE_VALUE (0xdeadbeef + 0x3bdc96)

/*
 * HashUint32() -- hash a 32-bit value
 *
 * This has the same result as: HashAny(&k, sizeof(uint32_t))
 * but is faster and doesn't force the caller to store k into memory.
 */
UTILS_INLINE static uint32_t HashUint32(uint32_t k)
{
    uint32_t length = (uint32_t)sizeof(uint32_t);
    /* Set up the internal state */
    uint32_t a = INTERNAL_STATE_VALUE + length;
    uint32_t b = INTERNAL_STATE_VALUE + length;
    uint32_t c = INTERNAL_STATE_VALUE + length;

    a += k;
    /*------------------------------------------------------ report the result */
    return Final(a, b, c);
}

#if HASH_LITTLE_ENDIAN == 1
#define ONE_BYTE_TO_BIT24 (24)
#define ONE_BYTE_TO_BIT16 (16)
#define ONE_BYTE_TO_BIT8  (8)
#define ONE_BYTE_TO_BIT0  (0)
#define TWO_BYTE_TO_BIT16 (16)
#define TWO_BYTE_TO_BIT0  (0)
#else /* HASH_BIG_ENDIAN */
#define ONE_BYTE_TO_BIT24 (0)
#define ONE_BYTE_TO_BIT16 (8)
#define ONE_BYTE_TO_BIT8  (16)
#define ONE_BYTE_TO_BIT0  (24)
#define TWO_BYTE_TO_BIT16 (0)
#define TWO_BYTE_TO_BIT0  (16)
#endif

UTILS_INLINE static uint32_t LeftShift(uint32_t k, uint32_t shift)
{
    return (k << shift);
}

#define LENGTH_PER_PAIR 12

static uint32_t HashAnyAligned4(const unsigned char *key, int keyLen)
{
    uint32_t length = (uint32_t)keyLen;
    /* Set up the internal state */
    uint32_t a = INTERNAL_STATE_VALUE + length;
    uint32_t b = INTERNAL_STATE_VALUE + length;
    uint32_t c = INTERNAL_STATE_VALUE + length;
    const uint32_t *k = (const uint32_t *)(uintptr_t)key; /* read 32-bit chunks */
    /*------ all but last block: aligned reads and affect 32 bits of (a,b,c) */
    while (length > LENGTH_PER_PAIR) {
        a += *k++;
        b += *k++;
        c += *k++;
        Mix(&a, &b, &c);
        length -= LENGTH_PER_PAIR;
    }
    const uint8_t *k8 = (const uint8_t *)k;
    /*----------------------------- handle the last (probably partial) block */
    switch (length) {
        case 12: /* last 12 bytes, deal with once times */
            a += *k;
            b += *(++k);
            c += *(++k);
            break;
        case 11: /* last 11 bytes, deal with the byte 10, and take it to 16 ~ 23 bit in c */
            c += LeftShift((uint32_t)k8[10], ONE_BYTE_TO_BIT16);
            /* fall-through */
        case 10: /* last 10 bytes, deal with the byte 9, and take it to 8 ~ 15 bit in c */
            c += LeftShift((uint32_t)k8[9], ONE_BYTE_TO_BIT8);
            /* fall-through */
        case 9: /* last 9 bytes, deal with the byte 8, and take it to 0 ~ 7 bit in c */
            c += LeftShift((uint32_t)k8[8], ONE_BYTE_TO_BIT0);
            /* fall-through */
        case 8: /* last 8 bytes, deal with once times */
            a += *k;
            b += *(++k);
            break;
        case 7: /* last 7 bytes, deal with the byte 6, and take it to 16 ~ 23 bit in b */
            b += LeftShift((uint32_t)k8[6], ONE_BYTE_TO_BIT16);
            /* fall-through */
        case 6: /* last 6 bytes, deal with the byte 5, and take it to 8 ~ 15 bit in b */
            b += LeftShift((uint32_t)k8[5], ONE_BYTE_TO_BIT8);
            /* fall-through */
        case 5: /* last 5 bytes, deal with the byte 4, and take it to 0 ~ 7 bit in b */
            b += LeftShift((uint32_t)k8[4], ONE_BYTE_TO_BIT0);
            /* fall-through */
        case 4: /* last 4 bytes, deal with once times */
            a += *k;
            break;
        case 3: /* last 3 bytes, deal with the byte 2, and take it to 16 ~ 23 bit in a */
            a += LeftShift((uint32_t)k8[2], ONE_BYTE_TO_BIT16);
            /* fall-through */
        case 2: /* last 2 bytes, deal with the byte 1, and take it to 8 ~ 15 bit in a */
            a += LeftShift((uint32_t)k8[1], ONE_BYTE_TO_BIT8);
            /* fall-through */
        case 1: /* last 1 bytes, deal with the byte 0, and take it to 0 ~ 7 bit in a */
            a += LeftShift((uint32_t)k8[0], ONE_BYTE_TO_BIT0);
            break;
        default:
            return c; /* zero length requires no Mixing */
    }
    return Final(a, b, c);
}

static uint32_t HashAnyAligned2(const unsigned char *key, int keyLen)
{
    uint32_t length = (uint32_t)keyLen;
    /* Set up the internal state */
    uint32_t a = INTERNAL_STATE_VALUE + length;
    uint32_t b = INTERNAL_STATE_VALUE + length;
    uint32_t c = INTERNAL_STATE_VALUE + length;
    const uint16_t *k = (const uint16_t *)(uintptr_t)key; /* read 16-bit chunks */

    /*--------------- all but last block: aligned reads and different Mixing */
    while (length > LENGTH_PER_PAIR) {
        a += LeftShift((uint32_t)(*k++), TWO_BYTE_TO_BIT0);
        a += LeftShift((uint32_t)(*k++), TWO_BYTE_TO_BIT16);
        b += LeftShift((uint32_t)(*k++), TWO_BYTE_TO_BIT0);
        b += LeftShift((uint32_t)(*k++), TWO_BYTE_TO_BIT16);
        c += LeftShift((uint32_t)(*k++), TWO_BYTE_TO_BIT0);
        c += LeftShift((uint32_t)(*k++), TWO_BYTE_TO_BIT16);
        length -= LENGTH_PER_PAIR;
        Mix(&a, &b, &c);
    }

    /*----------------------------- handle the last (probably partial) block */
    const uint8_t *k8 = (const uint8_t *)k;
    switch (length) {
        case 12: /* last 12 bytes, deal with once times */
            a += LeftShift((uint32_t)(*k), TWO_BYTE_TO_BIT0);
            a += LeftShift((uint32_t)(*(++k)), TWO_BYTE_TO_BIT16);
            b += LeftShift((uint32_t)(*(++k)), TWO_BYTE_TO_BIT0);
            b += LeftShift((uint32_t)(*(++k)), TWO_BYTE_TO_BIT16);
            c += LeftShift((uint32_t)(*(++k)), TWO_BYTE_TO_BIT0);
            c += LeftShift((uint32_t)(*(++k)), TWO_BYTE_TO_BIT16);
            break;
        case 11: /* last 11 bytes, deal with the byte 10, and take it to 16 ~ 23 bit in c */
            c += LeftShift((uint32_t)k8[10], TWO_BYTE_TO_BIT16);
            /* fall-through */
        case 10: /* last 10 bytes, deal with  once times */
            a += LeftShift((uint32_t)(*k), TWO_BYTE_TO_BIT0);
            a += LeftShift((uint32_t)(*(++k)), TWO_BYTE_TO_BIT16);
            b += LeftShift((uint32_t)(*(++k)), TWO_BYTE_TO_BIT0);
            b += LeftShift((uint32_t)(*(++k)), TWO_BYTE_TO_BIT16);
            c += LeftShift((uint32_t)(*(++k)), TWO_BYTE_TO_BIT0);
            break;
        case 9: /* last 9 bytes, deal with the byte 8, and take it to 0 ~ 7 bit in c */
            c += LeftShift((uint32_t)k8[8], TWO_BYTE_TO_BIT0);
            /* fall-through */
        case 8: /* last 8 bytes, deal with  once times */
            a += LeftShift((uint32_t)(*k), TWO_BYTE_TO_BIT0);
            a += LeftShift((uint32_t)(*(++k)), TWO_BYTE_TO_BIT16);
            b += LeftShift((uint32_t)(*(++k)), TWO_BYTE_TO_BIT0);
            b += LeftShift((uint32_t)(*(++k)), TWO_BYTE_TO_BIT16);
            break;
        case 7: /* last 7 bytes, deal with the byte 6, and take it to 16 ~ 23 bit in b */
            b += LeftShift((uint32_t)k8[6], TWO_BYTE_TO_BIT16);
            /* fall-through */
        case 6: /* last 6 bytes, deal with  once times */
            a += LeftShift((uint32_t)(*k), TWO_BYTE_TO_BIT0);
            a += LeftShift((uint32_t)(*(++k)), TWO_BYTE_TO_BIT16);
            b += LeftShift((uint32_t)(*(++k)), TWO_BYTE_TO_BIT0);
            break;
        case 5: /* last 5 bytes, deal with the byte 4, and take it to 0 ~ 7 bit in b */
            b += LeftShift((uint32_t)k8[4], TWO_BYTE_TO_BIT0);
            /* fall-through */
        case 4: /* last 4 bytes, deal with  once times */
            a += LeftShift((uint32_t)(*k), TWO_BYTE_TO_BIT0);
            a += LeftShift((uint32_t)(*(++k)), TWO_BYTE_TO_BIT16);
            break;
        case 3: /* last 3 bytes, deal with the byte 2, and take it to 16 ~ 23 bit in a */
            a += LeftShift((uint32_t)k8[2], TWO_BYTE_TO_BIT16);
            /* fall-through */
        case 2: /* last 2 bytes, deal with  once times */
            a += LeftShift((uint32_t)(*k), TWO_BYTE_TO_BIT0);
            break;
        case 1: /* last 1 bytes, deal with the byte 0, and take it to 0 ~ 7 bit in a */
            a += LeftShift((uint32_t)k8[0], TWO_BYTE_TO_BIT0);
            break;
        default:
            return c; /* zero length requires no Mixing */
    }
    return Final(a, b, c);
}

static uint32_t HashAnyNoAligned(const unsigned char *key, int keyLen)
{
    uint32_t length = (uint32_t)keyLen;
    /* Set up the internal state */
    uint32_t a = INTERNAL_STATE_VALUE + length;
    uint32_t b = INTERNAL_STATE_VALUE + length;
    uint32_t c = INTERNAL_STATE_VALUE + length;
    const uint8_t *k = (const uint8_t *)key;

    /*--------------- all but the last block: affect some 32 bits of (a,b,c) */
    while (length > LENGTH_PER_PAIR) {
        a += LeftShift((uint32_t)(*k++), ONE_BYTE_TO_BIT0);
        a += LeftShift((uint32_t)(*k++), ONE_BYTE_TO_BIT8);
        a += LeftShift((uint32_t)(*k++), ONE_BYTE_TO_BIT16);
        a += LeftShift((uint32_t)(*k++), ONE_BYTE_TO_BIT24);
        b += LeftShift((uint32_t)(*k++), ONE_BYTE_TO_BIT0);
        b += LeftShift((uint32_t)(*k++), ONE_BYTE_TO_BIT8);
        b += LeftShift((uint32_t)(*k++), ONE_BYTE_TO_BIT16);
        b += LeftShift((uint32_t)(*k++), ONE_BYTE_TO_BIT24);
        c += LeftShift((uint32_t)(*k++), ONE_BYTE_TO_BIT0);
        c += LeftShift((uint32_t)(*k++), ONE_BYTE_TO_BIT8);
        c += LeftShift((uint32_t)(*k++), ONE_BYTE_TO_BIT16);
        c += LeftShift((uint32_t)(*k++), ONE_BYTE_TO_BIT24);
        Mix(&a, &b, &c);
        length -= LENGTH_PER_PAIR;
    }

    /*-------------------------------- last block: affect all 32 bits of (c) */
    switch (length) {
        case 12: /* last 12 bytes, deal with the byte 11, and take it to 24 ~ 31 bit in c */
            c += LeftShift((uint32_t)k[11], ONE_BYTE_TO_BIT24);
            /* fall-through */
        case 11: /* last 11 bytes, deal with the byte 10, and take it to 16 ~ 23 bit in c */
            c += LeftShift((uint32_t)k[10], ONE_BYTE_TO_BIT16);
            /* fall-through */
        case 10: /* last 10 bytes, deal with the byte 9, and take it to 8 ~ 15 bit in c */
            c += LeftShift((uint32_t)k[9], ONE_BYTE_TO_BIT8);
            /* fall-through */
        case 9: /* last 9 bytes, deal with the byte 8, and take it to 0 ~ 7 bit in c */
            c += LeftShift((uint32_t)k[8], ONE_BYTE_TO_BIT0);
            /* fall-through */
        case 8: /* last 8 bytes, deal with the byte 7, and take it to 24 ~ 31 bit in b */
            b += LeftShift((uint32_t)k[7], ONE_BYTE_TO_BIT24);
            /* fall-through */
        case 7: /* last 7 bytes, deal with the byte 6, and take it to 16 ~ 23 bit in b */
            b += LeftShift((uint32_t)k[6], ONE_BYTE_TO_BIT16);
            /* fall-through */
        case 6: /* last 6 bytes, deal with the byte 5, and take it to 8 ~ 15 bit in b */
            b += LeftShift((uint32_t)k[5], ONE_BYTE_TO_BIT8);
            /* fall-through */
        case 5: /* last 5 bytes, deal with the byte 4, and take it to 0 ~ 7 bit in b */
            b += LeftShift((uint32_t)k[4], ONE_BYTE_TO_BIT0);
            /* fall-through */
        case 4: /* last 4 bytes, deal with the byte 3, and take it to 24 ~ 31 bit in c */
            a += LeftShift((uint32_t)k[3], ONE_BYTE_TO_BIT24);
            /* fall-through */
        case 3: /* last 3 bytes, deal with the byte 2, and take it to 16 ~ 23 bit in c */
            a += LeftShift((uint32_t)k[2], ONE_BYTE_TO_BIT16);
            /* fall-through */
        case 2: /* last 2 bytes, deal with the byte 1, and take it to 8 ~ 15 bit in c */
            a += LeftShift((uint32_t)k[1], ONE_BYTE_TO_BIT8);
            /* fall-through */
        case 1: /* last 1 bytes, deal with the byte 0, and take it to 0 ~ 7 bit in c */
            a += LeftShift((uint32_t)k[0], ONE_BYTE_TO_BIT0);
            break;
        default:
            return c; /* zero length requires no Mixing */
    }
    return Final(a, b, c);
}

/*
 * HashAny() -- hash a variable-length key into a 32-bit value
 *		key     : the key (the unaligned variable-length array of bytes)
 *		length  : the length of the key, counting by bytes
 *
 * Returns a 32-bit value.  Every bit of the key affects every bit of
 * the return value.  Two keys differing by one or two bits will have
 * totally different hash values.
 *
 * The best hash table sizes are powers of 2.  There is no need to do
 * mod a prime (mod is sooo slow!).
 */
uint32_t HashAny(const unsigned char *key, int length)
{
    if (((uintptr_t)key & 0x3) == 0) { /* 0x3 in lowerest two bit, the address is 32-bit aligned */
        return HashAnyAligned4(key, length);
    } else if (((uintptr_t)key & 0x1) == 0) {
        return HashAnyAligned2(key, length);
    } else { /* not aligned completely, need to read the key one byte at a time */
        return HashAnyNoAligned(key, length);
    }
}

/*
 * StringHash: hash function for keys that are NUL-terminated strings.
 *
 * NOTE: this is the default hash function if none is specified.
 */
UTILS_EXPORT uint32_t StringHash(const void *key, size_t keySize)
{
    /*
     * If the string exceeds keySize-1 bytes, we want to hash only that many,
     * because when it is copied into the hash table it will be truncated at
     * that length.
     */
    size_t len = strlen((const char *)key);

    len = Min(len, keySize - 1);
    return HashAny((const unsigned char *)key, (int)len);
}

/*
 * TagHash: hash function for fixed-size tag values
 */
UTILS_EXPORT uint32_t TagHash(const void *key, size_t keySize)
{
    return HashAny((const unsigned char *)key, (int)keySize);
}

/*
 * Uint32Hash: hash function for keys that are uint32 or int32
 *
 * (TagHash works for this case too, but is slower)
 */
UTILS_EXPORT uint32_t Uint32Hash(const void *key, SYMBOL_UNUSED size_t keySize)
{
    ASSERT(keySize == sizeof(uint32_t));
    return HashUint32(*((const uint32_t *)key));
}

/*
 * OidHash: hash function for keys that are OIDs
 *
 * (TagHash works for this case too, but is slower)
 */
UTILS_EXPORT uint32_t OidHash(const void *key, SYMBOL_UNUSED size_t keySize)
{
    ASSERT(keySize == sizeof(Oid));
    return HashUint32((uint32_t)(*((const Oid *)key)));
}

UTILS_EXPORT int BitmapMatch(const void *key1, const void *key2, SYMBOL_UNUSED size_t keysize)
{
    ASSERT(keysize == sizeof(Bitmap *));
    return (int)(!BitmapEqual(*((const Bitmap *const *)key1), *((const Bitmap *const *)key2)));
}

UTILS_EXPORT uint32_t BitmapHash(const void *key, SYMBOL_UNUSED size_t keysize)
{
    ASSERT(keysize == sizeof(Bitmap *));
    return BitmapHashValue(*((const Bitmap *const *)key));
}
