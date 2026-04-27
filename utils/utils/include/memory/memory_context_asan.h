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

#ifndef UTILS_MEMORY_CONTEXT_ASAN_H
#define UTILS_MEMORY_CONTEXT_ASAN_H

#include <stdbool.h>
#include "defines/common.h"

#define SANITIZE_MEMORY_CONTEXT

#define ASAN_ACCESS_MASK        0b111         /* the asan access mask is low 3 bit */
#define ASAN_ACCESS_ADDRESSABLE (uint8_t)0x0  /* Addressable:             00, all the 8 bytes are can write */
#define ASAN_ACCESS_DENY        (uint8_t)0xf7 /* Poisoned by user:        f7, all the 8 bytes access forbidden */

#if defined(__SANITIZE_ADDRESS__) && defined(SANITIZE_MEMORY_CONTEXT)

/* the following code is decidied according to gcc source file,
 * libsanitizer/asan/asan_mapping.h
 * libsanitizer/sanitizer_common/sanitizer_platform.h
 * arm64 is used kAArch64_ShadowOffset64 and x86_64 is used kDefaultShort64bitShadowOffset */
static const uint64_t kDefaultShort64bitShadowOffset = 0x7FFFFFFF & (~0xFFFULL << 3); // < 2G.
static const uint64_t kAArch64_ShadowOffset64 = 1ULL << 36;

#ifdef __x86_64__
#define ASAN_SHADOW_OFFSET kDefaultShort64bitShadowOffset
static inline __attribute__((no_sanitize_address)) void MarkAsanShadow(void *address, uint8_t type)
{
    *((uint8_t *)((uintptr_t)address >> 3) + ASAN_SHADOW_OFFSET) = type; // asan 8(2^3) bytes to 1 byte shadow memory
}
#elif defined __aarch64__
#define ASAN_SHADOW_OFFSET kAArch64_ShadowOffset64
/*
 * mark memory context custom address poisoned type
 * performance sensitive, __attribute__((no_sanitize("address"))) will alway lead to no inline
 */
static inline void MarkAsanShadow(void *address, uint8_t type)
{
    uintptr_t offset = ASAN_SHADOW_OFFSET;
    asm volatile("lsr  x15, %0, #3    \n\t"
                 "strb %w1, [x15, %2] \n\t"
                 :
                 : "r"(address), "r"(type), "r"(offset)
                 : "x15");
}
#endif /* __x86_64__ */

static inline void PoisonedAddress(void *address)
{
    uint8_t type = (uint8_t)((uintptr_t)address & ASAN_ACCESS_MASK);
    /* for asan check, only marked the first byte in sentinel, if the OOB write is skip the first byte,
       this case maybe escape detection by asan */
    if (type == ASAN_ACCESS_ADDRESSABLE) {
        /* transfer ASAN_ACCESS_ADDRESSABLE to ASAN_ACCESS_DENY for out of bounds write detection purposes */
        type = ASAN_ACCESS_DENY;
    }
    /* type 1 to 7 is parital access, these types do not require conversion and already can intercept
     * out of bounds write effectively */
    MarkAsanShadow(address, type);
}

#else
static inline void MarkAsanShadow(SYMBOL_UNUSED void *address, SYMBOL_UNUSED uint8_t type)
{}

static inline void PoisonedAddress(SYMBOL_UNUSED void *address)
{}
#endif /* __SANITIZE_ADDRESS__ && SANITIZE_MEMORY_CONTEXT */

/**
 * set user area Sentinel poisone, if the user area Sentinel poisone is cleared by UnPoisonedSentinel
 *
 * @pointer    - user pointer
 * @userSize   - user memory size
 */
static inline void PoisonedSentinel(void *pointer, size_t userSize)
{
    SYMBOL_UNUSED uint8_t *magic = (uint8_t *)((uintptr_t)pointer + userSize);
    PoisonedAddress(magic);
}

/* unset user area Sentinel poisone, when this user area pointer is freed if need */
static inline void UnPoisonedSentinel(void *pointer, size_t userSize)
{
    SYMBOL_UNUSED uint8_t *magic = (uint8_t *)((uintptr_t)pointer + userSize);
    MarkAsanShadow(magic, ASAN_ACCESS_ADDRESSABLE);
}

#endif /* UTILS_MEMORY_CONTEXT_ASAN_H */
