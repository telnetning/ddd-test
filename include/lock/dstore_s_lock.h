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

#ifndef DSTORE_S_LOCK_H
#define DSTORE_S_LOCK_H
#include <atomic>
#include <thread>
#ifdef __x86_64__
#include <xmmintrin.h>
#endif
#include "common/dstore_tsan_annotation.h"
#include "common/dstore_datatype.h"
namespace DSTORE {
/* Support for dynamic adjustment of spins_per_delay */
#define DEFAULT_SPINS_PER_DELAY 100

/*
 * Support for spin delay which is useful in various places where spinlock-like procedures take place.
 */
struct SpinDelayStatus {
    int spins;        /* spins */
    int delays;       /* delays */
    int cur_delay;    /* cur_delay */
    void *ptr;        /* ptr */
    const char *file; /* file */
    int line;         /* line */
};

static inline SpinDelayStatus InitSpinDelay(const char *fileName, int lineNumber)
{
    SpinDelayStatus dummy = {0, 0, 0, nullptr, fileName, lineNumber};
    return dummy;
}
void PerformSpinDelay(SpinDelayStatus *status, int spinsPerDelay);
void AdjustSpinsPerDelay(SpinDelayStatus *status, int &spinsPerDelay); /* It must be used without race condition. */

struct DstoreSpinLock {
    enum : uint8 { RELEASE = 0, LOCKED = 1 };
    std::atomic<uint8> lock;
    int spinsPerDelay;
    void Init() noexcept
    {
        lock.store(RELEASE);
        spinsPerDelay = DEFAULT_SPINS_PER_DELAY;
    }

    void Acquire() noexcept
    {
        SpinDelayStatus delayStatus = InitSpinDelay(__FILE__, __LINE__);
        while (lock.exchange(LOCKED, std::memory_order_acq_rel) != RELEASE) {
            do {
                PerformSpinDelay(&delayStatus, spinsPerDelay);
            } while (lock.load(std::memory_order_relaxed) == LOCKED);
        }
        AdjustSpinsPerDelay(&delayStatus, spinsPerDelay);
    }

    void Release() noexcept
    {
        lock.store(RELEASE, std::memory_order_release);
    }
};

static inline void SpinDelay(void)
{
    /*
     * This sequence is equivalent to the PAUSE instruction ("rep" is ignored by old IA32 processors if the following
     * instruction is not a string operation); the IA-32 Architecture Software Developer's Manual, Vol. 3, Section 7.7.2
     * describes why using PAUSE in the inner loop of a spin lock is necessary for good performance:
     *
     *     The PAUSE instruction improves the performance of IA-32 processors supporting Hyper-Threading Technology when
     *     executing spin-wait loops and other routines where one thread is accessing a shared lock or semaphore in
     *     a tight polling loop. When executing a spin-wait loop, the processor can suffer a severe performance penalty
     *     when exiting the loop because it detects a possible memory order violation and flushes the core processor's
     *     pipeline. The PAUSE instruction provides a hint to the processor that the code sequence is a spin-wait loop.
     *     The processor uses this hint to avoid the memory order violation and prevent the pipeline flush.
     *     In addition, the PAUSE instruction de-pipelines the spin-wait loop to prevent it from consuming execution
     *     resources excessively.
     */
#ifdef __x86_64__
    _mm_pause();
#else
    asm volatile("isb");
#endif
}
}  // namespace DSTORE
#endif /* STORAGE_S_LOCK_H */
