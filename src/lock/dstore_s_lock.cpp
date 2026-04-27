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
 * Hardware-dependent implementation of spinlocks.
 *
 * When waiting for a contended spinlock we loop tightly for awhile, then delay using GaussUsleep() and try again.
 * Preferably, "awhile" should be a small multiple of the maximum time we expect a spinlock to be held.  100
 * iterations seems about right as an initial guess.  However, on a uniprocessor the loop is a waste of cycles,
 * while in a multi-CPU scenario it's usually better to spin a bit longer than to call the kernel, so we try
 * to adapt the spin loop count depending on whether we seem to be in a uniprocessor or multiprocessor.
 *
 * Note: you might think MIN_SPINS_PER_DELAY should be just 1, but you'd be wrong; there are platforms where that
 * can result in a "stuck spinlock" failure.  This has been seen particularly on Alphas; it seems that the first TAS
 * after returning from kernel space will always fail on that hardware.
 *
 * Once we do decide to block, we use randomly increasing GaussUsleep()  delays. The first delay is 1 msec,
 * then the delay randomly increases to about one second, after which we reset to 1 msec and start again.
 * The idea here is tha in the presence of heavy contention we need to increase the delay, else the
 * spinlock holder may never get to run and release the lock.  (Consider situation where spinlock holder
 * has been nice'd down in priority by the scheduler
 * --- it will not get scheduled until all would-be acquirers are sleeping, so if we always use a 1-msec sleep,
 * there is a real possibility of starvation.)  But we can't just clamp the delay to an upper bound,
 * else it would take a long time to make a reasonable number of tries.
 *
 * We time out and declare error after NUM_DELAYS delays (thus, exactly that many tries).  With the given settings,
 * this will usually take 2 or so minutes.  It seems better to fix the total number of tries (and thus
 * the probability of unintended failure) than to fix the total time spent.
 *
 * -------------------------------------------------------------------------------------------------------------
 */
#include "lock/dstore_s_lock.h"
#include <cstdlib>
#include "common/error/dstore_error.h"
#include "port/dstore_port.h"
#include "common/dstore_datatype.h"
#include "framework/dstore_thread.h"
#include "errorcode/dstore_lock_error_code.h"

namespace DSTORE {

#define NUM_DELAYS 1000
#define MIN_DELAY_USEC 1000L
#define MAX_DELAY_USEC 1000000L

#define MIN_SPINS_PER_DELAY 10
#define MAX_SPINS_PER_DELAY 1000
static constexpr double DELAY_MAX_RANDOM_VALUE = static_cast<double>(MAX_RANDOM_VALUE);
static constexpr double DELAY_RANDOM_FACTOR = 0.5;

/* SpinLockStuck() - complain about a stuck spinlock */
static void SpinLockStuck(const char *file, int line)
{
    ErrLog(DSTORE_WARNING, MODULE_LOCK, ErrMsg("stuck spinlock detected at %s:%d.", file, line));
}

/*
 * Wait while spinning on a contended spinlock.
 */
void PerformSpinDelay(SpinDelayStatus *s, int spinsPerDelay)
{
    /* CPU-specific delay each time through the loop */
    SpinDelay();

    /* Block the process every spins_per_delay tries */
    if (++(s->spins) >= spinsPerDelay) {
        if ((++(s->delays) % NUM_DELAYS) == 0) {
            SpinLockStuck(s->file, s->line);
        }
        if (s->cur_delay == 0) { /* first time to delay? */
            s->cur_delay = MIN_DELAY_USEC;
        }
        GaussUsleep(s->cur_delay);

        /* increase delay by a random fraction between 1X and 2X */
        s->cur_delay += static_cast<int>(
            s->cur_delay * (static_cast<double>(random()) / DELAY_MAX_RANDOM_VALUE) + DELAY_RANDOM_FACTOR);

        /* wrap back to minimum delay when max is exceeded */
        if (s->cur_delay > MAX_DELAY_USEC) {
            s->cur_delay = MIN_DELAY_USEC;
        }

        s->spins = 0;
    }
}

/*
 * After acquiring a spinlock, update estimates about how long to loop.
 *
 * If we were able to acquire the lock without delaying, it's a good indication we are in a multiprocessor.
 * If we had to delay, it's a sign (but not a sure thing) that we are in a uniprocessor. Hence, we decrement
 * spins_per_delay slowly when we had to delay, and increase it rapidly when we didn't.  It's expected that
 * spins_per_delay will converge to the minimum value on a uniprocessor and to the maximum value on a multiprocessor.
 *
 * Note: spins_per_delay is local within our current process. We want to average these observations across
 * multiple backends, since it's relatively rare for this function to even get entered, and so a single backend
 * might not live long enough to converge on a good value. That is handled by the two routines below.
 */
void AdjustSpinsPerDelay(SpinDelayStatus *status, int &spinsPerDelay)
{
    if (status->cur_delay == 0) {
        /* we never had to delay */
        if (spinsPerDelay < MAX_SPINS_PER_DELAY) {
            spinsPerDelay = DstoreMin(spinsPerDelay + 100, MAX_SPINS_PER_DELAY); /* 100 is by experience */
        }
    } else {
        if (spinsPerDelay > MIN_SPINS_PER_DELAY) {
            spinsPerDelay = DstoreMax(spinsPerDelay - 1, MIN_SPINS_PER_DELAY);
        }
    }
}

}  // namespace DSTORE
