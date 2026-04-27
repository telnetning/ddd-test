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
 * ref_object.c
 *
 * Description:
 * This file implements the reference counting class.
 *
 * ---------------------------------------------------------------------------------------
 */
#include "types/ref_object.h"

#ifdef GSDB_DEBUG
#define MAX_REFOBJECT_REFCOUNT 10000000
#define ASSERT_REFCOUNT(self)                                                   \
    do {                                                                        \
        ASSERT((self) != NULL);                                                 \
        ASSERT(GSDB_ATOMIC32_GET(&(self)->refCount) > 0);                       \
        ASSERT(GSDB_ATOMIC32_GET(&(self)->refCount) <= MAX_REFOBJECT_REFCOUNT); \
    } while (0)
#else
#define ASSERT_REFCOUNT(self)
#endif /* GSDB_DEBUG */

static ErrorCode RefObjectInit(RefObject *self, SYMBOL_UNUSED TypeInitParams *initData)
{
    ASSERT(self != NULL);
    self->useSpinLock = false;
    MutexInit(&self->mutex);
    GSDB_ATOMIC32_SET(&self->refCount, 1);
    GSDB_ATOMIC32_SET(&self->killed, 0);
    return ERROR_SYS_OK;
}

static void RefObjectOnKill(SYMBOL_UNUSED RefObject *self)
{
    ASSERT(self != NULL);
}

static void RefObjectFinalize(RefObject *self)
{
    ASSERT(self != NULL);
    if (!self->useSpinLock) {
        MutexDestroy(&self->mutex);
    } else {
        SpinLockDestroy(&self->spinLock);
    }
}

static void RefObjectOpsInit(RefObjectOps *self)
{
    ASSERT(self != NULL);
    GET_FOPS(RefObject)->kill = RefObjectOnKill;
}

DEFINE_NEW_TYPED_CLASS(RefObject, TypeObject)

/* It must be called immediately after the object is created. */
UTILS_EXPORT void RefObjectUseSpinlock(RefObject *self)
{
    ASSERT_REFCOUNT(self);
    if (!self->useSpinLock) {
        MutexDestroy(&self->mutex);
        SpinLockInit(&self->spinLock);
        self->useSpinLock = true;
        GSDB_ATOMIC_FULL_BARRIER();
    }
}

UTILS_EXPORT void RefObjectIncRef(RefObject *self)
{
    ASSERT_REFCOUNT(self);
    GSDB_ATOMIC32_INC(&self->refCount);
}

UTILS_EXPORT void RefObjectDecRef(RefObject *self)
{
    ASSERT_REFCOUNT(self);
    if (GSDB_ATOMIC32_DEC_AND_TEST_ZERO(&self->refCount)) {
        FreeRefObject(self);
    }
}

UTILS_EXPORT void RefObjectLock(RefObject *self)
{
    ASSERT_REFCOUNT(self);
    if (!self->useSpinLock) {
        MutexLock(&self->mutex);
    } else {
        SpinLockAcquire(&self->spinLock);
    }
}

UTILS_EXPORT void RefObjectUnlock(RefObject *self)
{
    ASSERT_REFCOUNT(self);
    if (!self->useSpinLock) {
        MutexUnlock(&self->mutex);
    } else {
        SpinLockRelease(&self->spinLock);
    }
}

UTILS_EXPORT bool RefObjectKill(RefObject *self)
{
    ASSERT_REFCOUNT(self);
    bool invoked = false;
    RefObjectLock(self);
    if (GSDB_ATOMIC32_CAS(&self->killed, 0, 1)) {
        GET_FAP(RefObject)->kill(self);
        invoked = true;
    }
    RefObjectUnlock(self);
    return invoked;
}

UTILS_EXPORT bool RefObjectIsKilled(const RefObject *self)
{
    ASSERT_REFCOUNT(self);
    return GSDB_ATOMIC32_GET(&self->killed) == 1;
}

UTILS_EXPORT void RefObjectKillAndDecRef(RefObject *self)
{
    ASSERT(self != NULL);
    (void)RefObjectKill(self);
    RefObjectDecRef(self);
}
