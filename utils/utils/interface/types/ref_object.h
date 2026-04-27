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
 * ref_object.h
 *
 * Description:
 * This file provides a class with reference counting and state that other classes will
 * get these capabilities if they derive from this class. Using reference count and state
 * to manage the life cycle of an object can easily minimize lock dependency when accessing
 * and destroying an object.
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef UTILS_REF_OBJECT_H
#define UTILS_REF_OBJECT_H

#include "types/type_object.h"
#include "port/platform_port.h"

GSDB_BEGIN_C_CODE_DECLS

typedef struct RefObject RefObject;
struct RefObject {
    TypeObject super;
    bool useSpinLock;
    union {
        Mutex mutex;
        SpinLock spinLock;
    };
    Atomic32 refCount;
    Atomic32 killed;
};

typedef struct RefObjectOps RefObjectOps;
struct RefObjectOps {
    TypeObjectOps super;
    void (*kill)(RefObject *self);
};

DECLARE_NEW_TYPED_CLASS(RefObject)

void RefObjectUseSpinlock(RefObject *self);
void RefObjectIncRef(RefObject *self);
void RefObjectDecRef(RefObject *self);
void RefObjectLock(RefObject *self);
void RefObjectUnlock(RefObject *self);
bool RefObjectKill(RefObject *self);
bool RefObjectIsKilled(const RefObject *self);
void RefObjectKillAndDecRef(RefObject *self);

GSDB_END_C_CODE_DECLS

#endif /* UTILS_REF_OBJECT_H */
