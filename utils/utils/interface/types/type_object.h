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
 * type_object.h
 *
 * Description:
 *                          TYPE SYSTEMS USE C LANGUAGE
 * 1. This file defines a base class for all types to inherit, as well as a set of methods
 *    and specifications for OOP programming in C. By using this mechanism, your types will
 *    get capabilities like virtual functions, constructs, and destructors, etc.
 * 2. This file provides a type "TypeObject" from which other types can be derived, and some
 *    macros to help you do everything you need.
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef UTILS_TYPE_OBJECT_H
#define UTILS_TYPE_OBJECT_H

#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include "securec.h"
#include "memory/memory_ctx.h"
#include "memory/memory_allocator.h"
#include "defines/err_code.h"
#include "types/atomic_type.h"
#include "port/platform_port.h"

GSDB_BEGIN_C_CODE_DECLS
/*
 * The following code will be rearranged and moved out later to match the error
 * code for the entire project.
 */
#define ERROR_COMPONENT_UTILS    0x01
#define ERROR_UTILS_CTYPE_MODULE 0x01

#define E_UTILS_TYPE_OUT_OF_MEMORY  MAKE_ERROR_CODE(ERROR_COMPONENT_UTILS, ERROR_UTILS_CTYPE_MODULE, 1)
#define E_UTILS_TYPE_OPS_FUNC_NULL  MAKE_ERROR_CODE(ERROR_COMPONENT_UTILS, ERROR_UTILS_CTYPE_MODULE, 2)
#define E_UTILS_TYPE_OPS_SIZE_ERR   MAKE_ERROR_CODE(ERROR_COMPONENT_UTILS, ERROR_UTILS_CTYPE_MODULE, 3)
#define E_UTILS_TYPE_DEPTH_OVERFLOW MAKE_ERROR_CODE(ERROR_COMPONENT_UTILS, ERROR_UTILS_CTYPE_MODULE, 4)
#define E_UTILS_TYPE_OPS_FORMAT_ERR MAKE_ERROR_CODE(ERROR_COMPONENT_UTILS, ERROR_UTILS_CTYPE_MODULE, 5)
#define E_UTILS_TYPE_OPS_INIT_ERR   MAKE_ERROR_CODE(ERROR_COMPONENT_UTILS, ERROR_UTILS_CTYPE_MODULE, 6)

/*
 * We provide a module name mechanism for implementations of this type system
 * in order to separate the naming and definition of global variables so as to
 * facilitate componentization.
 */
#ifndef TYPE_MODULE
#define TYPE_MODULE default
#endif /* TYPE_MODULE */

#define DOWN_TYPE_CAST(ptr, derivedClass) ((derivedClass *)((uintptr_t)(ptr)))
#define UP_TYPE_CAST(ptr, superClass)     ((superClass *)(uintptr_t)(ptr))
#define PTR_TYPE_CAST(ptr, newType)       ((newType *)(uintptr_t)(ptr))
/* Tip: Be careful with this macro. Ascension of signed numbers cannot be used. */
#define VAR_TYPE_CAST(var, newType) ((newType)(uintptr_t)(var))
/* Get Function Access Point, can only be used in class member functions */
#define GET_FAP(accessPointClass) GetAccessPointOps(self, TypeObject, accessPointClass)
/* Get Function Ops object address, can only be used in OpsInit() functions */
#define GET_FOPS(typeClass) ((typeClass##Ops *)(uintptr_t)self)
/* The initialization parameters of the constructor, You can use this transformation */
#define CONSTRUCTOR_PARAM(ptr) UP_TYPE_CAST((ptr), TypeInitParams)

/*
 * The following macro tools are used to construct names related to predefined
 * module name. Pay attention to the macro expansion rules.
 */
#define UTIL_PASTE3_ARGS(a, b, c) a##b##c

#define UTIL_PASTE4_ARGS(a, b, c, d) a##b##c##d

#define UTIL_PASTE3_EVAL(arg1, arg2, arg3) UTIL_PASTE3_ARGS(arg1, arg2, arg3)

#define UTIL_PASTE4_EVAL(arg1, arg2, arg3, arg4) UTIL_PASTE4_ARGS(arg1, arg2, arg3, arg4)

#define MAKE_NAME2_WITH_MOUDLE(prefix, arg1) UTIL_PASTE3_EVAL(prefix, TYPE_MODULE, arg1)

#define MAKE_NAME3_WITH_MOUDLE(prefix, arg1, arg2) UTIL_PASTE4_EVAL(prefix, TYPE_MODULE, arg1, arg2)

/* This code is written to avoid alarms when codecheck scans the code. */
#ifndef HAS_NO_TYPEOF
#define TYPE_OF(className) __typeof__(className)
#else
#define TYPE_OF(className) className
#endif

#ifndef offsetof
#define offsetof(type, member) ((size_t)(&(type *)0->member))
#endif

#define DECL_TYPED_CLASS(thisClass) \
    struct thisClass##Ops *ops;     \
    void *memOrigPtr

#define SetTypedOpsAddr(rootClassName, objAddr, opsAddr) \
    (((rootClassName *)(uintptr_t)(objAddr))->ops = (rootClassName##Ops *)((uintptr_t)(opsAddr)))

#define GetOpsFromProto(rootClassName, protoAddr) \
    (rootClassName##Ops *)(((uintptr_t)(protoAddr)) + (uintptr_t)offsetof(rootClassName##Proto, ops))

#define GetProtoFromOps(rootClassName, opsAddr) \
    ((TypeProto *)(((uintptr_t)(opsAddr)) - (uintptr_t)offsetof(rootClassName##Proto, ops)))

#define GetAccessPointOps(selfAddr, rootClass, accessPointClass) \
    ((accessPointClass##Ops *)(uintptr_t)((rootClass *)(uintptr_t)(selfAddr))->ops)

#define PROTO_INITIALIZER1(thisClass)                            \
    .prot = {                                                    \
        .clsName = #thisClass,                                   \
        .parent = NULL,                                          \
        .objSize = sizeof(thisClass),                            \
        .opsSize = sizeof(thisClass##Ops),                       \
        .opsInited = 0,                                          \
        .opsPadding = 0,                                         \
        .objInit = (ObjectInitFunc)(uintptr_t)thisClass##Init,   \
        .objFinalize = (ObjectFinalizeFunc)thisClass##Finalize,  \
        .opsInit = (OpsInitFunc)(uintptr_t)thisClass##Ops##Init, \
    },

#define PROTO_INITIALIZER2(thisClass, superClass)                      \
    .prot = {                                                          \
        .clsName = #thisClass,                                         \
        .parent = &MAKE_NAME3_WITH_MOUDLE(g_, superClass, Proto.prot), \
        .objSize = sizeof(thisClass),                                  \
        .opsSize = sizeof(thisClass##Ops),                             \
        .opsInited = 0,                                                \
        .opsPadding = 0,                                               \
        .objInit = (ObjectInitFunc)(uintptr_t)thisClass##Init,         \
        .objFinalize = (ObjectFinalizeFunc)(uintptr_t)thisClass##Finalize,        \
        .opsInit = (OpsInitFunc)(uintptr_t)thisClass##Ops##Init,       \
    },

// clang-format off
#define OPS_INITIALIZER \
    .ops = {            \
    },                  \
// clang-format on

#define MAX_DERIVED_DEPTH 8

#define ATOMIC_CAS(addr, oldValue, newValue) GSDB_ATOMIC32_CAS(addr, oldValue, newValue)
#define ATOMIC_GET(addr) GSDB_ATOMIC32_GET(addr)
#define ATOMIC_SET(addr, value) GSDB_ATOMIC32_SET(addr, value)
#define ATOMIC_FMB() GSDB_ATOMIC_FULL_BARRIER()

#define USECS_PER_MSEC 1000
static inline void TypeOpsInitDelay(int msec)
{
    Usleep(msec * USECS_PER_MSEC);
}

#define TYPE_OPS_INIT_NOT 0    /* Init state */
#define TYPE_OPS_INIT_ING 1    /* Initializing */
#define TYPE_OPS_INIT_OK 2     /* Initialized ok */
#define TYPE_OPS_INIT_FAILED 3 /* Initialized failed */

/*
 * The Macro below is used to generate @NewTypeName() or @FreeTypeName() function
 * code for each type derived from @TypeObject.
 *
 */
#define DEFINE_NEW_CLASS_FUNCTION(rootClass, thisClass)                                                              \
    static int GetAncestorsOf##thisClass(TypeProto *thisProto, TypeProto *ancestors[], int maxDepth, ErrorCode *err) \
    {                                                                                                                \
        TypeProto *proto = thisProto;                                                                                \
        int depth = 0;                                                                                               \
                                                                                                                     \
        while (depth < maxDepth) {                                                                                   \
            ancestors[depth++] = proto;                                                                              \
            if (proto->parent != NULL) {                                                                             \
                proto = proto->parent;                                                                               \
                if (depth == maxDepth) {                                                                             \
                    if (err != NULL) {                                                                               \
                        *err = E_UTILS_TYPE_DEPTH_OVERFLOW;                                                          \
                    }                                                                                                \
                    return -1;                                                                                       \
                }                                                                                                    \
            } else {                                                                                                 \
                break;                                                                                               \
            }                                                                                                        \
        }                                                                                                            \
        return depth;                                                                                                \
    }                                                                                                                \
                                                                                                                     \
    static ErrorCode HasNullFuncPointerOf##thisClass(size_t opsSize, size_t superSize, rootClass##Ops *opsAddr)      \
    {                                                                                                                \
        if (opsSize % sizeof(OpsInitFunc) != 0 || superSize % sizeof(OpsInitFunc) != 0) {                            \
            return ERROR_SYS_OK;                                                                                     \
        }                                                                                                            \
        OpsInitFunc *start = (OpsInitFunc *)((uintptr_t)opsAddr + superSize),                                        \
                    *end = (OpsInitFunc *)((uintptr_t)opsAddr + opsSize);                                            \
        while (start < end) {                                                                                        \
            if (*start == NULL) {                                                                                    \
                return E_UTILS_TYPE_OPS_FUNC_NULL;                                                                   \
            }                                                                                                        \
            ++start;                                                                                                 \
        }                                                                                                            \
        return ERROR_SYS_OK;                                                                                         \
    }                                                                                                                \
                                                                                                                     \
    static ErrorCode InitAncestorsClassOpsOf##thisClass(void)                                                        \
    {                                                                                                                \
        TypeProto *proto = &MAKE_NAME3_WITH_MOUDLE(g_, thisClass, Proto.prot);                                       \
        TypeProto *ancestors[MAX_DERIVED_DEPTH] = {0}; /* extension */                                               \
                                                                                                                     \
        ErrorCode errNo = ERROR_SYS_OK;                                                                              \
        int depth = GetAncestorsOf##thisClass(proto, ancestors, MAX_DERIVED_DEPTH, &errNo);                          \
        if (depth < 0) {                                                                                             \
            return errNo;                                                                                            \
        }                                                                                                            \
                                                                                                                     \
        for (int inited = depth - 1; inited >= 0 && inited < MAX_DERIVED_DEPTH; --inited) {                          \
            proto = ancestors[inited];                                                                               \
            rootClass##Ops *opsAddr = &(UP_TYPE_CAST(proto, rootClass##Proto))->ops;                                 \
            size_t superSize = 0;                                                                                    \
            if (ATOMIC_CAS(&proto->opsInited, TYPE_OPS_INIT_NOT, TYPE_OPS_INIT_ING)) {                               \
                if (proto->parent != NULL) {                                                                         \
                    superSize = proto->parent->opsSize;                                                              \
                    errno_t ret =                                                                                    \
                        memcpy_s(opsAddr, proto->opsSize, GetOpsFromProto(rootClass, proto->parent), superSize);     \
                    if (ret != 0) {                                                                                  \
                        return E_UTILS_TYPE_OPS_SIZE_ERR;                                                            \
                    }                                                                                                \
                }                                                                                                    \
                OpsInitFunc initOps = proto->opsInit;                                                                \
                if (initOps != NULL) {                                                                               \
                    (*initOps)(opsAddr);                                                                             \
                }                                                                                                    \
                errNo = HasNullFuncPointerOf##thisClass(proto->opsSize, superSize, opsAddr);                         \
                if (errNo != ERROR_SYS_OK) {                                                                         \
                    ATOMIC_SET(&proto->opsInited, TYPE_OPS_INIT_FAILED);                                             \
                    return errNo;                                                                                    \
                }                                                                                                    \
                ATOMIC_SET(&proto->opsInited, TYPE_OPS_INIT_OK);                                                     \
            } else {                                                                                                 \
                for (;;) {                                                                                           \
                    int state = ATOMIC_GET(&proto->opsInited);                                                       \
                    if (state == TYPE_OPS_INIT_ING) {                                                                \
                        TypeOpsInitDelay(1);                                                                         \
                        continue;                                                                                    \
                    }                                                                                                \
                    if (state == TYPE_OPS_INIT_OK) {                                                                 \
                        break;                                                                                       \
                    } else {                                                                                         \
                        return E_UTILS_TYPE_OPS_FUNC_NULL;                                                           \
                    }                                                                                                \
                }                                                                                                    \
            }                                                                                                        \
        }                                                                                                            \
        return ERROR_SYS_OK;                                                                                         \
    }                                                                                                                \
                                                                                                                     \
    static ErrorCode InitClassOpsOf##thisClass(const TypeProto *proto)                                               \
    {                                                                                                                \
        ErrorCode errNo = ERROR_SYS_OK;                                                                              \
                                                                                                                     \
        ATOMIC_FMB();                                                                                                \
        int lastedState = ATOMIC_GET(&proto->opsInited);                                                             \
        switch (lastedState) {                                                                                       \
            case TYPE_OPS_INIT_FAILED:                                                                               \
                errNo = E_UTILS_TYPE_OPS_FUNC_NULL;                                                                  \
                break;                                                                                               \
                                                                                                                     \
            case TYPE_OPS_INIT_OK:                                                                                   \
                break;                                                                                               \
                                                                                                                     \
            case TYPE_OPS_INIT_NOT:                                                                                  \
            case TYPE_OPS_INIT_ING:                                                                                  \
                errNo = InitAncestorsClassOpsOf##thisClass();                                                        \
                break;                                                                                               \
                                                                                                                     \
            default:                                                                                                 \
                errNo = E_UTILS_TYPE_OPS_INIT_ERR;                                                                   \
                break;                                                                                               \
        }                                                                                                            \
        return errNo;                                                                                                \
    }                                                                                                                \
                                                                                                                     \
    static ErrorCode ConstructObjectOf##thisClass(TYPE_OF(thisClass) * object, TypeProto * ancestors[], int maxElem, \
        int depth, int *isCalled, TypeInitParams *initData)                                                          \
    {                                                                                                                \
        for (*isCalled = depth - 1; *isCalled < maxElem && *isCalled >= 0; --*isCalled) {                            \
            TypeProto *proto = ancestors[*isCalled];                                                                 \
            ObjectInitFunc constructor = proto->objInit;                                                             \
            if (constructor != NULL) {                                                                               \
                ErrorCode errNo = (*constructor)(UP_TYPE_CAST(object, rootClass), initData);                         \
                if (errNo != ERROR_SYS_OK) {                                                                         \
                    return errNo;                                                                                    \
                }                                                                                                    \
            }                                                                                                        \
            SetTypedOpsAddr(rootClass, object, GetOpsFromProto(rootClass, proto));                                   \
        }                                                                                                            \
        return ERROR_SYS_OK;                                                                                         \
    }                                                                                                                \
                                                                                                                     \
    static void DestroyObjectOf##thisClass(                                                                          \
        TYPE_OF(thisClass) * object, TypeProto * ancestors[], int depth, int *isCalled)                              \
    {                                                                                                                \
        for (*isCalled += 1; *isCalled < depth; ++*isCalled) {                                                       \
            TypeProto *proto = ancestors[*isCalled];                                                                 \
            ObjectFinalizeFunc destructor = proto->objFinalize;                                                      \
            if (destructor != NULL) {                                                                                \
                (*destructor)(UP_TYPE_CAST(object, rootClass));                                                      \
            }                                                                                                        \
            if (proto->parent != NULL) {                                                                             \
                SetTypedOpsAddr(rootClass, object, GetOpsFromProto(rootClass, proto->parent));                       \
            }                                                                                                        \
        }                                                                                                            \
    }                                                                                                                \
                                                                                                                     \
    static TYPE_OF(thisClass) *                                                                                      \
        New##thisClass##WithAligned(TypeInitParams *initData, size_t alignedSize, ErrorCode *err)                    \
    {                                                                                                                \
        TYPE_OF(thisClass) *object = NULL;                                                                           \
        char *objectOrigPtr = NULL;                                                                                  \
        TypeProto *proto = &MAKE_NAME3_WITH_MOUDLE(g_, thisClass, Proto.prot);                                       \
        TypeProto *ancestors[MAX_DERIVED_DEPTH] = {0}; /* extension */                                               \
        size_t objSize = proto->objSize + alignedSize;                                                               \
                                                                                                                     \
        ErrorCode errNo = InitClassOpsOf##thisClass(proto);                                                          \
        if (errNo != ERROR_SYS_OK) {                                                                                 \
            if (err != NULL) {                                                                                       \
                *err = errNo;                                                                                        \
            }                                                                                                        \
            return NULL;                                                                                             \
        }                                                                                                            \
                                                                                                                     \
        int depth = GetAncestorsOf##thisClass(proto, ancestors, MAX_DERIVED_DEPTH, &errNo);                          \
        if (depth < 0) {                                                                                             \
            if (err != NULL) {                                                                                       \
                *err = errNo;                                                                                        \
            }                                                                                                        \
            return NULL;                                                                                             \
        }                                                                                                            \
                                                                                                                     \
        objectOrigPtr = (char *)TypedObjectAlloc(objSize);                                                           \
        if (objectOrigPtr == NULL) {                                                                                 \
            if (err != NULL) {                                                                                       \
                *err = E_UTILS_TYPE_OUT_OF_MEMORY;                                                                   \
            }                                                                                                        \
            return NULL;                                                                                             \
        }                                                                                                            \
                                                                                                                     \
        if (alignedSize != 0) {                                                                                      \
            object = (thisClass *)(uintptr_t)(                                                                       \
                objectOrigPtr + (alignedSize - ((size_t)(uintptr_t)objectOrigPtr % alignedSize)));                   \
        } else {                                                                                                     \
            object = (thisClass *)(uintptr_t)objectOrigPtr;                                                          \
        }                                                                                                            \
        ((rootClass *)(uintptr_t)(object))->memOrigPtr = objectOrigPtr;                                              \
                                                                                                                     \
        SetTypedOpsAddr(rootClass, object, NULL);                                                                    \
        int called = 0;                                                                                              \
        errNo = ConstructObjectOf##thisClass(object, ancestors, MAX_DERIVED_DEPTH, depth, &called, initData);        \
        if (errNo == ERROR_SYS_OK) {                                                                                 \
            if (err) {                                                                                               \
                *err = ERROR_SYS_OK;                                                                                 \
            }                                                                                                        \
            return object;                                                                                           \
        } else {                                                                                                     \
            DestroyObjectOf##thisClass(object, ancestors, depth, &called);                                           \
            TypeObjectFree(objectOrigPtr);                                                                           \
            if (err) {                                                                                               \
                *err = errNo;                                                                                        \
            }                                                                                                        \
            return NULL;                                                                                             \
        }                                                                                                            \
    }                                                                                                                \
                                                                                                                     \
    UTILS_EXPORT TYPE_OF(thisClass) * New##thisClass(TypeInitParams *initData, ErrorCode *err)                       \
    {                                                                                                                \
        return New##thisClass##WithAligned(initData, 0, err);                                                        \
    }                                                                                                                \
                                                                                                                     \
    UTILS_EXPORT TYPE_OF(thisClass) * New##thisClass##Aligned(TypeInitParams *initData, ErrorCode *err)              \
    {                                                                                                                \
        return New##thisClass##WithAligned(initData, GS_CACHE_LINE_SIZE, err);                                       \
    }                                                                                                                \
                                                                                                                     \
    UTILS_EXPORT void Free##thisClass(TYPE_OF(thisClass) * object)                                                   \
    {                                                                                                                \
        rootClass##Ops *ops = (UP_TYPE_CAST(object, rootClass))->ops;                                                \
        TypeProto *proto = GetProtoFromOps(rootClass, ops);                                                          \
        TypeProto *ancestors[MAX_DERIVED_DEPTH] = {0};                                                               \
                                                                                                                     \
        int depth = GetAncestorsOf##thisClass(proto, ancestors, MAX_DERIVED_DEPTH, NULL);                            \
        int called = -1;                                                                                             \
        DestroyObjectOf##thisClass(object, ancestors, depth, &called);                                               \
        TypeObjectFree(((rootClass *)(uintptr_t)(object))->memOrigPtr);                                              \
    }

#define DECLARE_NEW_TYPED_CLASS(thisClass)                                                  \
    typedef struct thisClass##Proto thisClass##Proto;                                       \
    struct thisClass##Proto {                                                               \
        TypeProto prot;                                                                     \
        thisClass##Ops ops;                                                                 \
    };                                                                                      \
    extern thisClass##Proto MAKE_NAME3_WITH_MOUDLE(g_, thisClass, Proto);                   \
    TYPE_OF(thisClass) * New##thisClass(TypeInitParams *initData, ErrorCode *err);          \
    TYPE_OF(thisClass) * New##thisClass##Aligned(TypeInitParams *initData, ErrorCode *err); \
    void Free##thisClass(TYPE_OF(thisClass) * object);

#define DEFINE_NEW_TYPED_CLASS(thisClass, superClass)                                                               \
    thisClass##Proto MAKE_NAME3_WITH_MOUDLE(g_, thisClass, Proto) UTILS_EXPORT                                      \
        __attribute__((aligned(GS_CACHE_LINE_SIZE))) = {PROTO_INITIALIZER2(thisClass, superClass) OPS_INITIALIZER}; \
    DEFINE_NEW_CLASS_FUNCTION(TypeObject, thisClass)

#define DECLARE_ROOT_TYPED_CLASS(thisClass) DECLARE_NEW_TYPED_CLASS(thisClass)

#define DEFINE_ROOT_TYPED_CLASS(thisClass)                                                              \
    thisClass##Proto MAKE_NAME3_WITH_MOUDLE(g_, thisClass, Proto) UTILS_EXPORT                          \
        __attribute__((aligned(GS_CACHE_LINE_SIZE))) = {PROTO_INITIALIZER1(thisClass) OPS_INITIALIZER}; \
    DEFINE_NEW_CLASS_FUNCTION(TypeObject, TypeObject)

/*
 * Internally used to maintain the memory allocator structure. We do not
 * directly use global variables because instance conflicts may occur
 * during componentization.
 */
#define DEFINE_TYPE_SYSTEM_MEMORY_FACILITY                                   \
                                                                             \
    static MemAllocator MAKE_NAME2_WITH_MOUDLE(g_, Allocator);               \
                                                                             \
    UTILS_EXPORT void SetTypeSystemMemHandler(const MemAllocator *allocator) \
    {                                                                        \
        MemAllocator *typeAlloctor = &MAKE_NAME2_WITH_MOUDLE(g_, Allocator); \
        typeAlloctor->context = allocator->context;                          \
        typeAlloctor->alloc = allocator->alloc;                              \
        typeAlloctor->free = allocator->free;                                \
    }                                                                        \
                                                                             \
    UTILS_EXPORT void *TypedObjectAlloc(size_t size)                         \
    {                                                                        \
        MemAllocator *typeAlloctor = &MAKE_NAME2_WITH_MOUDLE(g_, Allocator); \
        if (typeAlloctor->alloc != NULL && typeAlloctor->free != NULL) {     \
            void *ptr = (*typeAlloctor->alloc)(typeAlloctor, size);          \
            if (ptr == NULL) {                                               \
                return ptr;                                                  \
            }                                                                \
            if (memset_s(ptr, size, 0, size) != EOK) {                       \
                (*typeAlloctor->free)(typeAlloctor, ptr);                    \
                return NULL;                                                 \
            }                                                                \
            return ptr;                                                      \
        } else {                                                             \
            return calloc(1, size);                                          \
        }                                                                    \
    }                                                                        \
                                                                             \
    UTILS_EXPORT void TypeObjectFree(void *ptr)                              \
    {                                                                        \
        MemAllocator *typeAlloctor = &MAKE_NAME2_WITH_MOUDLE(g_, Allocator); \
        if (typeAlloctor->alloc != NULL && typeAlloctor->free != NULL) {     \
            (*typeAlloctor->free)(typeAlloctor, ptr);                        \
        } else {                                                             \
            free(ptr);                                                       \
        }                                                                    \
    }

/*
 * The following two functions are not directly used by the caller,
 * but are used by the generated tool functions New##ClassName() and
 * Free##ClassName(). Therefore, the declaration is required here.
 */
void *TypedObjectAlloc(size_t size);
void TypeObjectFree(void *ptr);

/*
 * The base class of all types, from which your type can gain OOP
 * capabilities. It defines a pointer to a virtual function table
 * only, with no additional overhead.
 */
typedef struct TypeObject TypeObject;
struct TypeObject {
    DECL_TYPED_CLASS(TypeObject);
};

/* This type is an accompanying type of the'TypeObject' class and
 * is used to define virtual functions. Virtual functions defined
 * in this structure need to be assigned in the TypeObjectOpsInit()
 * function. 'TypeObject' can also be the name of a derived type.
 */
typedef struct TypeObjectOps TypeObjectOps;
struct TypeObjectOps {
    ErrorCode (*getObjectInfo)(const TypeObject *self, char info[], size_t size);
};

/*
 * Parameters are encapsulated and used for constructors. We specify
 * how parameters are transferred to constructors. regardless of the
 * number of parameters. It can also accomplish the parameter passing
 * of derived and base classes in a combined way.
 */
typedef struct TypeInitParams TypeInitParams;
struct TypeInitParams {
    int dummy; /* To avoid empty structure complaints by PC-LINT */
};

#define TYPE_INIT_PARAMS_DEFAULT \
    {                            \
        0                        \
    }

typedef ErrorCode (*ObjectInitFunc)(TypeObject *self, TypeInitParams *initData);
typedef void (*ObjectFinalizeFunc)(TypeObject *self);
typedef void (*OpsInitFunc)(TypeObjectOps *self);

/*
 * This type is used for the inheritance relationship of the internal
 * tracing and is invisible to users.
 */
typedef struct TypeProto TypeProto;
struct TypeProto {
    const char *clsName;
    TypeProto *parent;
    size_t objSize;
    size_t opsSize;
    Atomic32 opsInited;
    uint32_t opsPadding;
    ObjectInitFunc objInit;
    ObjectFinalizeFunc objFinalize;
    OpsInitFunc opsInit;
};

/*
 * Customize the memory allocation method for type objects, allowing
 * users to allocate and release memory. If you do not proactively set,
 * the default malloc and free will be used.
 */
void SetTypeSystemMemHandler(const MemAllocator *allocator);

/*
 * The real information about an object can be obtained through this
 * interface. The upper layer can rewrite this function to change the
 * output format of the information.
 */
ErrorCode GetTypeObjectInfo(const TypeObject *self, char info[], size_t size);

DECLARE_ROOT_TYPED_CLASS(TypeObject)

GSDB_END_C_CODE_DECLS

#endif /* UTILS_TYPE_OBJECT_H */
