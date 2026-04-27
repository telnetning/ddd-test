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
 * IDENTIFICATION
 *        include/common/dstore_tsan_annotation.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_TSAN_ANNOTATION_H
#define DSTORE_TSAN_ANNOTATION_H

#if defined(__SANITIZE_THREAD__)
#define TSAN_ENABLED
#elif defined(__has_feature)
#if __has_feature(thread_sanitizer)
#define TSAN_ENABLED
#endif
#endif

#ifdef TSAN_ENABLED
extern "C" {
    void AnnotateHappensBefore(const char *f, int l, uintptr_t addr);
    void AnnotateHappensAfter(const char *f, int l, uintptr_t addr);
    void AnnotateRWLockCreate(const char *f, int l, uintptr_t m);
    void AnnotateRWLockDestroy(const char *f, int l, uintptr_t m);
    void AnnotateRWLockAcquired(const char *f, int l, uintptr_t m, uintptr_t is_w);
    void AnnotateRWLockReleased(const char *f, int l, uintptr_t m, uintptr_t is_w);
    void AnnotateBenignRaceSized(const char *f, int l, uintptr_t m, uintptr_t size, char *desc);
}

#define TsAnnotateRWLockCreate(m)          AnnotateRWLockCreate(__FILE__, __LINE__, (uintptr_t)m)
#define TsAnnotateRWLockDestroy(m)         AnnotateRWLockDestroy(__FILE__, __LINE__, (uintptr_t)m)
#define TsAnnotateHappensBefore(addr)      AnnotateHappensBefore(__FILE__, __LINE__, (uintptr_t)addr)
#define TsAnnotateHappensAfter(addr)       AnnotateHappensAfter(__FILE__, __LINE__, (uintptr_t)addr)
#define TsAnnotateRWLockAcquired(m, is_w)  AnnotateRWLockAcquired(__FILE__, __LINE__, (uintptr_t)m, is_w)
#define TsAnnotateRWLockReleased(m, is_w)  AnnotateRWLockReleased(__FILE__, __LINE__, (uintptr_t)m, is_w)
#define TsAnnotateBenignRaceSized(m, size) AnnotateBenignRaceSized(__FILE__, __LINE__, (uintptr_t)m, size, NULL)
#else
#define TsAnnotateRWLockCreate(m)
#define TsAnnotateRWLockDestroy(m)
#define TsAnnotateHappensBefore(addr)
#define TsAnnotateHappensAfter(addr)
#define TsAnnotateRWLockAcquired(m, is_w)
#define TsAnnotateRWLockReleased(m, is_w)
#define TsAnnotateBenignRaceSized(m, size)

#endif /* endif ENABLE_THREAD_CHECK */

#endif // STORAGE_TSAN_ANNOTATION_H
