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
 * Description: portable safe random function implementation in win32 platform
 */
#include <stdbool.h>
#include <windows.h>
#include <wincrypt.h>

#include "port/win32_random.h"

static bool PrepareRandomSource(HCRYPTPROV *hProvider)
{
    if (!CryptAcquireContext(hProvider, NULL, MS_DEF_PROV, PROV_RSA_FULL, CRYPT_VERIFYCONTEXT | CRYPT_MACHINE_KEYSET)) {
        if (!CryptAcquireContext(hProvider, NULL, MS_DEF_PROV, PROV_RSA_FULL,
                                 CRYPT_VERIFYCONTEXT | CRYPT_MACHINE_KEYSET | CRYPT_NEWKEYSET)) {
            return false;
        }
    }
    return true;
}

static bool ReadRandomSource(HCRYPTPROV hProvider, void *buf, size_t len)
{
    if (CryptGenRandom(hProvider, len, (BYTE *)(buf))) {
        return true;
    }
    return false;
}

static void ReleaseRandomSource(HCRYPTPROV hProvider)
{
    (void)CryptReleaseContext(hProvider, 0);
}

uint32_t GetSafeRandomValue(void)
{
    HCRYPTPROV hProvider = 0;
    if (!PrepareRandomSource(&hProvider)) {
        goto ERROR_EXIT;
    }

    uint32_t randomVal = 0;
    if (!ReadRandomSource(hProvider, &randomVal, sizeof(uint32_t))) {
        ReleaseRandomSource(hProvider);
        goto ERROR_EXIT;
    }
    ReleaseRandomSource(hProvider);
    return randomVal;

ERROR_EXIT:
    return UINT32_MAX;
}

bool GenerateSecureRandomByteArray(size_t size, unsigned char *out)
{
    HCRYPTPROV hProvider = 0;
    if (!PrepareRandomSource(&hProvider)) {
        return false;
    }
    if (!ReadRandomSource(hProvider, out, size)) {
        ReleaseRandomSource(hProvider);
        return false;
    }
    ReleaseRandomSource(hProvider);
    return true;
}
