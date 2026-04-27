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
#include "PCommon.h"

// debug功能开关变量，默认关闭
int isLogOpen = 0;
int debug_level = 2;

void OPEN_Log(void)
{
    isLogOpen = 1;
}

void CLOSE_Log(void)
{
    isLogOpen = 0;
}

// 一般在调试代码中，调用此接口，许多参数用默认的，不需要了解太多细节
void DEBUG_Element(S_Element* pElement)
{
    int i;

    hw_printf("\n---------------------------------DEBUG	S_Element\n");
    hw_printf("pElement->para.type is		 	 %d\n", pElement->para.type);
    hw_printf("pElement->isHasInitValue is		 %d\n", pElement->isHasInitValue);
    hw_printf("pElement->isNeedFree is		 	 %d\n", pElement->isNeedFree);
    hw_printf("pElement->isAddWholeRandom is		 %d\n", pElement->isAddWholeRandom);
    hw_printf("pElement->inLen is			 %d\n", (int)pElement->inLen);

    if (pElement->inBuf != NULL) {
        hw_printf("pElement->inBuf is:		\n");
        HEX_Dump((u8*)(pElement->inBuf), (u32)(pElement->inLen / 8) + IsAddone(pElement->inLen % 8));
    }

    hw_printf("pElement->pos is		 	 %d\n", pElement->pos);
    hw_printf("pElement->count is		 	 %d\n", pElement->count);
    hw_printf("pElement->para.len is		 	 %d\n", (int)pElement->para.len);
    hw_printf("pElement->isNeedFreeOutBuf is		 %d\n", pElement->isNeedFreeOutBuf);

    if (pElement->para.value != NULL) {
        hw_printf("pElement->para.value is:		\n");
        HEX_Dump((u8*)(pElement->para.value), (u32)(pElement->para.len / 8) + IsAddone(pElement->inLen % 8));
    }

    for (i = 0; i < enum_MutatedMAX; i++) {
        if (g_Mutater_group[i] == NULL)
            continue;

        hw_printf("[%d] Mutator: %s\n", i, g_Mutater_group[i]->name);
        hw_printf("isMutatedClose %d isMutatedSupport %d posStart %d num %d\n",
            pElement->isMutatedClose[i],
            pElement->isMutatedSupport[i],
            pElement->posStart[i],
            pElement->num[i]);
    }
}

void DEBUG_SuportMutator(S_Element* pElement)
{
    int i;

    hw_printf("S_Element support mutator:\n");
    for (i = 0; i < enum_MutatedMAX; i++) {
        if (enum_Yes == pElement->isMutatedSupport[i]) {
            hw_printf("\t%s\n", g_Mutater_group[i]->name);
        }
    }

    hw_printf("S_Element not support mutator:\n");
    for (i = 0; i < enum_MutatedMAX; i++) {
        if (enum_No == pElement->isMutatedSupport[i]) {
            hw_printf("\t%s\n", g_Mutater_group[i]->name);
        }
    }
}

void DEBUG_ClosedMutator(void)
{
    int i;

    hw_printf("closed mutator:\n");
    for (i = 0; i < enum_MutatedMAX; i++) {
        if (enum_Yes == g_IsMutatedClose[i]) {
            hw_printf("\t%s\n", g_Mutater_group[i]->name);
        }
    }
}

//需要增加一个打印，把其他的全局变量打印出来
