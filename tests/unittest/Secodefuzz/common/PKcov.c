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
/*

本模块实现kcov 驱动调用

实现记录内核代码覆盖率的功能

如果编译器不支持，需要注释掉HAS_KCOV声明


下边两个函数为本模块实现的对外提供的覆盖率反馈函数，
如果有其他方法也能实现覆盖率反馈，如afl，则实现下边两函数即可替换本模块
kcov_is_has_new_feature
kcov_start_feature
kcov_end_feature

*/

#include "PCommon.h"

#ifdef HAS_KCOV

#define KCOV_INIT_TRACE _IOR('c', 1, unsigned long)
#define KCOV_DISABLE _IO('c', 101)
#define COVER_SIZE (64 << 10)
#define KCOV_ENABLE _IO('c', 100)

#define KCOV_TRACE_PC 0
#define KCOV_TRACE_CMP 1

static int* kcov_8bit_counters = NULL;
static uintptr_t* kcov_pcs = NULL;
static int g_kcov_is_has_init = 0;

static int is_has_new_feature = 0;
static int is_print_pc = 0;
static int has_cov_pc_num = 0;
static int g_fd;
static unsigned long* g_cover = NULL;
static char buffer1[MAX_PcDescr_SIZE];
static char buffer2[MAX_PcDescr_SIZE];

int is_start = 0;

static void init_malloc(void)
{
    if (g_kcov_is_has_init == 0) {
        g_kcov_is_has_init = 1;
        kcov_8bit_counters = (int*)hw_Malloc(sizeof(int) * MAX_Kernel_PC_NUM);
        kcov_pcs = (uintptr_t*)hw_Malloc(sizeof(uintptr_t) * MAX_Kernel_PC_NUM);

        hw_Memset(kcov_8bit_counters, 0, sizeof(int) * MAX_Kernel_PC_NUM);
        hw_Memset(kcov_pcs, 0, sizeof(uintptr_t) * MAX_Kernel_PC_NUM);
    }
}

int kcov_is_has_new_feature()
{
    int n = 0;
    int i = 0;
    int ret = 0;

    if (is_start == 0)
        return 0;

    is_start = 0;

    /* Read number of PCs collected. */
    n = __atomic_load_n(&g_cover[0], __ATOMIC_RELAXED);

    for (i = 0; i < n; i++)

    {
        int* PC = (int*)(g_cover[i + 1]);
        int Idx = (size_t)PC % (MAX_Kernel_PC_NUM);

        //只对第一次负责，目前忽略其他
        if (kcov_pcs[Idx] == 0) {
            is_has_new_feature = 1;
            has_cov_pc_num++;

            if (is_print_pc) {
                FILE* fp;
                buffer2[0] = 0;
                hw_sprintf(buffer2, "addr2line -f  -e  /usr/src/linux-4.9/vmlinux  0x%lx", ((size_t)PC - 0x12000000));
                fp = popen(buffer2, "r");
                fgets(buffer1, sizeof(buffer1), fp);
                hw_printf("NEW_PC-0x%lx(Idx-%d;CovRate-%d):%s", (size_t)PC, Idx, has_cov_pc_num, buffer1);
                pclose(fp);
            }

            kcov_pcs[Idx] = PC;
        }

        kcov_8bit_counters[Idx]++;
    }
    ret = is_has_new_feature;
    is_has_new_feature = 0;

    if (ioctl(g_fd, KCOV_DISABLE, 0))
        perror("ioctl"), exit(1);

    return ret;
}

// KCOV_TRACE_PC = 0,
/* Collecting comparison operands mode. */
// KCOV_TRACE_CMP = 1,
//其他模式留给以后发觉.gcc现在还不支持，clang支持

void kcov_start_feature()
{
    if (is_start == 1)
        return;
    is_start = 1;

    is_has_new_feature = 0;

    /* Enable coverage collection on the current thread. */
    if (ioctl(g_fd, KCOV_ENABLE, KCOV_TRACE_PC))
        perror("ioctl KCOV_ENABLE"), exit(1);

    /* Reset coverage from the tail of the ioctl() call. */
    __atomic_store_n(&g_cover[0], 0, __ATOMIC_RELAXED);
}

void kcov_end_feature()
{}

// http://www.spinics.net/lists/linux-mm/msg134419.html
// KCOV_MODE_DISABLED = 0,
// KCOV_MODE_INIT = 1,
// KCOV_MODE_TRACE = 1,
// KCOV_MODE_TRACE_PC = 2,
// KCOV_MODE_TRACE_CMP = 3,
//其他模式留给以后发觉

void init_kcov(void)
{
    init_malloc();

    hw_Memset(kcov_8bit_counters, 0, sizeof(int) * MAX_Kernel_PC_NUM);
    hw_Memset(kcov_pcs, 0, sizeof(uintptr_t) * MAX_Kernel_PC_NUM);
    has_cov_pc_num = 0;

    g_fd = open("/sys/kernel/debug/kcov", O_RDWR);
    if (g_fd == -1)
        perror("open kcov"), exit(1);

    /* Setup trace mode and trace size. */
    if (ioctl(g_fd, KCOV_INIT_TRACE, COVER_SIZE))
        perror("ioctl KCOV_INIT_TRACE"), exit(1);

    /* Mmap buffer shared between kernel- and user-space. */
    g_cover =
        (unsigned long*)mmap(NULL, COVER_SIZE * sizeof(unsigned long), PROT_READ | PROT_WRITE, MAP_SHARED, g_fd, 0);

    if ((void*)g_cover == MAP_FAILED)
        perror("MAP_FAILED"), exit(1);
}

#else

int kcov_is_has_new_feature()
{
    return 0;
}

void kcov_start_feature()
{}

void kcov_end_feature()
{}

void init_kcov(void)
{}

#endif
