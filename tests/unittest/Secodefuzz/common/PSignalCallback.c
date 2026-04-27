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

本模块注册新号函数，exit退出函数，asan退出函数

只初始化一次
对外只提供初始化函数，其他均为模块内部函数

去掉本模块功能，只要注释掉HAS_SIGNAL的声明即可

*/

#include "PCommon.h"

#ifdef HAS_SIGNAL

static int g_has_init = 0;
extern int is_need_record_corpus;

int g_is_SIGINT = 0;
int g_is_CRASH = 0;

static void ExitHandler(void)
{

    if (g_is_SIGINT)  //来自主动退出，就不打印样本了
        return;

    if (g_is_CRASH)  //来自crash 退出，就不打印样本了
        return;

    hw_printf("\r\n  ********The program exit for run complete!!!********  \r\n ");
    // Corpus_Show_Cur();
    exit(0);
}
#ifndef _MSC_VER
static void CrashHandler(int aaa, siginfo_t* bbb, void* ccc)
{
    hw_printf("\r\n  ********The program exit for crash !!!********  \r\n ");
    Corpus_Show_Cur();
    llvmhook_print_stack_trace();
    g_is_CRASH = 1;
    exit(0);
}

extern int is_doing_leak;

static void SIGINTHandler(int aaa, siginfo_t* bbb, void* ccc)
{

    if (is_doing_leak) {
        hw_printf("\r\n  ********SIGINT Handler !!!********  \r\n ");
        hw_printf("\r\n  ********For CTRL +C !!!********  \r\n ");
        hw_printf("\r\n  ********is doing leak, so not exit********  \r\n ");
        return;
    }

    hw_printf("\r\n  ********SIGINT Handler !!!********  \r\n ");
    hw_printf("\r\n  ********The program exit for CTRL +C !!!********  \r\n ");
    llvmhook_print_stack_trace();
    g_is_SIGINT = 1;
    exit(0);
}

static void SIGTERMHandler(int aaa, siginfo_t* bbb, void* ccc)
{
    hw_printf("\r\n  ********SIGTERM Handler !!!********  \r\n ");
    CrashHandler(aaa, bbb, ccc);
}

static void SIGSEGVHandler(int aaa, siginfo_t* bbb, void* ccc)
{
    hw_printf("\r\n  ********SIGSEGV Handler !!!********  \r\n ");
    CrashHandler(aaa, bbb, ccc);
}

static void SIGBUSHandler(int aaa, siginfo_t* bbb, void* ccc)
{
    hw_printf("\r\n  ********SIGBUS Handler !!!********  \r\n ");
    CrashHandler(aaa, bbb, ccc);
}

static void SIGABRTHandler(int aaa, siginfo_t* bbb, void* ccc)
{
    hw_printf("\r\n  ********SIGABRT Handler !!!********  \r\n ");
    CrashHandler(aaa, bbb, ccc);
}

static void SIGILLHandler(int aaa, siginfo_t* bbb, void* ccc)
{
    hw_printf("\r\n  ********SIGILL Handler !!!********  \r\n ");
    CrashHandler(aaa, bbb, ccc);
}

static void SIGFPEHandler(int aaa, siginfo_t* bbb, void* ccc)
{
    hw_printf("\r\n  ********SIGFPE Handler !!!********  \r\n ");
    CrashHandler(aaa, bbb, ccc);
}

static void SIGXFSZHandler(int aaa, siginfo_t* bbb, void* ccc)
{
    hw_printf("\r\n  ********SIGXFSZ Handler !!!********  \r\n ");
    CrashHandler(aaa, bbb, ccc);
}
#endif

static void AsanHandler()
{
    hw_printf("\r\n  ********The program exit from asan !!!********  \r\n ");
    is_need_record_corpus = 2;
    Corpus_Show_Cur();
}
#ifndef _MSC_VER
static void SetSigaction(int signum, void (*callback)(int, siginfo_t*, void*))
{
    struct sigaction sigact = {};
    if (sigaction(signum, NULL, &sigact)) {
        hw_printf("libFuzzer: sigaction failed with %d\n", 1);
        exit(1);
    }
    if (sigact.sa_flags & SA_SIGINFO) {
        if (sigact.sa_sigaction)
            return;
    } else {
        if (sigact.sa_handler != SIG_DFL && sigact.sa_handler != SIG_IGN && sigact.sa_handler != SIG_ERR)
            return;
    }

    // sigact = {};
    sigact.sa_sigaction = callback;
    if (sigaction(signum, &sigact, 0)) {
        hw_printf("libFuzzer: sigaction failed with %d\n", 1);
        exit(1);
    }
}
#endif

void init_SignalCallback(void)
{
    if (g_has_init == 1)
        return;
    g_has_init = 1;

#ifndef _MSC_VER
    //先一起都注册了，以后有问题再说
    SetSigaction(SIGINT, SIGINTHandler);  // ctrl +c  还是不要注册这个了
    SetSigaction(SIGTERM, SIGTERMHandler);
    SetSigaction(SIGSEGV, SIGSEGVHandler);
    SetSigaction(SIGBUS, SIGBUSHandler);
    SetSigaction(SIGABRT, SIGABRTHandler);
    SetSigaction(SIGILL, SIGILLHandler);
    SetSigaction(SIGFPE, SIGFPEHandler);
    SetSigaction(SIGXFSZ, SIGXFSZHandler);
#endif

    atexit(ExitHandler);

    llvmhook_register_asan_callback(AsanHandler);
    return;
}

#else

void init_SignalCallback(void)
{
    return;
}
#endif
