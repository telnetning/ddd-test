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

#include "ut_utilities/ut_debug_point_api.h"
#include "common/error/dstore_error.h"
#include "common/instrument/trace/dstore_trace.h"
#include <pthread.h>
#include <gtest/gtest.h>
#include <sys/mman.h>

namespace DSTORE {

/*
 * Unique name for the shared memory used by DebugPoint API.
 * Preprocessor timestamp added to avoid conflicts when multiple users are
 * running on the same machine.
 */
static const char *UT_DEBUG_POINT_API_SHARED_MEM_NAME = "UTDebugPointAPISharedMem_" __TIME__;
/* Currently, only 32 bit integers are supported for CHANGE_DATA actions. */
static const int UT_DEBUG_POINT_CHANGE_DATA_SUPPORTED_DATASIZE = sizeof(int);

/*
 * Static data should be defined outside the class.
 */
DebugPointMgr *DebugPointMgr::m_instance{nullptr};
std::mutex DebugPointMgr::m_mutex;

/* ======= UTILITY HELPER FUNCTIONS ======== */

/* ======= PRIVATE HELPER FUNCTIONS ======== */

/*
 * Performs Suspend debug point action.
 * Uses debug point index in the debug points array which means that the match
 * has been already done inside CheckDebugPoint which calls this function.
 *
 * isBarrier is set to true when SuspendDebugPoint() is used by the BARRIER
 * action. In that case, we just need the "suspend" part of this function since
 * curNumSuspended counter has already been increased by the
 * ResumeIfBarrierReached() function. Default value is false.
 */
void DebugPointMgr::SuspendDebugPoint(int dpIndex, bool isBarrier)
{
    ThreadId threadId = (ThreadId) pthread_self();

    /* Update shared memory data and suspend thread. */
    int rc = pthread_rwlock_wrlock(&m_debugPointCB->rwLock);
    EXPECT_TRUE(rc == 0);
    DebugPointData *debugPoint = &(m_debugPointCB->debugPoints)[dpIndex];
    /* For barrier, counter has been already incremented in ResumeIfBarrierReached() */
    if (!isBarrier) {
        debugPoint->curNumSuspended++;
    }
    /* debug printout */
    rc = pthread_rwlock_unlock(&m_debugPointCB->rwLock);
    EXPECT_TRUE(rc == 0);
    /*
     * Wait until ResumeDebugPoint is called: we are not holding a lock.
     * Note: In case of a problem, this loop will be interrupted by the UT
     *   infrastructure's own 120 seconds timeout.
     */
    while (debugPoint->keepSuspended > 0) {
        /*
         * useExtraChecks will be set by the overloaded ResumeDebugPoint().
         * Extra checks for now include VNode comparison: resumes based on VNodeId.
         */
        if (debugPoint->useExtraChecks == 1) {
            break;
        }
        GaussUsleep(UT_DEBUG_POINT_SLEEP_WHILE_SUSPENDED_MS);
    }
    /* Update suspend count after resuming */
    (m_debugPointCB->debugPoints)[dpIndex].curNumSuspended -= 1;
    rc = pthread_rwlock_wrlock(&m_debugPointCB->rwLock);
    EXPECT_TRUE(rc == 0);
    rc = pthread_rwlock_unlock(&m_debugPointCB->rwLock);
    EXPECT_TRUE(rc == 0);
    return;
}
/*
 * Helper function to find the index of the given debug point
 * in shared memory.
 * Caller should acquire shared lock before calling this function.
 */
int DebugPointMgr::GetDebugPointIndex(DebugPointId *debugPointId)
{
    /* Return "not-found" error if the debug point array is empty */
    if (m_debugPointCB->lastUsedDebugPointIndex == 0) {
        return UT_DEBUG_POINT_NOT_FOUND;
    }
    int dpIndex = 0;
    /*
     * Search the debug point array for point that matches *debugPointId.
     */
    for (dpIndex = 0; dpIndex <= m_debugPointCB->lastUsedDebugPointIndex; dpIndex++) {
        DebugPointData *debugPoint = &(m_debugPointCB->debugPoints)[dpIndex];
        if ((debugPoint->traceId == debugPointId->targetTraceId) &&
            (debugPoint->traceType == debugPointId->targetTraceType) &&
            (debugPoint->traceType != TraceType::TRACE_DATA ||
             debugPoint->probeId == debugPointId->targetProbeId) &&
            ((debugPoint->traceDataLen == 0) ||
                ((debugPoint->traceDataLen == debugPointId->targetDataLen) &&
                    (memcmp((void *)debugPointId->targetData, (void *)debugPoint->traceData, debugPoint->traceDataLen) == 0)))) {
            break;
        }
    }

    /* This debug point was not found in the debug point array, return error. */
    if (dpIndex > m_debugPointCB->lastUsedDebugPointIndex) {
        return UT_DEBUG_POINT_NOT_FOUND;
    }

    return dpIndex;
}

/*
 * CleanDebugPoint resets the specified section of
 * shared memory using the input index.
 * Note: Caller should lock to protect critical sections.
 */
void DebugPointMgr::CleanDebugPoint(int index)
{
    /*
     * The usual aggregation initialization doesn't work:
     *     (m_debugPointCB->debugPoints)[index] = {0};
     * since std::atomic inside DebugPointData doesn't allow to use copy
     * constructor (it's deleted).
     */
    errno_t rc = memset_s(&(m_debugPointCB->debugPoints)[index], sizeof((m_debugPointCB->debugPoints)[index]),
        0, sizeof((m_debugPointCB->debugPoints)[index]));
    storage_securec_check(rc, "\0", "\0");
    /* Make sure that the memory is reset: for 2 fields */
    ASSERT_TRUE((int)m_debugPointCB->debugPoints[index].traceType == 0);
    ASSERT_TRUE(m_debugPointCB->debugPoints[index].traceId == 0);
    /* Some fields should be initialized to the default value that is not 0. */
    (m_debugPointCB->debugPoints)[index].probeId = -1;
    for (int i = 0; i < UT_DEBUG_POINT_MAX_NUM_NODES; i++) {
        (m_debugPointCB->debugPoints)[index].vNodeId[i] = -1;
    }
}

/*
 * Resumes threads suspended in debug point when barrier is reached.
 * Ssuspends the thread when barrier is not reached.
 * Note that the Debug Point API allows us to have barriers shared by threads
 * and processes (nodes).
 * Important: this function will wait for all threads to resume.
 */
void DebugPointMgr::ResumeIfBarrierReached(int index)
{
    bool barrierReached = false;
    /* Need write lock since we change keepSuspended. */
    int rc = pthread_rwlock_wrlock(&m_debugPointCB->rwLock);
    EXPECT_TRUE(rc == 0);
    DebugPointData *debugPoint = &(m_debugPointCB->debugPoints)[index];
    /*
     * This function is called when we reach the debug point. If barrier is
     * reached we should resume all threads but if not, we need to suspend.
     * Note that we can't suspend and then check for barrier in the same thread
     * so we check for barrier first. The last thread that reached the barrier
     * will not be suspended.
     * That's why we increment curNumSuspended by 1 before checking the
     * condition below: the current thread is included then.
     */
    debugPoint->curNumSuspended++;
    if (debugPoint->curNumSuspended >= debugPoint->expectedNumSuspended) {
        debugPoint->keepSuspended = 0;
        /* Since the last thread will not be suspended, we may exclude it right now. */
        debugPoint->curNumSuspended--;
        barrierReached = true;
    }
    rc = pthread_rwlock_unlock(&m_debugPointCB->rwLock);
    EXPECT_TRUE(rc == 0);
    if (barrierReached) {
        /* Wait for all threads to resume (lock is not needed) */
        while (debugPoint->curNumSuspended != 0) {
            GaussUsleep(UT_DEBUG_POINT_SLEEP_WHILE_SUSPENDED_MS);
        }
    } else {
        /*
         * While barrier is not reached, we still need to suspend the thread.
         * isBarrier is set to true here to indicate that debug point is a
         * barrier and so SuspendDebugPoint() doesn't need to increment
         * curNumSuspended counter (or we will increment it twice).
         * The call below uses only a "suspend" part of SuspendDebugPoint().
         */
        SuspendDebugPoint(index, /* isBarrier */ true);
    }
}

/*
 * Create shared memory used by the debug point Control Block.
 * It's kept separate from shared memory that UTMultiNodeDstore uses for its
 * testing. It's done so that API may be used independently.
 */
void DebugPointMgr::CreateDebugPointCB()
{
    size_t cbSize = sizeof(struct DebugPointControlBlock);
    /* Open file descriptor. */
    int fd = shm_open(UT_DEBUG_POINT_API_SHARED_MEM_NAME, O_CREAT|O_RDWR, 0777);
    ASSERT_TRUE(fd > 0);
    /* Resize the file to size we need for shared memory. */
    int rc = ftruncate(fd, cbSize);
    ASSERT_TRUE(rc != -1);
    m_debugPointCB = (DebugPointControlBlock *) mmap(nullptr, cbSize, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    ASSERT_TRUE(m_debugPointCB != nullptr);
    return;
}

/* Destroy shared memory used by DebugPointApi. */
void DebugPointMgr::DestroyDebugPointCB()
{
    /* Shared memory clean up. */
    ASSERT_TRUE(m_debugPointCB != nullptr);
    size_t cbSize = sizeof(struct DebugPointControlBlock);
    munmap((void *)m_debugPointCB, cbSize);
    shm_unlink(UT_DEBUG_POINT_API_SHARED_MEM_NAME);
    return;
}


/* =======  USER INTERFACE FUNCTIONS: DebugPoint API  ======== */

/*
 * More info about the API is provided at the beginning of the DebugPoint API
 * header file.
 */

/*
 * Callback function used by trace calls within the codebase.
 *
 * Important: this function may not be a non-static method of the class since
 * we use its address in Trace::Initialize().
 *
 * This function will be called inside storage_trace_<> function when the trace
 * point (at least {traceType, traceID, targetProbeId}) will match the debug
 * point that was set previously in the shared memory. The previously set debug
 * action stored inside debug point will be executed by this function.
 */
void CheckDebugPointCallback(const TraceType type, const uint32_t probe,
    const uint32_t rec_id, const TraceDataFmt fmt_type, const char *data, size_t data_len)
{
    DebugPointMgr::GetDebugPointMgr()->CheckDebugPoint(type, probe, rec_id, fmt_type, data, data_len);
}

/*
 * Uses mutex to ensure that DebugPointMgr instance is initialized once.
 */
DebugPointMgr *DebugPointMgr::GetDebugPointMgr()
{
    std::lock_guard<std::mutex> lock(m_mutex);
    if (m_instance == nullptr)
    {
        m_instance = new DebugPointMgr();
    }
    return m_instance;
}

/* Create shared memory used by the DebugPoint API and initialize rwlock */
void DebugPointMgr::Initialize()
{
    /* Create shared memory which will store the debugPoint API control block. */
    CreateDebugPointCB();
    /* Make sure all counters are set to 0 */
    errno_t ret = memset_s(m_debugPointCB, sizeof(struct DebugPointControlBlock), 0,
        sizeof(struct DebugPointControlBlock));
    storage_securec_check(ret, "\0", "\0");
    /* Setup the rwlock shared by all threads and processes. */
    int rc = pthread_rwlockattr_init(&m_rwLockAttribute);
    EXPECT_TRUE(rc == 0);
    rc = pthread_rwlockattr_setpshared(&m_rwLockAttribute, PTHREAD_PROCESS_SHARED);
    EXPECT_TRUE(rc == 0);
    rc = pthread_rwlock_init(&(m_debugPointCB)->rwLock, &m_rwLockAttribute);
    EXPECT_TRUE(rc == 0);

    /*
     * Read flag to show logs for concurrency infrastructure logs from env.
     * The flag is set to 0 by default.
     * Example cmd:
     * export UT_CONCURRENCY_USE_VERBOSE_DIAGNOSTICS=1
     * unset UT_CONCURRENCY_USE_VERBOSE_DIAGNOSTICS
     */
    m_useVerboseDiagnostics = false;
    char *useVerboseDiagnostics = getenv("UT_CONCURRENCY_USE_VERBOSE_DIAGNOSTICS");
    if (useVerboseDiagnostics != nullptr) {
        std::string param(useVerboseDiagnostics);
        m_useVerboseDiagnostics = (std::stoi(param) != 0);
    }
    return;
}

/* Destroy shared memory, rwLock properties and rwLock */
void DebugPointMgr::Destroy()
{
    std::lock_guard<std::mutex> lock(m_mutex);
    /* Destroy rwLock properties and rwLock */
    int rc = pthread_rwlockattr_destroy(&m_rwLockAttribute);
    EXPECT_TRUE(rc == 0);
    rc = pthread_rwlock_destroy(&(m_debugPointCB)->rwLock);
    EXPECT_TRUE(rc == 0);
    /* Free memory allocated in Initialize() */
    if (m_instance != nullptr) {
        delete m_instance;
        m_instance = nullptr;
    }
    return;
}

/* Helper function to return names for the enum values of DebugAction. */
static const char *GetActionName(const DebugAction action)
{
    switch(action) {
        case DEBUG_ACTION_SUSPEND: return "DEBUG_ACTION_SUSPEND";
        case DEBUG_ACTION_COUNT: return "DEBUG_ACTION_COUNT";
        case DEBUG_ACTION_BARRIER: return "DEBUG_ACTION_BARRIER";
        case DEBUG_ACTION_CHANGE_DATA: return "DEBUG_ACTION_CHANGE_DATA";
        case DEBUG_ACTION_TRAP: return "DEBUG_ACTION_TRAP";
        default: return "Unsupported action";
    }
}

/* Helper function to return names for the trace types. */
static const char *GetTraceTypeName(TraceType type)
{
    switch(type) {
        case TraceType::TRACE_ENTRY: return "TRACE_ENTRY";
        case TraceType::TRACE_EXIT: return "TRACE_EXIT";
        case TraceType::TRACE_DATA: return "TRACE_DATA";
        default: return "Unsupported type";
    }
}

/*
 * Setup the debug point in the shared memory used by Debug Point API.
 *
 * This is the main API function. It sets debugPointID so that trace may detect
 * when that debug point is hit (target trace type (entry, exit, or data),
 * trace ID, probe ID).
 * When the trace encounters that point, it takes action specified by
 * DebugAction parameter.
 * debugPointOptions are used to control that injection by specifying target
 * node, etc.
 * IMPORTANT: most options have defaults. For example:
 *      DebugPointId debugPointId = {.targetTraceType = TRACE_ENTRY, .targetTraceId = UT_DUMMY_TRACE_ID};
 *      DebugPointOptions debugPointOptions;
 *      DebugPointMgr::GetDebugPointMgr()->SetupDebugPoint(&debugPointId, &debugPointOptions, DEBUG_ACTION_SUSPEND);
 *
 * Error handling: this function will assert when debug point with the same ID
 * exists or when number of debug points reached maximum.
 *
 * More info (overview of the API) is at the beginning of the corresponding header file.
 */
void DebugPointMgr::SetupDebugPoint(DebugPointId *debugPointId, DebugPointOptions *debugPointOptions, DebugAction action)
{
    EXPECT_TRUE(debugPointId != nullptr);
    EXPECT_TRUE(debugPointOptions != nullptr);
    int index = 0;

    /*
     * Determine whether the same debug point (with different options) exists.
     * Note that there could be only one action per debug point.
     */
    int rc = pthread_rwlock_rdlock(&m_debugPointCB->rwLock);
    EXPECT_TRUE(rc == 0);
    index = GetDebugPointIndex(debugPointId);
    /* Check that debug point doesn't already exist in the debug point array. */
    ASSERT_TRUE(index == UT_DEBUG_POINT_NOT_FOUND);
    rc = pthread_rwlock_unlock(&m_debugPointCB->rwLock);
    EXPECT_TRUE(rc == 0);

    /* Lock in EXCLUSIVE MODE since we will write to the control block. */
    rc = pthread_rwlock_wrlock(&m_debugPointCB->rwLock);
    EXPECT_TRUE(rc == 0);

    /* Add new debug point at the end of the debug point array */
    index = m_debugPointCB->lastUsedDebugPointIndex;
    CleanDebugPoint(index);

    /*
     * All debug points need targetTraceType, targetTraceId, targetProbeId,
     * targetVNodeId, and a debugAction.
     */
    DebugPointData *debugPoint = &(m_debugPointCB->debugPoints)[index];
    debugPoint->traceType = debugPointId->targetTraceType;
    debugPoint->traceId = debugPointId->targetTraceId;
    debugPoint->probeId = debugPointId->targetProbeId;
    for (int i = 0; i < UT_DEBUG_POINT_MAX_NUM_NODES; i++) {
        debugPoint->vNodeId[i] = debugPointOptions->targetVNodeId[i];
    }
    debugPoint->debugAction = action;
    debugPoint->expectedNumSuspended = debugPointOptions->expectedSuspended;
    debugPoint->expectedNumPass = debugPointOptions->expectedNumPass;
    if (debugPointId->targetDataLen > 0) {
        debugPoint->traceDataLen = debugPointId->targetDataLen;
        /* data length shouldn't exceed pre-allocated array and data pointer shouldn't be nullptr. */
        EXPECT_TRUE(debugPointId->targetDataLen <= UT_DEBUG_POINT_MAX_DATA_SIZE_BYTES);
        EXPECT_TRUE(debugPointId->targetData != nullptr);
        int rc = memcpy_s(debugPoint->traceData, debugPoint->traceDataLen,
            debugPointId->targetData, debugPoint->traceDataLen);
        storage_securec_check(rc, "\0", "\0");
     }

    /*
     * Certain debugActions need additional data to run.
     * Check the debugAction and get data as needed.
     */
    if (action == DEBUG_ACTION_SUSPEND || action == DEBUG_ACTION_BARRIER) {
        /* Initialize keep suspended flag to 1 so that execution would be suspended. */
        debugPoint->keepSuspended = 1;
    } else if (action == DEBUG_ACTION_CHANGE_DATA) {
        /* Set the value to change the data in the trace function. */
        debugPoint->newIntValue = debugPointOptions->newIntValue;
        debugPoint->errorCode = debugPointOptions->errorCode;
    }

    /* Update the lastUsedDebugPointIndex for next usage. */
    m_debugPointCB->lastUsedDebugPointIndex++;
    EXPECT_TRUE(m_debugPointCB->lastUsedDebugPointIndex < UT_DEBUG_POINTS_NUM_MAX);
    rc = pthread_rwlock_unlock(&m_debugPointCB->rwLock);
    EXPECT_TRUE(rc == 0);
    return;
}

/* Helper function to return names for the enum values. */
static const char *GetActionByName(DebugAction action)
{
    switch(action) {
        case DEBUG_ACTION_SUSPEND: return "DEBUG_ACTION_SUSPEND";
        case DEBUG_ACTION_COUNT: return "DEBUG_ACTION_COUNT";
        case DEBUG_ACTION_BARRIER: return "DEBUG_ACTION_BARRIER";
        case DEBUG_ACTION_CHANGE_DATA: return "DEBUG_ACTION_CHANGE_DATA";
        case DEBUG_ACTION_TRAP: return "DEBUG_ACTION_TRAP";
        default: return "Unsupported action";
    }
}

/*
 * Determines whether current trace point has been setup previously as the
 * debug point and if it was, takes the action stored in the debug point array
 * for this trace point.
 *
 * More info (overview of the API) is at the beginning of the corresponding header file.
 */
void DebugPointMgr::CheckDebugPoint(const TraceType type, const uint32_t probe, const uint32_t rec_id,
    const TraceDataFmt fmt_type, const char* data, size_t data_len)
{
    int dpIndex = 0;

    /* Take exclusive lock. */
    /* TODO: change this to shared lock and use atomics to change counters */
    int rc = pthread_rwlock_wrlock(&m_debugPointCB->rwLock);
    EXPECT_TRUE(rc == 0);

    /* Debug points become invalid once their traceId is set to UT_DEBUG_POINT_INVALID_SLOT. */
    ASSERT_TRUE(rec_id != UT_DEBUG_POINT_INVALID_SLOT);
    /*
     * Search the debug point array for debug point that matches this trace
     * point's traceID, trace type, probeId, target VNodeID, and so on.
     */
    DebugPointData *debugPoint;
    for (dpIndex = 0; dpIndex <= m_debugPointCB->lastUsedDebugPointIndex; dpIndex++) {
        debugPoint = &((m_debugPointCB->debugPoints)[dpIndex]);
        if ((debugPoint->traceId != UT_DEBUG_POINT_INVALID_SLOT) &&
            (debugPoint->traceId == rec_id) &&
            (debugPoint->traceType == type) &&
            (type != TraceType::TRACE_DATA || debugPoint->probeId == probe) &&
            (debugPoint->traceDataLen == 0 || ((data_len == debugPoint->traceDataLen) &&
                memcmp((void *)data, (void *)debugPoint->traceData, data_len) == 0))) {
            break;
        }
    }
    /* This debug point was not found in the control block, return as we have nothing to do. */
    if (dpIndex > m_debugPointCB->lastUsedDebugPointIndex) {
        rc = pthread_rwlock_unlock(&m_debugPointCB->rwLock);
        EXPECT_TRUE(rc == 0);
        return;
    }

    debugPoint->curNumPass += 1;
    /* Skip the first expectedNumPass times the debug point has been hit */
    if (debugPoint->curNumPass <= debugPoint->expectedNumPass) {
        rc = pthread_rwlock_unlock(&m_debugPointCB->rwLock);
        EXPECT_TRUE(rc == 0);
        return;
    }

    /*
     * Performance optimization: no need to take suspend or barrier action if
     * the debug point has been disabled, i.e. keepSuspended is 0.
     */
    if ((debugPoint->debugAction == DEBUG_ACTION_SUSPEND ||
           debugPoint->debugAction == DEBUG_ACTION_BARRIER) &&
        debugPoint->keepSuspended == 0) {
        rc = pthread_rwlock_unlock(&m_debugPointCB->rwLock);
        EXPECT_TRUE(rc == 0);
        return;
    }

    /*
     * Important: The current code may not work if we implement reuse of the
     * debug point slots. Then unlocking here and locking in
     * SuspendDebugPoint() may result in the race condition when debug point
     * will be changed or disappear on us.
     * TODO: change locking protocol if/when the slot reuse is implemented.
     */
    /* Unlock since "actions" will take their own locks. */
    rc = pthread_rwlock_unlock(&m_debugPointCB->rwLock);
    EXPECT_TRUE(rc == 0);

    /* Perform action specified for this debug point. */
    switch (debugPoint->debugAction) {
        case DEBUG_ACTION_SUSPEND: {
            SuspendDebugPoint(dpIndex);
            break;
        }
        case DEBUG_ACTION_COUNT: {
            /*
             * Do nothing as curNumPass has already been incremented.
             * The "hit" has been counted and so action has already been taken.
             */
            break;
        }
        case DEBUG_ACTION_BARRIER: {
            ResumeIfBarrierReached(dpIndex);
            break;
        }
        case DEBUG_ACTION_TRAP: {
            /* Trigger SIGSEGV by writing to address 0. Alternative is raise(SIGSEGV). */
            memcpy(0, "sigsegv", sizeof("sigsegv"));
            break;
        }
        case DEBUG_ACTION_CHANGE_DATA: {
            ASSERT_TRUE(data != nullptr);
            /*
             * For now, only integers are supported. The change-data action may
             * be used to change the RetStatus of the function call.
             * TODO: add data_type (in addition to data_len) to control the type of data.
             */
            ASSERT_TRUE(data_len <= UT_DEBUG_POINT_CHANGE_DATA_SUPPORTED_DATASIZE);
            /* Need to cast away constness of data pointer. */
            char *dstData = const_cast<char *>(data);
            /* The change-data action simply overwrites data with a new value. */
            char *srcData = reinterpret_cast<char *>(&debugPoint->newIntValue);
            errno_t rc = memcpy_s(dstData, data_len, srcData, data_len);
            storage_securec_check(rc, "\0", "\0");
            /* Set errorCode for the error injection. */
            if (debugPoint->errorCode != STORAGE_OK) {
                storage_set_error(debugPoint->errorCode);
            }
            break;
        }
        default: {
            break;
        }
    }
}

/* TODO: add DebugPointMgr::UpdateDebugPoint() to be able to change the debug point info. */

/*
 * Resume all threads suspended for debug point identified by *debugPointId.
 *
 * ResumeDebugPoint() has 2 overloaded versions to make code easier to
 * maintain. This version resumes all threads on all nodes that hit the debug
 * point.
 *
 * Important: this function will wait for all threads to resume.
 */
void DebugPointMgr::ResumeDebugPoint(DebugPointId *debugPointId)
{
    /*
     * We take shared lock here since we use atomic counter.
     * Shared lock is needed for GetDebugPointIndex().
     */
    int rc = pthread_rwlock_rdlock(&m_debugPointCB->rwLock);
    EXPECT_TRUE(rc == 0);
    int dpIndex = GetDebugPointIndex(debugPointId);
    ASSERT_TRUE(dpIndex != UT_DEBUG_POINT_NOT_FOUND);
    (m_debugPointCB->debugPoints)[dpIndex].keepSuspended = 0;
    (m_debugPointCB->debugPoints)[dpIndex].useExtraChecks = 0;
    rc = pthread_rwlock_unlock(&m_debugPointCB->rwLock);
    EXPECT_TRUE(rc == 0);
    /* Wait for all threads to resume (lock is not needed) */
    while ((m_debugPointCB->debugPoints)[dpIndex].curNumSuspended != 0) {
        GaussUsleep(UT_DEBUG_POINT_SLEEP_WHILE_SUSPENDED_MS);
    }
    return;
}

/*
 * Note: this is an overloaded function.
 *
 * Resume threads suspended for debug point identified by *debugPointId but
 * only on the specified list of nodes.
 *
 * It resumes all threads on the specified list of nodes that hit the debug point.
 *
 * Important: this function will wait for all threads on the list of resumed
 * nodes to resume.
 *
 * Example of usage:
 *     ResumeNodeList list = { 1, VNode2 };
 *     DebugPointMgr::GetDebugPointMgr()->ResumeDebugPoint(&debugPointId, &list);
 */
void DebugPointMgr::ResumeDebugPoint(DebugPointId *debugPointId, ResumeNodeList *nodeList)
{
    ASSERT_TRUE(nodeList != nullptr);
    ASSERT_TRUE(nodeList->numNodes > 0);
    /*
     * For now, we take exclusive/write lock here.
     * TODO: We may switch to atomic counters and avoid it for empty nodeList.
     */
    int rc = pthread_rwlock_wrlock(&m_debugPointCB->rwLock);
    EXPECT_TRUE(rc == 0);
    int dpIndex = GetDebugPointIndex(debugPointId);
    ASSERT_TRUE(dpIndex != UT_DEBUG_POINT_NOT_FOUND);
    DebugPointData *debugPoint = &(m_debugPointCB->debugPoints)[dpIndex];
    /* Disable extra checks while we change them */
    debugPoint->useExtraChecks = 0;
    ASSERT_TRUE(nodeList->numNodes <= UT_DEBUG_POINT_MAX_NUM_NODES);
    debugPoint->numResumeNodes = nodeList->numNodes;
    for (int i = 0; i < nodeList->numNodes; i++) {
       debugPoint->resumeNodeIds[i] = nodeList->nodeIds[i];
    }
    /* This should be set after all other changes. */
    debugPoint->useExtraChecks = 1;
    /* keepSuspended has to stay at 1 so nodes not in the list stay suspended. */
    rc = pthread_rwlock_unlock(&m_debugPointCB->rwLock);
    EXPECT_TRUE(rc == 0);
    /* Wait for all threads to resume on each node (lock is not needed) */
    for (int i = 0; i < nodeList->numNodes; i++) {
        while (debugPoint->numSuspendedPerNode[i] != 0) {
            GaussUsleep(UT_DEBUG_POINT_SLEEP_WHILE_SUSPENDED_MS);
        }
    }
    return;
}

/*
 * Function to get the number of suspended threads given the debug point.
 */
int DebugPointMgr::GetNumSuspended(DebugPointId *debugPointId)
{
    int count = 0;
    /* Shared/read lock is enough here */
    int rc = pthread_rwlock_rdlock(&m_debugPointCB->rwLock);
    EXPECT_TRUE(rc == 0);
    int dpIndex = GetDebugPointIndex(debugPointId);
    /* Assert (use EXPECT since function returns int) if debug point is not found. */
    EXPECT_TRUE(dpIndex != UT_DEBUG_POINT_NOT_FOUND);
    count = (m_debugPointCB->debugPoints)[dpIndex].curNumSuspended;
    rc = pthread_rwlock_unlock(&m_debugPointCB->rwLock);
    EXPECT_TRUE(rc == 0);
    return count;
}

/*
 * Function to determine if certain debug point has at least one thread
 * suspended in it.
 */
bool DebugPointMgr::IsDebugPointSuspended(DebugPointId *debugPointId)
{
    return (GetNumSuspended(debugPointId) > 0);
}

/*
 * Function to get the number of times function has been reached.
 */
int DebugPointMgr::GetHitCount(DebugPointId *debugPointId)
{
    int count = 0;
    /* Shared/read lock is enough here */
    int rc = pthread_rwlock_rdlock(&m_debugPointCB->rwLock);
    EXPECT_TRUE(rc == 0);
    int dpIndex = GetDebugPointIndex(debugPointId);
    /* Assert (use EXPECT since function returns int) if debug point is not found. */
    EXPECT_TRUE(dpIndex != UT_DEBUG_POINT_NOT_FOUND);
    count = (m_debugPointCB->debugPoints)[dpIndex].curNumPass;
    rc = pthread_rwlock_unlock(&m_debugPointCB->rwLock);
    EXPECT_TRUE(rc == 0);
    return count;
}

/*
 * Function to determine if certain debug point has been hit/reached at least
 * once.
 */
bool DebugPointMgr::IsDebugPointHit(DebugPointId *debugPointId)
{
    return (GetNumSuspended(debugPointId) > 0);
}

/*
 * Used to validate that debug point is set in control block.
 * Returns true if debug point is set, false otherwise.
 */
bool DebugPointMgr::IsDebugPointSet(DebugPointId *debugPointId)
{
    /* Shared/read lock is enough here */
    int rc = pthread_rwlock_rdlock(&m_debugPointCB->rwLock);
    EXPECT_TRUE(rc == 0);
    int dpIndex = GetDebugPointIndex(debugPointId);
    bool result = (dpIndex != UT_DEBUG_POINT_NOT_FOUND);
    rc = pthread_rwlock_unlock(&m_debugPointCB->rwLock);
    EXPECT_TRUE(rc == 0);
    return result;
}


/*
 * Activates debug point after it has been deactivated after ResumeDebugPoint()
 * call. ResumeDebugPoint() will set keepSuspended flag to 0 which means that
 * threads hitting this debug point will no longer be suspended.
 * Setting keepSuspended back to 1 will re-"activate" the debug point.
 */
void DebugPointMgr::ResetDebugPoint(DebugPointId *debugPointId)
{
    /* Exclusive/write lock is needed here */
    int rc = pthread_rwlock_wrlock(&m_debugPointCB->rwLock);
    EXPECT_TRUE(rc == 0);
    int dpIndex = GetDebugPointIndex(debugPointId);
    /* ResetDebugPoint() expects to find the debug point so we assert on error. */
    ASSERT_FALSE(dpIndex == UT_DEBUG_POINT_NOT_FOUND);
    /*
     * Reset requires that point has been resumed and if not, assert will be
     * triggered. Call Resume prior to Reset or use RemoveDebugPoint() function
     * to completely exclude this point.
     */
    ASSERT_TRUE((m_debugPointCB->debugPoints)[dpIndex].curNumSuspended == 0);
    (m_debugPointCB->debugPoints)[dpIndex].keepSuspended = 1;
    (m_debugPointCB->debugPoints)[dpIndex].curNumPass = 0;
    (m_debugPointCB->debugPoints)[dpIndex].useExtraChecks = 0;
    rc = pthread_rwlock_unlock(&m_debugPointCB->rwLock);
    EXPECT_TRUE(rc == 0);
    return;
}

/*
 * Disables/"removes" debug point from the debug point array. Note that slot is
 * not reused in the current implementation. Instead slot becomes unusable.
 * This is done by setting traceID to UT_DEBUG_POINT_INVALID_SLOT.
 *
 * TODO: add functionality to reuse the slots.
 */
void DebugPointMgr::RemoveDebugPoint(DebugPointId *debugPointId)
{
    /* Exclusive/write lock is needed here */
    int rc = pthread_rwlock_wrlock(&m_debugPointCB->rwLock);
    EXPECT_TRUE(rc == 0);
    int dpIndex = GetDebugPointIndex(debugPointId);
    ASSERT_TRUE(dpIndex != UT_DEBUG_POINT_NOT_FOUND);
    (m_debugPointCB->debugPoints)[dpIndex].traceId = UT_DEBUG_POINT_INVALID_SLOT;
    /*
     * Lazy removal: debug point is "resumed" so all threads suspended on
     * this debug point will continue execution.
     * No new threads will suspend on this point.
     * This is lazy approach because we first remove then resume.
     */
    (m_debugPointCB->debugPoints)[dpIndex].keepSuspended = 0;
    /* RemoveDebugPoint() expects to find the debug point so we assert on error. */
    ASSERT_FALSE(dpIndex == UT_DEBUG_POINT_NOT_FOUND);
    rc = pthread_rwlock_unlock(&m_debugPointCB->rwLock);
    EXPECT_TRUE(rc == 0);
    /* Wait until all threads waiting for this point resumed execution. */
    while ((m_debugPointCB->debugPoints)[dpIndex].curNumSuspended > 0) {
        GaussUsleep(UT_DEBUG_POINT_SLEEP_WHILE_SUSPENDED_MS);
    }
    return;
}

/* Debug function to print all debug points set in memory. */
void DebugPointMgr::PrintDebugPoints()
{
    /* Exit if we don't have debug verbose disgnostics set up. */
    if (!m_useVerboseDiagnostics) {
        return;
    }

    int maxIndex = m_debugPointCB->lastUsedDebugPointIndex;
    int rc = pthread_rwlock_rdlock(&m_debugPointCB->rwLock);
    EXPECT_TRUE(rc == 0);
    /*
     * Helper macro to print data elements of the debug point control block.
     * Uses std::cout and not printf since it works with different data types.
     */
#define DumpDebugPointDataMember(name) \
    do { \
        std::cout << "   " #name " = " << (m_debugPointCB->debugPoints)[i].name << std::endl; \
    } while (0)

    for (int i = 1; i <= maxIndex; i++) {
        printf("Debug point %d:\n   TraceType = %d\n", i, (int)((m_debugPointCB->debugPoints)[i].traceType));
        DumpDebugPointDataMember(traceId);
        DumpDebugPointDataMember(probeId);
        DumpDebugPointDataMember(vNodeId);
        DumpDebugPointDataMember(debugAction);
        DumpDebugPointDataMember(expectedNumSuspended);
        DumpDebugPointDataMember(expectedNumPass);
        DumpDebugPointDataMember(curNumSuspended);
        DumpDebugPointDataMember(keepSuspended);
        DumpDebugPointDataMember(curNumPass);
        DumpDebugPointDataMember(useExtraChecks);
        DumpDebugPointDataMember(numResumeNodes);
        DumpDebugPointDataMember(newIntValue);
        DumpDebugPointDataMember(errorCode);
        for (int j = 0; j < (m_debugPointCB->debugPoints)[i].numResumeNodes; j++) {
            DumpDebugPointDataMember(resumeNodeIds[j]);
            DumpDebugPointDataMember(numSuspendedPerNode[j]);
        }
    }
    rc = pthread_rwlock_unlock(&m_debugPointCB->rwLock);
    EXPECT_TRUE(rc == 0);
}

}  /* namespace DSTORE */
