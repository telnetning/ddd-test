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
 * dstore_relation_space.cpp
 *
 *
 * IDENTIFICATION
 *        include/tablespace/dstore_relation_space.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_RELATION_SPACE_H
#define DSTORE_RELATION_SPACE_H

#include "lock/dstore_lwlock.h"
#include "index/dstore_scankey.h"
#include "common/dstore_datatype.h"
#include "common/memory/dstore_mctx.h"
#include "page/dstore_page_struct.h"
#include "index/dstore_index_struct.h"
#include "tablespace/dstore_tablespace.h"
#include "tablespace/dstore_heap_segment.h"

namespace DSTORE {

enum ObjSpaceMgrTaskType : uint8 {
    INVALID_TASK_TYPE = 0,
    EXTEND_TASK = 1,
    RECYCLE_FSM_TASK = 2,
    RECYCLE_BTREE_TASK = 3,
    RECLAIM_BTREE_RECYCLE_PARTITION_TASK = 4,
    EXTEND_INDEX_TASK = 5,
};

union TaskExtraInfo {
    uint64 placeHolder = -1;
    PageId pageIdInfo;
    Xid xidInfo;
    bool operator==(const TaskExtraInfo &extraInfo) const
    {
        return this->placeHolder == extraInfo.placeHolder;
    }
};

/* Base info for ObjSpaceMgrTask */
class ObjSpaceMgrTaskInfo : public BaseObject {
public:
    ObjSpaceMgrTaskInfo(PdbId pdbId, const ObjSpaceMgrTaskType taskType, const TablespaceId tablespaceId,
        const PageId segmentId)
        : m_pdbId(pdbId),
          m_taskType(taskType),
          m_tablespaceId(tablespaceId),
          m_segmentId(segmentId)
    {
    }

    explicit ObjSpaceMgrTaskInfo(ObjSpaceMgrTaskInfo *taskInfo)
        : m_pdbId(taskInfo->m_pdbId), m_taskType(taskInfo->GetTaskType()), m_tablespaceId(taskInfo->GetTablespaceId()),
          m_segmentId(taskInfo->GetSegmentId())
    {
    }

    virtual ~ObjSpaceMgrTaskInfo() = default;

    inline ObjSpaceMgrTaskType GetTaskType() const
    {
        return m_taskType;
    }

    inline TablespaceId GetTablespaceId() const
    {
        return m_tablespaceId;
    }

    inline PageId GetSegmentId() const
    {
        return m_segmentId;
    }

    inline PdbId GetPdbId() const
    {
        return m_pdbId;
    }

    virtual void GetTaskExtraInfo(TaskExtraInfo &extraInfo)
    {
        extraInfo.placeHolder = -1;
    }

private:
    PdbId m_pdbId;
    ObjSpaceMgrTaskType m_taskType;
    TablespaceId m_tablespaceId;
    PageId m_segmentId;
};

/* Info used by different task types */
class ObjSpaceMgrExtendTaskInfo : public ObjSpaceMgrTaskInfo {
public:
    ObjSpaceMgrExtendTaskInfo(PdbId pdbId, const PageId heapSegMetaPageId,
        const TablespaceId tablespaceId, const PageId segmentId, const PageId fsmMetaPageId);

    explicit ObjSpaceMgrExtendTaskInfo(ObjSpaceMgrTaskInfo *taskInfo);

    ~ObjSpaceMgrExtendTaskInfo() = default;

    inline PageId GetFsmMetaPageId() const
    {
        return m_fsmMetaPageId;
    }

    inline PageId GetHeapSegMetaPageId() const
    {
        return m_heapSegMetaPageId;
    }

    void GetTaskExtraInfo(TaskExtraInfo &extraInfo) override
    {
        /* sizeof(PageId) = 6 while sizeof(TaskExtraInfo) = 8
         * Init the extraInfo with 0 first to make it comparable */
        extraInfo.placeHolder = 0;
        extraInfo.pageIdInfo = m_fsmMetaPageId;
    }

private:
    PageId m_fsmMetaPageId;
    PageId m_heapSegMetaPageId;
};

class ObjSpaceMgrRecycleBtreeTaskInfo : public ObjSpaceMgrTaskInfo {
public:
    Xid m_createdXid;
    IndexInfo *m_indexInfo;
    ScanKey m_scanKey;
    bool m_needFree = true;

    ObjSpaceMgrRecycleBtreeTaskInfo(PdbId pdbId, TablespaceId tablespaceId, const PageId segmentId,
        Xid createdXid, IndexInfo *indexInfo, ScanKey scanKey);

    explicit ObjSpaceMgrRecycleBtreeTaskInfo(ObjSpaceMgrTaskInfo *taskInfo);

    ~ObjSpaceMgrRecycleBtreeTaskInfo()
    {
        /* Free memory if needed */
        if (m_needFree) {
            if (m_indexInfo != nullptr) {
                m_indexInfo->Free();
                m_indexInfo = nullptr;
            }
            DstorePfreeExt(m_scanKey);
        } else {
            m_indexInfo = nullptr;
            m_scanKey = nullptr;
        }
    }

    inline void NeedFreeIndexMeta(bool needFree)
    {
        m_needFree = needFree;
    }

    void GetTaskExtraInfo(TaskExtraInfo &extraInfo) override
    {
        extraInfo.xidInfo = m_createdXid;
    }
};

/* Recycle and reassignment pages in unused BtreeRecyclePartition */
class ObjSpaceMgrReclaimBtrRecyclePartTaskInfo : public ObjSpaceMgrTaskInfo {
public:
    Xid m_createdXid;

    ObjSpaceMgrReclaimBtrRecyclePartTaskInfo(PdbId pdbId, const TablespaceId tablespaceId,
        const PageId segmentId, const Xid createdXid);

    explicit ObjSpaceMgrReclaimBtrRecyclePartTaskInfo(ObjSpaceMgrTaskInfo *taskInfo);

    ~ObjSpaceMgrReclaimBtrRecyclePartTaskInfo() = default;

    void GetTaskExtraInfo(TaskExtraInfo &extraInfo) override
    {
        extraInfo.xidInfo = m_createdXid;
    }
};

/* Recycle and reassignment of unused FSM partitions for maximized efficiency and minimized space waste */
class ObjSpaceMgrRecycleFsmTaskInfo : public ObjSpaceMgrTaskInfo {
public:
    ObjSpaceMgrRecycleFsmTaskInfo(PdbId pdbId, const TablespaceId tablespaceId, const PageId segmentId);

    explicit ObjSpaceMgrRecycleFsmTaskInfo(ObjSpaceMgrTaskInfo *taskInfo);

    ~ObjSpaceMgrRecycleFsmTaskInfo() = default;
};

class ObjSpaceMgrExtendIndexTaskInfo : public ObjSpaceMgrTaskInfo {
public:
    PageId m_recyclePartitionMeta;
    Xid m_createdXid;
    ObjSpaceMgrExtendIndexTaskInfo(PdbId pdbId, const TablespaceId tablespaceId, const PageId segmentId,
                                   PageId recyclePartitionMeta, Xid m_createdXid);
 
    explicit ObjSpaceMgrExtendIndexTaskInfo(ObjSpaceMgrTaskInfo *taskInfo);
 
    ~ObjSpaceMgrExtendIndexTaskInfo() = default;

    void GetTaskExtraInfo(TaskExtraInfo &extraInfo) override
    {
        extraInfo.placeHolder = 0;
        extraInfo.pageIdInfo = m_recyclePartitionMeta;
        extraInfo.xidInfo = m_createdXid;
    }
};

class ObjSpaceMgrTask : public BaseObject {
public:
    ObjSpaceMgrTask *m_nextTask;
    ObjSpaceMgrTaskInfo *m_taskInfo;

    explicit ObjSpaceMgrTask(ObjSpaceMgrTaskInfo *taskInfo)
        : m_nextTask(nullptr), m_taskInfo(taskInfo)
    {
    }

    virtual ~ObjSpaceMgrTask()
    {
        if (m_taskInfo != nullptr) {
            delete m_taskInfo;
            m_taskInfo = nullptr;
        }
        if (m_nextTask != nullptr) {
            m_nextTask = nullptr;
        }
    }

    DISALLOW_COPY_AND_MOVE(ObjSpaceMgrTask);
    inline void SetNext(ObjSpaceMgrTask *next)
    {
        m_nextTask = next;
    }

    bool IsCurrentTask(ObjSpaceMgrTaskInfo *taskInfo);
    RetStatus Execute();

    static void GetTaskExtraInfo(ObjSpaceMgrTaskInfo *taskInfo, TaskExtraInfo &extrInfo);
    RetStatus ExecuteRecycleBtreeTask() const;

private:
    RetStatus ExecuteExtendTask() const;
    RetStatus Doextend(HeapSegment *segment, TableSpace *tablespace) const;
    RetStatus ExecuteExtendBtreeTask() const;
    RetStatus VerifyExtendTask(ObjSpaceMgrExtendTaskInfo *extendInfo, TableSpace *tablespace) const;
    RetStatus ExecuteRecycleFsmTask() const;
    RetStatus ExecuteColdBtrRecyclePartReclaimTask() const;
};

class ObjSpaceMgrTaskQueue : public BaseObject {
public:
    ObjSpaceMgrTask *m_head = nullptr;
    ObjSpaceMgrTask *m_tail = nullptr;
    LWLock m_objSpaceMgrTaskQueueLock;

    ObjSpaceMgrTaskQueue();
    ~ObjSpaceMgrTaskQueue();

    void Initialize();
    void Destroy();
    void PushTaskIfNeeded(ObjSpaceMgrTask *newTask, bool *registered);
    ObjSpaceMgrTask* PopNextTask();
    bool FindTask(ObjSpaceMgrTaskInfo *taskInfo, bool needLock = true);
};

class ObjSpaceMgr : public BaseObject {
public:
    explicit ObjSpaceMgr(uint32 numObjSpaceMgrTaskQueue);
    virtual ~ObjSpaceMgr();

    RetStatus Initialize(DstoreMemoryContext currMemoryContext);
    void Destroy();

    bool IsObjSpaceMgrInitialized();
    ObjSpaceMgrTask *AllocateObjSpaceMgrTask(ObjSpaceMgrTaskInfo *taskInfo);
    RetStatus RegisterObjSpaceMgrTaskIfNeeded(ObjSpaceMgrTaskInfo *taskInfo);
    bool FindObjSpaceMgrTask(ObjSpaceMgrTaskInfo *taskInfo);
    ObjSpaceMgrTask *GetObjSpaceMgrTask(uint32 workerId);

    static bool IsExtensionTaskRegistered(PageId heapSegMetaPageId, const TablespaceId tbsId,
                                          const PageId segmentId, const PageId fsmId, PdbId pdbId);
    static bool IsRecycleFsmTaskRegistered(const TablespaceId tbsId, const PageId segmentId, PdbId pdbId);
    static bool IsRecycleBtreeTaskRegistered(const TablespaceId tbsId, const PageId segmentId, const Xid createdXid,
                                             PdbId pdbId);
    static bool IsReclaimColdBtrRecyclePartTaskRegistered(const TablespaceId tbsId, const PageId segmentId,
                                                          const Xid createdXid, PdbId pdbId);
    static bool IsExtensionIndexTaskRegistered(const TablespaceId tbsId, const PageId segmentId,
                                               PageId recyclePartitionMeta, Xid createdXid, PdbId pdbId);

private:
    ObjSpaceMgrTaskQueue *FindObjSpaceMgrTaskQueue(uint32 id);

    bool m_initialized;
    DstoreMemoryContext m_ctx;
    uint32 m_numObjSpaceMgrTaskQueue;
    ObjSpaceMgrTaskQueue *m_objSpaceMgrTaskQueueList;
};

} /* namespace DSTORE */
#endif
