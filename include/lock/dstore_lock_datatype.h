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

#ifndef DSTORE_LOCK_DATATYPE_H
#define DSTORE_LOCK_DATATYPE_H

#include "common/dstore_datatype.h"
#include "lock/dstore_lwlock.h"
#include "undo/dstore_undo_types.h" /* Xid */
#include "transaction/dstore_transaction_types.h"
#include "logical_replication/dstore_logical_types.h"
#include "common/algorithm/dstore_string_info.h"
#include "lock/dstore_lock_struct.h"
#include "diagnose/dstore_lock_mgr_diagnose.h"
#include "framework/dstore_thread.h"

namespace DSTORE {

constexpr uint32 DatabaseRelationId = 1262;
union RwLock {
    pthread_rwlock_t lock;
    char pad[DSTORE_CACHELINE_SIZE];
};

enum LockMessageType : uint8 {
    LOCK_MSG_DEADLOCK_DETECT = 0x0,
    LOCK_MSG_DIST_LOCK_MGR,
    LOCK_MSG_TABLE_DIST_LOCK_MGR,
    LOCK_MSG_XACT_DIST_LOCK_MGR,
    LOCK_MSG_MAX
};

/*
 * LOCKTAG is the key information needed to look up a LOCK item in the
 * lock hashtable.	A LOCKTAG value uniquely identifies a lockable object.
 *
 * The LockTagType enum defines the different kinds of objects we can lock.
 * We can handle up to 256 different LockTagTypes.
 */
enum LockTagType : uint16 {
    LOCKTAG_TABLE,        /* whole table */
    LOCKTAG_TABLE_EXTEND, /* the right to extend a table */
    LOCKTAG_TBS_EXTEND,   /* the right to extend a tablespace */
    LOCKTAG_TRANSACTION,  /* transaction (for waiting for xact done) */
    LOCKTAG_CSN,
    LOCKTAG_ZONE,
    LOCKTAG_CONTROL_FILE,
    LOCKTAG_CONTROL_FILE_WAL_STREAM_ASSIGN,
    LOCKTAG_PARTITION,       /* one table partition */
    LOCKTAG_DEADLOCK_DETECT,
    LOCKTAG_PDB,
    LOCKTAG_LOGICAL_REPLICATION, /* logical replication type lock */
    LOCKTAG_PDB_REPLICA, /* pdb replica type lock */
    LOCKTAG_OBJECT, /* object type lock */
    LOCKTAG_ADVISORY,  /* advisory lock */
    LOCKTAG_PACKAGE,
    LOCKTAG_PROCEDURE,
    LOCKTAG_CLEANPD,
    LOCKTAG_TABLESPACE,
    LOCKTAG_BACKUP_RESTORE,
    LOCKTAG_MAX_NUM
};

/*
 * Lock methods are identified by LOCKMETHODID.  (Despite the declaration as
 * uint16, we are constrained to 256 lockmethods by the layout of LOCKTAG.)
 */
enum LockMethodId : uint8 {
    DEFAULT_LOCKMETHOD = 1,
    USER_LOCKMETHOD,
    NODELEVEL_LOCKMETHOD, /* Only one lock per node. Any thread of this node can release the lock. */
    MAX_LOCKMETHOD
};

enum LockRecoveryMode : uint8 {
    RELEASE_AFTER_LOCK_RECOVERY,
    RELEASE_AFTER_SYSTEM_RECOVERY
};

constexpr uint32 OFFSET_32_BITS = 32;
constexpr uint32 MASK_32_BITS = 0xFFFFFFFF;

/*
 * The LockTag struct is defined with malice aforethought to fit into 16
 * bytes with no padding.  Note that this would need adjustment if we were
 * to widen Oid, BlockNumber, or TransactionId to more than 32 bits.
 * If you modify this data structure, the padding space must be zero.
 * We include lockMethodId and recoveryMode in the lockTag so that a single hash
 * table in shared memory can store all types of lock.
 */
struct LockTag {
    uint32 field1;        /* a 32-bit ID field */
    uint32 field2;        /* a 32-bit ID field */
    uint32 field3;        /* a 32-bit ID field */
    uint32 field4;        /* a 32-bit ID field */
    uint32 field5;        /* a 32-bit ID field */
    LockTagType lockTagType;       /* a 16-bit tag type field */
    LockMethodId lockMethodId;     /* a 8-bit method id field */
    LockRecoveryMode recoveryMode; /* a 8-bit recovery mode field */
    /*
     * Default LockTag constructor. This constructor
     * sets everything to zero.
     */
    LockTag()
        : field1(0),
          field2(0),
          field3(0),
          field4(0),
          field5(0),
          lockTagType(LOCKTAG_MAX_NUM),
          lockMethodId(DEFAULT_LOCKMETHOD),
          recoveryMode(RELEASE_AFTER_LOCK_RECOVERY)
    {}

    /*
     * Set locktag field 1-5.
     */
    inline void SetFields(uint32 field1In, uint32 field2In, uint32 field3In, uint32 field4In,
        uint32 field5In)
    {
        field1 = field1In;
        field2 = field2In;
        field3 = field3In;
        field4 = field4In;
        field5 = field5In;
    }

    /*
     * Set locktag method id.
     */
    void SetLockMethodId()
    {
        StorageAssert(static_cast<uint16>(lockTagType) < static_cast<uint16>(LOCKTAG_MAX_NUM));
        static const struct {
            LockTagType tagType;
            LockMethodId methodId;
        } LOCK_METHOD_MAP[LOCKTAG_MAX_NUM] = {
            {LOCKTAG_TABLE,                          DEFAULT_LOCKMETHOD},
            {LOCKTAG_TABLE_EXTEND,                   DEFAULT_LOCKMETHOD},
            {LOCKTAG_TBS_EXTEND,                     DEFAULT_LOCKMETHOD},
            {LOCKTAG_TRANSACTION,                    DEFAULT_LOCKMETHOD},
            {LOCKTAG_CSN,                            NODELEVEL_LOCKMETHOD},
            {LOCKTAG_ZONE,                           NODELEVEL_LOCKMETHOD},
            {LOCKTAG_CONTROL_FILE,                   DEFAULT_LOCKMETHOD},
            {LOCKTAG_CONTROL_FILE_WAL_STREAM_ASSIGN, DEFAULT_LOCKMETHOD},
            {LOCKTAG_PARTITION,                      DEFAULT_LOCKMETHOD},
            {LOCKTAG_DEADLOCK_DETECT,                NODELEVEL_LOCKMETHOD},
            {LOCKTAG_PDB,                            DEFAULT_LOCKMETHOD},
            {LOCKTAG_LOGICAL_REPLICATION,            DEFAULT_LOCKMETHOD},
            {LOCKTAG_PDB_REPLICA,                    DEFAULT_LOCKMETHOD},
            {LOCKTAG_OBJECT,                         DEFAULT_LOCKMETHOD},
            {LOCKTAG_ADVISORY,                       USER_LOCKMETHOD},
            {LOCKTAG_PACKAGE,                        DEFAULT_LOCKMETHOD},
            {LOCKTAG_PROCEDURE,                      DEFAULT_LOCKMETHOD},
            {LOCKTAG_CLEANPD,                        DEFAULT_LOCKMETHOD},
            {LOCKTAG_TABLESPACE,                     DEFAULT_LOCKMETHOD},
            {LOCKTAG_BACKUP_RESTORE,                 DEFAULT_LOCKMETHOD}
        };
        lockMethodId = LOCK_METHOD_MAP[lockTagType].methodId;
    }

    /*
     * Set locktag recovery mode.
     */
    inline void SetRecoveryMode()
    {
        StorageAssert(static_cast<uint16>(lockTagType) < static_cast<uint16>(LOCKTAG_MAX_NUM));
        static const struct {
            LockTagType tagType;
            LockRecoveryMode recoveryMode;
        } LOCK_RECOVERY_MAP[LOCKTAG_MAX_NUM] = {
            {LOCKTAG_TABLE,                          RELEASE_AFTER_SYSTEM_RECOVERY},
            {LOCKTAG_TABLE_EXTEND,                   RELEASE_AFTER_LOCK_RECOVERY},
            {LOCKTAG_TBS_EXTEND,                     RELEASE_AFTER_LOCK_RECOVERY},
            {LOCKTAG_TRANSACTION,                    RELEASE_AFTER_LOCK_RECOVERY},
            {LOCKTAG_CSN,                            RELEASE_AFTER_LOCK_RECOVERY},
            {LOCKTAG_ZONE,                           RELEASE_AFTER_LOCK_RECOVERY},
            {LOCKTAG_CONTROL_FILE,                   RELEASE_AFTER_LOCK_RECOVERY},
            {LOCKTAG_CONTROL_FILE_WAL_STREAM_ASSIGN, RELEASE_AFTER_LOCK_RECOVERY},
            {LOCKTAG_PARTITION,                      RELEASE_AFTER_LOCK_RECOVERY},
            {LOCKTAG_DEADLOCK_DETECT,                RELEASE_AFTER_LOCK_RECOVERY},
            {LOCKTAG_PDB,                            RELEASE_AFTER_LOCK_RECOVERY},
            {LOCKTAG_LOGICAL_REPLICATION,            RELEASE_AFTER_LOCK_RECOVERY},
            {LOCKTAG_PDB_REPLICA,                    RELEASE_AFTER_LOCK_RECOVERY},
            {LOCKTAG_OBJECT,                         RELEASE_AFTER_LOCK_RECOVERY},
            {LOCKTAG_ADVISORY,                       RELEASE_AFTER_SYSTEM_RECOVERY},
            {LOCKTAG_PACKAGE,                        RELEASE_AFTER_SYSTEM_RECOVERY},
            {LOCKTAG_PROCEDURE,                      RELEASE_AFTER_SYSTEM_RECOVERY},
            {LOCKTAG_CLEANPD,                        RELEASE_AFTER_LOCK_RECOVERY},
            {LOCKTAG_TABLESPACE,                     RELEASE_AFTER_LOCK_RECOVERY},
            {LOCKTAG_BACKUP_RESTORE,                 RELEASE_AFTER_LOCK_RECOVERY}
        };
        recoveryMode = LOCK_RECOVERY_MAP[lockTagType].recoveryMode;
    }

    /*
     * Set fields and other flags given lock tag type.
     */
    inline void FillInData(uint32 field1In, uint32 field2In, uint32 field3In, uint32 field4In,
        uint32 field5In)
    {
        SetLockMethodId();
        SetRecoveryMode();
        SetFields(field1In, field2In, field3In, field4In, field5In);
    }

    /*
     * Set locktag type.
     */
    inline void SetLockTagType(LockTagType tagType)
    {
        StorageAssert(static_cast<uint16>(tagType) < static_cast<uint16>(LOCKTAG_MAX_NUM));
        lockTagType = tagType;
    }

    /*
     * The following function will set the locktag
     * field in accordance with a transaction locktag.
     */
    void SetTransactionLockTag(Oid pdbId, Xid xid)
    {
        lockTagType = LOCKTAG_TRANSACTION;
        FillInData(pdbId, GetLow32Bits(xid.m_placeHolder), GetHigh32Bits(xid.m_placeHolder), 0, 0);
    }

    /*
     * The following function will set the locktag
     * field in accordance with a undo zone locktag.
     * This locktag will use NODELEVEL_LOCKMETHOD because any threads
     * from the same node can release the lock.
     * There should be only one undo lock for each node.
     */
    void SetUndoZoneLockTag(Oid pdbId, ZoneId zoneId)
    {
        StorageAssert(zoneId != INVALID_ZONE_ID);
        lockTagType = LOCKTAG_ZONE;
        FillInData(pdbId, static_cast<uint32>(zoneId), 0, 0, 0);
    }
    /*
     * The following function will set the locktag
     * field in accordance with a CSN locktag.
     */
    void SetCSNLockTag()
    {
        lockTagType = LOCKTAG_CSN;
        FillInData(0, 0, 0, 0, 0);
    }
    /*
     * The following function will set the locktag
     * field in accordance with a table for extension.
     */
    void SetTableExtensionLockTag(Oid pdbId, const PageId segmentId)
    {
        lockTagType = LOCKTAG_TABLE_EXTEND;
        FillInData(pdbId, segmentId.m_fileId, segmentId.m_blockId, 0, 0);
    }

    /* Sets locktag for a table lock. */
    void SetTableLockTag(Oid pdbId, Oid relId)
    {
        lockTagType = LOCKTAG_TABLE;
        FillInData(pdbId, relId, 0, 0, 0);
    }

    /* Sets locktag for a tablespace lock. */
    void SetTablespaceLockTag(Oid pdbId, Oid tablespaceId)
    {
        lockTagType = LOCKTAG_TABLESPACE;
        FillInData(pdbId, tablespaceId, 0, 0, 0);
    }

    /* Sets locktag for a backup restore operation lock. */
    void SetBackupRestoreLockTag(Oid pdbId)
    {
        lockTagType = LOCKTAG_BACKUP_RESTORE;
        FillInData(pdbId, 0, 0, 0, 0);
    }

    /* Sets locktag for a pdb lock. */
    void SetPdbLockTag(Oid pdbId)
    {
        lockTagType = LOCKTAG_PDB;
        FillInData(0, DSTORE::DatabaseRelationId, pdbId, 0, 0);
    }

    /* Sets locktag for a table partition lock. */
    void SetPartitionLockTag(Oid pdbId, Oid relId, Oid partId)
    {
        lockTagType = LOCKTAG_PARTITION;
        FillInData(pdbId, relId, partId, 0, 0);
    }

    /* Sets locktag for a table extension lock. */
    void SetTbsExtensionLockTag(Oid pdbId, const PageId bitmapMetaPageId)
    {
        lockTagType = LOCKTAG_TBS_EXTEND;
        FillInData(pdbId, bitmapMetaPageId.m_fileId, bitmapMetaPageId.m_blockId, 0, 0);
    }

    /* Sets locktag for a control file operation lock. */
    void SetControlFileLockTag(PdbId pdbId, uint32 groupType = 0)
    {
        lockTagType = LOCKTAG_CONTROL_FILE;
        FillInData(pdbId, groupType, 0, 0, 0);
    }

    /* Sets locktag for a wal stream assign lock. */
    void SetControlFileWalStreamAssignLockType(PdbId pdbId)
    {
        lockTagType = LOCKTAG_CONTROL_FILE_WAL_STREAM_ASSIGN;
        FillInData(pdbId, 0, 0, 0, 0);
    }

    /* Sets locktag for select deadlock detect owner. */
    void SetDeadlockDetectorLockTag()
    {
        lockTagType = LOCKTAG_DEADLOCK_DETECT;
        FillInData(0, 0, 0, 0, 0);
    }

    /* Sets locktag for decode dict lock. */
    void SetDecodeDictLockTag(PdbId pdbId)
    {
        lockTagType = LOCKTAG_LOGICAL_REPLICATION;
        FillInData(pdbId, LOGICAL_META_LOCK_ID, 0, 0, 0);
    }

    /* Sets locktag for logical slot lock. */
    void SetLogicalSlotLockTag(PdbId pdbId)
    {
        lockTagType = LOCKTAG_LOGICAL_REPLICATION;
        FillInData(pdbId, LOGICAL_SLOT_LOCK_ID, 0, 0, 0);
    }

    /* Sets locktag for pdb replica lock. */
    void SetPdbReplicaLockTag(Oid pdbId)
    {
        lockTagType = LOCKTAG_PDB_REPLICA;
        FillInData(pdbId, 0, 0, 0, 0);
    }

    /* Sets locktag for a object lock. */
    void SetObjectLockTag(Oid dbId, Oid classId, Oid objId, Oid objSubId1, Oid objSubId2)
    {
        lockTagType = LOCKTAG_OBJECT;
        FillInData(dbId, classId, objId, objSubId1, objSubId2);
    }

    /* Sets locktag for Advisory lock. */
    void SetAdvisoryLockTag(Oid pdbId, Oid key1, Oid key2, Oid type)
    {
        lockTagType = LOCKTAG_ADVISORY;
        FillInData(pdbId, key1, key2, type, 0);
    }

    /* Sets locktag for Package lock. */
    void SetPackageLockTag(Oid pdbId, Oid pkgId)
    {
        lockTagType = LOCKTAG_PACKAGE;
        FillInData(pdbId, pkgId, 0, 0, 0);
    }

    /* Sets locktag for Procedure lock. */
    void SetProcedureLockTag(Oid pdbId, Oid procId)
    {
        lockTagType = LOCKTAG_PROCEDURE;
        FillInData(pdbId, procId, 0, 0, 0);
    }

    /* Sets locktag for cleanup crashed PO lock. */
    void SetCleanPdLockTag()
    {
        lockTagType = LOCKTAG_CLEANPD;
        FillInData(0, 0, 0, 0, 0);
    }

    inline void SetInvalid()
    {
        lockTagType = LOCKTAG_MAX_NUM;
    }

    inline bool IsInvalid() const
    {
        return (lockTagType == LOCKTAG_MAX_NUM);
    }

    /*
     * Append a description of a lockable object to buf.
     *
     * Ideally we would print names for the numeric values, but that requires
     * getting locks on system tables, which might cause problems since this is
     * typically used to report deadlock situations.
     */
    RetStatus DescribeLockTag(StringInfo buf) const
    {
        RetStatus ret = DSTORE_SUCC;
        switch (lockTagType) {
            case LOCKTAG_TABLE:
                ret = buf->append("Table lock: pdb id %u, rel id %u", field1, field2);
                break;
            case LOCKTAG_PARTITION:
                ret = buf->append("Partition lock: pdb id %u, rel id %u, part id %u", field1, field2, field3);
                break;
            case LOCKTAG_TABLE_EXTEND:
                ret = buf->append("Table extend lock: pdb id %u, file id %u, block id %u", field1, field2, field3);
                break;
            case LOCKTAG_TBS_EXTEND:
                ret =
                    buf->append("Table space extend lock: pdb id %u, file id %u, block id %u", field1, field2, field3);
                break;
            case LOCKTAG_TRANSACTION: {
                Xid xid(Construct64Bits(field2, field3));
                ret = buf->append("Transaction lock: pdb id %u, zone id %lu, slot id %lu, (xid: %lu)",
                    field1, (uint64)xid.m_zoneId, xid.m_logicSlotId, xid.m_placeHolder);
                break;
            }
            case LOCKTAG_CSN:
                ret = buf->append("Csn owner lock");
                break;
            case LOCKTAG_ZONE:
                ret = buf->append("Undo zone lock: pdb id %u, zone id %u", field1, field2);
                break;
            case LOCKTAG_CONTROL_FILE:
                ret = buf->append("Control file lock: pdb id %u", field1);
                break;
            case LOCKTAG_CONTROL_FILE_WAL_STREAM_ASSIGN:
                ret = buf->append("Wal stream assign lock: pdb id %u", field1);
                break;
            case LOCKTAG_DEADLOCK_DETECT:
                ret = buf->append("Deadlock detect lock");
                break;
            case LOCKTAG_PDB:
                ret = buf->append("Pdb lock: pdb id %u", field1);
                break;
            case LOCKTAG_LOGICAL_REPLICATION:
                ret = buf->append("Logical replication lock: pdb id %u", field1);
                break;
            case LOCKTAG_PDB_REPLICA:
                ret = buf->append("Pdb replica lock: pdb id %u", field1);
                break;
            case LOCKTAG_OBJECT:
                ret = buf->append(
                    "Object lock: pdb id %u, class id %u, object id %u, sub object id1 %u, sub object id2 %u", field1,
                    field2, field3, field4, field5);
                break;
            case LOCKTAG_ADVISORY:
                if (field4 == static_cast<uint32>(AdvisoryLockType::ADVISORY_INT8)) {
                    ret = buf->append("Advisory int8 lock: pdb id %u, low 32 bits %u, high 32 bits %u",
                        field1, field2, field3);
                } else {
                    ret = buf->append("Advisory int4 lock: pdb id %u, first key %u, second key %u",
                        field1, field2, field3);
                }
                break;
            case LOCKTAG_PACKAGE:
                ret = buf->append("Package lock: pdb id %u, pkg id %u", field1, field2);
                break;
            case LOCKTAG_PROCEDURE:
                ret = buf->append("Procedure lock: pdb id %u, procedure id %u", field1, field2);
                break;
            case LOCKTAG_CLEANPD:
                ret = buf->append("Clean po lock");
                break;
            case LOCKTAG_TABLESPACE:
                if (field2 == LOCK_TAG_TABLESPACE_MGR_ID) {
                    ret = buf->append("Tablespace manager lock: pdbId %u", field1);
                } else if (field2 >= TABLESPACE_ID_COUNT && field2 <= MAX_TABLESPACE_ID + TABLESPACE_ID_COUNT) {
                    ret = buf->append("Tablespace lock for drop: pdbId %u, tablespaceId %u", field1,
                                      field2 - TABLESPACE_ID_COUNT);
                } else {
                    ret = buf->append("Tablespace lock: pdbId %u, tablespaceId %u", field1, field2);
                }
                break;
            case LOCKTAG_BACKUP_RESTORE:
                ret = buf->append("BackupRestore lock: pdbId %u.", field1);
                break;
            case LOCKTAG_MAX_NUM:
            default:
                ret = buf->append("Unrecognized locktag type %d, field(%u/%u/%u/%u/%u)", static_cast<int>(lockTagType),
                    field1, field2, field3, field4, field5);
                break;
        }
        return ret;
    }

    /*
     * Returns the corresponding string for the lock tag's tag type.
     */
    const char *GetTypeName() const
    {
        switch (lockTagType) {
            case LOCKTAG_TABLE:
                return "LOCKTAG_TABLE";
            case LOCKTAG_PARTITION:
                return "LOCKTAG_PARTITION";
            case LOCKTAG_TABLE_EXTEND:
                return "LOCKTAG_TABLE_EXTEND";
            case LOCKTAG_TBS_EXTEND:
                return "LOCKTAG_TBS_EXTEND";
            case LOCKTAG_TRANSACTION:
                return "LOCKTAG_TRANSACTION";
            case LOCKTAG_CSN:
                return "LOCKTAG_CSN";
            case LOCKTAG_ZONE:
                return "LOCKTAG_ZONE";
            case LOCKTAG_CONTROL_FILE:
                return "LOCKTAG_CONTROL_FILE";
            case LOCKTAG_CONTROL_FILE_WAL_STREAM_ASSIGN:
                return "LOCKTAG_CONTROL_FILE_WAL_STREAM_ASSIGN";
            case LOCKTAG_DEADLOCK_DETECT:
                return "LOCKTAG_DEADLOCK_DETECT";
            case LOCKTAG_PDB:
                return "LOCKTAG_PDB";
            case LOCKTAG_LOGICAL_REPLICATION:
                return "LOCKTAG_LOGICAL_REPLICATION";
            case LOCKTAG_PDB_REPLICA:
                return "LOCKTAG_PDB_REPLICA";
            case LOCKTAG_OBJECT:
                return "LOCKTAG_OBJECT";
            case LOCKTAG_MAX_NUM:
                return "LOCKTAG_MAX_NUM";
            case LOCKTAG_ADVISORY:
                return "LOCKTAG_ADVISORY";
            case LOCKTAG_PACKAGE:
                return "LOCKTAG_PACKAGE";
            case LOCKTAG_PROCEDURE:
                return "LOCKTAG_PROCEDURE";
            case LOCKTAG_CLEANPD:
                return "LOCKTAG_CLEANPD";
            case LOCKTAG_TABLESPACE:
                return "LOCKTAG_TABLESPACE";
            case LOCKTAG_BACKUP_RESTORE:
                return "LOCKTAG_BACKUP_RESTORE";
            default:
                return "unrecognized locktag type";
        }
    }

    /*
     * The following three functions are
     * utility function to help breakdown
     * and reconstruct uint64's
     * so that they can fit in the locktag fields.
     */
    /*
     * Obtain the lower least-significant 32 bits.
     */
    uint32 GetLow32Bits(uint64 value) const
    {
        return static_cast<uint32>(value & MASK_32_BITS);
    }
    /*
     * Obtain the higher most-significant 32 bits.
     */
    uint32 GetHigh32Bits(uint64 value) const
    {
        return static_cast<uint32>(value >> OFFSET_32_BITS);
    }
    /*
     * This function concatenates two 32-bit uints and returns a 64 bit uint.
     */
    uint64 Construct64Bits(uint32 lower32Bits, uint32 higher32Bits) const
    {
        uint64 ret = static_cast<uint64>(higher32Bits) << OFFSET_32_BITS;
        return (ret | lower32Bits);
    }

    static int HashCompareFunc(const void *key1, const void *key2, UNUSE_PARAM Size keySize)
    {
        const LockTag *tag1 = static_cast<const LockTag *>(key1);
        const LockTag *tag2 = static_cast<const LockTag *>(key2);
        StorageAssert(keySize == sizeof(LockTag));

        if (tag1->lockTagType != tag2->lockTagType) {
            return ((tag1->lockTagType > tag2->lockTagType) ? 1 : (-1));
        }

        int result = -1;
        switch (tag1->lockTagType) {
            case LOCKTAG_CSN:
            case LOCKTAG_DEADLOCK_DETECT:
            case LOCKTAG_CLEANPD: {
                result = 0;
                break;
            }
            case LOCKTAG_PDB_REPLICA:
            case LOCKTAG_CONTROL_FILE_WAL_STREAM_ASSIGN: {
                result = memcmp(&(tag1->field1), &(tag2->field1), sizeof(uint32));
                break;
            }
            case LOCKTAG_TABLE:
            case LOCKTAG_TABLESPACE:
            case LOCKTAG_BACKUP_RESTORE:
            case LOCKTAG_ZONE:
            case LOCKTAG_LOGICAL_REPLICATION:
            case LOCKTAG_PACKAGE:
            case LOCKTAG_CONTROL_FILE:
            case LOCKTAG_PROCEDURE: {
                result = memcmp(&(tag1->field1), &(tag2->field1), sizeof(uint32) * 2);
                break;
            }
            case LOCKTAG_TRANSACTION:
            case LOCKTAG_TABLE_EXTEND:
            case LOCKTAG_PARTITION:
            case LOCKTAG_TBS_EXTEND: {
                result = memcmp(&(tag1->field1), &(tag2->field1), sizeof(uint32) * 3);
                break;
            }
            case LOCKTAG_ADVISORY: {
                result = memcmp(&(tag1->field1), &(tag2->field1), sizeof(uint32) * 4);
                break;
            }
            case LOCKTAG_PDB:
            case LOCKTAG_OBJECT: {
                result = memcmp(&(tag1->field1), &(tag2->field1), sizeof(uint32) * 5);
                break;
            }
            case LOCKTAG_MAX_NUM: {
                StorageAssert(0);
                break;
            }
            default: {
                result = memcmp(tag1, tag2,  sizeof(LockTag));
                ErrLog(DSTORE_ERROR, MODULE_LOCK,
                    ErrMsg("Unexpected locktag type(%u, %u, %u, %u, %u, %u) when do hash compare.",
                    tag1->lockTagType, tag1->field1, tag1->field2, tag1->field3, tag1->field4, tag1->field5));
                break;
            }
        }

        StorageAssert(tag1->lockMethodId == tag2->lockMethodId);
        StorageAssert(tag1->recoveryMode == tag2->recoveryMode);
        return result;
    }

    /*
     * Compares two lock tags for equality.
     * Utility function used by deadlock detector.
     */
    bool operator==(const LockTag &tag) const
    {
        /* Note that the padding of LockTag must be zero to avoid random value */
        return memcmp(this, &tag, sizeof(LockTag)) == 0;
    }

    StringLog ToString() const
    {
        AutoMemCxtSwitch autoSwitch{thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LOCK)};
        StringLog dumpInfo;
        dumpInfo.stringData.init();
        if (STORAGE_VAR_NULL(dumpInfo.stringData.data)) {
            ErrLog(DSTORE_ERROR, MODULE_LOCK,
                ErrMsg("Out of memory when append locktag(%s, %u, %u, %u, %u, %u) to stringlog.",
                GetTypeName(), field1, field2, field3, field4, field5));
            return dumpInfo;
        }

        if (STORAGE_FUNC_FAIL(dumpInfo.stringData.append("[locktag: ")) ||
            STORAGE_FUNC_FAIL(this->DescribeLockTag(&dumpInfo.stringData)) ||
            STORAGE_FUNC_FAIL(dumpInfo.stringData.append("]"))) {
            ErrLog(DSTORE_ERROR, MODULE_LOCK,
                ErrMsg("Out of memory when append locktag(%s, %u, %u, %u, %u, %u) to stringlog.",
                GetTypeName(), field1, field2, field3, field4, field5));
        }

        return dumpInfo;
    }
};

static_assert(sizeof(LockTag) == (sizeof(uint32) * 5 + sizeof(LockTagType) + sizeof(LockMethodId) +
    sizeof(LockRecoveryMode)), "make sure lock tag is a tight structure");

struct LockTagCache {
public:
    LockTagCache() : lockTag(nullptr), hashCode(0) {}
    explicit LockTagCache(const LockTag *tag);
    const LockTag *GetLockTag() const
    {
        return lockTag;
    }
    uint32 GetHashCode() const
    {
        return hashCode;
    }

    const LockTag *lockTag;
    uint32 hashCode;
};

inline const char *GetLockModeString(LockMode mode)
{
    switch (mode) {
        case DSTORE_NO_LOCK:                     return "NO_LOCK";
        case DSTORE_ACCESS_SHARE_LOCK:           return "ACCESS_SHARE_LOCK";
        case DSTORE_ROW_SHARE_LOCK:              return "ROW_SHARE_LOCK";
        case DSTORE_ROW_EXCLUSIVE_LOCK:          return "ROW_EXCLUSIVE_LOCK";
        case DSTORE_SHARE_UPDATE_EXCLUSIVE_LOCK: return "SHARE_UPDATE_EXCLUSIVE_LOCK";
        case DSTORE_SHARE_LOCK:                  return "SHARE_LOCK";
        case DSTORE_SHARE_ROW_EXCLUSIVE_LOCK:    return "SHARE_ROW_EXCLUSIVE_LOCK";
        case DSTORE_EXCLUSIVE_LOCK:              return "EXCLUSIVE_LOCK";
        case DSTORE_ACCESS_EXCLUSIVE_LOCK:       return "ACCESS_EXCLUSIVE_LOCK";
        case DSTORE_LOCK_MODE_MAX:
        default:                          return "Unknown lock mode type";
    }
}

inline LockMode GetNextLockMode(LockMode mode)
{
    StorageAssert(mode != DSTORE_LOCK_MODE_MAX);
    return static_cast<LockMode>(static_cast<unsigned char>(mode) + 1);
}

using LockMask = uint32;
inline LockMask GetLockMask(LockMode mode)
{
    return 1U << static_cast<int>(mode);
}

/* Assign meaningful names to lock()'s dontWait argument. */
const bool LOCK_DONT_WAIT = true;
const bool LOCK_WAIT = false;

/* Indicate lock manager type. */
enum LockMgrType : uint8 {
    LOCK_MGR,
    TABLE_LOCK_MGR,
    XACT_LOCK_MGR,
    LOCK_MGR_TYPE_MAX
};

#ifdef DSTORE_LOCK_DEBUG
/* Used to dump debug messages. */
static const char *GetLockMgrTypeName(LockMgrType type)
{
    switch (type) {
        case LOCK_MGR: return "LOCK_MGR";
        case TABLE_LOCK_MGR: return "TABLE_LOCK_MGR";
        case XACT_LOCK_MGR: return "XACT_LOCK_MGR";
        default: return "Unknown lock mgr type";
    }
}
#endif /* DSTORE_LOCK_DEBUG */

struct LockErrorInfo {
    uint32 lockHolder; /* If lock acquire fails, return one of the nodes that holding a conflicting lock. */
    bool isHolder = false;
    NodeId nodeId = UINT32_MAX;
    uint64 processTimelineId = 0;
    uint64 nodeTimelineId = 0;
    ThreadId threadId = 0;
    LockMode lockMode = DSTORE_NO_LOCK;

    void Initialize()
    {
        lockHolder = UINT32_MAX;
        isHolder = false;
        nodeId = UINT32_MAX;
        processTimelineId = 0;
        nodeTimelineId = 0;
        threadId = 0;
        lockMode = DSTORE_NO_LOCK;
    }
};

struct LockAcquireContext {
    LockTagCache tagCache;
    LockMode mode;
    bool dontWait;
    LockErrorInfo *info;
};

}

#endif
