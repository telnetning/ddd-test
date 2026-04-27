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
#include "ut_utilities/ut_dstore_framework.h"

class UTDynaHash : public DSTORETEST {
protected:
    void SetUp() override
    {
        DSTORETEST::SetUp();
        UtInstance *instance = DstoreNew(m_ut_memory_context)UtInstance();
        instance->Install(&DSTORETEST::m_guc, m_ut_memory_context);
        instance->Startup(&DSTORETEST::m_guc);
        /* More steps may be added according to future needs. */
    }

    void TearDown() override
    {
        UtInstance *instance = (UtInstance *)g_storageInstance;
        instance->Shutdown();
        delete instance;
        DSTORETEST::TearDown();
    }

    long GetFreeEntryNumber(FreeListData *freeList)
    {
        long count = 0;
        HASHELEMENT *next = freeList->freeList;
        while (next != nullptr) {
            count++;
            next = next->link;
        }
        return count;
    }
};

struct TestHashEntry {
    uint64 key;
    uint64 value;
};

TEST_F(UTDynaHash, Test1)
{
    HASHCTL info;
    info.keysize = sizeof(uint64);
    info.entrysize = sizeof(TestHashEntry);
    info.hash = tag_hash;
    info.batch_alloc_num = 128;
    info.dsize = info.max_dsize = hash_select_dirsize(1024);
    info.hcxt = DstoreAllocSetContextCreate(nullptr, "UTDynaHashMemoryContext", ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_INITSIZE, MemoryContextType::SHARED_CONTEXT);

    HTAB *hashTable = hash_create(
        "UTDynaHash Table", 1024, &info,
        HASH_ELEM | HASH_FUNCTION | HASH_DIRSIZE | HASH_CONTEXT | HASH_SHRCTX | HASH_BATCH_ALLOC_NUM);

    uint64 key = 0;
    bool isFound = false;
    for (int i = 0; i < NUM_FREELISTS; i++) {
        ASSERT_EQ(hashTable->hctl->freeList[i].nentries, 0);
    }

    for (int i = 0; i < hashTable->hctl->nelem_alloc; i++) {
        key = i;
        isFound = false;
        TestHashEntry *entry = (TestHashEntry *)hash_search(hashTable, &key, HASH_ENTER, &isFound);
        entry->value = entry->key * 2;
        ASSERT_EQ(isFound, false);
        ASSERT_EQ(GetFreeEntryNumber(&hashTable->hctl->freeList[0]), hashTable->hctl->nelem_alloc - i - 1);
    }

    /* check */
    for (int i = 0; i < hashTable->hctl->nelem_alloc; i++) {
        key = i;
        isFound = false;
        TestHashEntry *entry = (TestHashEntry *)hash_search(hashTable, &key, HASH_ENTER, &isFound);
        ASSERT_EQ(isFound, true);
        ASSERT_EQ(entry->value, entry->key * 2);
    }

    key = 128;
    isFound = false;
    TestHashEntry *entry = (TestHashEntry *)hash_search(hashTable, &key, HASH_ENTER, &isFound);
    entry->value = entry->key * 2;

    ASSERT_EQ(GetFreeEntryNumber(&hashTable->hctl->freeList[0]), hashTable->hctl->nelem_alloc - 1);
    hash_destroy(hashTable);
}

TEST_F(UTDynaHash, Test2)
{
    HASHCTL info;
    info.keysize = sizeof(uint64);
    info.entrysize = sizeof(TestHashEntry);
    info.hash = tag_hash;
    info.batch_alloc_num = 128;
    info.dsize = info.max_dsize = hash_select_dirsize(1024);
    info.hcxt = DstoreAllocSetContextCreate(nullptr, "UTDynaHashMemoryContext", ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_INITSIZE, MemoryContextType::SHARED_CONTEXT);

    HTAB *hashTable = hash_create(
        "UTDynaHash Table", 1024, &info,
        HASH_ELEM | HASH_FUNCTION | HASH_DIRSIZE | HASH_CONTEXT | HASH_SHRCTX | HASH_BATCH_ALLOC_NUM);

    uint64 key = 0;
    bool isFound = false;

    for (int i = 0; i < NUM_FREELISTS; i++) {
        ASSERT_EQ(hashTable->hctl->freeList[i].nentries, 0);
    }

    for (int i = 0; i < hashTable->hctl->nelem_alloc; i++) {
        key = i;
        isFound = false;
        TestHashEntry *entry = (TestHashEntry *)hash_search(hashTable, &key, HASH_ENTER, &isFound);
        entry->value = entry->key * 2;
        ASSERT_EQ(isFound, false);
        ASSERT_EQ(GetFreeEntryNumber(&hashTable->hctl->freeList[0]), hashTable->hctl->nelem_alloc - i - 1);
    }

    /* check */
    for (int i = 0; i < hashTable->hctl->nelem_alloc; i++) {
        key = i;
        isFound = false;
        TestHashEntry *entry = (TestHashEntry *)hash_search(hashTable, &key, HASH_ENTER, &isFound);
        ASSERT_EQ(isFound, true);
        ASSERT_EQ(entry->value, entry->key * 2);
    }

    for (int i = 0; i < hashTable->hctl->nelem_batch_alloc; i++) {
        key = 200 + i;
        isFound = false;
        TestHashEntry *entry = (TestHashEntry *)hash_search(hashTable, &key, HASH_ENTER_PRE_ALLOC, &isFound);
        entry->value = entry->key * 2;
        ASSERT_EQ(isFound, false);
        ASSERT_EQ(GetFreeEntryNumber(&hashTable->hctl->freeList[0]), hashTable->hctl->nelem_batch_alloc - i - 1);
    }

    /* check */
    for (int i = 0; i < hashTable->hctl->nelem_batch_alloc; i++) {
        key = 200 + i;
        isFound = false;
        TestHashEntry *entry = (TestHashEntry *)hash_search(hashTable, &key, HASH_ENTER_PRE_ALLOC, &isFound);
        ASSERT_EQ(isFound, true);
        ASSERT_EQ(entry->value, entry->key * 2);
    }

    key = 128;
    isFound = false;
    TestHashEntry *entry = (TestHashEntry *)hash_search(hashTable, &key, HASH_ENTER_PRE_ALLOC, &isFound);
    entry->value = entry->key * 2;
    ASSERT_EQ(isFound, false);
    ASSERT_EQ(GetFreeEntryNumber(&hashTable->hctl->freeList[0]), hashTable->hctl->nelem_batch_alloc - 1);

    hash_destroy(hashTable);
}