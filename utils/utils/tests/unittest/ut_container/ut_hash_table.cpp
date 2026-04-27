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
 * ---------------------------------------------------------------------------------
 *
 * ut_hash_table.cpp
 *
 * Description:
 * 1. test hash table
 *
 * ---------------------------------------------------------------------------------
 */
#include <chrono>
#include "securec.h"
#include "gtest/gtest.h"
#include "container/hash_table.h"
#include "container/bitmap.h"
#include "container/hash_table_internal.h"
#include "types/data_types.h"

struct Position {
    int x;
    int y;
};

typedef Bitmap* Bitmapkey;
const uint32_t  MAX_NAME_LEN  = 32;
const int  INIT_TEST_ENTRY_SIZE = 100;
const int32_t  MAX_TEST_ENTRY_SIZE  = 10000;
const char * const  TEST_TARGET_NAME = "Test10";
const uint32_t TEST_TARGET_ID = 10;
const Position TEST_TARGET_POSITION = {10, 11};

struct TestData
{
    uint32_t id;
    int age;
    char name[MAX_NAME_LEN];
    Position pos;
};

struct TestDataEntry {
    uint32_t key;      /* id of TestData */
    TestData *data;
};

struct TestDataTagEntry {
    Position key;      /* position of TestData */
    TestData *data;
};

struct TestDataStringEntry {
    char key[MAX_NAME_LEN];      /* name of TestData */
    TestData *data;
};

struct TestDataBitmapEntry {
    Bitmapkey key;      /* bitmap of TestData */
    TestData *data;
};
template<class EntryType> HashTab *CreateTestHashTable();
template<class KeyType, class EntryType> void DestroyTestHashTable(HashTab *hashTab);
template<class KeyType, class EntryType> void CleanTestHashTable(HashTab *hashTab);
template<class KeyType, class EntryType> bool TestHashTableInsert(HashTab *hashTab, KeyType key, TestData *data);
template<class KeyType, class EntryType> TestData *TestHashTableFind(HashTab *hashTab, KeyType key);
template<class KeyType, class EntryType> TestData *TestHashTableRemove(HashTab *hashTab, KeyType key);
template<class KeyType = const char *, class EntryType = TestDataStringEntry>
TestData *TestHashTableRemove(HashTab *hashTab, const char *key);
TestData* CreateTestData(uint32_t _id, int _age, const char *_name, Position pos);
void DestroyTestData(TestData *data);

template<class EntryType>
HashTab *CreateTestHashTable()
{
    HashCTL ctl;
    HashTab *hashTab;
    uint32 flags = HASH_ELEM | HASH_FUNCTION;

    (void)memset_s(&ctl, sizeof(HashCTL), 0, sizeof(HashCTL));
    ctl.entrySize = sizeof(EntryType);
    if (typeid(EntryType) == typeid(TestDataEntry)) {
        ctl.keySize = sizeof(uint32_t);
        ctl.hash = Uint32Hash;
    } else if (typeid(EntryType) == typeid(TestDataTagEntry)) {
        ctl.keySize = sizeof(Position);
        ctl.hash = TagHash;
    } else if (typeid(EntryType) == typeid(TestDataStringEntry)) {
        ctl.keySize = MAX_NAME_LEN;
        ctl.hash = StringHash;
    } else if (typeid(EntryType) == typeid(TestDataBitmapEntry)) {
        ctl.keySize = sizeof(Bitmapkey);
        ctl.hash = BitmapHash;
        ctl.match = BitmapMatch;
        flags = HASH_ELEM | HASH_FUNCTION | HASH_COMPARE;
    } else {
        (void)fprintf(stderr, "Key type is not supported.");
        return NULL;
    }

    /* create hash table */
    hashTab = HashCreate("TestData hashtable", MAX_TEST_ENTRY_SIZE,
                         &ctl, flags);
    if (hashTab == NULL) {
        (void)fprintf(stderr, "create hash table failed");
    }

    return hashTab;
}

template<class KeyType, class EntryType>
void CleanTestHashTable(HashTab *hashTab)
{
    HashSeqStatus status;
    EntryType *dataEntry = NULL;

    /* init seq scan */
    HashSeqInit(&status, hashTab);
    while ((dataEntry = (EntryType *)HashSeqSearch(&status)) != NULL) {
        TestData *data = TestHashTableRemove<KeyType, EntryType>(hashTab, dataEntry->key);
        DestroyTestData(data);
    }
    HashSeqTerm(&status);
}

template<class KeyType, class EntryType>
void DestroyTestHashTable(HashTab *hashTab)
{
    if (hashTab) {
        CleanTestHashTable<KeyType, EntryType>(hashTab);
        HashDestroy(hashTab);
    }
}

template<class KeyType, class EntryType>
bool TestHashTableInsert(HashTab *hashTab, KeyType key, TestData *data)
{
    EntryType *entry = NULL;
    bool found = false;

    entry = (EntryType *)HashSearch(hashTab, (void*)&key, HASH_ENTER, &found);
    if (found) {
        (void)printf("test data has been existed\n");
        return false;
    } else {
        entry->key = key;
        entry->data = data;
        return true;
    }
}

template<class KeyType, class EntryType>
TestData *TestHashTableFind(HashTab *hashTab, KeyType key)
{
    EntryType *entry = NULL;

    entry = (EntryType *)HashSearch(hashTab, (void*)&key, HASH_FIND, NULL);
    if (entry == NULL) {
        return NULL;
    }

    return entry->data;
}

template<class KeyType, class EntryType>
TestData *TestHashTableRemove(HashTab *hashTab, KeyType key)
{
    EntryType *entry = NULL;

    entry = (EntryType *)HashSearch(hashTab, (void*)&key, HASH_REMOVE, NULL);
    if (entry == NULL) {
        return NULL;
    }
    if (typeid(EntryType) == typeid(TestDataBitmapEntry)) {
        BitmapDestroy(((TestDataBitmapEntry *)entry)->key);
    }
    return entry->data;
}

/* special case for string key */
template<class KeyType = const char *, class EntryType = TestDataStringEntry>
bool TestHashTableInsert(HashTab *hashTab, const char * key, TestData *data)
{
    EntryType *entry = NULL;
    bool found = false;

    entry = (EntryType *)HashSearch(hashTab, (void*)key, HASH_ENTER, &found);
    if (found) {
        (void)printf("test data has been existed\n");
        return false;
    } else {
        (void)strcpy_s(entry->key, MAX_NAME_LEN, key);
        entry->data = data;
        return true;
    }
}

template<class KeyType = const char *, class EntryType = TestDataStringEntry>
TestData *TestHashTableFind(HashTab *hashTab, const char *key)
{
    EntryType *entry = NULL;

    entry = (EntryType *)HashSearch(hashTab, (void*)key, HASH_FIND, NULL);
    if (entry == NULL) {
        return NULL;
    }

    return entry->data;
}

template<class KeyType = const char *, class EntryType = TestDataStringEntry>
TestData *TestHashTableRemove(HashTab *hashTab, const char *key)
{
    EntryType *entry = NULL;

    entry = (EntryType *)HashSearch(hashTab, (void*)key, HASH_REMOVE, NULL);
    if (entry == NULL) {
        return NULL;
    }
    return entry->data;
}

TestData* CreateTestData(uint32_t _id, int _age, const char *_name, Position pos)
{
    TestData *data = (TestData *)MemAlloc(sizeof(TestData));
    data->id = _id;
    data->age = _age;
    data->pos = pos;
    strncpy_s(data->name, sizeof(data->name), _name, MAX_NAME_LEN);

    return data;
}

void DestroyTestData(TestData *data)
{
    MemFree(data);
}


class HashTableTest : public testing::Test {
public:
    void SetUp() override {
        MemoryContextSwitchTo(MemoryContextCreate(NULL, MEM_CXT_TYPE_GENERIC, "ut_bitmapset_mctx",
                                                  0, DEFAULT_UNLIMITED_SIZE, DEFAULT_UNLIMITED_SIZE));
    };

    void TearDown() override {
        MemoryContextDelete(MemoryContextSwitchTo(NULL));
    };
};

/* Test insert, find, delete for uint32_t key -- Uint32Hash */
TEST_F(HashTableTest, HashTableCRUDTest01)
{
    /* create hash table */
    HashTab *hashTab = CreateTestHashTable<TestDataEntry>();
    ASSERT_NE(hashTab, (HashTab*)NULL);

    /* batch insert data */
    for (int i = 1; i <= INIT_TEST_ENTRY_SIZE; ++i) {
        char testName[MAX_NAME_LEN];
        (void)snprintf_s(testName, MAX_NAME_LEN, MAX_NAME_LEN - 1, "Test%d", i);
        Position position = {i, i+1};
        TestData *data = CreateTestData(i, 10 * i, testName, position);
        TestHashTableInsert<uint32_t, TestDataEntry>(hashTab, i, data);
    }

    /* search hash table */
    TestData  *data = TestHashTableFind<uint32_t, TestDataEntry>(hashTab, TEST_TARGET_ID);
    ASSERT_NE(data, (TestData *)NULL);
    EXPECT_EQ(data->id, TEST_TARGET_ID);


    /* remove data */
    TestData *target = TestHashTableRemove<uint32_t, TestDataEntry>(hashTab, TEST_TARGET_ID);
    ASSERT_EQ(target, data);
    EXPECT_EQ(target->id, TEST_TARGET_ID);
    EXPECT_EQ(strcmp(target->name, TEST_TARGET_NAME), 0);
    DestroyTestData(target);

    /* since TEST_TARGET_ID has been deleted, data will not be found */
    data = TestHashTableFind<uint32_t, TestDataEntry>(hashTab, TEST_TARGET_ID);
    ASSERT_EQ(data, (TestData *)NULL);

    /* destroy hash table */
    DestroyTestHashTable<uint32_t, TestDataEntry>(hashTab);
}

/* Test insert, find, delete for variable-length key -- TagHash */
TEST_F(HashTableTest, HashTableCRUDTest02)
{
    /* create hash table */
    HashTab *hashTab = CreateTestHashTable<TestDataTagEntry>();
    ASSERT_NE(hashTab, (HashTab*)NULL);

    /* batch insert data */
    for (int i = 1; i <= INIT_TEST_ENTRY_SIZE; ++i) {
        char testName[MAX_NAME_LEN];
        (void)snprintf_s(testName, MAX_NAME_LEN, MAX_NAME_LEN - 1, "Test%d", i);
        Position position = {i, i+1};
        TestData *data = CreateTestData(i, 10 * i, testName, position);
        TestHashTableInsert<Position, TestDataTagEntry>(hashTab, position, data);
    }

    /* search hash table */
    TestData  *data = TestHashTableFind<Position, TestDataTagEntry>(hashTab, TEST_TARGET_POSITION);
    ASSERT_NE(data, (TestData *)NULL);
    EXPECT_EQ(memcmp(&data->pos, &TEST_TARGET_POSITION, sizeof(Position)), 0);


    /* remove data */
    TestData *target = TestHashTableRemove<Position, TestDataTagEntry>(hashTab, TEST_TARGET_POSITION);
    ASSERT_EQ(target, data);
    EXPECT_EQ(target->id, TEST_TARGET_ID);
    EXPECT_EQ(memcmp(&target->pos, &TEST_TARGET_POSITION, sizeof(Position)), 0);
    DestroyTestData(target);

    /* since TEST_TARGET_POSITION has been deleted, data will not be found */
    data = TestHashTableFind<Position, TestDataTagEntry>(hashTab, TEST_TARGET_POSITION);
    ASSERT_EQ(data, (TestData *)NULL);

    /* destroy hash table */
    DestroyTestHashTable<Position, TestDataTagEntry>(hashTab);
}

/* Test insert, find, delete for string key -- StringHash */
TEST_F(HashTableTest, HashTableCRUDTest03)
{
    /* create hash table */
    HashTab *hashTab = CreateTestHashTable<TestDataStringEntry>();
    ASSERT_NE(hashTab, (HashTab*)NULL);

    /* batch insert data */
    for (int i = 1; i <= INIT_TEST_ENTRY_SIZE; ++i) {
        char testName[MAX_NAME_LEN];
        (void)snprintf_s(testName, MAX_NAME_LEN, MAX_NAME_LEN - 1, "Test%d", i);
        Position position = {i, i+1};
        TestData *data = CreateTestData(i, 10 * i, testName, position);
        TestHashTableInsert<const char *, TestDataStringEntry>(hashTab, testName, data);
    }

    /* search hash table */
    TestData  *data = TestHashTableFind<const char *, TestDataStringEntry>(hashTab, TEST_TARGET_NAME);
    ASSERT_NE(data, (TestData *)NULL);
    EXPECT_EQ(strcmp(data->name, TEST_TARGET_NAME), 0);


    /* remove data */
    TestData *target = TestHashTableRemove<const char *, TestDataStringEntry>(hashTab, TEST_TARGET_NAME);
    ASSERT_EQ(target, data);
    EXPECT_EQ(target->id, TEST_TARGET_ID);
    EXPECT_EQ(strcmp(target->name, TEST_TARGET_NAME), 0);
    DestroyTestData(target);

    /* since id=10 has been deleted, data will not be found */
    data = TestHashTableFind<const char *, TestDataStringEntry>(hashTab, TEST_TARGET_NAME);
    ASSERT_EQ(data, (TestData *)NULL);

    /* destroy hash table */
    DestroyTestHashTable<const char *, TestDataStringEntry>(hashTab);
}

/* Test insert, find, delete for bitmap key -- BitmapHash */
TEST_F(HashTableTest, HashTableCRUDTest04)
{
    /* create hash table */
    HashTab *hashTab = CreateTestHashTable<TestDataBitmapEntry>();
    ASSERT_NE(hashTab, (HashTab*)NULL);

    /* batch insert data */
    for (int i = 1; i <= INIT_TEST_ENTRY_SIZE; i++) {
        Bitmapkey bms = BitmapCreateWithSet(i);
        char testName[MAX_NAME_LEN];
        (void)snprintf_s(testName, MAX_NAME_LEN, MAX_NAME_LEN - 1, "Test%d", i);
        Position position = {i, i+1};
        TestData *data = CreateTestData(i, 10 * i, testName, position);
        TestHashTableInsert<Bitmapkey, TestDataBitmapEntry>(hashTab, bms, data);
        //BitmapDestroy(bms);
    }

    /* search hash table */
    Bitmapkey targetBitmap = BitmapCreateWithSet(TEST_TARGET_ID);
    TestData  *data = TestHashTableFind<Bitmapkey, TestDataBitmapEntry>(hashTab, targetBitmap);
    ASSERT_NE(data, (TestData *)NULL);
    EXPECT_EQ(data->id, TEST_TARGET_ID);


    /* remove data */
    TestData *target = TestHashTableRemove<Bitmapkey, TestDataBitmapEntry>(hashTab, targetBitmap);
    ASSERT_EQ(target, data);
    EXPECT_EQ(target->id, TEST_TARGET_ID);
    EXPECT_EQ(strcmp(target->name, TEST_TARGET_NAME), 0);
    DestroyTestData(target);

    /* since id=10 has been deleted, data will not be found */
    data = TestHashTableFind<Bitmapkey, TestDataBitmapEntry>(hashTab, targetBitmap);
    ASSERT_EQ(data, (TestData *)NULL);

    /* destroy hash table */
    DestroyTestHashTable<Bitmapkey, TestDataBitmapEntry>(hashTab);
    BitmapDestroy(targetBitmap);
}

TEST_F(HashTableTest, HashTableSeqScanTest)
{
    /* create hash table */
    HashTab *hashTab = CreateTestHashTable<TestDataEntry>();
    ASSERT_NE(hashTab, (HashTab*)NULL);

    /* batch insert data */
    for (int i = 1; i <= MAX_TEST_ENTRY_SIZE; ++i) {
        char testName[MAX_NAME_LEN];
        (void)snprintf_s(testName, MAX_NAME_LEN, MAX_NAME_LEN - 1, "Test%d", i);
        Position position = {i, i+1};
        TestData *data = CreateTestData(i, 10 * i, testName, position);
        TestHashTableInsert<uint32_t, TestDataEntry>(hashTab, i, data);
    }

    /* sequence scan */
    HashSeqStatus status;
    /* init seq scan */
    HashSeqInit(&status, hashTab);
    TestDataEntry *dataEntry = (TestDataEntry *)HashSeqSearch(&status);
    ASSERT_NE(dataEntry, (TestDataEntry *)NULL);

    while (dataEntry) {
        TestData *data = TestHashTableRemove<uint32_t, TestDataEntry>(hashTab, dataEntry->data->id);
        DestroyTestData(data);
        dataEntry = (TestDataEntry *)HashSeqSearch(&status);
    }

    /* terminate seq scan */
    HashSeqTerm(&status);

    /* destroy hash table */
    DestroyTestHashTable<uint32_t, TestDataEntry>(hashTab);
}

UTILS_EXPORT uint32 ExpandHash(const void *key, Size keySize)
{
    /* hash_value and key are consistent to control which bucket is allocated to hash_value. */
    return (uint32)(*((const uint32 *)key));
}

HashTab *CreateHashTable(size_t numPartitions, Size nelem, size_t keySize, size_t entrySize, HashValueFunc hashFn, uint32 flags)
{
    HashCTL ctl = {0};
    HashTab *hashTab;

    ctl.entrySize = entrySize;
    ctl.keySize = keySize;
    ctl.hash = hashFn;
    ctl.maxDSize = NO_MAX_DSIZE;
    ctl.ffactor = 2;
    ctl.dsize = 1;
    ctl.ssize = 2;
    ctl.numPartitions = numPartitions;

    /* create hash table */
    hashTab = HashCreate("TestData hashtable", nelem, &ctl, flags);
    if (hashTab == NULL) {
        (void)fprintf(stderr, "create hash table failed");
    }

    return hashTab;
}

/* Test dynamic expand of the hash table -- Uint32Hash */
TEST_F(HashTableTest, HashTableExpandTest)
{
    const int INIT_TEST_EXPAND_NUM = 9;
    const int INIT_TEST_DIR_NUM = 0;
    const int FIRST_TEST_EXPAND_NUM = 4;
    const int SECOND_TEST_EXPAND_NUM = 6;
    const int THIRD_TEST_EXPAND_NUM = 8;
    const int FIRST_TEST_BUCKET_NUM = 3;
    const int SECOND_TEST_BUCKET_NUM = 4;
    const int THIRD_TEST_BUCKET_NUM = 5;
    const int FIRST_TEST_CHECK_VALUE = 2;
    const int SECOND_TEST_CHECK_VALUE = 3;
    const int THIRD_TEST_CHECK_VALUE = 4;
    /* create hash table */
    uint32 flags = HASH_ELEM | HASH_FUNCTION | HASH_DIRSIZE | HASH_SEGMENT | HASH_FFACTOR;
    HashTab *hashTab = CreateHashTable(0, 3, sizeof(uint32_t), sizeof(TestDataEntry), ExpandHash, flags);
    ASSERT_NE(hashTab, (HashTab *)NULL);
    HashSegment seg;
    HashBucket bucket;

    /* first insert data */
    int insertValue[INIT_TEST_EXPAND_NUM] = {0, 1, 2, 3, 4, 5, 6, 7, 8};
    for (int i = 0; i <= 8; i++) {
        int insertNum = insertValue[i];
        char testName[MAX_NAME_LEN];
        (void)snprintf_s(testName, MAX_NAME_LEN, MAX_NAME_LEN - 1, "Test%d", i);
        Position position = {insertNum, insertNum + 1};
        TestData *data = CreateTestData(insertNum, 10 * insertNum, testName, position);
        TestHashTableInsert<uint32_t, TestDataEntry>(hashTab, insertNum, data);
        Size entries = HashGetNumEntries(hashTab);
        ASSERT_EQ(entries, i + 1);

        switch (insertNum) {
            /* first expand */
            case FIRST_TEST_EXPAND_NUM: {
                seg = hashTab->dir[1];
                bucket = seg[0];
                ASSERT_EQ(hashTab->hctl->maxBucket, 2);
                ASSERT_EQ(bucket->hashValue, 2);
                break;
            }
            case SECOND_TEST_EXPAND_NUM: {
                seg = hashTab->dir[1];
                bucket = seg[1];
                ASSERT_EQ(hashTab->hctl->maxBucket, 3);
                ASSERT_EQ(bucket->hashValue, 3);
                break;
            }
            case THIRD_TEST_EXPAND_NUM: {
                seg = hashTab->dir[2];
                bucket = seg[0];
                ASSERT_EQ(hashTab->hctl->maxBucket, 4);
                ASSERT_EQ(bucket->hashValue, 4);
                break;
            }
            default:
                break;
        }
    }

    /* destroy hash table */
    DestroyTestHashTable<uint32_t, TestDataEntry>(hashTab);
}

/* test HASH_PARTITION */
TEST_F(HashTableTest, TestPartitions)
{
    uint32 flags = HASH_ELEM | HASH_FUNCTION | HASH_DIRSIZE | HASH_SEGMENT | HASH_FFACTOR | HASH_PARTITION;
    HashTab *hashTab = CreateHashTable(512, 3, sizeof(uint32_t), sizeof(TestDataEntry), ExpandHash, flags);
    ASSERT_NE(hashTab, (HashTab *)NULL);

    int insertNum = 0;
    bool success = TestHashTableInsert<uint32_t, TestDataEntry>(hashTab, insertNum, NULL);
    ASSERT_TRUE(success);

    Size entries = HashGetNumEntries(hashTab);
    ASSERT_EQ(entries, 1);

    bool found;
    HashSearch(hashTab, (const void *)&insertNum, HASH_REMOVE, &found);
    ASSERT_TRUE(found);
    
    DestroyTestHashTable<uint32_t, TestDataEntry>(hashTab);
}

TEST_F(HashTableTest, TestFixed)
{
    uint32 flags = HASH_ELEM | HASH_FUNCTION | HASH_DIRSIZE | HASH_SEGMENT | HASH_FFACTOR | HASH_FIXED_SIZE;
    HashTab *hashTab = CreateHashTable(0, 2048, sizeof(uint32_t), sizeof(TestDataEntry), ExpandHash, flags);
    ASSERT_NE(hashTab, (HashTab *)NULL);

    int insertNum = 0;
    TestDataEntry *entry = NULL;
    bool found;
    entry = (TestDataEntry *)HashSearch(hashTab, (void *)&insertNum, HASH_ENTER, &found);
    ASSERT_FALSE(found);
    ASSERT_EQ(entry, (TestDataEntry *)NULL);

    DestroyTestHashTable<uint32_t, TestDataEntry>(hashTab);
}

TEST_F(HashTableTest, TestDynaHashAllocNoExcept)
{
    uint32 flags = HASH_ELEM | HASH_FUNCTION | HASH_DIRSIZE | HASH_SEGMENT | HASH_FFACTOR | HASH_NOEXCEPT;
    HashTab *hashTab = CreateHashTable(0, 2048, sizeof(uint32_t), sizeof(TestDataEntry), ExpandHash, flags);
    ASSERT_NE(hashTab, (HashTab *)NULL);

    int insertNum = 0;
    TestDataEntry *entry = NULL;
    bool found;
    entry = (TestDataEntry *)HashSearch(hashTab, (void *)&insertNum, HASH_ENTER_NULL, &found);
    ASSERT_FALSE(found);
    ASSERT_NE(entry, (TestDataEntry *)NULL);

    entry->key = insertNum;
    entry->data = NULL;

    DestroyTestHashTable<uint32_t, TestDataEntry>(hashTab);
}

TEST_F(HashTableTest, TestHashFreeze)
{
    uint32 flags = HASH_ELEM | HASH_FUNCTION | HASH_DIRSIZE | HASH_SEGMENT | HASH_FFACTOR;
    HashTab *hashTab = CreateHashTable(0, 2048, sizeof(uint32_t), sizeof(TestDataEntry), ExpandHash, flags);
    ASSERT_NE(hashTab, (HashTab *)NULL);

    int key = 0;
    TestDataEntry *entry = NULL;
    bool found;
    entry = (TestDataEntry *)HashSearch(hashTab, (void *)&key, HASH_ENTER_NULL, &found);
    ASSERT_FALSE(found);
    ASSERT_NE(entry, (TestDataEntry *)NULL);
    entry->key = key;
    entry->data = NULL;

    HashSearch(hashTab, (void *)&key, HASH_FIND, &found);
    ASSERT_TRUE(found);

    HashFreeze(hashTab);

    int key1 = 1;
    entry = (TestDataEntry *)HashSearch(hashTab, (void *)&key1, HASH_ENTER_NULL, &found);
    ASSERT_FALSE(found);
    ASSERT_EQ(entry, (TestDataEntry *)NULL);
    
    DestroyTestHashTable<uint32_t, TestDataEntry>(hashTab);
}

TEST_F(HashTableTest, TestUint32Hash)
{
    uint32 flags = HASH_BLOBS | HASH_ELEM | HASH_DIRSIZE | HASH_SEGMENT | HASH_FFACTOR;
    HashTab *hashTab = CreateHashTable(0, 2048, sizeof(uint32), sizeof(TestDataEntry), NULL, flags);
    ASSERT_NE(hashTab, (HashTab *)NULL);

    int key = 8;
    TestDataEntry *entry = NULL;
    bool found;
    entry = (TestDataEntry *)HashSearch(hashTab, (void *)&key, HASH_ENTER_NULL, &found);
    entry->key = key;
    entry->data = NULL;
    ASSERT_FALSE(found);
    ASSERT_NE(entry, (TestDataEntry *)NULL);

    HashSearch(hashTab, (void *)&key, HASH_FIND, &found);
    ASSERT_TRUE(found);
    
    DestroyTestHashTable<uint32, TestDataEntry>(hashTab);
}

TEST_F(HashTableTest, TestTagHash)
{
    uint32 flags = HASH_BLOBS | HASH_ELEM | HASH_DIRSIZE | HASH_SEGMENT | HASH_FFACTOR;
    HashTab *hashTab = CreateHashTable(0, 2048, sizeof(struct Position), sizeof(TestDataTagEntry), NULL, flags);
    ASSERT_NE(hashTab, (HashTab *)NULL);

    struct Position key = {
        .x=1,
        .y=2
    };
    TestDataTagEntry *entry = NULL;
    bool found;
    entry = (TestDataTagEntry *)HashSearch(hashTab, (void *)&key, HASH_ENTER_NULL, &found);
    ASSERT_FALSE(found);
    ASSERT_NE(entry, (TestDataTagEntry *)NULL);
    entry->key = key;
    entry->data = NULL;

    HashSearch(hashTab, (void *)&key, HASH_FIND, &found);
    ASSERT_TRUE(found);
    
    DestroyTestHashTable<Position, TestDataTagEntry>(hashTab);
}

TEST_F(HashTableTest, TestStringHash)
{
    uint32 flags = HASH_ELEM | HASH_DIRSIZE | HASH_SEGMENT | HASH_FFACTOR;
    HashTab *hashTab = CreateHashTable(0, 2048, sizeof(struct Position), sizeof(TestDataTagEntry), NULL, flags);
    ASSERT_NE(hashTab, (HashTab *)NULL);

    struct Position key = {
        .x=1,
        .y=2
    };
    TestDataTagEntry *entry = NULL;
    bool found;
    entry = (TestDataTagEntry *)HashSearch(hashTab, (void *)&key, HASH_ENTER_NULL, &found);
    ASSERT_FALSE(found);
    ASSERT_NE(entry, (TestDataTagEntry *)NULL);
    entry->key = key;
    entry->data = NULL;

    HashSearch(hashTab, (void *)&key, HASH_FIND, &found);
    ASSERT_TRUE(found);

    DestroyTestHashTable<Position, TestDataTagEntry>(hashTab);
}

/* Test HASH_PARTITION of the hash table -- Uint32Hash */
TEST_F(HashTableTest, HashTablePartitionTest)
{
    /* create hash table */
    HashCTL ctl;
    HashTab *hashTab;
    uint32 flags = HASH_ELEM | HASH_FUNCTION | HASH_PARTITION;

    (void)memset_s(&ctl, sizeof(HashCTL), 0, sizeof(HashCTL));
    ctl.entrySize = sizeof(TestDataEntry);
    ctl.keySize = sizeof(uint32_t);
    ctl.hash = Uint32Hash;
    ctl.numPartitions = 32;

    /* estimate hash size */
    Size estiSize = HashEstimateSize(MAX_TEST_ENTRY_SIZE, sizeof(TestDataEntry));
    EXPECT_EQ(estiSize, 454488);
    Size DirSize = HashSelectDirSize(MAX_TEST_ENTRY_SIZE);
    EXPECT_EQ(DirSize, 256);

    /* create hash table */
    hashTab = HashCreate("TestData hashtable", MAX_TEST_ENTRY_SIZE,
                         &ctl, flags);
    ASSERT_NE(hashTab, (HashTab*)NULL);

    /* batch insert data */
    for (int i = 1; i <= INIT_TEST_ENTRY_SIZE; ++i) {
        char testName[MAX_NAME_LEN];
        (void)snprintf_s(testName, MAX_NAME_LEN, MAX_NAME_LEN - 1, "Test%d", i);
        Position position = {i, i+1};
        TestData *data = CreateTestData(i, 10 * i, testName, position);
        TestHashTableInsert<uint32_t, TestDataEntry>(hashTab, i, data);
    }

    /* search hash table */
    TestData  *data = TestHashTableFind<uint32_t, TestDataEntry>(hashTab, TEST_TARGET_ID);
    ASSERT_NE(data, (TestData *)NULL);
    EXPECT_EQ(data->id, TEST_TARGET_ID);


    /* remove data */
    TestData *target = TestHashTableRemove<uint32_t, TestDataEntry>(hashTab, TEST_TARGET_ID);
    ASSERT_EQ(target, data);
    EXPECT_EQ(target->id, TEST_TARGET_ID);
    EXPECT_EQ(strcmp(target->name, TEST_TARGET_NAME), 0);
    DestroyTestData(target);

    /* since TEST_TARGET_ID has been deleted, data will not be found */
    data = TestHashTableFind<uint32_t, TestDataEntry>(hashTab, TEST_TARGET_ID);
    ASSERT_EQ(data, (TestData *)NULL);

    /* destroy hash table */
    CleanTestHashTable<uint32_t, TestDataEntry>(hashTab);
    HashRemove(hashTab);
}

TEST_F(HashTableTest, HashAnyTest)
{
#define TEST_STR "test xxx yyy zzz test $%^&@*&!)({}\|_+`"
    char content[sizeof(TEST_STR) + 8];
    memcpy(content, TEST_STR, sizeof(TEST_STR));

    auto hashVal = HashAny((const unsigned char *)content, (int)sizeof(TEST_STR));
    memcpy(content + 1, TEST_STR, sizeof(TEST_STR));
    ASSERT_EQ(hashVal, HashAny((const unsigned char *)content + 1, (int)sizeof(TEST_STR)));
    memcpy(content + 2, TEST_STR, sizeof(TEST_STR));
    ASSERT_EQ(hashVal, HashAny((const unsigned char *)content + 2, (int)sizeof(TEST_STR)));
    memcpy(content + 3, TEST_STR, sizeof(TEST_STR));
    ASSERT_EQ(hashVal, HashAny((const unsigned char *)content + 3, (int)sizeof(TEST_STR)));
    memcpy(content + 4, TEST_STR, sizeof(TEST_STR));
    ASSERT_EQ(hashVal, HashAny((const unsigned char *)content + 4, (int)sizeof(TEST_STR)));
    memcpy(content + 5, TEST_STR, sizeof(TEST_STR));
    ASSERT_EQ(hashVal, HashAny((const unsigned char *)content + 5, (int)sizeof(TEST_STR)));
    memcpy(content + 6, TEST_STR, sizeof(TEST_STR));
    ASSERT_EQ(hashVal, HashAny((const unsigned char *)content + 6, (int)sizeof(TEST_STR)));
    memcpy(content + 7, TEST_STR, sizeof(TEST_STR));
    ASSERT_EQ(hashVal, HashAny((const unsigned char *)content + 7, (int)sizeof(TEST_STR)));
}

TEST_F(HashTableTest, HashAnyPerfTest)
{
    size_t memSize = 300 * 1024 * 1024;
    uint64_t *content = (uint64_t *)malloc(memSize + 8);
    ASSERT_NE(content, nullptr);
    srand(time(NULL));
    uint64_t random = (rand() << 32) + rand();
    for (size_t i = 0; i < memSize / sizeof(uint64_t); i++) {
        content[i] = random++;
    }
    printf("[..........] begin...\n");
    std::chrono::time_point<std::chrono::high_resolution_clock> startTime = std::chrono::high_resolution_clock::now();

    auto hashVal = HashAny((const unsigned char *)content + 2, (int)memSize);
    ASSERT_EQ(hashVal, HashAny((const unsigned char *)content + 2, (int)memSize));

    std::chrono::time_point<std::chrono::high_resolution_clock> endTime = std::chrono::high_resolution_clock::now();
    std::chrono::microseconds duration = std::chrono::duration_cast<std::chrono::microseconds>(endTime - startTime);
    printf("[..........] cost = %.3lfms\n", (double)duration.count() / 1000);
    free(content);
}
