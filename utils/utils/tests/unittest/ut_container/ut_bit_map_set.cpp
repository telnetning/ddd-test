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
 * Description: Implement bitmap
 */
#include "securec.h"
#include "gtest/gtest.h"
#include <vector>
#include <array>
#include <tuple>
#include "memory/memory_ctx.h"
#include "container/bitmap.h"
#include "types/data_types.h"

using namespace std;
/** ************************************************************************************************************* **/
class BitMapTest : public testing::Test {
public:
    void SetUp() override {
        MemoryContextSwitchTo(MemoryContextCreate(NULL, MEM_CXT_TYPE_GENERIC, "ut_bitmapset_mctx",
                                                  0, DEFAULT_UNLIMITED_SIZE, DEFAULT_UNLIMITED_SIZE));
    };

    void TearDown() override {
        MemoryContextDelete(MemoryContextSwitchTo(NULL));
    };
};

/* prototype copied from bitmap.c */
struct BitmapTest {
    uint32_t        size;                    /* flexible array size */
    uint64_t        bits[0];                 /* flexible array */
};

/**
 * @tc.name:  BitMapTest_Base
 * @tc.desc:  Test the bitmap set/clear bit.
 * @tc.type: FUNC
 */
TEST_F(BitMapTest, BitMapTest_Base)
{
    constexpr int  bitmapBitsPerElement = sizeof(decltype(((BitmapTest *)0)->bits[0])) * 8;
    vector<array<uint32_t, 3>> testData = {
        {0, 0, 1},
        {1, 0, 1},
        {0, 5, 6},
        {1, 5, 8},
        {3, 6, 7},
        {bitmapBitsPerElement - 1, 0, 7},
    };

    auto baseBms = BitmapCreateWithSet(0);
    ASSERT_NE(baseBms, nullptr);
    auto copyBms = BitmapCopy(baseBms);
    BitmapClearBit(copyBms, 0);

    for (auto &vec : testData) {
        auto elementIdx = vec[0];
        auto bitPos0 = vec[1] + elementIdx * bitmapBitsPerElement;
        auto bitPos1 = vec[2] + elementIdx * bitmapBitsPerElement;
        /* input is the bitpos in one element */
        ASSERT_LT(vec[1], bitmapBitsPerElement);
        ASSERT_LT(vec[2], bitmapBitsPerElement);

        copyBms = BitmapSetBit(copyBms, bitPos0);
        ASSERT_NE(copyBms, nullptr);
        auto elementValue = ((BitmapTest *)copyBms)->bits[elementIdx];
        ASSERT_EQ(elementValue, 1 << bitPos0);
        copyBms = BitmapSetBit(copyBms, bitPos1);
        ASSERT_NE(copyBms, nullptr);
        elementValue = ((BitmapTest *)copyBms)->bits[elementIdx];
        ASSERT_EQ(elementValue, 1 << bitPos0 | 1 << bitPos1);
        BitmapClearBit(copyBms, bitPos0);
        elementValue = ((BitmapTest *)copyBms)->bits[elementIdx];
        ASSERT_EQ(elementValue, 1 << bitPos1);
        BitmapClearBit(copyBms, bitPos1);
        elementValue = ((BitmapTest *)copyBms)->bits[elementIdx];
        ASSERT_EQ(elementValue, 0);
    }
    BitmapDestroy(baseBms);
    BitmapDestroy(copyBms);
}
/**
 * @tc.name:  BitMapTest_Judge
 * @tc.desc:  Test whether there are null, subset, equality, overlap, etc.
 * @tc.type: FUNC
 */
TEST_F(BitMapTest, BitMapTest_Judge)
{
    const int  FIRST_TEST_BITMAP_NUM = 0;
    const int  SECOND_TEST_BITMAP_NUM = 5;
    const int  THIRD_TEST_BITMAP_NUM = 36;
    Bitmap* bms1 = NULL;
    Bitmap* bms2 = NULL;
    bool isMember = BitmapIsBitSet(bms2, SECOND_TEST_BITMAP_NUM);
    ASSERT_FALSE(isMember);
    bool isNonEmptyDifference = BitmapAndComplementIsNonEmpty(bms1, bms2);
    ASSERT_FALSE(isNonEmptyDifference);
    BitmapCmpResult subSetCompare = BitmapCompare(bms1, bms2);
    ASSERT_EQ(subSetCompare, BITMAP_EQUAL);
    BitmapCountShip membership = BitmapCountBitsFast(bms1);
    ASSERT_EQ(membership, BITMAP_ALL_NOT_SET);

    bms1 = BitmapCreateWithSet(FIRST_TEST_BITMAP_NUM);
    isMember = BitmapIsBitSet(bms1, -1);
    ASSERT_FALSE(isMember);
    isNonEmptyDifference = BitmapAndComplementIsNonEmpty(bms1, bms2);
    ASSERT_TRUE(isNonEmptyDifference);
    subSetCompare = BitmapCompare(bms1, bms2);
    ASSERT_EQ(subSetCompare, BITMAP_FIRST_CONTAIN_SECOND);
    subSetCompare = BitmapCompare(bms2, bms1);
    ASSERT_EQ(subSetCompare, BITMAP_SECOND_CONTAIN_FIRST);
    membership = BitmapCountBitsFast(bms1);
    ASSERT_EQ(membership, BITMAP_SINGLE_BIT_SET);
    bool isEmpty = BitmapIsEmpty(bms1);
    ASSERT_FALSE(isEmpty);
    isEmpty = BitmapIsEmpty(bms2);
    ASSERT_TRUE(isEmpty);

    bms2 = BitmapCreateWithSet(SECOND_TEST_BITMAP_NUM);
    isNonEmptyDifference = BitmapAndComplementIsNonEmpty(bms1, bms2);
    ASSERT_TRUE(isNonEmptyDifference);
    subSetCompare = BitmapCompare(bms1, bms2);
    ASSERT_EQ(subSetCompare, BITMAP_DIFFERENT);
    membership = BitmapCountBitsFast(bms2);
    ASSERT_EQ(membership, BITMAP_SINGLE_BIT_SET);

    Bitmap* bms3 = BitmapCreateWithSet(SECOND_TEST_BITMAP_NUM);
    bool isEqual = BitmapEqual(bms2, bms3);
    ASSERT_TRUE(isEqual);
    bool isSubset = BitmapIsSubset(bms2, bms3);
    ASSERT_TRUE(isSubset);
    BitmapDestroy(bms3);
    Bitmap* bmsb = BitmapCopy(bms1);
    bms3 = BitmapSetBit(bms1, SECOND_TEST_BITMAP_NUM);
    isEqual = BitmapEqual(bms2, bms3);
    ASSERT_FALSE(isEqual);
    subSetCompare = BitmapCompare(bmsb, bms3);
    ASSERT_EQ(subSetCompare, BITMAP_SECOND_CONTAIN_FIRST);
    subSetCompare = BitmapCompare(bms3, bmsb);
    ASSERT_EQ(subSetCompare, BITMAP_FIRST_CONTAIN_SECOND);
    isSubset = BitmapIsSubset(bms1, bms3);
    ASSERT_TRUE(isSubset);
    isSubset = BitmapIsSubset(bmsb, bms3);
    ASSERT_TRUE(isSubset);
    isSubset = BitmapIsSubset(bms3, bmsb);
    ASSERT_FALSE(isSubset);
    isMember = BitmapIsBitSet(bms3, THIRD_TEST_BITMAP_NUM);
    ASSERT_FALSE(isMember);

    Bitmap* bms4 = BitmapSetBit(bms3, THIRD_TEST_BITMAP_NUM);
    isMember = BitmapIsBitSet(bms4, THIRD_TEST_BITMAP_NUM);
    ASSERT_TRUE(isMember);
    membership = BitmapCountBitsFast(bms4);
    ASSERT_EQ(membership, BITMAP_MULTIPLE_BITS_SET);

    Bitmap* bms5 = BitmapSetBit(bms2, THIRD_TEST_BITMAP_NUM - 1);
    Bitmap* bms6 = BitmapCreateWithSet(THIRD_TEST_BITMAP_NUM + 1);
    bool isOverlap = BitmapOverlap(bms4, bms5);
    ASSERT_TRUE(isOverlap);
    subSetCompare = BitmapCompare(bms4, bms5);
    ASSERT_EQ(subSetCompare, BITMAP_DIFFERENT);
    isOverlap = BitmapOverlap(bms4, bms6);
    ASSERT_FALSE(isOverlap);
    subSetCompare = BitmapCompare(bms4, bms6);
    ASSERT_EQ(subSetCompare, BITMAP_DIFFERENT);

    BitmapDestroy(bmsb);
    BitmapDestroy(bms4);
    BitmapDestroy(bms5);
    BitmapDestroy(bms6);
}
/**
 * @tc.name:  BitMapTest_Set
 * @tc.desc:  Test obtain difference sets, intersection sets, union sets, and so on.
 * @tc.type: FUNC
 */
TEST_F(BitMapTest, BitMapTest_Set)
{
PREPARE_TEST:
    uint32_t testData[][4][3] = {
        /*        same set,     set in first,    set in second,  clear set */
        {{  0,    1,    2}, {   3,   4,   5}, {   6,   7,   8}, {-1, -1, -1}},  // -1(UINT32_MAX) is not set, clear nothing
        {{  0,  100, 1000}, {2000,3000,4000}, {5000,6000,7000}, {-1, -1, -1}},  // first bitmap shorter than second
        {{  0,  100, 1000}, {5000,6000,7000}, {2000,3000,4000}, {-1, -1, -1}},  // first bitmap longer than second
        {{ 31,   32,   33}, {  62,  63,  64}, {  30,  34,  65}, {-1, -1, -1}},
        {{ 20,   30,   40}, {  50,  60,  70}, {  80,  90, 100}, {-1, -1, -1}},
        {{ 20,   30,   40}, {  50,  60,  70}, {  80,  90, 100}, {20, 30, 40}},  // clear same set, let first and second have no intersect
    };
#define TEST_CASES          sizeof(testData) / sizeof(testData[0])
#define SET_BIT_COUNT       sizeof(testData[0][0]) / sizeof(testData[0][0][0])

    for (int cases = 0; cases < TEST_CASES; cases++) {
        auto posSame = testData[cases][0];
        auto posSet1 = testData[cases][1];
        auto posSet2 = testData[cases][2];
        auto posClear = testData[cases][3];

        Bitmap *first = nullptr;
        Bitmap *second = nullptr;
        Bitmap *check = nullptr;
        Bitmap *firstInplace = nullptr;

        /* set posSame and clear posClear to two input bitmap */
        for (int i = 0; i < SET_BIT_COUNT; i++) {
            first = BitmapSetBit(first, posSame[i]);
            ASSERT_NE(first, nullptr);
            second = BitmapSetBit(second, posSame[i]);
            ASSERT_NE(second, nullptr);
        }
        for (int i = 0; i < SET_BIT_COUNT; i++) {
            first = BitmapSetBit(first, posSet1[i]);
            ASSERT_NE(first, nullptr);
        }
        for (int i = 0; i < SET_BIT_COUNT; i++) {
            second = BitmapSetBit(second, posSet2[i]);
            ASSERT_NE(first, nullptr);
        }
        for (int i = 0; i < SET_BIT_COUNT; i++) {
            BitmapClearBit(first, posClear[i]);
            BitmapClearBit(second, posClear[i]);
        }
TEST_CASE1:
        /* BitmapOr and BitmapOrInplace */  // do (first | second)
        auto orResult = BitmapOr(first, nullptr);
        ASSERT_TRUE(BitmapEqual(orResult, first));
        BitmapDestroy(orResult);
        orResult = BitmapOr(nullptr, second);
        ASSERT_TRUE(BitmapEqual(orResult, second));
        BitmapDestroy(orResult);
        orResult = BitmapOr(first, second);
        ASSERT_NE(orResult, nullptr);

        for (int i = 0; i < SET_BIT_COUNT; i++) {
            check = BitmapSetBit(check, posSame[i]);
            ASSERT_NE(check, nullptr);
            check = BitmapSetBit(check, posSet1[i]);
            ASSERT_NE(check, nullptr);
            check = BitmapSetBit(check, posSet2[i]);
            ASSERT_NE(check, nullptr);
            BitmapClearBit(check, posClear[i]);
        }
        ASSERT_TRUE(BitmapEqual(orResult, check));
        BitmapDestroy(orResult);
        // ******* inplace ******
        firstInplace = BitmapOrInplace(nullptr, second);
        ASSERT_TRUE(BitmapEqual(firstInplace, second));
        BitmapDestroy(firstInplace);
        firstInplace = BitmapCopy(first); /* copy the first for inplace operate */
        ASSERT_NE(firstInplace, nullptr);
        auto temp = BitmapOrInplace(firstInplace, NULL);
        ASSERT_EQ(temp, firstInplace);  // only ignore return nullptr in test code

        BitmapDestroy(firstInplace);
        firstInplace = BitmapCopy(first); /* copy the first for inplace operate */
        ASSERT_NE(firstInplace, nullptr);
        firstInplace = BitmapOrInplace(firstInplace, second);  // only ignore return nullptr in test code
        ASSERT_NE(firstInplace, nullptr);
        ASSERT_TRUE(BitmapEqual(firstInplace, check));
        BitmapDestroy(firstInplace);
        /***** BitmapOrWithDestroy *****/
        auto firstCopy = BitmapCopy(first); /* copy the first */
        auto secondCopy = BitmapCopy(second); /* copy the first */
        ASSERT_EQ(BitmapOrWithDestroy(firstCopy, nullptr), firstCopy);
        ASSERT_EQ(BitmapOrWithDestroy(nullptr, secondCopy), secondCopy);
        orResult = BitmapOrWithDestroy(firstCopy, secondCopy);
        ASSERT_NE(orResult, nullptr);
        ASSERT_TRUE(BitmapEqual(orResult, check));
        BitmapDestroy(orResult);  /* no need to destroy firstCopy and secondCopy */

        BitmapDestroy(check);
TEST_CASE2:
        /* BitmapAnd and BitmapAndInplace */  // do (first & second)
        ASSERT_EQ(BitmapAnd(first, nullptr), nullptr);
        ASSERT_EQ(BitmapAnd(nullptr, second), nullptr);

        check = nullptr;
        for (int i = 0; i < SET_BIT_COUNT; i++) {
            check = BitmapSetBit(check, posSame[i]);
            BitmapClearBit(check, posClear[i]);
        }
        auto andResult = BitmapAnd(first, second);
        ASSERT_NE(andResult, nullptr);
        ASSERT_TRUE(BitmapEqual(andResult, check));
        BitmapDestroy(andResult);
        // ******* inplace ******
        firstInplace = BitmapCopy(first); /* copy the first for inplace operate */
        ASSERT_NE(firstInplace, nullptr);
        ASSERT_EQ(BitmapAndInplace(firstInplace, nullptr), nullptr);
        ASSERT_EQ(BitmapAndInplace(nullptr, second), nullptr);

        firstInplace = BitmapCopy(first); /* copy the first for inplace operate */
        ASSERT_NE(firstInplace, nullptr);
        firstInplace = BitmapAndInplace(firstInplace, second); // only ignore return nullptr in test code
        ASSERT_TRUE(BitmapEqual(firstInplace, check));
        BitmapDestroy(firstInplace);

        BitmapDestroy(check);
TEST_CASE3:
        /* BitmapAndComplement and BitmapAndComplementInplace */ // do (first & (~second))
        auto andnotRes = BitmapAndComplement(first, nullptr);
        ASSERT_NE(andnotRes, nullptr);
        ASSERT_TRUE(BitmapEqual(andnotRes, first));
        BitmapDestroy(andnotRes);
        ASSERT_EQ(BitmapAndComplement(nullptr, second), nullptr);
        /* andnotRes = first & (~second) */
        andnotRes = BitmapAndComplement(first, second);
        ASSERT_NE(andnotRes, nullptr);
        check = nullptr;
        for (int i = 0; i < SET_BIT_COUNT; i++) {
            check = BitmapSetBit(check, posSet1[i]);
            BitmapClearBit(check, posClear[i]);
        }
        ASSERT_TRUE(BitmapEqual(andnotRes, check));
        BitmapDestroy(andnotRes);
        // ******* inplace first & (~second) ******
        firstInplace = BitmapCopy(first); /* copy the first for inplace operate */
        ASSERT_NE(firstInplace, nullptr);
        firstInplace = BitmapAndComplementInplace(firstInplace, nullptr);
        ASSERT_TRUE(BitmapEqual(firstInplace, first));
        ASSERT_EQ(BitmapAndComplementInplace(nullptr, second), nullptr);
        firstInplace = BitmapAndComplementInplace(firstInplace, second); // firstInplace == first
        ASSERT_NE(firstInplace, nullptr);
        ASSERT_TRUE(BitmapEqual(firstInplace, check));
        BitmapDestroy(firstInplace);
        BitmapDestroy(check);
        /* andnotRes = second & (~first) */
        andnotRes = BitmapAndComplement(second, first);
        ASSERT_NE(andnotRes, nullptr);
        check = nullptr;
        for (int i = 0; i < SET_BIT_COUNT; i++) {
            check = BitmapSetBit(check, posSet2[i]);
        }
        ASSERT_TRUE(BitmapEqual(andnotRes, check));
        BitmapDestroy(andnotRes);
        // ******* inplace second & (~first) ******
        auto secondInplace = BitmapCopy(second); /* copy the first for inplace operate */
        ASSERT_NE(secondInplace, nullptr);
        secondInplace = BitmapAndComplementInplace(secondInplace, first); // firstInplace == first
        ASSERT_NE(secondInplace, nullptr);
        ASSERT_TRUE(BitmapEqual(secondInplace, check));
        BitmapDestroy(secondInplace);
        BitmapDestroy(check);
TEST_END:
        /* clear input */
        BitmapDestroy(first);
        BitmapDestroy(second);
    }
}

/**
 * @tc.name:  BitMapTest_Search
 * @tc.desc:  Test search number of members, first member, next member, and so on.
 * @tc.type: FUNC
 */
TEST_F(BitMapTest, BitMapTest_Search)
{
    struct TestDataSetType {
        vector<uint32_t> bitSetPos;     // [in]
        int64_t          singleBitPos;  // [out]
        uint64_t         countBits;     // [out]
        BitmapCountShip  countBitsFast; // [out]
        int64_t          firstBit;      // [out]
        int64_t          nextBit;       // [out] nextBit after firstBit
    };
PREPARE_TEST:
    TestDataSetType dataSet[] = {
        {{                  0},  0, 1,    BITMAP_SINGLE_BIT_SET,     0,    -1},
        {{                  1},  1, 1,    BITMAP_SINGLE_BIT_SET,     1,    -1},
        {{                  8},  8, 1,    BITMAP_SINGLE_BIT_SET,     8,    -1},
        {{                 33}, 33, 1,    BITMAP_SINGLE_BIT_SET,    33,    -1},
        {{                 63}, 63, 1,    BITMAP_SINGLE_BIT_SET,    63,    -1},
        {{                 64}, 64, 1,    BITMAP_SINGLE_BIT_SET,    64,    -1},
        {{         200,    10}, -1, 2, BITMAP_MULTIPLE_BITS_SET,    10,   200},
        {{200,      63,    64}, -1, 3, BITMAP_MULTIPLE_BITS_SET,    63,    64},
        {{10000, 20000, 30000}, -1, 3, BITMAP_MULTIPLE_BITS_SET, 10000, 20000},
    };

    /* first, check empty bitmap */
    auto bitmap = BitmapCreateWithSet(100);
    ASSERT_NE(bitmap, nullptr);
    BitmapClearBit(bitmap, 100);
    ASSERT_EQ(BitmapGetPositionForSingleton(bitmap), -1);
    ASSERT_EQ(BitmapCountBits(bitmap), 0);
    ASSERT_EQ(BitmapCountBitsFast(bitmap), BITMAP_ALL_NOT_SET);
    ASSERT_EQ(BitmapGetFirstBit(bitmap), -1);
    ASSERT_EQ(BitmapGetNextBit(bitmap, 1), -1);

    for (int i = 0; i < sizeof(dataSet) / sizeof(dataSet[0]); i++) {
        auto &testData = dataSet[i].bitSetPos;
        for (auto setPos : testData) {
            bitmap = BitmapSetBit(bitmap, setPos);
            ASSERT_NE(bitmap, nullptr);
        }
        ASSERT_EQ(BitmapGetPositionForSingleton(bitmap),         dataSet[i].singleBitPos);
        ASSERT_EQ(BitmapCountBits(bitmap),                       dataSet[i].countBits);
        ASSERT_EQ(BitmapCountBitsFast(bitmap),                   dataSet[i].countBitsFast);
        ASSERT_EQ(BitmapGetFirstBit(bitmap),                     dataSet[i].firstBit);
        ASSERT_EQ(BitmapGetNextBit(bitmap, dataSet[i].firstBit), dataSet[i].nextBit);
        BitmapDestroy(bitmap);
        bitmap = nullptr;
    }
}

/**
 * @tc.name:  BitMapTest_Exception
 * @tc.desc:  bitmap can't alloc memory and nullptr parameter
 * @tc.type: FUNC
 */
TEST_F(BitMapTest, BitMapTest_Exception)
{
#define MAX_ALLOCATE_MEMORY      (1024 * 8)  // 8K
#define MAX_ALLOCATE_MEMORY_BIT  (MAX_ALLOCATE_MEMORY * 8)
    auto ctx = MemoryContextCreate(NULL, MEM_CXT_TYPE_SHARE, "bitmap_oom_ctx", 0, 0, MAX_ALLOCATE_MEMORY);
    ASSERT_NE(ctx, nullptr);
    MemoryContextSetSilent(ctx, true);
    auto old = MemoryContextSwitchTo(ctx);
    auto bitmap = BitmapCreateWithSet(MAX_ALLOCATE_MEMORY_BIT / 2); // 4K
    ASSERT_NE(bitmap, nullptr);
    auto bitmapEmpty = BitmapCreateWithSet(MAX_ALLOCATE_MEMORY_BIT / 4); // 2K
    ASSERT_NE(bitmapEmpty, nullptr);
    BitmapClearBit(bitmapEmpty, MAX_ALLOCATE_MEMORY_BIT / 4); // clear it

    ASSERT_EQ(BitmapCreateWithSet(MAX_ALLOCATE_MEMORY_BIT), nullptr);
    ASSERT_EQ(BitmapCopy(nullptr), nullptr);
    ASSERT_EQ(BitmapCopy(bitmap), nullptr); // copy (MAX_ALLOCATE_MEMORY_BIT / 2) will can't alloc memory
    ASSERT_EQ(BitmapSetBit(bitmap, MAX_ALLOCATE_MEMORY_BIT), nullptr);
    BitmapClearBit(nullptr, 0);
    ASSERT_EQ(BitmapOr(bitmapEmpty, bitmap), nullptr);
    ASSERT_EQ(BitmapOrInplace(bitmapEmpty, bitmap), nullptr);  // first is smaller
    ASSERT_EQ(BitmapAnd(bitmap, bitmapEmpty), nullptr);
    ASSERT_EQ(BitmapAndComplement(bitmap, bitmapEmpty), nullptr);
    ASSERT_EQ(BitmapGetPositionForSingleton(nullptr), -1);
    ASSERT_EQ(BitmapGetFirstBit(nullptr), -1);
    ASSERT_EQ(BitmapGetNextBit(nullptr, 0), -1);
    ASSERT_EQ(BitmapGetNextBit(bitmap, MAX_ALLOCATE_MEMORY_BIT), -1);
    ASSERT_EQ(BitmapGetNextBit(bitmap, -1), -1); // get next bit after max pos
    ASSERT_EQ(BitmapCountBits(nullptr), 0);
    ASSERT_EQ(BitmapIsSubset(nullptr, bitmap), true);
    ASSERT_EQ(BitmapIsSubset(bitmap, nullptr), false);
    ASSERT_EQ(BitmapIsSubset(bitmapEmpty, nullptr), true);
    ASSERT_EQ(BitmapEqual(nullptr, nullptr), true);
    ASSERT_EQ(BitmapEqual(nullptr, bitmapEmpty), true);
    ASSERT_EQ(BitmapEqual(bitmapEmpty, nullptr), true);
    ASSERT_EQ(BitmapEqual(nullptr, bitmap), false);
    ASSERT_EQ(BitmapEqual(bitmap, nullptr), false);
    ASSERT_EQ(BitmapOverlap(nullptr, nullptr), false);

    MemoryContextDelete(MemoryContextSwitchTo(old));
}

TEST_F(BitMapTest, BitMapTest_Subset)
{
    int64_t bitampBits[][2][20] = {
        {{0, 1, 16, 31, 80, 100, 300}, {0, 1, 15, 16, 31, 80}}, // different
        {{0, 1, 15, 16, 31,  80, 500}, {0, 1, 15, 16, 31, 80}}, // second is first subset
        {{0, 1, 15, 16, 31,  80,    }, {0, 1, 15, 16, 31, 80, 500}}, // first is second subset
        {{0, 1, 15, 16, 31, 900, -900}, {0, 1, 15, 16, 31}},    // equal
    };
    tuple<           BitmapCmpResult,  bool, bool> result[] = {
        /*                   compare, subset, &~ */
        {           BITMAP_DIFFERENT, false, true},
        {BITMAP_FIRST_CONTAIN_SECOND, false, true},
        {BITMAP_SECOND_CONTAIN_FIRST,  true, false},
        {               BITMAP_EQUAL,  true, false}, // equal is also subset each other
    };
    for (int cases = 0; cases < sizeof(bitampBits) / sizeof(bitampBits[0]); cases ++) {
        Bitmap *bitmap0 = nullptr;
        Bitmap *bitmap1 = nullptr;
        auto setBitmapFunc = [](Bitmap **bitmap, int64_t *bitsArray, size_t size) {
            for (auto i = 0; i < size; i++) {
                if (bitsArray[i] < 0) {
                    BitmapClearBit(*bitmap, -bitsArray[i]);
                    continue;
                }
                *bitmap = BitmapSetBit(*bitmap, bitsArray[i]);
                ASSERT_NE(*bitmap, nullptr);
            }
        };
        setBitmapFunc(&bitmap0, bitampBits[cases][0], sizeof(bitampBits[cases][0]) / sizeof(bitampBits[cases][0][0]));
        setBitmapFunc(&bitmap1, bitampBits[cases][1], sizeof(bitampBits[cases][1]) / sizeof(bitampBits[cases][1][0]));
        // do test
        ASSERT_EQ(BitmapCompare(bitmap0, bitmap1),  get<0>(result[cases]));
        ASSERT_EQ(BitmapIsSubset(bitmap0, bitmap1), get<1>(result[cases]));
        ASSERT_EQ(BitmapAndComplementIsNonEmpty(bitmap0, bitmap1), get<2>(result[cases]));
        BitmapDestroy(bitmap0);
        BitmapDestroy(bitmap1);
    }
}
