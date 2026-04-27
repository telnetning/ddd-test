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
#include "common/algorithm/dstore_type_compress.h"
#include <string>
#include <cmath>
using namespace DSTORE;

class CompressUnsigned32Test : public testing::TestWithParam<uint32> {
protected:
};

INSTANTIATE_TEST_CASE_P(Unsigned32Test, CompressUnsigned32Test,
                        testing::Values(0, (uint32)std::pow(2, 7) - 1, (uint32)std::pow(2, 7),
                                        (uint32)std::pow(2, 14) - 1, (uint32)std::pow(2, 14),
                                        (uint32)std::pow(2, 21) - 1, (uint32)std::pow(2, 21),
                                        (uint32)std::pow(2, 28) - 1, (uint32)std::pow(2, 28),
                                        (uint32)std::pow(2, 32) - 1));
TEST_P(CompressUnsigned32Test, test01)
{
    VarintCompress compress;
    uint32 originValue = GetParam();
    uint32 computeSize = compress.GetUnsigned32CompressedSize(originValue);
    char *compressedData = (char *)malloc(computeSize);
    uint8 compressedSize = compress.CompressUnsigned32(originValue, compressedData);
    ASSERT_EQ(computeSize, compressedSize);
    uint8 deCompressedSize;
    ASSERT_EQ(compress.DecompressUnsigned32(compressedData, deCompressedSize), originValue);
    ASSERT_EQ(computeSize, deCompressedSize);
    free(compressedData);
    std::cout << "originValue: " << originValue << std::endl;
    std::cout << "computeSize: " << computeSize << std::endl;
}

class CompressSigned32Test : public testing::TestWithParam<int32> {
protected:
};

INSTANTIATE_TEST_CASE_P(Signed32Test, CompressSigned32Test,
                        testing::Values(0, (int32)(-1), (int32)(-std::pow(2, 6) - 1), (int32)(-std::pow(2, 13)),
                                        (int32)(-std::pow(2, 13) - 1), (int32)(-std::pow(2, 21)),
                                        (int32)(-std::pow(2, 21) - 1), (int32)(-std::pow(2, 28)),
                                        (int32)(-std::pow(2, 28) - 1), -(int32)std::pow(2, 31)));
TEST_P(CompressSigned32Test, test02)
{
    VarintCompress compress;
    int32 originValue = GetParam();
    int32 computeSize = compress.GetSigned32CompressedSize(originValue);
    char *compressedData = (char *)malloc(computeSize);
    uint8 compressedSize = compress.CompressSigned32(originValue, compressedData);
    ASSERT_EQ(computeSize, compressedSize);
    uint8 deCompressedSize;
    ASSERT_EQ(compress.DecompressSigned32(compressedData, deCompressedSize), originValue);
    ASSERT_EQ(computeSize, deCompressedSize);
    free(compressedData);
    std::cout << "originValue: " << originValue << std::endl;
    std::cout << "computeSize: " << computeSize << std::endl;
}

class CompressUnsigned64Test : public testing::TestWithParam<uint64> {
protected:
};

INSTANTIATE_TEST_CASE_P(Unsigned64Test, CompressUnsigned64Test,
                        testing::Values((uint64)std::pow(2, 0) - 1, (uint64)std::pow(2, 0), (uint64)std::pow(2, 7) - 1,
                                        (uint64)std::pow(2, 7), (uint64)std::pow(2, 14) - 1, (uint64)std::pow(2, 14),
                                        (uint64)std::pow(2, 21) - 1, (uint64)std::pow(2, 21),
                                        (uint64)std::pow(2, 35) - 1, (uint64)std::pow(2, 35),
                                        (uint64)std::pow(2, 42) - 1, (uint64)std::pow(2, 42),
                                        (uint64)std::pow(2, 49) - 1, (uint64)std::pow(2, 49),
                                        (uint64)std::pow(2, 56) - 1, (uint64)std::pow(2, 56),
                                        (uint64)std::pow(2, 63) - 1));

TEST_P(CompressUnsigned64Test, test01)
{
    VarintCompress compress;
    uint64 originValue = GetParam();
    uint8 computeSize = compress.GetUnsigned64CompressedSize(originValue);
    char *compressedData = (char *)malloc(computeSize);
    uint8 compressedSize = compress.CompressUnsigned64(originValue, compressedData);
    EXPECT_EQ(computeSize, compressedSize);
    uint8 deCompressedSize;
    EXPECT_EQ(compress.DecompressUnsigned64(compressedData, deCompressedSize), originValue);
    EXPECT_EQ(computeSize, deCompressedSize);
    free(compressedData);
    printf("originValue: %llu\n", originValue);
    printf("computeSize: %d\n", computeSize);
}

class CompressSigned64Test : public testing::TestWithParam<int64> {
protected:
};

INSTANTIATE_TEST_CASE_P(Signed64Test, CompressSigned64Test,
                        testing::Values(-1, 0, -(int64)std::pow(2, 6), (int64)std::pow(2, 7) - 1,
                                        -(int64)std::pow(2, 13), (int64)std::pow(2, 14) - 1, -(int64)std::pow(2, 20),
                                        (int64)std::pow(2, 21) - 1, -(int64)std::pow(2, 27), (int64)std::pow(2, 28) - 1,
                                        -(int64)std::pow(2, 34), (int64)std::pow(2, 35) - 1, -(int64)std::pow(2, 41),
                                        (int64)std::pow(2, 42) - 1, -(int64)std::pow(2, 48), (int64)std::pow(2, 49) - 1,
                                        -(int64)std::pow(2, 55), (int64)std::pow(2, 56) - 1, -(int64)std::pow(2, 63),
                                        (int64)std::pow(2, 63) - 1));

TEST_P(CompressSigned64Test, test01)
{
    VarintCompress compress;
    int64 originValue = GetParam();
    uint8 computeSize = compress.GetSigned64CompressedSize(originValue);
    char *compressedData = (char *)malloc(computeSize);
    uint8 compressedSize = compress.CompressSigned64(originValue, compressedData);
    EXPECT_EQ(computeSize, compressedSize);
    uint8 deCompressedSize;
    EXPECT_EQ(compress.DecompressSigned64(compressedData, deCompressedSize), originValue);
    EXPECT_EQ(computeSize, deCompressedSize);
    free(compressedData);
    printf("originValue: %lld\n", originValue);
    printf("computeSize: %d\n", computeSize);
}
