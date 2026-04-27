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
 * Description:
 * This file exists for the benefit of external programs that may wish to
 * check openGauss page checksums.  They can #include this to get the code
 * referenced by dstore_checksum_impl.h.  (Note: you may need to redefine
 * Assert() as empty to compile this successfully externally.)
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * IDENTIFICATION
 *        src/gausskernel/dstore/include/common/algorithm/dstore_checksum_impl.h
 *
 * The algorithm used to checksum pages is chosen for very fast calculation.
 * Workloads where the database working set fits into OS file cache but not
 * into shared buffers can read in pages at a very fast pace and the checksum
 * algorithm itself can become the largest bottleneck.
 *
 * The checksum algorithm itself is based on the FNV-1a hash (FNV is shorthand
 * for Fowler/Noll/Vo).  The primitive of a plain FNV-1a hash folds in data 1
 * byte at a time according to the formula:
 *
 *	   hash = (hash ^ value) * FNV_PRIME
 *
 * FNV-1a algorithm is described at http://www.isthe.com/chongo/tech/comp/fnv/
 *
 * openGauss doesn't use FNV-1a hash directly because it has bad mixing of
 * high bits - high order bits in input data only affect high order bits in
 * output data. To resolve this we xor in the value prior to multiplication
 * shifted right by 17 bits. The number 17 was chosen because it doesn't
 * have common denominator with set bit positions in FNV_PRIME and empirically
 * provides the fastest mixing for high order bits of final iterations quickly
 * avalanche into lower positions. For performance reasons we choose to combine
 * 4 bytes at a time. The actual hash formula used as the basis is:
 *
 *	   hash = (hash ^ value) * FNV_PRIME ^ ((hash ^ value) >> 17)
 *
 * The main bottleneck in this calculation is the multiplication latency. To
 * hide the latency and to make use of SIMD parallelism multiple hash values
 * are calculated in parallel. The page is treated as a 32 column two
 * dimensional array of 32 bit values. Each column is aggregated separately
 * into a partial checksum. Each partial checksum uses a different initial
 * value (offset basis in FNV terminology). The initial values actually used
 * were chosen randomly, as the values themselves don't matter as much as that
 * they are different and don't match anything in real data. After initializing
 * partial checksums each value in the column is aggregated according to the
 * above formula. Finally two more iterations of the formula are performed with
 * value 0 to mix the bits of the last value added.
 *
 * The partial checksums are then folded together using xor to form a single
 * 32-bit checksum. The caller can safely reduce the value to 16 bits
 * using modulo 2^16-1. That will cause a very slight bias towards lower
 * values but this is not significant for the performance of the
 * checksum.
 *
 * The algorithm choice was based on what instructions are available in SIMD
 * instruction sets. This meant that a fast and good algorithm needed to use
 * multiplication as the main mixing operator. The simplest multiplication
 * based checksum primitive is the one used by FNV. The prime used is chosen
 * for good dispersion of values. It has no known simple patterns that result
 * in collisions. Test of 5-bit differentials of the primitive over 64bit keys
 * reveals no differentials with 3 or more values out of 100000 random keys
 * colliding. Avalanche test shows that only high order bits of the last word
 * have a bias. Tests of 1-4 uncorrelated bit errors, stray 0 and 0xFF bytes,
 * overwriting page from random position to end with 0 bytes, and overwriting
 * random segments of page with 0x00, 0xFF and random data all show optimal
 * 2e-16 false positive rate within margin of error.
 *
 * Vectorization of the algorithm requires 32bit x 32bit -> 32bit integer
 * multiplication instruction. As of 2013 the corresponding instruction is
 * available on x86 SSE4.1 extensions (pmulld) and ARM NEON (vmul.i32).
 * Vectorization requires a compiler to do the vectorization for us. For recent
 * GCC versions the flags -msse4.1 -funroll-loops -ftree-vectorize are enough
 * to achieve vectorization.
 *
 * The optimal amount of parallelism to use depends on CPU specific instruction
 * latency, SIMD instruction width, throughput and the amount of registers
 * available to hold intermediate state. Generally, more parallelism is better
 * up to the point that state doesn't fit in registers and extra load-store
 * instructions are needed to swap values in/out. The number chosen is a fixed
 * part of the algorithm because changing the parallelism changes the checksum
 * result.
 *
 * The parallelism number 32 was chosen based on the fact that it is the
 * largest state that fits into architecturally visible x86 SSE registers while
 * leaving some free registers for intermediate values. For future processors
 * with 256bit vector registers this will leave some performance on the table.
 * When vectorization is not available it might be beneficial to restructure
 * the computation to calculate a subset of the columns at a time and perform
 * multiple passes to avoid register spilling. This optimization opportunity
 * is not used. Current coding also assumes that the compiler has the ability
 * to unroll the inner loop to avoid loop overhead and minimize register
 * spilling. For less sophisticated compilers it might be beneficial to
 * manually unroll the inner loop.

 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_DSTORE_CHECKSUM_IMPL_H
#define DSTORE_DSTORE_CHECKSUM_IMPL_H

namespace DSTORE {
enum CHECKSUM_ALGORITHM : uint8 {
    CHECKSUM_FNV,
    CHECKSUM_CRC
};

/*
 * Block checksum algorithm. The length of the parameter buf must be an integer multiple of 4 bytes.
 */
uint32 CompChecksum(const void* buf, uint32 size, CHECKSUM_ALGORITHM alg);

}

#endif