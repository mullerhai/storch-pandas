/*
 * storch -- Data frames for Java
 * Copyright (c) 2014, 2015 IBM Corp.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package torch.pandas.operate

import java.lang.Math.max
import java.lang.Math.min

import scala.collection.mutable.{LinkedHashMap, ListBuffer, *}
/**
 * A sparse bit set implementation inspired by Drs. Haddon and Lemire.
 *
 * https://github.com/brettwooldridge/SparseBitSet/blob/master/SparseBitSet.pdf
 * http://lemire.me/blog/archives/2012/11/13/fast-sets-of-Ints/
 */
object SparseBitSet {
  //
  // these are the tuning knobs
  //
  // after taking out the bits for indexing longs,
  // how to divide up the remaining bits among the levels
  // larger numbers means levels 2 and 3 are smaller
  // leaving more bits to be indexed in level 1
  private val INDEX_FACTOR = 4
  // how much bigger is the second level than the first
  // (the third is just whatever bits are leftover)
  private val INDEX_GROWTH = 1
  // l3 constants
  //                                        v-- number of bits to index a long value
  private val L3_SHIFT = 64 - 1 - java.lang.Long.numberOfLeadingZeros(64) //long.size
  //                                        v-- divide remaining bits by factor
  private val L3_BITS = (32 - L3_SHIFT) / INDEX_FACTOR //int.size
  private val L3_SIZE = 1 << L3_BITS
  private val L3_MASK = L3_SIZE - 1
  // l2 constants
  private val L2_SHIFT = L3_SHIFT + L3_BITS
  private val L2_BITS = L3_BITS + INDEX_GROWTH
  private val L2_SIZE = 1 << L2_BITS
  private val L2_MASK = L2_SIZE - 1
  // l1 constants
  // 32 bits - index size - 1 more prevent shifting the sign bit
  // into frame
  private val L1_SHIFT = L2_SHIFT + L2_BITS
  private val L1_BITS = 32 - L1_SHIFT - 1  //int.size
  private val L1_SIZE = 1 << L1_BITS
  // note the l1 mask is one more bit left than size would indicate
  // this prevents masking the sign bit, causing the appropriate
  // index out of bounds exception if a negative index is used
  private val L1_MASK = (L1_SIZE << 1) - 1
  // l4 mask
  private val L4_MASK = 64 - 1 //long.size

  def parameters: String = {
    val sb = new StringBuilder
    sb.append(String.format("%s parameters:\n", classOf[SparseBitSet].getName)).append(String.format("size:\tlevel 1=%d\tlevel 2=%d\tlevel 3=%d\n", L1_SIZE, L2_SIZE, L3_SIZE)).append(String.format("bits:\tlevel 1=%d\tlevel 2=%d\tlevel 3=%d\n", L1_BITS, L2_BITS, L3_BITS)).append(String.format("shift:\tlevel 1=%d\tlevel 2=%d\tlevel 3=%d\n", L1_SHIFT, L2_SHIFT, L3_SHIFT)).append(String.format("mask:\tlevel 1=%s\tlevel 2=%s\tlevel 3=%s\n", java.lang.Integer.toHexString(L1_MASK), java.lang.Integer.toHexString(L2_MASK), java.lang.Integer.toHexString(L3_MASK)))
    sb.toString
  }
}

class SparseBitSet {
  private var bits = new Array[Array[Array[Long]]](SparseBitSet.L3_SIZE / SparseBitSet.INDEX_FACTOR)
  private var cardinality = 0

  def get(index: Int): Boolean = {
    val l1i = (index >> SparseBitSet.L1_SHIFT) & SparseBitSet.L1_MASK
    val l2i = (index >> SparseBitSet.L2_SHIFT) & SparseBitSet.L2_MASK
    val l3i = (index >> SparseBitSet.L3_SHIFT) & SparseBitSet.L3_MASK
    val l4i = index & SparseBitSet.L4_MASK
    // index < 0 allowed through so appropriate index out of bounds exception is thrown
    if (index < 0 || l1i < bits.length && (bits(l1i) != null && bits(l1i)(l2i) != null)) return (bits(l1i)(l2i)(l3i) & (1L << l4i)) != 0L
    false
  }

  def set(index: Int, value: Boolean): Unit = {
    val l1i = (index >> SparseBitSet.L1_SHIFT) & SparseBitSet.L1_MASK
    val l2i = (index >> SparseBitSet.L2_SHIFT) & SparseBitSet.L2_MASK
    val l3i = (index >> SparseBitSet.L3_SHIFT) & SparseBitSet.L3_MASK
    val l4i = index & SparseBitSet.L4_MASK
    if (value) {
      if (bits.length <= l1i && l1i < SparseBitSet.L1_SIZE) {
        val size = min(SparseBitSet.L1_SIZE, max(bits.length << 1, 1 << (32 - java.lang.Integer.numberOfLeadingZeros(l1i)))) //int.size
        if (bits.length < size) bits = java.util.Arrays.copyOf(bits, size)
      }
      if (bits(l1i) == null) bits(l1i) = new Array[Array[Long]](SparseBitSet.L2_SIZE)
      if (bits(l1i)(l2i) == null) bits(l1i)(l2i) = new Array[Long](SparseBitSet.L3_SIZE)
      bits(l1i)(l2i)(l3i) |= (1L << l4i)
      cardinality += 1
    }
    else {
      // don't allocate blocks if clearing bits
      if (l1i < bits.length && bits(l1i) != null && bits(l1i)(l2i) != null) {
        bits(l1i)(l2i)(l3i) &= ~(1L << l4i)
        cardinality -= 1
      }
    }
  }

  def set(index: Int): Unit = {
    set(index, true)
  }

  def set(start: Int, end: Int): Unit = {
    for (i <- start until end) {
      set(i)
    }
  }

  def clear(index: Int): Unit = {
    set(index, false)
  }

  def clear(start: Int, end: Int): Unit = {
    for (i <- start until end) {
      clear(i)
    }
  }

  def flip(index: Int): Unit = {
    set(index, !get(index))
  }

  def flip(start: Int, end: Int): Unit = {
    for (i <- start until end) {
      flip(i)
    }
  }

  def clear(): Unit = {
//    java.util.Arrays.fill(bits, null)
    cardinality = 0
  }

  def getCardinality: Int = cardinality

  def nextSetBit(index: Int): Int = {
    var l1i = (index >> SparseBitSet.L1_SHIFT) & SparseBitSet.L1_MASK
    var l2i = (index >> SparseBitSet.L2_SHIFT) & SparseBitSet.L2_MASK
    var l3i = (index >> SparseBitSet.L3_SHIFT) & SparseBitSet.L3_MASK
    var l4i = index & SparseBitSet.L4_MASK
    while (l1i < bits.length) {
      while (bits(l1i) != null && l2i < bits(l1i).length) {
        while (bits(l1i)(l2i) != null && l3i < bits(l1i)(l2i).length) {
          l4i += java.lang.Long.numberOfTrailingZeros(bits(l1i)(l2i)(l3i) >> l4i)
          if ((bits(l1i)(l2i)(l3i) & (1L << l4i)) != 0L) return (l1i << SparseBitSet.L1_SHIFT) | (l2i << SparseBitSet.L2_SHIFT) | (l3i << SparseBitSet.L3_SHIFT) | l4i
          l3i += 1
          l4i = 0
        }
        l2i += 1
        l3i = 0
      }
      l1i += 1
      l2i = 0
    }
      -1
  }

  override def toString: String = {
    val sb = new StringBuilder
    sb.append("{")
    var i = nextSetBit(0)
    while (i >= 0) {
      if (sb.length > 1) sb.append(", ")
      sb.append(i)
      i = nextSetBit(i + 1)
    }
    sb.append("}")
    sb.toString
  }
}