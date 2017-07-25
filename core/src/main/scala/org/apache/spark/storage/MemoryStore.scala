/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.storage

import java.nio.ByteBuffer
import java.util.LinkedHashMap

import org.apache.spark.TaskContext
import org.apache.spark.memory.MemoryManager
import org.apache.spark.util.collection.SizeTrackingVector
import org.apache.spark.util.{SizeEstimator, Utils}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

private case class MemoryEntry(value: Any, size: Long, deserialized: Boolean)

/**
 * Stores blocks in memory, either as Arrays of deserialized Java objects or as
 * serialized ByteBuffers.
 */
private[spark] class MemoryStore(blockManager: BlockManager, memoryManager: MemoryManager)
  extends BlockStore(blockManager) {

  // Note: all changes to memory allocations, notably putting blocks, evicting blocks, and
  // acquiring or releasing unroll memory, must be synchronized on `memoryManager`!

  private val conf = blockManager.conf
  private val entries = new LinkedHashMap[BlockId, MemoryEntry](32, 0.75f, true)

  // A mapping from taskAttemptId to amount of memory used for unrolling a block (in bytes)
  // All accesses of this map are assumed to have manually synchronized on `memoryManager`
  private val unrollMemoryMap = mutable.HashMap[Long, Long]()
  // Same as `unrollMemoryMap`, but for pending unroll memory as defined below.
  // Pending unroll memory refers to the intermediate memory occupied by a task
  // after the unroll but before the actual putting of the block in the cache.
  // This chunk of memory is expected to be released *as soon as* we finish
  // caching the corresponding block as opposed to until after the task finishes.
  // This is only used if a block is successfully unrolled in its entirety in
  // memory (SPARK-4777).
  private val pendingUnrollMemoryMap = mutable.HashMap[Long, Long]()

  // Initial memory to request before unrolling any block
  private val unrollMemoryThreshold: Long =
    conf.getLong("spark.storage.unrollMemoryThreshold", 1024 * 1024)

  // csd flag controlling whether to apply Csd's caching block size policy
  private val csdCacheBlockSizeLimit: Long =
    conf.getLong("spark.storage.MemoryStore.csdCacheBlockSizeLimit", Integer.MAX_VALUE.toLong)
  assert(csdCacheBlockSizeLimit <= Integer.MAX_VALUE)

  /** Total amount of memory available for storage, in bytes. */
  private def maxMemory: Long = memoryManager.maxStorageMemory

  if (maxMemory < unrollMemoryThreshold) {
    logWarning(s"Max memory ${Utils.bytesToString(maxMemory)} is less than the initial memory " +
      s"threshold ${Utils.bytesToString(unrollMemoryThreshold)} needed to store a block in " +
      s"memory. Please configure Spark with more memory.")
  }

  logInfo("MemoryStore started with capacity %s".format(Utils.bytesToString(maxMemory)))

  /** Total storage memory used including unroll memory, in bytes. */
  private def memoryUsed: Long = memoryManager.storageMemoryUsed

  /**
   * Amount of storage memory, in bytes, used for caching blocks.
   * This does not include memory used for unrolling.
   */
  private def blocksMemoryUsed: Long = memoryManager.synchronized {
    memoryUsed - currentUnrollMemory
  }

  override def getSize(blockId: BlockId): Long = {
    entries.synchronized {
      entries.get(blockId).size
    }
  }

  override def putBytes(blockId: BlockId, _bytes: ByteBuffer, level: StorageLevel): PutResult = {
    // Work on a duplicate - since the original input might be used elsewhere.
    val bytes = _bytes.duplicate()
    bytes.rewind()
    if (level.deserialized) {
      val values = blockManager.dataDeserialize(blockId, bytes)
      putIterator(blockId, values, level, returnValues = true)
    } else {
      val droppedBlocks = new ArrayBuffer[(BlockId, BlockStatus)]
      tryToPut(blockId, bytes, bytes.limit, deserialized = false, droppedBlocks)
      PutResult(bytes.limit(), Right(bytes.duplicate()), droppedBlocks)
    }
  }

  /**
   * Use `size` to test if there is enough space in MemoryStore. If so, create the ByteBuffer and
   * put it into MemoryStore. Otherwise, the ByteBuffer won't be created.
   *
   * The caller should guarantee that `size` is correct.
   */
  def putBytes(blockId: BlockId, size: Long, _bytes: () => ByteBuffer): PutResult = {
    // Work on a duplicate - since the original input might be used elsewhere.
    lazy val bytes = _bytes().duplicate().rewind().asInstanceOf[ByteBuffer]
    val droppedBlocks = new ArrayBuffer[(BlockId, BlockStatus)]
    val putSuccess = tryToPut(blockId, () => bytes, size, deserialized = false, droppedBlocks)
    val data =
      if (putSuccess) {
        assert(bytes.limit == size)
        Right(bytes.duplicate())
      } else {
        null
      }
    PutResult(size, data, droppedBlocks)
  }

  override def putArray(
      blockId: BlockId,
      values: Array[Any],
      level: StorageLevel,
      returnValues: Boolean): PutResult = {
    val droppedBlocks = new ArrayBuffer[(BlockId, BlockStatus)]
    if (level.deserialized) {
      val sizeEstimate = SizeEstimator.estimate(values.asInstanceOf[AnyRef])
      tryToPut(blockId, values, sizeEstimate, deserialized = true, droppedBlocks)
      PutResult(sizeEstimate, Left(values.iterator), droppedBlocks)
    } else {
      val bytes = blockManager.dataSerialize(blockId, values.iterator)
      tryToPut(blockId, bytes, bytes.limit, deserialized = false, droppedBlocks)
      PutResult(bytes.limit(), Right(bytes.duplicate()), droppedBlocks)
    }
  }

  override def putIterator(
      blockId: BlockId,
      values: Iterator[Any],
      level: StorageLevel,
      returnValues: Boolean): PutResult = {
    putIterator(blockId, values, level, returnValues, allowPersistToDisk = true)
  }

  /**
   * Attempt to put the given block in memory store.
   *
   * There may not be enough space to fully unroll the iterator in memory, in which case we
   * optionally drop the values to disk if
   *   (1) the block's storage level specifies useDisk, and
   *   (2) `allowPersistToDisk` is true.
   *
   * One scenario in which `allowPersistToDisk` is false is when the BlockManager reads a block
   * back from disk and attempts to cache it in memory. In this case, we should not persist the
   * block back on disk again, as it is already in disk store.
   */
  private[storage] def putIterator(
      blockId: BlockId,
      values: Iterator[Any],
      level: StorageLevel,
      returnValues: Boolean,
      allowPersistToDisk: Boolean): PutResult = {
    val droppedBlocks = new ArrayBuffer[(BlockId, BlockStatus)]
    val unrolledValues = unrollSafely(blockId, values, droppedBlocks)
    unrolledValues match {
      case Left(arrayValues) =>
        // Values are fully unrolled in memory, so store them as an array
        val res = putArray(blockId, arrayValues, level, returnValues)
        droppedBlocks ++= res.droppedBlocks
        PutResult(res.size, res.data, droppedBlocks)
      case Right((iteratorValues, false)) =>
        // big block detected when unrolling
        PutResult(0, Left(iteratorValues), droppedBlocks)
      case Right((iteratorValues, true)) =>
        // Not enough space to unroll this block; drop to disk if applicable
        if (level.useDisk && allowPersistToDisk) {
          logWarning(s"Persisting block $blockId to disk instead.")
          val res =
            blockManager.diskStore.putIterator(blockId, iteratorValues, level, returnValues)
          PutResult(res.size, res.data, droppedBlocks)
        } else {
          PutResult(0, Left(iteratorValues), droppedBlocks)
        }
    }
  }

  override def getBytes(blockId: BlockId): Option[ByteBuffer] = {
    val entry = entries.synchronized {
      entries.get(blockId)
    }
    if (entry == null) {
      None
    } else if (entry.deserialized) {
      Some(blockManager.dataSerialize(blockId, entry.value.asInstanceOf[Array[Any]].iterator))
    } else {
      Some(entry.value.asInstanceOf[ByteBuffer].duplicate()) // Doesn't actually copy the data
    }
  }

  override def getValues(blockId: BlockId): Option[Iterator[Any]] = {
    val entry = entries.synchronized {
      entries.get(blockId)
    }
    if (entry == null) {
      None
    } else if (entry.deserialized) {
      Some(entry.value.asInstanceOf[Array[Any]].iterator)
    } else {
      val buffer = entry.value.asInstanceOf[ByteBuffer].duplicate() // Doesn't actually copy data
      Some(blockManager.dataDeserialize(blockId, buffer))
    }
  }

  override def remove(blockId: BlockId): Boolean = memoryManager.synchronized {
    val entry = entries.synchronized { entries.remove(blockId) }
    if (entry != null) {
      memoryManager.releaseStorageMemory(entry.size)
      logDebug(s"Block $blockId of size ${entry.size} dropped " +
        s"from memory (free ${maxMemory - blocksMemoryUsed})")
      true
    } else {
      false
    }
  }

  override def clear(): Unit = memoryManager.synchronized {
    entries.synchronized {
      entries.clear()
    }
    unrollMemoryMap.clear()
    pendingUnrollMemoryMap.clear()
    memoryManager.releaseAllStorageMemory()
    logInfo("MemoryStore cleared")
  }

  /**
   * This api is used by CSD as a post process of [[unrollSafely]] to detect
   * large size partitions under shortage of unroll memory.
   * This api continues fetching into user memory space until we "see" total object size
   * exceeds csdCacheBlockSizeLimit or when inputValues exhausted.
   * In practice, we need to make sure we have at least csdCacheBlockSizeLimit
   * amount of memory space available from user space per thread, for worst case.
   * We can also tune csdCacheBlockSizeLimit down according to our need.
   */
  private[this] def fetchUntilCsdBlockSizeLimit[T](
      blockId: BlockId,
      inputValues: Iterator[T],
      valuesSeen: SizeTrackingVector[Any]): Boolean = {
    // if switch is off, do nothing
    if (csdCacheBlockSizeLimit <= 0) {
      true
    } else {
      val start = System.currentTimeMillis
      var currentEstimatedSize = valuesSeen.estimateSize()
      try {
        var elementsExamined = 0L
        val memoryCheckPeriod = 16
        while (inputValues.hasNext && currentEstimatedSize <= csdCacheBlockSizeLimit) {
          valuesSeen += inputValues.next()
          elementsExamined += 1
          if (elementsExamined % memoryCheckPeriod == 0) {
            currentEstimatedSize = valuesSeen.estimateSize()
          }
        }
        (currentEstimatedSize <= csdCacheBlockSizeLimit)
      } finally {
        logWarning(s"fetchUntilCsdBlockSizeLimit($blockId) duration: " +
          s"${Utils.msDurationToString(System.currentTimeMillis - start)}")
      }
    }
  }

  /**
   * Unroll the given block in memory safely.
   *
   * The safety of this operation refers to avoiding potential OOM exceptions caused by
   * unrolling the entirety of the block in memory at once. This is achieved by periodically
   * checking whether the memory restrictions for unrolling blocks are still satisfied,
   * stopping immediately if not. This check is a safeguard against the scenario in which
   * there is not enough free memory to accommodate the entirety of a single block.
   *
   * This method returns either an array with the contents of the entire block or an iterator
   * containing the values of the block (if the array would have exceeded available memory).
   *
   * SPY-1394: CSD modified this API in the following way:
   * 1. It returns a tuple (iterator, boolean), when short of memory.
   *    The boolean is an indicator on whether caller should cache to disk, based
   *    on detection of over-sized block.
   * 2. When over-sized block is detected, terminate the unroll and tell the caller to not
   *    cache at all.
   */
  def unrollSafely(
      blockId: BlockId,
      values: Iterator[Any],
      droppedBlocks: ArrayBuffer[(BlockId, BlockStatus)])
    : Either[Array[Any], (Iterator[Any], Boolean)] = {

    // Number of elements unrolled so far
    var elementsUnrolled = 0
    // Whether there is still enough memory for us to continue unrolling this block
    var keepUnrolling = true
    // Initial per-task memory to request for unrolling blocks (bytes). Exposed for testing.
    val initialMemoryThreshold = unrollMemoryThreshold
    // How often to check whether we need to request more memory
    val memoryCheckPeriod = 16
    // Memory currently reserved by this task for this particular unrolling operation
    var memoryThreshold = initialMemoryThreshold
    // Memory to request as a multiple of current vector size
    val memoryGrowthFactor = 1.5
    // Keep track of pending unroll memory reserved by this method.
    var pendingMemoryReserved = 0L
    // Underlying vector for unrolling the block
    var vector = new SizeTrackingVector[Any]

    // Request enough memory to begin unrolling
    keepUnrolling = reserveUnrollMemoryForThisTask(blockId, initialMemoryThreshold, droppedBlocks)

    if (!keepUnrolling) {
      logWarning(s"Failed to reserve initial memory threshold of " +
        s"${Utils.bytesToString(initialMemoryThreshold)} for computing block $blockId in memory.")
    } else {
      pendingMemoryReserved += initialMemoryThreshold
    }

    // Unroll this block safely, checking whether we have exceeded our threshold periodically
    try {
      var currentSize = 0L
      var shouldCache = true
      while (values.hasNext && keepUnrolling && (csdCacheBlockSizeLimit <= 0 || shouldCache)) {
        vector += values.next()
        if (elementsUnrolled % memoryCheckPeriod == 0) {
          // If our vector's size has exceeded the threshold, request more memory
          currentSize = vector.estimateSize()

          if (csdCacheBlockSizeLimit > 0 && shouldCache && currentSize > csdCacheBlockSizeLimit) {
            shouldCache = false
          }

          if (currentSize >= memoryThreshold) {
            val amountToRequest = (currentSize * memoryGrowthFactor - memoryThreshold).toLong
            keepUnrolling = reserveUnrollMemoryForThisTask(
              blockId, amountToRequest, droppedBlocks)
            if (keepUnrolling) {
              pendingMemoryReserved += amountToRequest
            }
            // New threshold is currentSize * memoryGrowthFactor
            memoryThreshold += amountToRequest
          }
        }
        elementsUnrolled += 1
      }

      if (keepUnrolling && shouldCache) {
        // We successfully unrolled the entirety of this block
        Left(vector.toArray)
      } else {
        if (!shouldCache) {
          logBlockSizeLimitMessage(blockId, currentSize)
          Right(vector.iterator ++ values, shouldCache)
        } else {
          // When we ran out of unroll memory from spark's storage space,
          // a true value of shouldCache be false positive because we have
          // not seen enough of the values.
          // Try continue the fetching using memory from user space (assuming that
          // enough memory is available). see [[fetchUntilCsdBlockSizeLimit]]
          shouldCache = fetchUntilCsdBlockSizeLimit(blockId, values, vector)
          if (!shouldCache) {
            logBlockSizeLimitMessage(blockId, vector.estimateSize())
          } else {
            // spark's original logging message indicating insufficient Unroll memory
            // and caller will consider drop it to disk if applicable.
            logUnrollFailureMessage(blockId, vector.estimateSize())
          }
          Right(vector.iterator ++ values, shouldCache)
        }
      }

    } finally {
      // If we return an array, the values returned here will be cached in `tryToPut` later.
      // In this case, we should release the memory only after we cache the block there.
      if (keepUnrolling) {
        val taskAttemptId = currentTaskAttemptId()
        memoryManager.synchronized {
          // Since we continue to hold onto the array until we actually cache it, we cannot
          // release the unroll memory yet. Instead, we transfer it to pending unroll memory
          // so `tryToPut` can further transfer it to normal storage memory later.
          // TODO: we can probably express this without pending unroll memory (SPARK-10907)
          unrollMemoryMap(taskAttemptId) -= pendingMemoryReserved
          pendingUnrollMemoryMap(taskAttemptId) =
            pendingUnrollMemoryMap.getOrElse(taskAttemptId, 0L) + pendingMemoryReserved
        }
      } else {
        // Otherwise, if we return an iterator, we can only release the unroll memory when
        // the task finishes since we don't know when the iterator will be consumed.
      }
    }
  }

  /**
   * Return the RDD ID that a given block ID is from, or None if it is not an RDD block.
   */
  private def getRddId(blockId: BlockId): Option[Int] = {
    blockId.asRDDId.map(_.rddId)
  }

  private def tryToPut(
      blockId: BlockId,
      value: Any,
      size: Long,
      deserialized: Boolean,
      droppedBlocks: mutable.Buffer[(BlockId, BlockStatus)]): Boolean = {
    tryToPut(blockId, () => value, size, deserialized, droppedBlocks)
  }

  /**
   * Try to put in a set of values, if we can free up enough space. The value should either be
   * an Array if deserialized is true or a ByteBuffer otherwise. Its (possibly estimated) size
   * must also be passed by the caller.
   *
   * `value` will be lazily created. If it cannot be put into MemoryStore or disk, `value` won't be
   * created to avoid OOM since it may be a big ByteBuffer.
   *
   * Synchronize on `memoryManager` to ensure that all the put requests and its associated block
   * dropping is done by only on thread at a time. Otherwise while one thread is dropping
   * blocks to free memory for one block, another thread may use up the freed space for
   * another block.
   *
   * All blocks evicted in the process, if any, will be added to `droppedBlocks`.
   *
   * @return whether put was successful.
   */
  private def tryToPut(
      blockId: BlockId,
      value: () => Any,
      size: Long,
      deserialized: Boolean,
      droppedBlocks: mutable.Buffer[(BlockId, BlockStatus)]): Boolean = {

    /* TODO: Its possible to optimize the locking by locking entries only when selecting blocks
     * to be dropped. Once the to-be-dropped blocks have been selected, and lock on entries has
     * been released, it must be ensured that those to-be-dropped blocks are not double counted
     * for freeing up more space for another block that needs to be put. Only then the actually
     * dropping of blocks (and writing to disk if necessary) can proceed in parallel. */

    memoryManager.synchronized {
      // Note: if we have previously unrolled this block successfully, then pending unroll
      // memory should be non-zero. This is the amount that we already reserved during the
      // unrolling process. In this case, we can just reuse this space to cache our block.
      // The synchronization on `memoryManager` here guarantees that the release and acquire
      // happen atomically. This relies on the assumption that all memory acquisitions are
      // synchronized on the same lock.
      releasePendingUnrollMemoryForThisTask()
      val enoughMemory = memoryManager.acquireStorageMemory(blockId, size, droppedBlocks)
      if (enoughMemory) {
        // We acquired enough memory for the block, so go ahead and put it
        val entry = new MemoryEntry(value(), size, deserialized)
        entries.synchronized {
          entries.put(blockId, entry)
        }
        val valuesOrBytes = if (deserialized) "values" else "bytes"
        logInfo("Block %s stored as %s in memory (estimated size %s, free %s)".format(
          blockId, valuesOrBytes, Utils.bytesToString(size),
          Utils.bytesToString(maxMemory - blocksMemoryUsed)))
      } else {
        // Tell the block manager that we couldn't put it in memory so that it can drop it to
        // disk if the block allows disk storage.
        lazy val data = if (deserialized) {
          Left(value().asInstanceOf[Array[Any]])
        } else {
          Right(value().asInstanceOf[ByteBuffer].duplicate())
        }
        val droppedBlockStatus = blockManager.dropFromMemory(blockId, () => data)
        droppedBlockStatus.foreach { status => droppedBlocks += ((blockId, status)) }
      }
      enoughMemory
    }
  }

  /**
    * Try to evict blocks to free up a given amount of space to store a particular block.
    * Can fail if either the block is bigger than our memory or it would require replacing
    * another block from the same RDD (which leads to a wasteful cyclic replacement pattern for
    * RDDs that don't fit into memory that we want to avoid).
    *
    * @param blockId the ID of the block we are freeing space for, if any
    * @param space the size of this block
    * @param droppedBlocks a holder for blocks evicted in the process
    * @return whether the requested free space is freed.
    */
  private[spark] def evictBlocksToFreeSpace(
      blockId: Option[BlockId],
      space: Long,
      droppedBlocks: mutable.Buffer[(BlockId, BlockStatus)]): Boolean = {
    assert(space > 0)
    memoryManager.synchronized {
      var freedMemory = 0L
      val rddToAdd = blockId.flatMap(getRddId)
      val selectedBlocks = new ArrayBuffer[BlockId]
      // This is synchronized to ensure that the set of entries is not changed
      // (because of getValue or getBytes) while traversing the iterator, as that
      // can lead to exceptions.
      entries.synchronized {
        val iterator = entries.entrySet().iterator()
        while (freedMemory < space && iterator.hasNext) {
          val pair = iterator.next()
          val blockId = pair.getKey
          if (rddToAdd.isEmpty || rddToAdd != getRddId(blockId)) {
            selectedBlocks += blockId
            freedMemory += pair.getValue.size
          }
        }
      }

      if (freedMemory >= space) {
        logInfo(s"${selectedBlocks.size} blocks selected for dropping")
        for (blockId <- selectedBlocks) {
          val entry = entries.synchronized { entries.get(blockId) }
          // This should never be null as only one task should be dropping
          // blocks and removing entries. However the check is still here for
          // future safety.
          if (entry != null) {
            val data = if (entry.deserialized) {
              Left(entry.value.asInstanceOf[Array[Any]])
            } else {
              Right(entry.value.asInstanceOf[ByteBuffer].duplicate())
            }
            val droppedBlockStatus = blockManager.dropFromMemory(blockId, data)
            droppedBlockStatus.foreach { status => droppedBlocks += ((blockId, status)) }
          }
        }
        true
      } else {
        blockId.foreach { id =>
          logInfo(s"Will not store $id as it would require dropping another block " +
            "from the same RDD")
        }
        false
      }
    }
  }

  override def contains(blockId: BlockId): Boolean = {
    entries.synchronized { entries.containsKey(blockId) }
  }

  private def currentTaskAttemptId(): Long = {
    // In case this is called on the driver, return an invalid task attempt id.
    Option(TaskContext.get()).map(_.taskAttemptId()).getOrElse(-1L)
  }

  /**
   * Reserve memory for unrolling the given block for this task.
   * @return whether the request is granted.
   */
  def reserveUnrollMemoryForThisTask(
      blockId: BlockId,
      memory: Long,
      droppedBlocks: mutable.Buffer[(BlockId, BlockStatus)]): Boolean = {
    memoryManager.synchronized {
      val success = memoryManager.acquireUnrollMemory(blockId, memory, droppedBlocks)
      if (success) {
        val taskAttemptId = currentTaskAttemptId()
        unrollMemoryMap(taskAttemptId) = unrollMemoryMap.getOrElse(taskAttemptId, 0L) + memory
      }
      success
    }
  }

  /**
   * Release memory used by this task for unrolling blocks.
   * If the amount is not specified, remove the current task's allocation altogether.
   */
  def releaseUnrollMemoryForThisTask(memory: Long = Long.MaxValue): Unit = {
    val taskAttemptId = currentTaskAttemptId()
    memoryManager.synchronized {
      if (unrollMemoryMap.contains(taskAttemptId)) {
        val memoryToRelease = math.min(memory, unrollMemoryMap(taskAttemptId))
        if (memoryToRelease > 0) {
          unrollMemoryMap(taskAttemptId) -= memoryToRelease
          memoryManager.releaseUnrollMemory(memoryToRelease)
        }
        if (unrollMemoryMap(taskAttemptId) == 0) {
          unrollMemoryMap.remove(taskAttemptId)
        }
      }
    }
  }

  /**
   * Release pending unroll memory of current unroll successful block used by this task
   */
  def releasePendingUnrollMemoryForThisTask(memory: Long = Long.MaxValue): Unit = {
    val taskAttemptId = currentTaskAttemptId()
    memoryManager.synchronized {
      if (pendingUnrollMemoryMap.contains(taskAttemptId)) {
        val memoryToRelease = math.min(memory, pendingUnrollMemoryMap(taskAttemptId))
        if (memoryToRelease > 0) {
          pendingUnrollMemoryMap(taskAttemptId) -= memoryToRelease
          memoryManager.releaseUnrollMemory(memoryToRelease)
        }
        if (pendingUnrollMemoryMap(taskAttemptId) == 0) {
          pendingUnrollMemoryMap.remove(taskAttemptId)
        }
      }
    }
  }

  /**
   * Return the amount of memory currently occupied for unrolling blocks across all tasks.
   */
  def currentUnrollMemory: Long = memoryManager.synchronized {
    unrollMemoryMap.values.sum + pendingUnrollMemoryMap.values.sum
  }

  /**
   * Return the amount of memory currently occupied for unrolling blocks by this task.
   */
  def currentUnrollMemoryForThisTask: Long = memoryManager.synchronized {
    unrollMemoryMap.getOrElse(currentTaskAttemptId(), 0L)
  }

  /**
   * Return the number of tasks currently unrolling blocks.
   */
  private def numTasksUnrolling: Int = memoryManager.synchronized { unrollMemoryMap.keys.size }

  /**
   * Log information about current memory usage.
   */
  private def logMemoryUsage(): Unit = {
    logInfo(
      s"Memory use = ${Utils.bytesToString(blocksMemoryUsed)} (blocks) + " +
      s"${Utils.bytesToString(currentUnrollMemory)} (scratch space shared across " +
      s"$numTasksUnrolling tasks(s)) = ${Utils.bytesToString(memoryUsed)}. " +
      s"Storage limit = ${Utils.bytesToString(maxMemory)}."
    )
  }

  /**
   * Log a warning for failing to unroll a block.
   *
   * @param blockId ID of the block we are trying to unroll.
   * @param finalVectorSize Final size of the vector before unrolling failed.
   */
  private def logUnrollFailureMessage(blockId: BlockId, finalVectorSize: Long): Unit = {
    logWarning(
      s"Not enough space to cache $blockId in memory! " +
      s"(computed ${Utils.bytesToString(finalVectorSize)} so far)"
    )
    logMemoryUsage()
  }

  /**
   * Log a warning for when a over size block is detected.
   *
   * @param blockId ID of the block we are trying to unroll.
   * @param finalVectorSize Final size of the vector when the block size limit is reached.
   */
  private def logBlockSizeLimitMessage(blockId: BlockId, finalVectorSize: Long): Unit = {
    logWarning(
      s"Block size limit reached: $blockId! " +
        s"(computed ${Utils.bytesToString(finalVectorSize)} so far)"
    )
    logMemoryUsage()
  }
}
