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

import org.apache.spark._
import org.apache.spark.memory.StaticMemoryManager
import org.apache.spark.network.netty.NettyBlockTransferService
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.scheduler.LiveListenerBus
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.shuffle.hash.HashShuffleManager
import org.apache.spark.util._
import org.scalatest._

import scala.collection.mutable.ArrayBuffer
import scala.language.{implicitConversions, postfixOps}

/**
 * This suite covers [[org.apache.spark.storage.MemoryStore.unrollSafely]]
 * for CSD's cache black size policy
 */
class CsdUnrollSafetySuite extends SparkFunSuite with Matchers with BeforeAndAfterEach
  with PrivateMethodTester with ResetSystemProperties {

  private val conf = new SparkConf(false).set("spark.app.id", "test")
  var store: BlockManager = null
  var store2: BlockManager = null
  var store3: BlockManager = null
  var rpcEnv: RpcEnv = null
  var master: BlockManagerMaster = null
  conf.set("spark.authenticate", "false")
  val securityMgr = new SecurityManager(conf)
  val mapOutputTracker = new MapOutputTrackerMaster(conf)
  val shuffleManager = new HashShuffleManager(conf)

  // Reuse a serializer across tests to avoid creating a new thread-local buffer on each test
  conf.set("spark.kryoserializer.buffer", "1m")
  val serializer = new KryoSerializer(conf)

  // Implicitly convert strings to BlockIds for test clarity.
  implicit def StringToBlockId(value: String): BlockId = new TestBlockId(value)
  def rdd(rddId: Int, splitId: Int): RDDBlockId = RDDBlockId(rddId, splitId)

  private def makeBlockManager(
      maxMem: Long,
      name: String = SparkContext.DRIVER_IDENTIFIER): BlockManager = {
    val transfer = new NettyBlockTransferService(conf, securityMgr, numCores = 1)
    val memManager = new StaticMemoryManager(conf, Long.MaxValue, maxMem, numCores = 1)
    val blockManager = new BlockManager(name, rpcEnv, master, serializer, conf,
      memManager, mapOutputTracker, shuffleManager, transfer, securityMgr, 0)
    memManager.setMemoryStore(blockManager.memoryStore)
    blockManager.initialize("app-id")
    blockManager
  }

  override def beforeEach(): Unit = {
    rpcEnv = RpcEnv.create("test", "localhost", 0, conf, securityMgr)

    // Set the arch to 64-bit and compressedOops to true to get a deterministic test-case
    System.setProperty("os.arch", "amd64")
    conf.set("os.arch", "amd64")
    conf.set("spark.test.useCompressedOops", "true")
    conf.set("spark.driver.port", rpcEnv.address.port.toString)
    conf.set("spark.storage.unrollFraction", "0.4")
    conf.set("spark.storage.unrollMemoryThreshold", "512")
    conf.set("spark.storage.MemoryStore.csdCacheBlockSizeLimit", "8000")

    master = new BlockManagerMaster(rpcEnv.setupEndpoint("blockmanager",
      new BlockManagerMasterEndpoint(rpcEnv, true, conf, new LiveListenerBus)), conf, true)

    val initialize = PrivateMethod[Unit]('initialize)
    SizeEstimator invokePrivate initialize()
  }

  override def afterEach(): Unit = {
    if (store != null) {
      store.stop()
      store = null
    }
    if (store2 != null) {
      store2.stop()
      store2 = null
    }
    if (store3 != null) {
      store3.stop()
      store3 = null
    }
    rpcEnv.shutdown()
    rpcEnv.awaitTermination()
    rpcEnv = null
    master = null
  }

  /**
   * Verify the result of MemoryStore#unrollSafely is as expected.
   */
  private def verifyUnroll(
      expected: Iterator[Any],
      result: Either[Array[Any], Iterator[Any]],
      shouldBeArray: Boolean): Unit = {
    val actual: Iterator[Any] = result match {
      case Left(arr: Array[Any]) =>
        assert(shouldBeArray, "expected iterator from unroll!")
        arr.iterator
      case Right(it: Iterator[Any]) =>
        assert(!shouldBeArray, "expected array from unroll!")
        it
      case _ =>
        fail("unroll returned neither an iterator nor an array...")
    }
    expected.zip(actual).foreach { case (e, a) =>
      assert(e === a, "unroll did not return original values!")
    }
  }

  test("safely unroll blocks") {
    store = makeBlockManager(12000)
    val smallList = List.fill(40)(new Array[Byte](100))
    val bigList = List.fill(40)(new Array[Byte](200))
    val memoryStore = store.memoryStore
    val droppedBlocks = new ArrayBuffer[(BlockId, BlockStatus)]
    assert(memoryStore.currentUnrollMemoryForThisTask === 0)

    // Unroll with all the space in the world. This should succeed and return an array.
    var unrollResult = memoryStore.unrollSafely("unroll", smallList.iterator, droppedBlocks)
    verifyUnroll(smallList.iterator, unrollResult, shouldBeArray = true)
    assert(memoryStore.currentUnrollMemoryForThisTask === 0)
    memoryStore.releasePendingUnrollMemoryForThisTask()

    // Unroll huge block exceeding spark.storage.MemoryStore.csdCacheBlockSizeLimit
    // unrollSafely returns an iterator.
    store.putIterator("someBlock3", smallList.iterator, StorageLevel.MEMORY_ONLY)
    unrollResult = memoryStore.unrollSafely("unroll", bigList.iterator, droppedBlocks)
    verifyUnroll(bigList.iterator, unrollResult, shouldBeArray = false)
    assert(memoryStore.currentUnrollMemoryForThisTask > 0) // we returned an iterator
    assert(droppedBlocks.size === 0)
    droppedBlocks.clear()
  }

  test("safely unroll blocks through putIterator") {
    store = makeBlockManager(12000)
    val memOnly = StorageLevel.MEMORY_ONLY
    val memoryStore = store.memoryStore
    val smallList = List.fill(40)(new Array[Byte](100))
    val bigList = List.fill(40)(new Array[Byte](1000))
    def smallIterator: Iterator[Any] = smallList.iterator.asInstanceOf[Iterator[Any]]
    def bigIterator: Iterator[Any] = bigList.iterator.asInstanceOf[Iterator[Any]]
    assert(memoryStore.currentUnrollMemoryForThisTask === 0)

    // Unroll with plenty of space. This should succeed and cache both blocks.
    val result1 = memoryStore.putIterator("b1", smallIterator, memOnly, returnValues = true)
    val result2 = memoryStore.putIterator("b2", smallIterator, memOnly, returnValues = true)
    assert(memoryStore.contains("b1"))
    assert(memoryStore.contains("b2"))
    assert(result1.size > 0) // unroll was successful
    assert(result2.size > 0)
    assert(result1.data.isLeft) // unroll did not drop this block to disk
    assert(result2.data.isLeft)
    assert(memoryStore.currentUnrollMemoryForThisTask === 0)

    // Unroll huge block exceeding spark.storage.MemoryStore.csdCacheBlockSizeLimit
    // unrollSafely returns an iterator.
    val result4 = memoryStore.putIterator("b4", bigIterator, memOnly, returnValues = true)
    assert(result4.size === 0) // unroll was unsuccessful
    assert(result4.data.isLeft)
    assert(memoryStore.currentUnrollMemoryForThisTask > 0) // we returned an iterator
  }

  /**
   * This test is essentially identical to the preceding one, except that it uses MEMORY_AND_DISK.
   */
  test("safely unroll blocks through putIterator (disk)") {
    store = makeBlockManager(12000)
    val memAndDisk = StorageLevel.MEMORY_AND_DISK
    val memoryStore = store.memoryStore
    val diskStore = store.diskStore
    val smallList = List.fill(40)(new Array[Byte](100))
    val bigList = List.fill(40)(new Array[Byte](1000))
    def smallIterator: Iterator[Any] = smallList.iterator.asInstanceOf[Iterator[Any]]
    def bigIterator: Iterator[Any] = bigList.iterator.asInstanceOf[Iterator[Any]]
    assert(memoryStore.currentUnrollMemoryForThisTask === 0)

    store.putIterator("b1", smallIterator, memAndDisk)
    store.putIterator("b2", smallIterator, memAndDisk)

    // Unroll with not enough space. This should succeed but kick out b1 in the process.
    // Memory store should contain b2 and b3, while disk store should contain only b1
    val result3 = memoryStore.putIterator("b3", smallIterator, memAndDisk, returnValues = true)
    assert(result3.size > 0)
    assert(!memoryStore.contains("b1"))
    assert(memoryStore.contains("b2"))
    assert(memoryStore.contains("b3"))
    assert(diskStore.contains("b1"))
    assert(!diskStore.contains("b2"))
    assert(!diskStore.contains("b3"))
    memoryStore.remove("b3")
    store.putIterator("b3", smallIterator, StorageLevel.MEMORY_ONLY)
    assert(memoryStore.currentUnrollMemoryForThisTask === 0)


    // Unroll huge block exceeding spark.storage.MemoryStore.csdCacheBlockSizeLimit.
    // This should fail and drop the new block to disk
    // directly in addition to kicking out b2 in the process. Memory store should contain only
    // b3, while disk store should contain b1, b2 and b4.
    val result4 = memoryStore.putIterator("b4", bigIterator, memAndDisk, returnValues = true)
    assert(result4.size > 0)
    assert(result4.data.isRight) // unroll returned bytes from disk
    assert(!memoryStore.contains("b1"))
    assert(!memoryStore.contains("b2"))
    assert(memoryStore.contains("b3"))
    assert(!memoryStore.contains("b4"))
    assert(diskStore.contains("b1"))
    assert(diskStore.contains("b2"))
    assert(!diskStore.contains("b3"))
    assert(diskStore.contains("b4"))
    assert(memoryStore.currentUnrollMemoryForThisTask > 0) // we returned an iterator
  }

  test("multiple unrolls by the same thread") {
    store = makeBlockManager(12000)
    val memOnly = StorageLevel.MEMORY_ONLY
    val memoryStore = store.memoryStore
    val smallList = List.fill(40)(new Array[Byte](100))
    val bigList = List.fill(40)(new Array[Byte](1000))
    def smallIterator: Iterator[Any] = smallList.iterator.asInstanceOf[Iterator[Any]]
    def bigIterator: Iterator[Any] = bigList.iterator.asInstanceOf[Iterator[Any]]
    assert(memoryStore.currentUnrollMemoryForThisTask === 0)

    // All unroll memory used is released because unrollSafely returned an array
    memoryStore.putIterator("b1", smallIterator, memOnly, returnValues = true)
    assert(memoryStore.currentUnrollMemoryForThisTask === 0)
    memoryStore.putIterator("b2", smallIterator, memOnly, returnValues = true)
    assert(memoryStore.currentUnrollMemoryForThisTask === 0)

    // Unroll memory is not released because unrollSafely returned an iterator
    // that still depends on the underlying vector used in the process
    memoryStore.putIterator("b3", smallIterator, memOnly, returnValues = true)
    val unrollMemoryAfterB3 = memoryStore.currentUnrollMemoryForThisTask
    assert(unrollMemoryAfterB3 > 0)

    memoryStore.putIterator("b3.1", bigIterator, memOnly, returnValues = true)
    val unrollMemoryAfterB31 = memoryStore.currentUnrollMemoryForThisTask
    assert(unrollMemoryAfterB31 > unrollMemoryAfterB3)

    // The unroll memory owned by this thread builds on top of its value after the previous unrolls
    memoryStore.putIterator("b4", smallIterator, memOnly, returnValues = true)
    val unrollMemoryAfterB4 = memoryStore.currentUnrollMemoryForThisTask
    assert(unrollMemoryAfterB4 > unrollMemoryAfterB31)

    // ... but only to a certain extent (until we run out of free space to grant new unroll memory)
    memoryStore.putIterator("b5", smallIterator, memOnly, returnValues = true)
    val unrollMemoryAfterB5 = memoryStore.currentUnrollMemoryForThisTask
    memoryStore.putIterator("b6", smallIterator, memOnly, returnValues = true)
    val unrollMemoryAfterB6 = memoryStore.currentUnrollMemoryForThisTask
    memoryStore.putIterator("b7", smallIterator, memOnly, returnValues = true)
    val unrollMemoryAfterB7 = memoryStore.currentUnrollMemoryForThisTask

    memoryStore.putIterator("b8", bigIterator, memOnly, returnValues = true)
    val unrollMemoryAfterB8 = memoryStore.currentUnrollMemoryForThisTask

    assert(unrollMemoryAfterB5 === unrollMemoryAfterB4)
    assert(unrollMemoryAfterB6 === unrollMemoryAfterB4)
    assert(unrollMemoryAfterB7 === unrollMemoryAfterB4)
    assert(unrollMemoryAfterB8 === unrollMemoryAfterB4)
  }

}
