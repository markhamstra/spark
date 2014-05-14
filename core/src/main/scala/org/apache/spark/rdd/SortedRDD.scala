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

package org.apache.spark.rdd

import java.util.{HashMap => JHashMap}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.spark.Dependency
import org.apache.spark.OneToOneDependency
import org.apache.spark.Partition
import org.apache.spark.Partitioner
import org.apache.spark.ShuffleDependency
import org.apache.spark.SparkEnv
import org.apache.spark.TaskContext
import org.apache.spark.serializer.Serializer
import org.apache.spark.util.Utils

/**
 * An RDD[(K, V)] sorted by key
 */
private[spark] class SortedRDD[K: Ordering: ClassTag, V: ClassTag, P <: Product2[K, V]: ClassTag](
    @transient var prev: RDD[_ <: Product2[K, V]],
    part: Partitioner)
  extends RDD[(K, V)](prev.context, Nil) {

  private var serializer: Serializer = _
  private var countRDD: RDD[Long] = prev.mapPartitions(itr => Iterator(Utils.getIteratorSize(itr)))
    .coalesce(1)
    .mapPartitions(itr => Iterator(itr.sum))
  private var rangeBoundsRDD: RDD[K] = _

  def setSerializer(serializer: Serializer): SortedRDD[K, V, P] = {
    this.serializer = serializer
    this
  }

  override def getDependencies: Seq[Dependency[_]] = {
    Seq(new ShuffleDependency(prev, part, serializer)) // TODO: not part, but rather rangePartitioner
/*    Seq(rdd1, rdd2).map { rdd =>
      if (rdd.partitioner == Some(part)) {
        logDebug("Adding one-to-one dependency with " + rdd)
        new OneToOneDependency(rdd)
      } else {
        logDebug("Adding shuffle dependency with " + rdd)
        new ShuffleDependency(rdd, part, serializer)
      }
    }
*/
    // ++ OneToOneDependency on count RDD
    // ++ OneToOneDependency on rangeBounds RDD
  }

  override def getPartitions: Array[Partition] = {
    val array = new Array[Partition](part.numPartitions) // can use part.numPartitions or, equivalently, RangePartitioner#numPartitions
    for (i <- 0 until array.size) {
      // Each CoGroupPartition will depend on count RDD and rangeBounds RDD
      array(i) = new CoGroupPartition(i, Seq(rdd1, rdd2).zipWithIndex.map { case (rdd, j) =>
        dependencies(j) match {
          case s: ShuffleDependency[_, _] =>
            new ShuffleCoGroupSplitDep(s.shuffleId)
          case _ =>
            new NarrowCoGroupSplitDep(rdd, i, rdd.partitions(i))
        }
      }.toArray)
    }
    array
  }

  override val partitioner = Some(part) // TODO: create RangePartitioner instead of part

  override def compute(p: Partition, context: TaskContext): Iterator[(K, V)] = {
    val partition = p.asInstanceOf[CoGroupPartition]
    val ser = Serializer.getSerializer(serializer)
    val map = new JHashMap[K, ArrayBuffer[V]]
    def getSeq(k: K): ArrayBuffer[V] = {
      val seq = map.get(k)
      if (seq != null) {
        seq
      } else {
        val seq = new ArrayBuffer[V]()
        map.put(k, seq)
        seq
      }
    }
    def integrate(dep: CoGroupSplitDep, op: Product2[K, V] => Unit) = dep match {
      case NarrowCoGroupSplitDep(rdd, _, itsSplit) =>
        rdd.iterator(itsSplit, context).asInstanceOf[Iterator[Product2[K, V]]].foreach(op)

      case ShuffleCoGroupSplitDep(shuffleId) =>
        val iter = SparkEnv.get.shuffleFetcher.fetch[Product2[K, V]](shuffleId, partition.index,
          context, ser)
        iter.foreach(op)
    }
    // the first dep is rdd1; add all values to the map
    integrate(partition.deps(0), t => getSeq(t._1) += t._2)
    // the second dep is rdd2; remove all of its keys
    integrate(partition.deps(1), t => map.remove(t._1))
    map.iterator.map { t =>  t._2.iterator.map { (t._1, _) } }.flatten
  }

  override def clearDependencies() {
    super.clearDependencies()
    prev = null
    countRDD = null
    rangeBoundsRDD = null
  }

}
