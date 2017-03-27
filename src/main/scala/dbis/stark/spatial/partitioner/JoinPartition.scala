package dbis.stark.spatial.partitioner

import java.io.{IOException, ObjectOutputStream}

import org.apache.spark.Partition
import org.apache.spark.rdd.RDD

/**
  * A partition that represents the two partitions that have to be joined
  *
  * @param idx The index of the partition
  * @param rdd1 The left RDD
  * @param rdd2 The right RDD
  * @param s1Index The index of the partition in the left RDD
  * @param s2Index The index of the partition in the right RDD
  */
protected[stark] class JoinPartition(
    idx: Int,
    @transient private val rdd1: RDD[_],
    @transient private val rdd2: RDD[_],
    s1Index: Int,
    s2Index: Int
  ) extends Partition {

  var s1: Partition = rdd1.partitions(s1Index)
  var s2: Partition = rdd2.partitions(s2Index)
  override val index: Int = idx

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = {
    // Update the reference to parent split at the time of task serialization
    s1 = rdd1.partitions(s1Index)
    s2 = rdd2.partitions(s2Index)
    oos.defaultWriteObject()
  }

  override def toString: String = s"JoinPartition[idx=$idx, leftIdx=$s1Index, rightIdx=$s2Index, s1=$s1, s2=$s2]"
}