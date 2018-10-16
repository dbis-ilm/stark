package dbis.stark.spatial.partitioner

import java.io.{IOException, ObjectOutputStream}

import org.apache.spark.Partition
import org.apache.spark.rdd.RDD

/**
  * A partition that represents the two partitions that have to be joined
  *
  * @param idx The index of the partition
  * @param left The left RDD
  * @param right The right RDD
  * @param leftIndex The index of the partition in the left RDD
  * @param rightIndex The index of the partition in the right RDD
  */
protected[stark] case class OneToOnePartition(
                                      idx: Int,
                                      @transient private val left: RDD[_],
                                      @transient private val right: RDD[_],
                                      leftIndex: Int,
                                      rightIndex: Int) extends Partition {

  var leftPartition: Partition = left.partitions(leftIndex)
  var rightPartition: Partition = right.partitions(rightIndex)
  override val index: Int = idx

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = {
    // Update the reference to parent split at the time of task serialization
    leftPartition = left.partitions(leftIndex)
    rightPartition = right.partitions(rightIndex)
    oos.defaultWriteObject()
  }

  override def toString: String = s"JoinPartition[idx=$idx, leftIdx=$leftIndex, rightIdx=$rightIndex, s1=$leftPartition, s2=$rightPartition]"
}

protected[stark] case class OneToManyPartition(idx: Int, @transient private val left: RDD[_],
                                               @transient private val right: RDD[_],
                                               leftIndex: Int,
                                               rightIndex: Seq[Int]) extends Partition {

  override def index = idx

  var leftPartition: Partition = left.partitions(leftIndex)

  var rightPartitions: Seq[Partition] = rightIndex.map(i => right.partitions(i))

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = {
    // Update the reference to parent split at the time of task serialization
    leftPartition = left.partitions(leftIndex)
    rightPartitions = rightIndex.map(i => right.partitions(i))
    oos.defaultWriteObject()
  }

  override def toString = s"OneToMany(idx=$idx, left: $leftIndex rights: ${rightIndex.mkString(";")} )"
}