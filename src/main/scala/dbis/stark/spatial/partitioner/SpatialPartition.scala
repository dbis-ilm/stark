package dbis.stark.spatial.partitioner

import java.io.ObjectOutputStream

import org.apache.spark.Partition
import org.apache.spark.rdd.RDD


/**
  * A spatial partition represents a partitioning with spatial bounds.
  *
  * Instances of this class simply contain the pointer (index number) of the parent partition
  * @param idx The index number of this partition
  * @param parentPartitionId The index number of the parent partition
  * @param rdd The parent RDD
  */
case class SpatialPartition(
   private val idx: Int,
   parentPartitionId: Int,
   @transient private val rdd: RDD[_]) extends Partition {

  override def index: Int = idx

  var parentPartition: Partition = rdd.partitions(parentPartitionId)

  private def writeObject(oos: ObjectOutputStream): Unit = {
    parentPartition = rdd.partitions(parentPartitionId)
    oos.defaultWriteObject()
  }

  override def toString: String = s"SpatialPartition[idx=$idx, parentPartitionId=$parentPartitionId]"
}
