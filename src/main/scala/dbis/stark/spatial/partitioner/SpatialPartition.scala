package dbis.stark.spatial.partitioner

import java.io.ObjectOutputStream

import org.apache.spark.Partition
import org.apache.spark.rdd.RDD


/**
  * Created by hage on 27.03.17.
  */
case class SpatialPartition(private val idx: Int, origIndex: Int, @transient private val rdd: RDD[_]) extends Partition {
  override def index: Int = idx

  var split = rdd.partitions(origIndex)

  private def writeObject(oos: ObjectOutputStream): Unit = {
    split = rdd.partitions(origIndex)
    oos.defaultWriteObject()
  }
}
