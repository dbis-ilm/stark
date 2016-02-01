package dbis.spark.spatial.indexed

import org.apache.spark.Partition

class IndexedPartition[I](
  val partitionId: Int, 
  val theIndex: I) extends Partition with Serializable {
    
  override def index = partitionId
}