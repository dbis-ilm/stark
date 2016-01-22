package dbis.spark

import org.apache.spark.Partition

class IndexedPartition[I](
  val rddId: Long, 
  val slice: Int, 
  val theIndex: I) extends Partition with Serializable {
    
  
}