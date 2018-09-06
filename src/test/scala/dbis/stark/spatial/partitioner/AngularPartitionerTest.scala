package dbis.stark.spatial.partitioner

import dbis.stark.STObject
import org.scalatest.{FlatSpec, Matchers}

class AngularPartitionerTest extends FlatSpec with Matchers {
  
  "The Angular Partitioner" should "find two dimensional partitions" in {
    
    val parti = new AngularPartitioner(2, 4)
    
    parti.numPartitions shouldBe 4
    parti.phi shouldBe 90

    val points: Array[Array[Double]] = Array(
      Array(1,1),
      Array(-1,1),
      Array(-1,-1),
      Array(1,-1)
    )
    
    val sphereCoords = (0 to 3).map { i => STObject(points(i)(0), points(i)(1)) }
    
    sphereCoords.foreach(println)
    
    val a = sphereCoords.map { k => parti.getPartition(k) }
    
    a should contain only (0,1,2,3)
    
  }
  
}