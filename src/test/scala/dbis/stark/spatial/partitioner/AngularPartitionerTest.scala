package dbis.stark.spatial.partitioner

import dbis.stark.STObject
import org.scalatest.{FlatSpec, Matchers}

class AngularPartitionerTest extends FlatSpec with Matchers {
  
  "The Angular Partitioner" should "find two dimensional partitions" in {
    
    val parti = new AngularPartitioner(2, 4, firstQuadrantOnly = false)
    
    parti.numPartitions shouldBe 4
    parti.phi shouldBe math.Pi / 2

    val points: Array[Array[Double]] = Array(
      Array(1,1),
      Array(-1,1),
      Array(-1,-1),
      Array(1,-1)
    )
    
    val stobjects = (0 to 3).map { i => STObject(points(i)(0), points(i)(1)) }

    stobjects.map(SphereCoordinate(_)).foreach(println)

    val a = stobjects.map { k =>
      print(k)
      val p = parti.getPartition(k)
//      println(s" --> $p")
      p
    }
    
    a should contain only (0,1,2,3)
    
  }
  
}