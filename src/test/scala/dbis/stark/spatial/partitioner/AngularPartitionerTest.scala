package dbis.spark.spatial

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.vividsolutions.jts.io.WKTReader

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
    
    val sphereCoords = (0 to 3).map { i => SphereCoordinate(points(i)) }
    
    sphereCoords.foreach(println)
    
    val a = sphereCoords.map { k => parti.getPartition(k) }
    
    a should contain only (0,1,2,3)
    
  }
  
}