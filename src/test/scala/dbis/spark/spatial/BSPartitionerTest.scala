package dbis.spark.spatial

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import scala.collection.mutable.Map

import com.vividsolutions.jts.io.WKTReader

import dbis.spatial.NPoint
import dbis.spatial.NRectRange

import dbis.spark.SpatialObject

class BSPartitionerTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  
  private var sc: SparkContext = _
  
  override def beforeAll() {
    val conf = new SparkConf().setMaster("local").setAppName("paritioner_test2")
    sc = new SparkContext(conf)
  }
  
  override def afterAll() {
    if(sc != null)
      sc.stop()
  }
  
  
  private def createRDD(points: Seq[String] = List("POINT(2 2)", "POINT(2.5 2.5)", "POINT(2 4)", "POINT(4 2)", "POINT(4 4)")): RDD[(SpatialObject, Long)] = 
    sc.parallelize(points).zipWithIndex()
      .map { case (string, id) => (new WKTReader().read(string), id) 
  }
  
  "The BSP partitioner" should "find correct min/max values" in {
    
    val rdd = createRDD()    
    
    val parti = new BSPartitioner(rdd, 1, 1)
    
    withClue("wrong minX value") { parti.minX shouldBe 2 }
    withClue("wrong minX value") { parti.minY shouldBe 2 }
    withClue("wrong minX value") { parti.maxX shouldBe 4 + 1 } // max values are set to +1 to have "right open" intervals  
    withClue("wrong minX value") { parti.maxY shouldBe 4 + 1 } // max values are set to +1 to have "right open" intervals
  }
  
  it should "find the correct number of cells for X dimension" in {
    val rdd = createRDD()
    
    val parti = new BSPartitioner(rdd, 1, 1)
    
    parti.numXCells shouldBe 3
  }
  
  it should "create correct number of cells" in {
    
    val rdd = createRDD()
    
    val parti = new BSPartitioner(rdd, 1, 1)
    
//    println(s"${parti.cells.mkString("\n")}")
    
    parti.cells.size shouldBe 4
  }
  
  it should "create correct cell histogram" in {
    
    val rdd = createRDD()
    
    val parti = new BSPartitioner(rdd, 1, 1)

    val shouldSizes = Array(
      (NRectRange(NPoint(2,2), NPoint(3,3)), 2),
      (NRectRange(NPoint(2,4), NPoint(3,5)), 1),
      (NRectRange(NPoint(4,4), NPoint(5,5)), 1),
      (NRectRange(NPoint(4,2), NPoint(5,3)), 1)
    ) 
    
    parti.cells should contain only (shouldSizes:_*)
  }
  
  it should "return the correct partition id" in {
    val rdd = createRDD()
    val parti = new BSPartitioner(rdd, 1, 1)
    
//    val partIds = Array(0,0,1,2,3)
   
    val parts = rdd.map{ case (g,v) => parti.getPartition(g) }.collect()
    
    parts should contain inOrderOnly(0,1,2,3)
    
    
//    rdd.collect().foreach{ case (g,id) => 
//      val pId = parti.getPartition(g)
//      
//      pId shouldBe partIds(id.toInt)
//    }
  }
}