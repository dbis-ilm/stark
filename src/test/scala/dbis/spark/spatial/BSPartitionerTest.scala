package dbis.spark.spatial

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import com.vividsolutions.jts.io.WKTReader
import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.rdd.RDD
import org.scalatest.BeforeAndAfterAll
import dbis.spark.spatial.SpatialGridPartitioner.Point
import dbis.spark.spatial.SpatialGridPartitioner.RectRange

class BSPartitionerTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  
  private var sc: SparkContext = _
  
  override def beforeAll() {
    val conf = new SparkConf().setMaster("local").setAppName("paritioner_test")
    sc = new SparkContext(conf)
  }
  
  override def afterAll() {
    if(sc != null)
      sc.stop()
  }
  
  
  private def createRDD(points: Seq[String] = List("POINT(2 2)", "POINT(2.5 2.5)", "POINT(2 4)", "POINT(4 2)", "POINT(4 4)")): RDD[(Geometry, Long)] = 
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
  
  it should "find the correct number of cells for Y dimension" in {
    val rdd = createRDD()
    
    val parti = new BSPartitioner(rdd, 1, 1)
    
    parti.numYCells shouldBe 3
  }
  
  it should "create correct number of cells" in {
    
    val rdd = createRDD()
    
    val parti = new BSPartitioner(rdd, 1, 1)
    
    parti.cells.size shouldBe 4
  }
  
  it should "create correct number cell counts" in {
    
    val rdd = createRDD()
    
    val parti = new BSPartitioner(rdd, 1, 1)
    
    val shouldSizes = Array(2,1,1,1)
    
    (0 until parti.cells.size).foreach { i =>
      withClue(s"unexepected count in for cell #$i (cell id: ${parti.cells(i)._1.id}) :") { 
        parti.cells(i)._2 shouldBe shouldSizes(i) 
      }  
    }    
  }
  
  it should "compute the correct cell bounds" in {
    val rdd = createRDD()
    val parti = new BSPartitioner(rdd, 1, 1)

    val expLL = Array(Point(2,2), Point(4,2), Point(2,4), Point(4,4) )
    val expUR = Array(Point(3,3), Point(5,3), Point(3,5), Point(5,5) )
    
    (0 until parti.cells.size).foreach { i =>
      val ll = parti.cells(i)._1.ll
      val ur = parti.cells(i)._1.ur
      
      withClue(s"unexepected ll point for cell #$i (cell id: ${parti.cells(i)._1.id}") {
        ll shouldBe expLL(i)
      }
      
      withClue(s"unexepected UR point for cell #$i (cell id: ${parti.cells(i)._1.id}") {
        ur shouldBe expUR(i)
      }
    }
  }
  
  it should "compute the correct partition costs" in {
    val rdd = createRDD()
    val parti = new BSPartitioner(rdd, 1, 1)
    
    val part1 = RectRange(Point(2,2), Point(3,5))
    val part2 = RectRange(Point(3,2), Point(5,5))
    val part3 = RectRange(Point(1,1), Point(4,4))
    
    parti.costEstimation(part1) shouldBe 3
    parti.costEstimation(part2) shouldBe 2
    parti.costEstimation(part3) shouldBe 2
    
  }
  
  it should "split a partition correctly" in {
    val rdd = createRDD()
    val parti = new BSPartitioner(rdd, 1, 1)
    
    val part = RectRange(Point(2,2), Point(5,5))
    
    val (p1,p2) = parti.costBasedSplit(part)
    
    withClue("split 1") { p1 shouldBe RectRange(Point(2,2), Point(3,5)) }
    withClue("split 2") { p2 shouldBe RectRange(Point(3,2), Point(5,5)) }
  }
  
  it should "split a partition correctly - by Y" in {
    val rdd = createRDD()
    val parti = new BSPartitioner(rdd, 1, 1)
    
    val part = RectRange(Point(2,2), Point(3,5))
    
    val (p1,p2) = parti.costBasedSplit(part)
    
    withClue("split 1") { p1 shouldBe RectRange(Point(2,2), Point(3,3)) }
    withClue("split 2") { p2 shouldBe RectRange(Point(2,3), Point(3,5)) }
  }
  
  it should "return the number of partitions" in {
    val rdd = createRDD()
    val parti = new BSPartitioner(rdd, 1, 1)
    
    val parts = parti.bounds
    
    parts.size shouldBe 4    
  }
  
  it should "find the correct partitioning" in  {
    val rdd = createRDD()
    val parti = new BSPartitioner(rdd, 1, 1)
    
    val parts = parti.bounds
    
    parts(0)._1 shouldBe RectRange(Point(2,2), Point(3,3))
    parts(1)._1 shouldBe RectRange(Point(2,3), Point(3,5))
    parts(2)._1 shouldBe RectRange(Point(3,2), Point(5,3))
    parts(3)._1 shouldBe RectRange(Point(3,3), Point(5,5))
  }
  
  it should "return the correct partition id" in {
    val rdd = createRDD()
    val parti = new BSPartitioner(rdd, 1, 1)
    
    val partIds = Array(0,0,1,2,3)
    
    rdd.collect().foreach{ case (g,id) => 
      val pId = parti.getPartition(g)
      
      pId shouldBe partIds(id.toInt)
      
    
    }
    
    
  }
  
  
}