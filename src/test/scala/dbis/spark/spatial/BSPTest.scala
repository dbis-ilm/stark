package dbis.spark.spatial

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import com.vividsolutions.jts.io.WKTReader
import org.apache.spark.rdd.RDD
import com.vividsolutions.jts.geom.Geometry

class BSPTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  
  private var sc: SparkContext = _
  
  override def beforeAll() {
    val conf = new SparkConf().setMaster("local").setAppName("paritioner_test")
    sc = new SparkContext(conf)
  }
  
  override def afterAll() {
    if(sc != null)
      sc.stop()
  }
  
  
  private def createRDD(NPoints: Seq[String] = List("Point(2 2)", "Point(2.5 2.5)", "Point(2 4)", "Point(4 2)", "Point(4 4)")): RDD[(Geometry, Long)] = 
    sc.parallelize(NPoints).zipWithIndex()
      .map { case (string, id) => (new WKTReader().read(string), id) 
  }
  
  it should "compute the correct partition costs" in {
    val rdd = createRDD()
    val parti = new BSPartitioner(rdd, 1, 1)
    
    val bsp = parti.algo
    
    val part1 = NRectRange(NPoint(2,2), NPoint(3,5))
    val part2 = NRectRange(NPoint(3,2), NPoint(5,5))
    val part3 = NRectRange(NPoint(1,1), NPoint(4,4))
    
    bsp.costEstimation(part1) shouldBe 3
    bsp.costEstimation(part2) shouldBe 2
    bsp.costEstimation(part3) shouldBe 2
    
  }
  
  it should "split a partition correctly" in {
    val rdd = createRDD()
    val parti = new BSPartitioner(rdd, 1, 1)
    
    val part = NRectRange(NPoint(2,2), NPoint(5,5))
    
    val bsp = parti.algo
    
    val (p1,p2) = bsp.costBasedSplit(part)
    
    println(s"created split 1: $p1")
    println(s"created split 2: $p2")
    
    
    withClue("split 1") { p1 shouldBe NRectRange(NPoint(2,2), NPoint(3,5)) }
    withClue("split 2") { p2 shouldBe NRectRange(NPoint(3,2), NPoint(5,5)) }
  }
  
  it should "split a partition correctly - by Y" in {
    val rdd = createRDD()
    val parti = new BSPartitioner(rdd, 1, 1)
    val bsp = parti.algo
    
    val part = NRectRange(NPoint(2,2), NPoint(3,5))
    
    val (p1,p2) = bsp.costBasedSplit(part)
    
    withClue("split 1") { p1 shouldBe NRectRange(NPoint(2,2), NPoint(3,3)) }
    withClue("split 2") { p2 shouldBe NRectRange(NPoint(2,3), NPoint(3,5)) }
  }
  
  it should "return the number of partitions" in {
    val rdd = createRDD()
    val parti = new BSPartitioner(rdd, 1, 1)
    val bsp = parti.algo
    
    val parts = bsp.bounds
    
    parts.size shouldBe 4    
  }
  
  it should "find the correct partitioning" in  {
    val rdd = createRDD()
    val parti = new BSPartitioner(rdd, 1, 1)
    val bsp = parti.algo
    
    val exp = Array(NRectRange(NPoint(2,2), NPoint(3,3)),
                    NRectRange(NPoint(2,3), NPoint(3,5)),
                    NRectRange(NPoint(3,2), NPoint(5,3)),
                    NRectRange(NPoint(3,3), NPoint(5,5)))
    
    bsp.bounds.map(_._1) should contain only (exp:_*)

    
    
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
  
  "The partition statistics" should "have correct values" in {
    val rdd = createRDD()
    val parti = new BSPartitioner(rdd, 1, 1)
    val bsp = parti.algo
    
    val stats = bsp.partitionStats
    
    withClue("numParts") { stats.numPartitions shouldBe 4 }
    withClue("avgNPoints") { stats.avgPoints shouldBe 5.0/4.0}
    withClue("maxNPoints") { stats.maxPoints should contain only ((NRectRange(NPoint(2,2), NPoint(3,3)),2)) }
    withClue("minNPoints") { stats.minPoints should contain only ((NRectRange(NPoint(2,3), NPoint(3,5)),1),
                                                                 (NRectRange(NPoint(3,2), NPoint(5,3)),1), 
                                                                 (NRectRange(NPoint(3,3), NPoint(5,5)),1) )}
    
    withClue("variance") { stats.numPointsVariance shouldBe 0.75 }
    
    withClue("area") { stats.area shouldBe 9.0}
    withClue("avg area") { stats.avgArea shouldBe 9.0/4}
    withClue("minArea") { stats.minArea should contain only ((NRectRange(NPoint(2,2), NPoint(3,3)),1.0)) }
    withClue("maxArea") { stats.maxArea should contain only ((NRectRange(NPoint(3,3), NPoint(5,5)),4.0))}
    
  } 
  
}