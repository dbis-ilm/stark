package dbis.stark.spatial

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import scala.collection.mutable.Map

import com.vividsolutions.jts.io.WKTReader


import dbis.stark.STObject
import org.apache.spark.rdd.ShuffledRDD
import dbis.stark.TestUtils

class BSPartitionerTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  
  private var sc: SparkContext = _
  
  override def beforeAll() {
    val conf = new SparkConf().setMaster(s"local[${Runtime.getRuntime.availableProcessors()}]").setAppName("paritioner_test2")
    sc = new SparkContext(conf)
  }
  
  override def afterAll() {
    if(sc != null)
      sc.stop()
  }
  
  
  private def createRDD(points: Seq[String] = List("POINT(2 2)", "POINT(2.5 2.5)", "POINT(2 4)", "POINT(4 2)", "POINT(4 4)")): RDD[(STObject, Long)] = 
    sc.parallelize(points,4).zipWithIndex()
      .map { case (string, id) => (new WKTReader().read(string), id) }
  
  "The BSP partitioner" should "find correct min/max values" in {
    
    val rdd = createRDD()    
    
    val parti = new BSPartitioner(rdd, 1, 1)
    
    withClue("wrong minX value") { parti.minX shouldBe 2 }
    withClue("wrong minX value") { parti.minY shouldBe 2 }
    withClue("wrong minX value") { parti.maxX shouldBe 4 + 1 } // max values are set to +1 to have "right open" intervals  
    withClue("wrong minX value") { parti.maxY shouldBe 4 + 1 } // max values are set to +1 to have "right open" intervals
  }
  
  ignore should "have the correct min/max in real world scenario" in {
    
    val rdd = TestUtils.createRDD(sc)
    
    val parti = new BSPartitioner(rdd,1, 10)
    
    parti.minX shouldBe -35.8655
    parti.maxX shouldBe 61.5 + 1
    parti.minY shouldBe -157.74538
    parti.maxY shouldBe 153.02235 + 1
      
  }
  
  ignore should "have the correct number of x cells in reald world scenario with length = 1" in {
    
    val rdd = TestUtils.createRDD(sc)
    
    val parti = new BSPartitioner(rdd,1, 10)
    
    parti.numXCells shouldBe 99
    
  }
  
  
  ignore should "find the correct number of cells for X dimension" in {
    val rdd = createRDD()
    
    val parti = new BSPartitioner(rdd, 1, 1)
    
    parti.numXCells shouldBe 3
  }
  
  ignore should "create correct number of cells" in {
    
    val rdd = createRDD()
    
    val parti = new BSPartitioner(rdd, 1, 1)
    
//    println(s"${parti.cells.mkString("\n")}")
    
    parti.cells.size shouldBe 9
  }
  
  ignore should "create correct cell histogram" in {
    
    val rdd = createRDD()
    
    val parti = new BSPartitioner(rdd, 1, 1)

    val shouldSizes = Array(
      (Cell(NRectRange(NPoint(2,2), NPoint(3,3)),NRectRange(NPoint(2,2), NPoint(3,3))), 2), // 0
      (Cell(NRectRange(NPoint(3,2), NPoint(4,3)),NRectRange(NPoint(3,2), NPoint(4,3))), 0), // 1
      (Cell(NRectRange(NPoint(4,2), NPoint(5,3)),NRectRange(NPoint(4,2), NPoint(5,3))), 1), // 2
      (Cell(NRectRange(NPoint(2,3), NPoint(3,4)),NRectRange(NPoint(2,3), NPoint(3,4))), 0), // 3
      (Cell(NRectRange(NPoint(3,3), NPoint(4,4)),NRectRange(NPoint(3,3), NPoint(4,4))), 0), // 4
      (Cell(NRectRange(NPoint(4,3), NPoint(5,4)),NRectRange(NPoint(4,3), NPoint(5,4))), 0), // 5
      (Cell(NRectRange(NPoint(2,4), NPoint(3,5)),NRectRange(NPoint(2,4), NPoint(3,5))), 1), // 6
      (Cell(NRectRange(NPoint(3,4), NPoint(4,5)),NRectRange(NPoint(3,4), NPoint(4,5))), 0), // 7
      (Cell(NRectRange(NPoint(4,4), NPoint(5,5)),NRectRange(NPoint(4,4), NPoint(5,5))), 1)  // 8
    ) 
    
    parti.cells should contain only (shouldSizes:_*)
  }
  
  
  ignore should "return the correct partition id" in {
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
  
  
  ignore should "return all points for one partition" in {
    
    val rdd: RDD[(STObject, (String, Int, String, STObject))] = TestUtils.createRDD(sc, numParts = Runtime.getRuntime.availableProcessors())
    
    // with maxcost = size of RDD everything will end up in one partition
    val parti = new BSPartitioner(rdd, 2, _maxCostPerPartition = 1000) 
    
    val shuff = new ShuffledRDD(rdd, parti) 
       
    shuff.count() shouldBe rdd.count()
    
  }
  
  ignore should "return all points for two partitions" in {
    
    val rdd: RDD[(STObject, (String, Int, String, STObject))] = TestUtils.createRDD(sc, numParts = Runtime.getRuntime.availableProcessors())
    
    // with maxcost = size of RDD everything will end up in one partition
    val parti = new BSPartitioner(rdd, 2, _maxCostPerPartition = 500) 
    
    val shuff = new ShuffledRDD(rdd, parti) 
    // insert dummy action to make sure Shuffled RDD is evaluated
    shuff.foreach{f => }
    
    shuff.count() shouldBe 1000   
    shuff.count() shouldBe rdd.count()
    
  }
  
  ignore should "return all points for max cost 100 & sidelength = 1" in {
    
    val rdd: RDD[(STObject, (String, Int, String, STObject))] = TestUtils.createRDD(sc, numParts = Runtime.getRuntime.availableProcessors())
    
    // with maxcost = size of RDD everything will end up in one partition
    val parti = new BSPartitioner(rdd, 1, _maxCostPerPartition = 100) 
    
    val shuff = new ShuffledRDD(rdd, parti) 
    // insert dummy action to make sure Shuffled RDD is evaluated
    shuff.foreach{f => }
    
    shuff.count() shouldBe 1000   
    shuff.count() shouldBe rdd.count()
    
  }
  
  ignore should "return only one partition if max cost equals input size" in {
    
    val rdd: RDD[(STObject, (String, Int, String, STObject))] = TestUtils.createRDD(sc, numParts = Runtime.getRuntime.availableProcessors())
    
    // with maxcost = size of RDD everything will end up in one partition
    val parti = new BSPartitioner(rdd, 1, _maxCostPerPartition = 1000) 
    
    parti.numPartitions shouldBe 1    
    
  }
  
  it should "find use cells as partitions for taxi"  in {
    val rdd = sc.textFile("src/test/resources/yellow_ny_1.csv", 4)
      .map { line => line.split(";") }
      .map { arr => (STObject(arr(1)), arr(0))}
      
//    val parti = new BSPartitioner(rdd, 2, 100, withExtent= true)
      val minMax = SpatialPartitioner.getMinMax(rdd)
      println(s"min max: $minMax")
      BSPartitioner.numCellThreshold = Runtime.getRuntime.availableProcessors()
      val parti = BSPartitioner.withGridPPD(rdd, 100, 1000*1000, false, minMax._1, minMax._2, minMax._3, minMax._4)
      
      val nonempty = parti.cells.filter(_._2 > 0)
      nonempty.size shouldBe 3
      
      val cnt = rdd.count()
      
      nonempty.map(_._2).sum shouldBe cnt 
      
      parti.cells.headOption.get._1.range
      
      println(parti.bsp.partitions.mkString("\n"))
      
//      println(s"distinct: ${rdd.map(_._1.getGeo).distinct.count()}")
      
      parti.numPartitions shouldBe 3
      
      rdd.sample(false, 0.01).collect().foreach { case (st, name) =>
        try {
          val pNum = parti.getPartition(st)
          withClue(name) { pNum shouldBe oneOf(0, 1, 2) }
        } catch {
        case e:IllegalStateException =>
          
          val xOk = st.getGeo.getCentroid.getX >= minMax._1 && st.getGeo.getCentroid.getX <= minMax._2
          val yOk = st.getGeo.getCentroid.getY >= minMax._3 && st.getGeo.getCentroid.getY <= minMax._4
          
          parti.bsp.partitions.foreach { cell => 
            
            val xOk = st.getGeo.getCentroid.getX >= cell.range.ll(0) && st.getGeo.getCentroid.getX <= cell.range.ur(0)
            val yOk = st.getGeo.getCentroid.getY >= cell.range.ur(1) && st.getGeo.getCentroid.getY <= cell.range.ur(1)
            
            println(s"${cell.range.id} cell: ${cell.range.contains(NPoint(st.getGeo.getCentroid.getX, st.getGeo.getCentroid.getY))}  x: $xOk  y: $yOk")
          }
          
          val containingCell = parti.cells.find (cell => cell._1.range.contains(NPoint(st.getGeo.getCentroid.getX, st.getGeo.getCentroid.getY))).headOption
          if(containingCell.isDefined) {
            println(s"should be in ${containingCell.get} which has bounds ${parti.cells(0)._1.range}")
            
          } else {
            println("No cell contains this point!")
          }
          
          
          fail(s"$name: ${e.getMessage}  xok: $xOk  yOk: $yOk")
        }
        
      }
      
      
  }
  
  ignore should "create correct partitions for world data" in {
    
    val rdd = sc.textFile("src/test/resources/world3.csv", 4)
      .map { line => line.split(";") }
      .map { arr => (arr(0).toInt, arr(1).toInt, arr(2).toInt, arr(3).toInt,arr(4), STObject(arr(5))) }
      .keyBy(_._6)
    
    val parti = new BSPartitioner(rdd, 2, 100, withExtent= true)
    
    println(s"cells: ${parti.cells.size}")
    println(s"partitions: ${parti.numPartitions}")
    println(s"x: ${parti.minX}    ${parti.maxX}")
    println(s"y: ${parti.minY}    ${parti.maxY}")
    
    
  }
  
}
