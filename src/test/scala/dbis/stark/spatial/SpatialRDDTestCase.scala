package dbis.stark.spatial

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import scala.io.Source
import scala.reflect.io.File
import scala.collection.JavaConverters._

import com.vividsolutions.jts.io.WKTReader
import com.vividsolutions.jts.io.WKTWriter

import java.nio.file.Files
import java.nio.file.StandardOpenOption

import dbis.stark.spatial.SpatialRDD._
import dbis.stark.SpatialObject
import dbis.stark.SpatialObject._

object SpatialRDDTestCase {
  
  private val qryS = "POLYGON((59.263107 -4.618767 , 56.773145 -11.281927, 51.419398 -10.419636, 49.438952 -3.730346, 51.321523 2.227303 , 57.482247 7.009100, 59.263107 -4.618767))"
  val qry = new WKTReader().read(qryS)
  
}

class SpatialRDDTestCase extends FlatSpec with Matchers with BeforeAndAfterAll {

  import SpatialRDDTestCase._
  
  private var sc: SparkContext = _
  
  override def beforeAll() {
    val conf = new SparkConf().setMaster("local[1]").setAppName("spatialrddtestcase")
    sc = new SparkContext(conf)
  }
  
  override def afterAll() {
    if(sc != null)
      sc.stop()
  }
  
  
  "A PLAIN SpatialRDD" should "find the correct intersection result for points" in { 
    
    val rdd = Helper.createRDD(sc)
    
    val foundPoints = rdd.intersect(qry).collect()
    
    withClue("wrong number of intersected points") { foundPoints.size shouldBe 36 } // manually counted
    
    foundPoints.foreach{ case (p, _) => qry.intersects(p) shouldBe true }
    
  }
  
  it should "find all elements contained by a query" in { 
    val rdd = Helper.createRDD(sc)
    
    val foundPoints = rdd.containedby(qry).collect()
    
    withClue("wrong number of points contained by query object") { foundPoints.size shouldBe 36 } // manually counted
  }
  
  it should "find all elements that contain a given point" in { 
	  val rdd = Helper.createRDD(sc)
	  
	  // we look for all elements that contain a given point. 
	  // thus, the result should be all points in the RDD with the same coordinates
	  val q: SpatialObject = new WKTReader().read("POINT (53.483437 -2.2040706)")
	  val foundGeoms = rdd.contains(q).collect()
	  
	  foundGeoms.size shouldBe 6
	  foundGeoms.foreach{ case (g,_) => g shouldBe q}
    
  }
  
  it should "find the correct nearest neighbors" in { 
    val rdd = Helper.createRDD(sc)
	  
	  // we look for all elements that contain a given point. 
	  // thus, the result should be all points in the RDD with the same coordinates
	  val q: SpatialObject = "POINT (53.483437 -2.2040706)"
	  val foundGeoms = rdd.kNN(q, 6).collect()
	  
	  foundGeoms.size shouldBe 6
	  foundGeoms.foreach{ case (g,_) => g shouldBe q}
    
  } 
  
  
  "A clustering" should "return all points" in {
    val rdd = Helper.createRDD(sc)
    
    val f = new java.io.File("clusterresult").toPath()
    Helper.rmrf(f) // delete output directory if existing to avoid write problems 

    
    

    val res = rdd.cluster(minPts = 10, epsilon = 2.0, Some(f.toString()))
    
    res.count() shouldBe rdd.count() 
    
    
//    println(s"output size ${res.count()}")
//    val s = res.map{ case (g,(id, v)) => (id, g) }.groupByKey.zipWithIndex.map { case ((cid,l),i) => (i, l.size) }.cache()
//    s.foreach(println)
    
//    println(s"clustered points: ${s.map{ case (i,c) => c }.sum}")
    
    
    
    
  }  
  
  
}