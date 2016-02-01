package dbis.spark.spatial

import org.apache.spark.SparkContext
import com.vividsolutions.jts.io.WKTReader
import org.apache.spark.SparkConf
import dbis.spark.spatial.SpatialRDD._
import scala.io.Source
import dbis.spark.spatial.indexed.RTree
import com.vividsolutions.jts.geom.Geometry


object SpatialRDDTest {
  
	def main(args: Array[String]) {

	  val dataFile = args(0)
	  val queryFile = args(1)
	  
		val sc = new SparkContext()


//		val points = Array("POINT(0 0)", "POINT(2 2)", "POINT(7 7)").reverse
//		val qry = new WKTReader().read("POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))")
//		
//		val rand = new Random
//		
//		val rdd = sc.parallelize((0 until 1 * 1000 * 1000))
////		            .map { i => (rand.nextInt(20) , rand.nextInt(20), i)}
//		            .map { i => (rand.nextDouble() * 100 , rand.nextDouble() * 100, i)}
//	              .map { case (x,y,i) => (s"POINT($x $y)",i)}
//		
//		val r = rdd.map{ case (p,i) => (new WKTReader().read(p), i) }
//		          .index(2)
//		          .intersect(qry)
//		          .filter(_._2 == 3)
//		          .kNN(qry, 4)
//		
//		r.foreach(println)
		
		
//		val bayernWkt = Source.fromFile("/home/stha1in/DE-BY.txt").getLines().mkString(" ")
		val bayernWkt = Source.fromFile(queryFile).getLines().mkString(" ")
		val bayern = new WKTReader().read(bayernWkt)
		
		
		
		val start = System.currentTimeMillis()
		
		val res = sc.textFile(dataFile)
		             .map { line => line.split(",")}
	               .filter { arr => arr(7).matches("POINT\\(\\d+\\.?\\d* \\d+\\.?\\d*\\)")}
	               .map { arr => (new WKTReader().read(arr(7)), arr(0)) }
	               // choose one (or none) of the three below
//	               .index(3)   // create index on the fly
	               .grid(3)      // apply partitioning (but no indexing)
//	               .bla(3)     // partition and "persitable" index creation
	               .intersect(bayern)
//	               .map { case (geom, v) => v }
	               .count()

	      // create and persist index         
////	  val res = sc.textFile(dataFile)
////		             .map { line => line.split(",")}
////	               .filter { arr => arr(7).matches("POINT\\(\\d+\\.?\\d* \\d+\\.?\\d*\\)")}
////	               .map { arr => (new WKTReader().read(arr(7)), arr(0)) }
//////	               .index(3)
//////	               .grid(3)
////	               .bla(3)
////	               .saveAsObjectFile("/user/stha1in/rtree")

	               
	    // load persisted index objects		
//		val res = sc.objectFile[RTree[Geometry, (Geometry, String)]]("/user/stha1in/rtree")
//		            .intersect(bayern)
//		            .count()
	               
	               
	               
	  val end = System.currentTimeMillis()	               
	  println(s"${end - start} ms")
	               
	  println(res)             
		
		
	  
	  
	  
		
		sc.stop()
	  
	  
	}
}