package dbis.spatialspark

import org.apache.spark.SparkContext
import com.vividsolutions.jts.geom.Point
import com.vividsolutions.jts.io.WKTReader
import org.apache.spark.SparkConf
import scala.collection.mutable.Queue
import dbis.spark.spatial.plain.SpatialRDD
import dbis.spark.spatial.plain.SpatialRDD._


object SpatialRDDTest {
  
	def main(args: Array[String]) {

	  val conf = new SparkConf
		val sc = new SparkContext("local", "test", conf)


		val points = Array("POINT(0 0)", "POINT(2 2)", "POINT(7 7)").reverse
		val qry = new WKTReader().read("POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))")
		

//		val rdd = sc.parallelize(points,1)
//		val s = rdd.makeSpatial{ x => new WKTReader().read(x)}
//	              .coalesce(1)
//	              .intersect(qry)
//	              .filter { x => true }
//	              .kNN(qry, 1)
		
		
		val rdd = sc.parallelize(points)
		
		
		val r = rdd.map{ s => (new WKTReader().read(s), s) }.intersect(qry)
		
		r.foreach(println)
		
		sc.stop()
	  
	  
	}
}