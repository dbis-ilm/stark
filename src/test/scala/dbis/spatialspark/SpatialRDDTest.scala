package dbis.spatialspark

import org.apache.spark.SparkContext
import com.vividsolutions.jts.geom.Point
import ExtendedRDD._
import com.vividsolutions.jts.io.WKTReader
import org.apache.spark.SparkConf
import scala.collection.mutable.Queue

object SpatialRDDTest {
  
	def main(args: Array[String]) {

	  val conf = new SparkConf
		val sc = new SparkContext("local[1]", "test", conf)


		val points = Array("POINT(0 0)", "POINT(2 2)", "POINT(7 7)").reverse
		val qry = new WKTReader().read("POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))")
		

		val rdd = sc.parallelize(points,1)
		val s = rdd.makeSpatial{ x => new WKTReader().read(x)}
	              .coalesce(1)
	              .intersect(qry)
	              .filter { x => true }
	              .kNN(qry, 1)


	  
	  val d: Queue[org.apache.spark.Dependency[_]] = Queue(s.dependencies:_*)
	  while(d.nonEmpty) {
	    val h = d.dequeue().rdd
	    println(s" $h  --> ${h.dependencies}")
	    if(h.dependencies.nonEmpty)
	      d.enqueue(h.dependencies:_*)
	  }
	              
		s.foreach { println }
	}
}