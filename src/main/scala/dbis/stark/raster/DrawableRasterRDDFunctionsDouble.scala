package dbis.stark.raster

import dbis.stark.STObject.MBR
import dbis.stark.visualization.Visualization
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.rdd.RDD

//class DrawableRasterRDDFunctionsDouble(rdd: RDD[Tile[Double]]) {
//  def visualize(path: String, imgWidth: Int, imgHeight: Int, range: (Double,Double,Double,Double) = RasterGridPartitioner.getMinMax(rdd)) = {
//
//    val vis = new Visualization()
//    val jsc = new JavaSparkContext(rdd.context)
//
//    val env = new MBR(range._1, range._2, range._3, range._4)
//
//    println(env)
//
////    val o = rdd.map(t => t.map{ scalaInt =>
////      val i: java.lang.Double = scalaInt
////      i
////    })
//
////    val o = rdd.map(t => t.map(_.toDouble))
//
////    vis.visualize(jsc, o, imgWidth, imgHeight, env, path)
//    false
//  }
//}

//class DrawableRasterRDDFunctionsInt(rdd: RDD[Tile[Int]]) {
//  def visualize(path: String, imgWidth: Int, imgHeight: Int, range: (Double,Double,Double,Double) = RasterGridPartitioner.getMinMax(rdd)) = {
//
//    val vis = new Visualization()
//    val jsc = new JavaSparkContext(rdd.context)
//
//    val env = new MBR(range._1, range._2, range._3, range._4)
//
//    println(env)
//
////    val o = rdd.map(t => t.map{ scalaInt =>
////      val i: java.lang.Integer = scalaInt
////      i
////    })
//
//    //    val o = rdd.map(t => t.map(_.toDouble))
//
////    vis.visualizeInt(jsc, o, imgWidth, imgHeight, env, path)
//  }
//}
