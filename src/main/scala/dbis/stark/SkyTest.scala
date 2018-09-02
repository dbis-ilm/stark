package dbis.stark

import dbis.stark.spatial.Skyline
import org.apache.spark.{SparkConf, SparkContext}
//import dbis.stark.spatial.SpatialRDD._
import org.apache.spark.SpatialRDD._

/**
  * Created by hg on 13.04.17.
  */
object SkyTest {

  def main(args: Array[String]): Unit = {

    val showProgress = if(args.length >= 2) args(1).toBoolean else true

    val conf = new SparkConf().setAppName("skylinetest").set("spark.ui.showConsoleProgress", showProgress.toString)
    val sc = new SparkContext(conf)

    val rdd = sc.textFile(args(0)).map(_.split(";")).map{ arr=>
      val id = arr(0).toLong
      val start = arr(2).toLong
      val end = arr(3).toLong

      (STObject(arr(1), Interval(start, end)), id)

    }
    val q = STObject("POINT(-190 -91)", 0L)


    type PayloadType = (STObject, Long)
    type T = (STObject,PayloadType)

    def combine(sky: Skyline[PayloadType], tuple: PayloadType): Skyline[PayloadType] = {
      val dist = Distance.euclid(tuple._1, q)
      val distObj = STObject(dist._1.minValue, dist._2.minValue)
      sky.insert((distObj, tuple))
      sky
    }

    def merge(sky1: Skyline[PayloadType], sky2: Skyline[PayloadType]): Skyline[PayloadType] = {
      val sky3 = sky2
      sky1.skylinePoints.foreach(p => sky3.insert(p))
      sky3
    }

    val num_runs = 5

    var i = 0
    var sum1 = 0L
    while(i < num_runs) {
      val start1 = System.currentTimeMillis()
      val skyline1 = rdd.filter(_._1 != q).skyline(q,
        Distance.euclid,
        Skyline.centroidDominates,
        ppD = 6,
        allowCache = true)
        .collect()
      val end1 = System.currentTimeMillis()
      println(s"skyline1 ${skyline1.length}")

      i += 1
      sum1 += (end1 - start1)
    }

    println

    i = 0
    var sum2 = 0L
//    while(i < num_runs) {
//      val start2 = System.currentTimeMillis()
//
//      val skyline2 = rdd.aggregate(new Skyline[PayloadType]())(combine, merge).skylinePoints.map(_._2)
//      val end2 = System.currentTimeMillis()
//      println(s"skyline2 ${skyline2.length}")
//
//      i += 1
//      sum2 += (end2 - start2)
//    }

    println
    println
    println(s"skyline mappartitons:\t${sum1 / num_runs}ms")
    println(s"skyline aggregate:\t${sum2 / num_runs}ms")

  }

}
