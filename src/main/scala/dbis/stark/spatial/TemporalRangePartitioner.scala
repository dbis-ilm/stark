package dbis.stark.spatial

import java.io.ObjectOutputStream

import dbis.stark.{Instant, Interval, STObject, TemporalExpression}
import org.apache.spark.Partition
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Created by Jacob on 20.02.17.
  */


object TemporalRangePartitioner {
  def getCellId(start: Long, cells: Array[Long], partitions: Int): Int = {
    //println(cells.mkString(" "))
    for (i <- 1 to partitions - 1) {
      //println(cells(i)+">"+start)
      if (cells(i) > start) {
        return i - 1
      }
    }
    return partitions - 1
  }

  def getCellId(start: Long, minT: Long, maxT: Long, partitions: Int): Int = {
    val length = (maxT * 1.2) - minT
    val interval = length / partitions

    val position_in_range = start - minT

    val res = (position_in_range / interval).toInt

    val s = " l: " + length + "  int: " + interval + " pos: " + position_in_range

    require(res >= 0 && res < partitions, s"Cell ID out of bounds (0 .. $partitions): $res " + " object:" + start + s)
    res
  }


}


class TemporalRangePartitioner[G <: STObject : ClassTag, V: ClassTag](
                                                                       rdd: RDD[(G, V)],
                                                                       partitions: Int,
                                                                       autoRange: Boolean,
                                                                       _minT: Long,
                                                                       _maxT: Long, sampelsize: Double) extends TemporalPartitioner(_minT, _maxT) {



  //new Array[Long](partitions)
  private val cells: Array[Long] = {
    val arr = Array.fill[Long](partitions)(0)
    if (autoRange) {
      val sample = rdd.sample(false, sampelsize).map(x => x._1.getTemp.get.start)
      val sorted = sample.sortBy(k => k.value).collect()
      println("autorange from Temporalpartitioner is on | sampelfaktor: " + sampelsize + " | sampelsize: " + sorted.size)
      val maxitems = Math.round(sorted.size / (partitions))

      arr(0) = 0
      for (i <- 1 to partitions - 1) {
        // println(i*maxitems +" " +(sorted(i*maxitems).value))
        arr(i) = sorted(i * maxitems).value
      }

    } else {
      val range = maxT - minT
      val dist = Math.round(range / partitions)
      arr(0) = 0
      for (i <- 1 to partitions - 1) {
        arr(i) = minT + i * dist
      }
    }

    arr

  }

  var bounds: Array[Long] = {
    val arr = new Array[Long](partitions)
    rdd.map { case (g, _) =>
      val end = g.getTemp.get.end.get.value
      val start = g.getTemp.get.start.value
      val id = TemporalRangePartitioner.getCellId(start, cells, partitions)

      //        println(s"$center --> $id")
      (id, end)
    }
      .reduceByKey { case (a, b) => if (a > b) a else b }
      .collect
      .foreach { case (id, end) =>
        arr(id) = end
      }
    arr
  }


  def this(rdd: RDD[(G, V)],
           partitions: Int,
           autoRange: Boolean,
           minMax: (Long, Long),
           sampelsize: Double) = {
    this(rdd, partitions, autoRange, minMax._1, minMax._2, sampelsize)

  }

  def this(rdd: RDD[(G, V)],
           partitions: Int,
           autoRange: Boolean,
           minMax: (Long, Long)) =
    this(rdd, partitions, autoRange, minMax, 0.01)

  def this(rdd: RDD[(G, V)],
           partitions: Int,
           autoRange: Boolean
          ) =
    this(rdd, partitions, autoRange, TemporalPartitioner.getMinMax(rdd))

  def this(rdd: RDD[(G, V)],
           partitions: Int,
           autoRange: Boolean,
           sampelsize: Double
          ) =
    this(rdd, partitions, autoRange, if (autoRange) {
      (0, 0)
    } else {
      TemporalPartitioner.getMinMax(rdd)
    }, sampelsize)


  override def numPartitions: Int = partitions

  /**
    * Compute the partition for an input key.
    * In fact, this is a Geometry for which we use its centroid for
    * the computation
    *
    * @param key The key geometry to compute the partition for
    * @return The Index of the partition
    */
  override def getPartition(key: Any): Int = {
    val g = key.asInstanceOf[G]

    val start = g.getTemp.get.start.value

    var id = 0;
    // if(autoRange){

    id = TemporalRangePartitioner.getCellId(start, cells, partitions)
    /*}else {
      id = TemporalRangePartitioner.getCellId(start, minT, maxT, partitions)
    }*/





    require(id >= 0 && id < numPartitions, s"Cell ID out of bounds (0 .. $numPartitions): $id " + " object:" + key)

    id
  }


  override def partitionBounds(idx: Int): TemporalExpression = {
    //println(bounds.mkString(" , "))
    Interval(cells(idx), bounds(idx))
  }
}
