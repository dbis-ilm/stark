package dbis.stark.spatial.partitioner

/**
  * Created by Jacob on 20.02.17.
  */


import dbis.stark.{STObject, TemporalExpression}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD

object TemporalPartitioner {

  protected[stark] def getMinMax[G <: STObject, V](rdd: RDD[(G, V)]):(Long,Long) = {
    val (minT, maxT) = rdd.map { case (g, _) =>
      val time = g.getTemp.get
      (time.start.value, time.end.getOrElse(time.start).value)
//      val start = g.getTemp.get.start.value
//      (start,start)

    }.reduce { (oldMM, newMM) =>
      val newMinX = oldMM._1 min newMM._1
      val newMaxX = oldMM._2 max newMM._2
      (newMinX, newMaxX)
    }
    (minT, maxT + 2)
  }
}

abstract class TemporalPartitioner(private val _minT: Long, private val _maxT: Long) extends Partitioner {

  def minT = _minT

  def maxT = _maxT


  //TODO JTM: was macht das hier ?
  def partitionBounds(idx: Int): TemporalExpression

  //def partitionExtent(idx: Int): NRectRange
}

