package dbis.stark.spatial.partitioner

import dbis.stark.STObject
import dbis.stark.spatial.{Cell, NRectRange}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD

/**
  * Contains convenience functions used in spatial partitioners
  */
object SpatialPartitioner {

  var EPS: Double = 1 / 1000000.0


  /**
    * Determine the min/max extents of a given RDD
    *
    * Since we use right-open intervals in [[NRectRange]] we add 1 to the both max values
    *
    * @param rdd The RDD
    * @tparam G The data type representing spatio-temporal data
    * @tparam V The type for payload data
    * @return Returns a 4-tuple for min/max values in the two dimensions in the form <code>(min-x, max-x, min-y, max-y)</code>
    */
  protected[stark] def getMinMax[G <: STObject, V](rdd: RDD[(G,V)]) = {
    val (minX, maxX, minY, maxY) = rdd.map{ case (g,_) =>
      val env = g.getEnvelopeInternal
      (env.getMinX, env.getMaxX, env.getMinY, env.getMaxY)
      
    }.reduce { (oldMM, newMM) => 
      val newMinX = oldMM._1 min newMM._1
      val newMaxX = oldMM._2 max newMM._2
      val newMinY = oldMM._3 min newMM._3
      val newMaxY = oldMM._4 max newMM._4
      
      (newMinX, newMaxX, newMinY, newMaxY)  
    }
    
    // do +1 for the max values to achieve right open intervals 
    (minX, maxX + EPS, minY, maxY + EPS)
  }
}

/**
  * Base class for spatial partitioners
  * @param _minX The min value in x dimension
  * @param _maxX The max value in x dimension
  * @param _minY The min value in y dimension
  * @param _maxY The max value in y dimension
  */
abstract class SpatialPartitioner(
    private val _minX: Double, private val _maxX: Double, private val _minY: Double, private val _maxY: Double
  ) extends Partitioner {

  def minX = _minX
  def maxX = _maxX
  def minY = _minY
  def maxY = _maxY
  
  def partitionBounds(idx: Int): Cell
  def partitionExtent(idx: Int): NRectRange
}

