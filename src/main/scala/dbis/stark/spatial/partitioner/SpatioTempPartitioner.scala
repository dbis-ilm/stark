package dbis.stark.spatial.partitioner

import java.nio.file.Path

import dbis.stark.STObject.MBR
import dbis.stark.spatial.{Cell, NPoint, NRectRange, StarkUtils}
import dbis.stark.{Instant, STObject}
import org.apache.spark.rdd.RDD
import spire.ClassTag

object SpatioTempPartitioner {
  def getMinMax[G <: STObject, T](rdd:RDD[(G,T)]): (Double, Double, Double, Double, Long, Long) = {
    val (minX, maxX, minY, maxY, start, end) = rdd.map{ case (g,_) =>
      val env = g.getEnvelopeInternal
      val t = g.getTemp.get
      (env.getMinX, env.getMaxX, env.getMinY, env.getMaxY, t.start.value, t.end.getOrElse(StarkUtils.MAX_LONG_INSTANT).value)

    }.reduce { (oldMM, newMM) =>
      val newMinX = oldMM._1 min newMM._1
      val newMaxX = oldMM._2 max newMM._2
      val newMinY = oldMM._3 min newMM._3
      val newMaxY = oldMM._4 max newMM._4

      val newStart = oldMM._5 min newMM._5
      val newEnd = oldMM._6 max newMM._6

      (newMinX, newMaxX, newMinY, newMaxY, newStart, newEnd)
    }

    // do +1 for the max values to achieve right open intervals
    (minX, maxX + GridPartitioner.EPS, minY, maxY + GridPartitioner.EPS, start, end)
  }

  def apply[G <: STObject : ClassTag, T : ClassTag](rdd: RDD[(G,T)], minmax: (Double, Double, Double, Double, Long, Long),
                                                    pointsOnly: Boolean): SpatioTempPartitioner[G] = {

    val sample = rdd.cache().sample(withReplacement = false, fraction = 0.01).map{case (g,_) => g}.collect()
    val sampleEnvs: Array[MBR] = sample.map(_.getGeo.getEnvelopeInternal)
    // spatial partitioner
    val rtreeParti: RTreePartitioner = RTreePartitioner(sampleEnvs, 10000, (minmax._1, minmax._2, minmax._3, minmax._4), pointsOnly)

    // assign ever element in the sample to a spatial partition and record the min and max temp value for each partition
    var i = 0
    val sPartitionsTempMinMax = new Array[(Instant,Instant)](rtreeParti.numPartitions)
    while(i < sample.length) {
      val curr = sample(i)
      val sID = rtreeParti.getPartitionId(curr)

      val currTemp = curr.getTemp.get
      val currStart = currTemp.start
      val currEnd = currTemp.end.getOrElse(StarkUtils.MAX_LONG_INSTANT)

      val newStart = if(sPartitionsTempMinMax(sID)._1 > currStart)  currStart else sPartitionsTempMinMax(sID)._1
      val newEnd = if(sPartitionsTempMinMax(sID)._2 < currEnd)  currEnd else sPartitionsTempMinMax(sID)._2

      sPartitionsTempMinMax(i) = (newStart, newEnd)

      i += 1
    }


    i = 0
    val partitions = new Array[(Cell, Array[Long])](sPartitionsTempMinMax.length)
    while(i < sPartitionsTempMinMax.length) {
      val (start,end) = sPartitionsTempMinMax(i)
      val tempPartitions = TemporalRangePartitioner.fixedRange(start.value,end.value, 10)

      val sCell = rtreeParti.partitionBounds(i)
      partitions(i) = (sCell, tempPartitions)

      i += 1
    }

    new SpatioTempPartitioner[G](partitions, minmax._1, minmax._2, minmax._3, minmax._4, pointsOnly)
  }


  def apply[G <: STObject : ClassTag, T : ClassTag](rdd: RDD[(G,T)], pointsOnly: Boolean = true): SpatioTempPartitioner[G] =
    SpatioTempPartitioner(rdd, SpatioTempPartitioner.getMinMax(rdd), pointsOnly)


}

class SpatioTempPartitioner[G <: STObject : ClassTag] private(partitions: Array[(Cell, Array[Long])],
                                                              _minX: Double, _maxX: Double, _minY: Double, _maxY: Double,
                                                              pointsOnly: Boolean)
  extends GridPartitioner(_minX, _maxX, _minX, _maxY) {

  private val _numPartitions = {
    var sum = 0
    var i = 0
    while(i < partitions.length) {
      sum += partitions(i)._2.length
      i += 1
    }
    sum
  }

  override def partitionBounds(idx: Int): Cell = partitions(idx)._1

  override def partitionExtent(idx: Int): NRectRange = partitions(idx)._1.extent

  override def getPartitionId(key: Any): Int = {
    val g = key.asInstanceOf[G]
    val c = g.getGeo.getCentroid
    val p = NPoint(c.getX, c.getY)

    var i = 0
    var minSDist = 0.0
    var minDistId = -1
    var offset = 0
    while(i < partitions.length) {
      if(partitions(i)._1.range.contains(p)) { // FOUND a spatial partition

        // now find a temporal partition in there that contains g's temp start
        val tID = TemporalRangePartitioner.getCellId(g.getTemp.get.start.value, partitions(i)._2)

        // compute the partition ID and return
        return offset + tID

      } else { // the current spatial partition does not contain g

        offset += partitions(i)._2.length // update offset: add current number of temp partitions

        // compute distance to nearest spatial partition - just in case no spatial partition will be found
        val d = partitions(i)._1.range.dist(p)
        if(d < minSDist || i == 0){
          minSDist = d
          minDistId = i
        }
      }

      i += 1
    }



    /* IF WE GET HERE no spatial partition found that contains p
     * assign to the one with the smallest distance
     */
    // 1. compute offset until the minDistId Partition
    offset = partitions.iterator.take(minDistId).map(_._2.length).sum
    // 2. find temporal partition for g
    val tId = TemporalRangePartitioner.getCellId(g.getTemp.get.start.value, partitions(minDistId)._2)

    // 3. return the according partition id
    offset + tId
  }

  override def printPartitions(fName: Path): Unit = ???



  override def numPartitions: Int = _numPartitions
}
