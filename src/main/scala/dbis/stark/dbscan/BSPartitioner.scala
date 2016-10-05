package dbis.stark.dbscan

import dbis.stark.spatial.{NRectRange, NPoint}
import dbis.stark.spatial.partitioner.BSP
import org.apache.log4j.Logger
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import dbis.stark.spatial.Cell

/**
  * Created by kai on 10.02.16.
  */
class BSPartitioner extends Partitioner with java.io.Serializable {
  var cellSize: Double = 0.0
  var mbb: MBB = MBB.zero
  var maxPoints: Int = 0
  var cellHistogram: Array[(NRectRange, Int)] = Array()
  
  implicit def npointToVector(n: NPoint) = Vectors.dense(n.c)

  def setMBB(m: MBB) = {
    this.mbb = m
    this
  }
  def setCellSize(v: Double) = {
    this.cellSize = v
    this
  }

  def computeHistogam[K, T : ClassTag](input: RDD[ClusterPoint[K, T]], eps: Double) = {
    def diffVector(v1: Vector, v2: Vector): Vector = {
      val v = new Array[Double](Math.max(v1.size, v2.size))
      for (i <- v.indices) v(i) = v1(i) - v2(i)
      Vectors.dense(v)
    }

    // make sure the required parameters are initialized already
    require(cellSize > 0 && (mbb.minVec != Vectors.zeros(0) || mbb.maxVec != Vectors.zeros(0)))

    // determine the max size of all dimensions
    val maxSize = diffVector(mbb.maxVec, mbb.minVec).toArray.toList.max

    // we use the regular grid partitioner to split the global MBB into MBBs of equal sizes
//    val gridPartitioner = new GridPartitioner()
//      .setMBB(mbb)
//      .setPPD((maxSize / eps).toInt) // number of partitions = maxSize of all dimensions / epsilon
//    val partitionMBBs = gridPartitioner.computePartitioning()

    
    val qGridPartitioner = new QuadGridPartitioner(mbb, cellSize)
    val partitionMBBs = qGridPartitioner.computePartitioning()
    
//    log.info(s"BSPartitioner: compute histogram for ${partitionMBBs.size} cells")

    // then we count the points per partition
    cellHistogram = input.aggregate(CellHistogram.zero(partitionMBBs))(CellHistogram.seq, CellHistogram.combine).buckets

//    logInfo(s"BSPartitioner: histogram constructed")
    this
  }

  def setMaxNumPoints(n: Int) = {
    this.maxPoints = n
    this
  }

  def computePartitioning(): List[MBB] = {
    require(cellHistogram.nonEmpty && maxPoints > 0)
    
//    cellHistogram.map{ case (r,cnt) => s"${r.ll(0)}, ${r.ll(1)}, ${r.ur(0)}, ${r.ur(1)}"}.foreach(log.warn)
    
//    logInfo(s"produced ${cellHistogram.size} cells")
    
//    logInfo(s"num points according to hist: ${cellHistogram.map(_._2).sum}")
    
    val bsp = new BSP(mbb.minVec.toArray, mbb.maxVec.toArray,
      cellHistogram.map{ case (r,i) => (Cell(r),i)}, // _cellHistogram: Array[(NRectRange, Int)],
      cellSize, // 2 * eps
      maxPoints.toDouble,
      withExtent = false)

    bsp.partitions.map{ rrange => MBB(rrange.range.ll, rrange.range.ur) }.toList
  }
}
