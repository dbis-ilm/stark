package dbis.stark.dbscan

import dbis.stark.spatial.partitioner.BSP2
import dbis.stark.spatial.{Cell, NPoint, NRectRange}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

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
    
    // then we count the points per partition
    cellHistogram = input.aggregate(CellHistogram.zero(partitionMBBs))(CellHistogram.seq, CellHistogram.combine).buckets

    this
  }

  def setMaxNumPoints(n: Int) = {
    this.maxPoints = n
    this
  }

  def computePartitioning(): List[MBB] = {
    require(cellHistogram.nonEmpty && maxPoints > 0)
    
    val start = NRectRange(NPoint(mbb.minVec.toArray), NPoint(mbb.maxVec.toArray))
    val a = cellHistogram.zipWithIndex.map{ case ((r,cnt),idx) => idx -> (Cell(r),cnt)}

    val bsp = new BSP2(start, dbis.stark.spatial.partitioner.CellHistogram(a), cellSize, _pointsOnly = true, maxPoints.toDouble, None)

    bsp.partitions.map{ rrange => MBB(rrange.range.ll, rrange.range.ur) }.toList
  }
}
