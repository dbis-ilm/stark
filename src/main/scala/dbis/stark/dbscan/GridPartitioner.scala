package dbis.stark.dbscan

import org.apache.spark.mllib.linalg.Vectors
import scala.collection.mutable.ListBuffer

/**
  * Created by kai on 10.02.16.
  */
class GridPartitioner extends Partitioner with java.io.Serializable{
  var ppd: Int = 4
  var mbb: MBB = MBB.zero

  def setMBB(m: MBB) = {
    this.mbb = m
    this
  }
  def setPPD(p: Int) = {
    this.ppd = p
    this
  }

  /**
    * Creates all MBBs for a simple (equal-sized) grid partitioning with ppd partitions per dimension.
    *
    * @param mbb the global MBB describing the entire dataset
    * @param ppd the number of partitions per dimension
    * @return the list of MBBs describing the partitions
    */
  def computePartitioning(): List[MBB] = {
    /**
      * Compute the "digit" at position p of number n with a ppd as base.
      * This function is used to get the individual dimension values from
      * a number, e.g. for 2 dimensions with ppd=2 we enumerate from 0..3
      * to get the indexes (0, 0), (0, 1), (1, 0), (1, 1).
      *
      * @param n the enumeration number
      * @param p the position of the digit
      * @return the digit at this position regarding base ppd
      */
    def digit(n: Int, p: Int): Int = {
      var v = n
      for (i <- 0 until p) v /= ppd
      v % ppd
    }

    /**
      * Constructs a MBB for partition number p with the given widths in
      * each dimension.
      *
      * @param p the partition number
      * @param widths the array of widths for each dimension
      * @return the partition MBB
      */
    def constructMBB(p: Int, widths: Array[Double]): MBB = {
//      val indices = new Array[Int](widths.length)
      val minVals = new Array[Double](widths.length)
      val maxVals = new Array[Double](widths.length)

      // for each dimension
      for (i <- widths.indices) {
        val d = digit(p, i)
        minVals(i) = mbb.minVec(i) + d * widths(i)
        maxVals(i) = mbb.minVec(i) + (d + 1) * widths(i)
      }
      MBB(Vectors.dense(minVals), Vectors.dense(maxVals))
    }

    val partitionList = ListBuffer[MBB]()

    // we compute the width of a grid cell in each dimension
    val widths = new Array[Double](mbb.minVec.size)
    for (i <- 0 until mbb.minVec.size) {
      widths(i) = (mbb.maxVec(i) - mbb.minVec(i)) / ppd.toDouble
    }
    // for a d dimensional data set we need ppd^d MBBs
    // let's enumerate them
    val numMBBs = Math.pow(ppd, widths.length).toInt
    var pos = 0
    for (i <- 0 until numMBBs) {
      // and construct a MBB for this partition
      partitionList += constructMBB(i, widths)
    }
    partitionList.toList
  }


}
