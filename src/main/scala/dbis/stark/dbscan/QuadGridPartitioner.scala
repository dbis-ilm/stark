package dbis.stark.dbscan

import org.apache.spark.mllib.linalg.Vectors
import scala.collection.mutable.ListBuffer

/**
  * Created by kai on 10.02.16.
  */
class QuadGridPartitioner(mbb: MBB, width: Double) extends Partitioner with java.io.Serializable{
//  var mbb: MBB = MBB.zero
//
//  def setMBB(m: MBB) = {
//    this.mbb = m
//    this
//  }
  
  private val dim = mbb.maxVec.size
  
  val numCellsPerDim = (0 until dim).map { dim => 
    Math.ceil((mbb.maxVec(dim) - mbb.minVec(dim)) / width).toInt 
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
      for (i <- 0 until p) v /= numCellsPerDim(i)
      v % numCellsPerDim(p)
    }

    /**
      * Constructs a MBB for partition number p with the given widths in
      * each dimension.
      *
      * @param p the partition number
      * @param widths the array of widths for each dimension
      * @return the partition MBB
      */
    def constructMBB(p: Int): MBB = {
      val minVals = new Array[Double](dim)
      val maxVals = new Array[Double](dim)

      // for each dimension
      for (i <- 0 until dim) {
        val d = digit(p, i)
        minVals(i) = mbb.minVec(i) + d * width
        maxVals(i) = mbb.minVec(i) + (d + 1) * width
      }
      MBB(Vectors.dense(minVals), Vectors.dense(maxVals))
    }

    val partitionList = ListBuffer[MBB]()

    // we compute the width of a grid cell in each dimension
//    val widths = new Array[Double](mbb.minVec.size)
//    for (i <- 0 until mbb.minVec.size) {
//      widths(i) = (mbb.maxVec(i) - mbb.minVec(i)) / ppd.toDouble
//    }
    
    // for a d dimensional data set we need ppd^d MBBs
    // let's enumerate them
    val numMBBs = numCellsPerDim.reduce(_*_) //Math.pow(ppd, widths.length).toInt
    var pos = 0
    for (i <- 0 until numMBBs) {
      // and construct a MBB for this partition
      partitionList += constructMBB(i)
    }
    partitionList.toList
  }


}
