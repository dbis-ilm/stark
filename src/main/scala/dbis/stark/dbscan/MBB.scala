package dbis.stark.dbscan

import org.apache.spark.mllib.linalg.{DenseVector, Vectors, Vector}

/**
  * An MBB represents a minimum bounding box around a set of points in an n-dimensional space.
  *
  * @param minVec a vector representing the lower left (in 2D) corner of the MBB
  * @param maxVec a vector representing the upper right (in 2D) corner of the MBB
  */
case class MBB(minVec: Vector, maxVec: Vector) {
  /**
    * Expand the box by the value epsilon in all dimensions.
    *
    * @param eps the epsilon distance for expansion
    * @return the updated MBB
    */
  def expand(eps: Double): MBB = {
    val v1 = this.minVec.toArray
    val v2 = this.maxVec.toArray
    for (i <- v1.indices) v1(i) -= eps
    for (i <- v2.indices) v2(i) += eps
    this
  }

  /**
    * Checks whether the MBB contains the given point.
    *
    * @param v the vector of coordinates of the point
    * @return true if the point is contained in the MBB, otherwise false
    */
  def contains(v: Vector): Boolean = {
    val v1 = this.minVec.toArray
    val v2 = this.maxVec.toArray
    var res: Boolean = true
    for (i <- v1.indices) res &= (v1(i) <= v(i) && v(i) <= v2(i))
    res
  }

  /**
    * Returns a string representation of a MBB.
    *
    * @return the string representation
    */
  override def toString(): String = s"${this.minVec.toArray.mkString(",")},${this.maxVec.toArray.mkString(",")}"
}

/**
  * The companion object of MBB.
  */
object MBB {
  /**
    * Returns a zero MBB. A zero MBB is distinguished from a MBB where the minimum corner
    * is (0.0, 0.0, 0.0, ...) by its size 0.
    *
    * @return a zero MBB
    */
  def zero: MBB = new MBB(Vectors.zeros(0),Vectors.zeros(0))

  /**
    * Returns vector representing the component-wise minima.
    *
    * @param v1 the first parameter vector
    * @param v2 the first parameter vector
    * @return a vector with minimum components of v1 and v2
    */
  def minVector(v1: Vector, v2: Vector): Vector = {
    if (v1.size == 0) v2
    else if (v2.size == 0) v1
    else {
      val v = new Array[Double](Math.max(v1.size, v2.size))
      for (i <- v.indices) v(i) = Math.min(v1(i), v2(i))
      Vectors.dense(v)
    }
  }

  /**
    * Returns vector representing the component-wise maxima.
    *
    * @param v1 the first parameter vector
    * @param v2 the first parameter vector
    * @return a vector with maximum components of v1 and v2
    */
  def maxVector(v1: Vector, v2: Vector): Vector = {
    if (v1.size == 0) v2
    else if (v2.size == 0) v1
    else {
      val v = new Array[Double](Math.max(v1.size, v2.size))
      for (i <- v.indices) v(i) = Math.max(v1(i), v2(i))
      Vectors.dense(v)
    }
  }

  /**
    * A helper function for combining a MBB and a single vector. Used mainly for RDD.aggregate.
    *
    * @param m a MBB
    * @param v a vector
    * @return a new MBB as a combination of m and the vector
    */
  def mbbSeq(m: MBB, v: Vector): MBB = MBB(minVector(m.minVec, v), maxVector(m.maxVec, v))

  /**
    * A helper function for combining two MBBs. Used mainly for RDD.aggregate.
    *
    * @param m1 a MBB
    * @param m2 a second MBB
    * @return a new MBB as a combination of m1 and m2
    */
  def mbbComb(m1: MBB, m2: MBB): MBB = MBB(minVector(m1.minVec, m2.minVec), maxVector(m1.maxVec, m2.maxVec))
}