package dbis.stark.spatial

import dbis.stark.{Distance, STObject}

import scala.collection.mutable.ListBuffer


case class KNN[PayloadT](k: Int) extends Serializable with Cloneable with Iterable[(Distance, PayloadT)] {

  override def toString() = s"""KNN(k=$k, m=$m, min=$posMin, max=$posMax, nn=${nn.mkString(",")}"""

  var nn = new Array[(Distance, PayloadT)](k)
  protected[stark] var posMax = -1
  private[stark] var posMin = -1
  private[stark] var m = -1


  def min = nn(posMin)
  def max = nn(posMax)
  def full = m >= k
  def empty = m < 0

  override def size = if(full) k else m + 1

  def sortedArray = nn.sortBy(_._1)

  protected[spatial] def set(nns: IndexedSeq[(Distance, PayloadT)]) = {
//    require(nns.length == k, "provided list must have exactly length k")

//    println(s"before   set: $this")

    var i = 0
    var maxPos = -1
    var minPos = -1
    while(i < nns.length) {
      nn(i) = nns(i)
      if(i == 0) {
        minPos = 0
        maxPos = 0
      } else {
        if(nns(i)._1 > nn(maxPos)._1)
          maxPos = i

        if(nns(i)._1 < nn(minPos)._1)
          minPos = i
      }

      i += 1
    }

    m = nns.length - 1
    posMax = maxPos
    posMin = minPos

//    println(s"after set: $this")
  }

  def insert(tuple: (Distance,PayloadT)) = {
    val pos = m+1
    if(pos < k) {
      nn(pos) = tuple
      if(posMax < 0 || nn(posMax)._1 < tuple._1) {
        posMax = pos
      }

      if(posMin < 0 || nn(posMin)._1 > tuple._1)
        posMin = pos

      m += 1
    } else if(nn(posMax)._1 > tuple._1) {
      nn(posMax) = tuple

      resetMinMax()
    }
  }

  override def iterator: Iterator[(Distance, PayloadT)] = new Iterator[(Distance,PayloadT)] {
    var j = 0
    override def hasNext: Boolean = j <= m && j < k

    override def next(): (Distance, PayloadT) = {
      val elem = nn(j)
      j += 1
      elem
    }
  }

  override def clone(): KNN[PayloadT] = {
    val arr = new Array[(Distance, PayloadT)](k)

    Array.copy(nn,0,arr,0,k)

    val newKnn = new KNN[PayloadT](k)
    newKnn.m = m
    newKnn.posMax = posMax
    newKnn.posMin = posMin
    newKnn.nn = arr
    newKnn
  }

  def merge(other: KNN[PayloadT]): KNN[PayloadT] = {
    if(empty)
      other.clone()
    else if(other.empty || (full && other.min._1 > max._1))
      this.clone()
    else {
//    else if(other.empty || (full && other.min._1 > max._1))
//      this.clone()
//    else {
      val knn = this.clone()

      other.iterator.foreach(knn.insert)

      knn
    }
  }

  private def resetMinMax() = {
    var i  = 0
    while(i < m+1) {
      if(nn(i)._1 > nn(posMax)._1)
        posMax = i

      if(nn(i)._1 < nn(posMin)._1)
        posMin = i

      i += 1
    }
  }

}

object KNN {

  /**
    * Merge the first k elements (ordered by their distance) into a single array
    *
    * The idea is that we have different kNN results from partition-local computations which need
    * to be merged into a single result.
    * The inner arrays must be sorted by their distances and basically perform a merge like in MergeJoin
    *
    * @param partialResults kNN results from partition-local execution
    * @param k The number of nearest neighbors to find
    **/
  def merge[G <: STObject, V](_partialResults: Array[Array[((G,V),Distance)]], k: Int): Iterator[(G,(Distance,V))] = {

    if(_partialResults.isEmpty)
      return Iterator.empty

    val result = ListBuffer.empty[(G,(Distance,V))]
    val partialResults = _partialResults.filter(_.nonEmpty)
    val ptrs = partialResults.map(_ => 0)

    // as long as we don't have enough result elements and there is one local result with more elements
    // continue to get one
    while(result.size < k && ptrs.zipWithIndex.exists { case (ptr, idx) => ptr < partialResults(idx).length }) {

      // the current minimum distance value and its index
      var minDist: Distance = null
      var minDistIdx = -1
      var i = 0

      // iterate over every single local result
      while(i < partialResults.length) {

        val ptr = ptrs(i)
        val partialResultLength = partialResults(i).length

        // check if the current position for that local result is smaller than current min value
        if((i == 0 && ptr < partialResultLength) || (ptr < partialResultLength) && (partialResults(i)(ptr)._2 < minDist)) {
          minDist = partialResults(i)(ptr)._2
          minDistIdx = i
        }

        i += 1
      }

      // get the element with minimum value in this iteration
      val t = partialResults(minDistIdx)(ptrs(minDistIdx))
      result += ((t._1._1, (minDist, t._1._2)))

      // increase pointer for the local result where we found the mininum
      ptrs(minDistIdx) = ptrs(minDistIdx) + 1
    }

    result.iterator
  }

}