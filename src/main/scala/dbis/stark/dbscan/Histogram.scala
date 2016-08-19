package dbis.stark.dbscan

/**
  * Represents a bucket in a histogram.
  *
  * @param lb lower bound of the bucket
  * @param ub upper bound of the bucket
  * @param num frequency of values in the interval [lb,ub]
  */
class Bucket(val lb: Double, val ub: Double, var num: Int) extends java.io.Serializable {
  override def toString: String = s"$lb,$ub,$num"
}

case class Histogram(buckets: Array[Bucket], width: Double) extends java.io.Serializable {

  def length = buckets.length

  def apply(idx: Int): Bucket = buckets(idx)

  /**
    * Updates the histogram (the frequency values) from the list of distance values.
    *
    * @param values a list of values used to update the histogram
    */
  def updateBuckets(values: Iterable[Double]): Unit = {
    values.foreach(v => {
      val idx = (v / width).toInt
      val bucket = buckets(idx)
      bucket.num += 1
    })
  }

  /**
    * Combines two histograms by adding the frequency values of the corresponding
    * buckets.
    *
    * @param other the other histogram
    * @return the resulting histogram
    */
  def mergeBuckets(other: Histogram): Histogram = {
    val res = Histogram(buckets.size, width)
    for (i <- buckets.indices) {
      res.buckets(i) = new Bucket(buckets(i).lb, buckets(i).ub, buckets(i).num + other.buckets(i).num)
    }
    res
  }

}

object Histogram {
  def apply(nBuckets: Int, width: Double, begin: Double = 0.0): Histogram = {
    val buckets = new Array[Bucket](nBuckets)
    val histo = new Histogram(buckets, width)
    for (i <- buckets.indices) {
      buckets(i) = new Bucket(i * width + begin, (i + 1) * width + begin, 0)
    }
    histo
  }
}