package dbis.stark.dbscan

import org.apache.spark.mllib.linalg.Vectors
import org.scalatest.{Matchers, FlatSpec}

/**
  * Created by kai on 28.01.16.
  */
class HistogramSpec extends FlatSpec with Matchers {
  "Histogram" should "create the given number of buckets" in {
    val hist1 = Histogram(5, 0.5)
    hist1.length should be (5)
    hist1(0).lb should be (0.0)
    hist1(0).ub should be (0.5)
    hist1(0).num should be (0)

    hist1(1).lb should be (0.5)
    hist1(1).ub should be (1.0)

    hist1(2).lb should be (1.0)
    hist1(2).ub should be (1.5)

    val hist2 = Histogram(5, 0.5, -5.0)
    hist2.length should be (5)
    hist2(0).lb should be (-5.0)
    hist2(0).ub should be (-4.5)
    hist2(1).lb should be (-4.5)
    hist2(1).ub should be (-4.0)
  }

  it should "update the frequencies from a list of values" in {
    val hist = Histogram(4, 10.0, 0.0)
    val values = List(0.0, 15.0, 20.0, 27.0, 1.0, 9.0, 10.0, 11.0, 29.5)
    hist.updateBuckets(values)
    hist(0).num should be (3)
    hist(1).num should be (3)
    hist(2).num should be (3)
    hist(3).num should be (0)
  }

  it should "merge two histograms" in {
    val hist1 = Histogram(4, 10.0, 0.0)
    val values1 = List(0.0, 15.0, 20.0, 27.0, 1.0, 9.0, 10.0, 11.0, 29.5)
    hist1.updateBuckets(values1)

    val hist2 = Histogram(4, 10.0, 0.0)
    val values2 = List(0.0, 15.0, 20.0, 27.0, 1.0, 9.0, 7.0, 11.0, 29.5, 35.1)
    hist2.updateBuckets(values2)

    val hist3 = hist1.mergeBuckets(hist2)

    hist3.length should be (4)
    for (i <- hist1.buckets.indices) {
      hist3(i).lb should be(hist1(i).lb)
      hist3(i).ub should be(hist1(i).ub)
    }
    hist3(0).num should be (7)
    hist3(1).num should be (5)
    hist3(2).num should be (6)
    hist3(3).num should be (1)
  }
}