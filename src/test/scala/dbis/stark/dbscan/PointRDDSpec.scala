package dbis.stark.dbscan

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._

class PointRDDSpec extends FlatSpec with Matchers with BeforeAndAfter {
  var sc: SparkContext = _
  var conf: SparkConf = _

  before {
    // to avoid Akka rebinding to the same port, since it doesn't unbind
    // immediately after shutdown
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")
    conf = new SparkConf().setMaster("local").setAppName(getClass.getSimpleName)
    sc = new SparkContext(conf)
  }

  after {
    // cleanup SparkContext data
    sc.stop()
    sc = null
    conf = null
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")
  }

  "Points" should "be created from RDD[Vector]" in {
    val rdd = sc.parallelize(Array(Vectors.dense(1.0, 1.0),
      Vectors.dense(2.0, 2.0),
      Vectors.dense(3.0, 3.0),
      Vectors.dense(4.0, 4.0)))
    val res = rdd.map(p => ClusterPoint(p))
    res.collect() should be (Array(
      ClusterPoint(Vectors.dense(1.0, 1.0), 0, ClusterLabel.Unclassified),
      ClusterPoint(Vectors.dense(2.0, 2.0), 0, ClusterLabel.Unclassified),
      ClusterPoint(Vectors.dense(3.0, 3.0), 0, ClusterLabel.Unclassified),
      ClusterPoint(Vectors.dense(4.0, 4.0), 0, ClusterLabel.Unclassified)))
  }
}
