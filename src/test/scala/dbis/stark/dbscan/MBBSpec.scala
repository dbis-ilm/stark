package dbis.stark.dbscan

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._

import scala.collection.mutable.ListBuffer

class MBBSpec extends FlatSpec with Matchers with BeforeAndAfter {
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

  "MBB" should "allow to construct zeros" in {
    val m = MBB.zero
    m.minVec should be (Vectors.zeros(0))
    m.maxVec should be (Vectors.zeros(0))
  }

  it should "compute the vectors incrementally" in {
    var m = MBB.zero
    m = MBB.mbbSeq(m, Vectors.dense(4.0, 1.0))
    m = MBB.mbbSeq(m, Vectors.dense(5.0, 4.0))
    m = MBB.mbbSeq(m, Vectors.dense(1.0, 4.5))
    m should be (MBB(Vectors.dense(1.0, 1.0), Vectors.dense(5.0, 4.5)))
  }

  it should "be expandible by eps" in {
    val m = MBB(Vectors.dense(1.0, 2.0), Vectors.dense(10.0, 15.0))
    m.expand(0.5)
    m should be (MBB(Vectors.dense(0.5, 1.5), Vectors.dense(10.5, 15.5)))
  }

  it should "check containment of points" in {
    val mbb = MBB(Vectors.dense(10.0, 10.0), Vectors.dense(70.0, 100.0))
    mbb.contains(Vectors.dense(15.0, 30.0)) should be (true)
    mbb.contains(Vectors.dense(0.0, 90.0)) should be (false)
    mbb.contains(Vectors.dense(80.0, 50.0)) should be (false)
    mbb.contains(Vectors.dense(60.0, 95.0)) should be (true)

    val mbb2 = MBB(Vectors.dense(-2.274474267187038,-1.8238006773251054),
      Vectors.dense(1.87043802693919,2.245794191703138))

    mbb2.contains(Vectors.dense(1.87043802693919,-0.564764189614707)) should be (true)
  }

  "Global MBB" should "be calculated from a small RDD" in {
    val input = sc.parallelize(Array(Vectors.dense(1.0, 1.0),
      Vectors.dense(2.0, 2.0),
      Vectors.dense(3.0, 3.0),
      Vectors.dense(4.0, 4.0)))
    val scan = new DBScan()
    val mbb = scan.getGlobalMBB(input)
    mbb.minVec should be (Vectors.dense(1.0, 1.0))
    mbb.maxVec should be (Vectors.dense(4.0, 4.0))
  }

  it should "be calculated from a larger RDD" in {
    val input = RandomRDDs.poissonVectorRDD(sc, 500.0 /* mean */,
      100000, /* num_rows */
      2, /* num_cols */
      10 /* num_partitions */)
    val scan = new DBScan()
    val mbb = scan.getGlobalMBB(input)

    /* An alternative way of calculating the MBB of the RDD data */
    val maxValues = ListBuffer.empty[Double]
    val minValues = ListBuffer.empty[Double]
    for(i <- 0 until 2) {
      maxValues += input.map { _(i) }.max()
      minValues += input.map { _(i) }.min()
    }
    mbb.minVec should be (Vectors.dense(minValues.toArray))
    mbb.maxVec should be (Vectors.dense(maxValues.toArray))
  }
}

