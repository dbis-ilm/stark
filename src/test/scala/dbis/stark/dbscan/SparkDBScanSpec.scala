package dbis.stark.dbscan

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._
import dbis.stark.TestUtils

class SparkDBScanSpec extends FlatSpec with Matchers with BeforeAndAfter {
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

  "BSPartitioner" should "derive a partitioning" in {
    val data = sc.textFile("src/test/resources/labeled_data.csv")
      .map(line => line.split(","))
      .map(t => (Vectors.dense(t(0).toDouble, t(1).toDouble)))

    val dbscan = new DBScan[Int]().setEpsilon(0.3).setMinPts(10)
    val mbb = dbscan.getGlobalMBB(data)

    val cdata = data.map(v => ClusterPoint[Int](v, payload = Some(0)))
    val partitioner = new BSPartitioner()
      .setMBB(mbb)
      .setCellSize(0.3)
      .setMaxNumPoints(50)
      .computeHistogam(cdata, 0.3)
    val partitions = partitioner.computePartitioning()
    println(s"${partitions.length} partitions: ---> ${partitions.mkString(",")}")
  }

  "SparkDBScan" should "find a clustering with grid partitioning" in {
    TestUtils.rmrf("grid-results")
    TestUtils.rmrf("grid-mbbs")

    val dbscan = new DBScan[List[String]]().setEpsilon(0.3).setMinPts(10).setPPD(4)
    val data = sc.textFile("src/test/resources/labeled_data.csv")
      .map(line => line.split(","))
      .map(t => (Vectors.dense(t(0).toDouble, t(1).toDouble), List(t(2))))

    val model = dbscan.run(data)
    model.points.coalesce(1).saveAsTextFile("grid-results")
    model.mbbs.coalesce(1).saveAsTextFile("grid-mbbs")

    /* access payload:
     model.points.coalesce(1).collect().map{ p => p.payload.get.asInstanceOf[List[String]].toString}.foreach(println)
     */

    /* TODO: compare results/part-00000 to data/labeled_data.csv -> requires a mapping of cluster ids
    val lines = scala.io.Source.fromFile("data/labeled_data.csv").getLines
    val expected = lines.map(l => l.split(","))
      .map(t => LabeledPoint(Vectors.dense(t(0).toDouble, t(1).toDouble), t(2).toInt)).toList

    model.points.coalesce(1).collect() should contain theSameElementsAs(expected)
    */
  }

  it should "find a clustering with binary space partitioning" in {
    TestUtils.rmrf("bsp-results")
    TestUtils.rmrf("bsp-mbbs")

    val dbscan = new DBScan[List[String]]().setEpsilon(0.3).setMinPts(10).setMaxPartitionSize(100)
    val data = sc.textFile("src/test/resources/labeled_data.csv")
      .map(line => line.split(","))
      .map(t => (Vectors.dense(t(0).toDouble, t(1).toDouble), List(t(2))))

    val model = dbscan.run(data)
    model.points.coalesce(1).saveAsTextFile("bsp-results")
    model.mbbs.coalesce(1).saveAsTextFile("bsp-mbbs")

    /* access payload:
     model.points.coalesce(1).collect().map{ p => p.payload.get.asInstanceOf[List[String]].toString}.foreach(println)
     */

    /* TODO: compare results/part-00000 to data/labeled_data.csv -> requires a mapping of cluster ids
    val lines = scala.io.Source.fromFile("data/labeled_data.csv").getLines
    val expected = lines.map(l => l.split(","))
      .map(t => LabeledPoint(Vectors.dense(t(0).toDouble, t(1).toDouble), t(2).toInt)).toList

    model.points.coalesce(1).collect() should contain theSameElementsAs(expected)
    */
  }
}
