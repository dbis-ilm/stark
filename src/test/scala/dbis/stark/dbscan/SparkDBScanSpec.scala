package dbis.stark.dbscan

import dbis.stark.StarkTestUtils
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._

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

//  "BSPartitioner"
  it  should "derive a partitioning" in {
    val data = sc.textFile("src/test/resources/labeled_data.csv")
      .map(line => line.split(","))
      .map(t => Vectors.dense(t(0).toDouble, t(1).toDouble))

    val cellSize = 0.3
      
    val dbscan = new DBScan[Long,Int]().setEpsilon(cellSize).setMinPts(10)
    val mbb = dbscan.getGlobalMBB(data)

    val cdata = data.zipWithUniqueId().map{ case (v,id) => ClusterPoint[Long, Int](id, v, payload = Some(0))}
    val partitioner = new BSPartitioner()
      .setMBB(mbb)
      .setCellSize(cellSize)
      .setMaxNumPoints(50)
      .computeHistogam(cdata, cellSize)
    
      
    val partitions = partitioner.computePartitioning()
//    println(s"${partitions.length} partitions: ---> \n${partitions.mkString("\n")}")
    partitioner.cellHistogram.length shouldBe 196
//    partitions.size shouldBe 26
    
  }

//  "SparkDBScan"
  it  should "find a clustering with grid partitioning" in {
    StarkTestUtils.rmrf("grid-results")
    StarkTestUtils.rmrf("grid-mbbs")

    val dbscan = new DBScan[Long,List[String]]().setEpsilon(0.3).setMinPts(10).setPPD(4)
    val data = sc.textFile("src/test/resources/labeled_data.csv")
      .map(line => line.split(","))
      .zipWithUniqueId()
      .map{ case (t, id) => (id, Vectors.dense(t(0).toDouble, t(1).toDouble), List(t(2))) }

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
    StarkTestUtils.rmrf("bsp-results")
    StarkTestUtils.rmrf("bsp-mbbs")

    val dbscan = new DBScan[Long, List[String]]().setEpsilon(0.3).setMinPts(10).setMaxPartitionSize(100)
    val data = sc.textFile("src/test/resources/labeled_data.csv")
      .map(line => line.split(","))
      .zipWithUniqueId()
      .map{ case (t,id) => (id,Vectors.dense(t(0).toDouble, t(1).toDouble), List(t(2)))}

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
