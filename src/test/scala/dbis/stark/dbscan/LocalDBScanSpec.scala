package dbis.stark.dbscan

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.apache.spark.mllib.linalg.{Vectors, Vector}


class LocalDBScanSpec extends FlatSpec with Matchers {
  def euclideanDistance(v1: Vector, v2: Vector): Double = {
    var d = 0.0
    for (i <- 0 until v1.size) { d += (v1(i) - v2(i)) * (v1(i) - v2(i)) }
    Math.sqrt(d)
  }

  "LocalDBScan" should "expand a 2D cluster by points from a list" in {
    val dbscan = new DBScan[Int, Int]()
      .setEpsilon(150)
      .setMinPts(3)
      .setDistanceFunc(euclideanDistance)

    val points = List(
      ClusterPoint[Int, Int](0,Vectors.dense(178.505256857926,13.0232102015073)),
      ClusterPoint[Int, Int](1,Vectors.dense(192.647916294684,-54.9397894514337)),
      ClusterPoint[Int, Int](2,Vectors.dense(221.447056375756,54.7299660721257)),
      ClusterPoint[Int, Int](3,Vectors.dense(27.3622293956578,-1401.0191693902)),
      ClusterPoint[Int, Int](4,Vectors.dense(88.692161857309,59.1680534628075)),
      ClusterPoint[Int, Int](5,Vectors.dense(-10.9376996678452,57.462273892661)),
      ClusterPoint[Int, Int](6,Vectors.dense(1125.10539018549,-99.3853457272053)),
      ClusterPoint[Int, Int](7,Vectors.dense(52.7498085418551,-9.04225181250282)),
      ClusterPoint[Int, Int](8,Vectors.dense(57.0464906188891,63.3980855359247)),
      ClusterPoint[Int, Int](9,Vectors.dense(9.09417446372015,-140.061523108837))
    )
    val start = ClusterPoint[Int, Int](10,Vectors.dense(52.7498085418551,-9.04225181250282))

    val res = dbscan.expandCluster(start, 42, points.toStream)

    res should be (true)

    val expectedClusterIDs = List(42,42,42,0,42,42,0,42,42,42)
    points.zip(expectedClusterIDs).foreach{ case (p, c) =>
      withClue("cluster id ") { p.clusterId shouldBe c }
    }
  }

  it should "not expand a cluster for a noise point" in {
    val dbscan = new DBScan[Int, Int]()
      .setEpsilon(150)
      .setMinPts(3)
      .setDistanceFunc(euclideanDistance)

    val points = List(
      ClusterPoint[Int, Int](0,Vectors.dense(178.505256857926,13.0232102015073)),
      ClusterPoint[Int, Int](1,Vectors.dense(192.647916294684,-54.9397894514337)),
      ClusterPoint[Int, Int](2,Vectors.dense(221.447056375756,54.7299660721257)),
      ClusterPoint[Int, Int](3,Vectors.dense(27.3622293956578,-1401.0191693902)),
      ClusterPoint[Int, Int](4,Vectors.dense(88.692161857309,59.1680534628075)),
      ClusterPoint[Int, Int](5,Vectors.dense(-10.9376996678452,57.462273892661)),
      ClusterPoint[Int, Int](6,Vectors.dense(1125.10539018549,-99.3853457272053)),
      ClusterPoint[Int, Int](7,Vectors.dense(52.7498085418551,-9.04225181250282)),
      ClusterPoint[Int, Int](8,Vectors.dense(57.0464906188891,63.3980855359247)),
      ClusterPoint[Int, Int](9,Vectors.dense(9.09417446372015,-140.061523108837))
    )
    val start = ClusterPoint[Int, Int](10,Vectors.dense(27.3622293956578,-1401.01916939022))
    val res = dbscan.expandCluster(start, 42, points.toStream)

    res should be (false)
  }

   it should "compute a local clustering for 2D data" in {
    // TODO
  }

  it should "return a list of containing partitions" in {
    val dbscan = new DBScan[Int, Int]()
    val point1 = Vectors.dense(20.0, 20.0)
    val point2 = Vectors.dense(45.0, 70.0)
    val point3 = Vectors.dense(45.0, 45.0)

    val partitionList = List(MBB(Vectors.dense(10.0, 10.0), Vectors.dense(50.0, 50.0)),
      MBB(Vectors.dense(10.0, 40.0), Vectors.dense(50.0, 80.0)),
      MBB(Vectors.dense(40.0, 40.0), Vectors.dense(80.0, 80.0)))
    dbscan.containingPartitions(partitionList, point1) should be (List(0))
    dbscan.containingPartitions(partitionList, point2) should be (List(1, 2))
    dbscan.containingPartitions(partitionList, point3) should be (List(0, 1, 2))
  }

  it should "update cluster ids of points using a mapping table" in {
    val dbscan = new DBScan[Int, Int]()
    val map = Map[Int, Int](2 -> 10, 3 -> 10, 4 -> 10, 5 -> 10, 6 -> 20, 7 -> 20)
    dbscan.mapClusterId(ClusterPoint[Int,Int](1,Vectors.zeros(2), 2, ClusterLabel.Core, None, isMerge = true), map).clusterId should be (10)
    dbscan.mapClusterId(ClusterPoint[Int,Int](2,Vectors.zeros(2), 4, ClusterLabel.Core, None, isMerge = true), map).clusterId should be (10)
    dbscan.mapClusterId(ClusterPoint[Int,Int](3,Vectors.zeros(2), 6, ClusterLabel.Core, None, isMerge = true), map).clusterId should be (20)
    dbscan.mapClusterId(ClusterPoint[Int,Int](4,Vectors.zeros(2), 1, ClusterLabel.Core, None, isMerge = true), map).clusterId should be (1)
  }

  it should "compute a global mapping table from a list of mappings" in {
    val localMappings = Array((2, 3), (3, 4), (5, 3), (6, 7))
    val dbscan = new DBScan()
    val map = dbscan.computeGlobalMapping(localMappings)
    map should be (Map(2 -> 100001, 3 -> 100001, 4 -> 100001, 5 -> 100001, 6 -> 100002, 7 -> 100002))
  }

  it should "compute a simple 2D grid partitioning" in {
    val dbscan = new DBScan()
    val mbb = MBB(Vectors.dense(0.0, 0.0), Vectors.dense(100.0, 100.0))
    val partitioner = new GridPartitioner().setMBB(mbb).setPPD(2)
    val partitionList = partitioner.computePartitioning()
    partitionList.length should be (4)
    partitionList should contain theSameElementsAs List(MBB(Vectors.dense(0.0, 0.0), Vectors.dense(50.0, 50.0)),
      MBB(Vectors.dense(50.0, 0.0), Vectors.dense(100.0, 50.0)),
      MBB(Vectors.dense(0.0, 50.0), Vectors.dense(50.0, 100.0)),
      MBB(Vectors.dense(50.0, 50.0), Vectors.dense(100.0, 100.0)))
  }
}