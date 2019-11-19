package dbis.stark.spatial.partitioner

import dbis.stark.{STObject, StarkKryoRegistrator, StarkTestUtils}
import dbis.stark.spatial.JoinPredicate
import dbis.stark.spatial.indexed.RTreeConfig
import dbis.stark.spatial.indexed.live.LiveIndexedSpatialRDDFunctions
import org.apache.spark.SpatialRDD._
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.tagobjects.Slow
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class RTreePartitionerTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  private var sc: SparkContext = _

  override def beforeAll() {
    val conf = new SparkConf().setMaster(s"local[${Runtime.getRuntime.availableProcessors()}]").setAppName("paritioner_test2")
    conf.set("spark.serializer", classOf[KryoSerializer].getName)
    conf.set("spark.kryo.registrator", classOf[StarkKryoRegistrator].getName)
    sc = new SparkContext(conf)
  }

  override def afterAll() {
    if(sc != null)
      sc.stop()
  }

  it should "create simple partitions" in {

    val data = Array(
      (STObject(1,1),1),
      (STObject(1, 100),1),
      (STObject(100, 1),1),
      (STObject(100, 100),1),
      (STObject(1,1),2),
      (STObject(1, 100),2),
      (STObject(100, 1),2),
      (STObject(100, 100),2)
    )

    val parti = RTreePartitioner(data.map(_._1.getGeo.getEnvelopeInternal), 4, (0, 101, 0, 101), pointsOnly = true)

//    println("BEFORE")
//    parti.partitions.foreach(println)

    data.foreach{ case (so, v) =>
      val pNum = parti.getPartition(so)
      println(s"$so v: $v --> $pNum" )
      withClue(s"$so v: $v") { pNum should (be >= 0 and be < parti.numPartitions) }
    }

//    println("AFTER")
//    parti.partitions.foreach(println)
  }

  it should "partition taxi data" in {
    val start = System.currentTimeMillis()
    val rdd = sc.textFile("src/test/resources/taxi_sample.csv", 4)
      .map { line => line.split(";") }
      .map { arr => (STObject(arr(1)), arr(0)) }.cache()

    println(rdd.count())

    val sample = rdd.sample(withReplacement = false, 0.1).map(_._1.getGeo.getEnvelopeInternal).collect()

    val parti = RTreePartitioner(sample, 10, pointsOnly = true)

    val end = System.currentTimeMillis()
    println(s"with sampling: ${end - start} ms")


    rdd.collect().foreach { case (st, name) =>
      val pNum = parti.getPartition(st)
      withClue(name) {
        pNum should (be >= 0 and be < parti.numPartitions)
      }
    }

    println(s"NUM partitions: ${parti.numPartitions}")

    val partCounts = rdd.map{ case (so, _) => (parti.getPartition(so), 1)}.countByKey()

    partCounts.values.sum shouldBe rdd.count()
  }

  it should "do yello sample" in {
    val rddblocks = sc.textFile("src/test/resources/blocks.csv", 4)
      .map { line => line.split(";") }
      .map { arr => (STObject(arr(1)), arr(0))}

    val minMaxBlocks = GridPartitioner.getMinMax(rddblocks.map(_._1.getGeo.getEnvelopeInternal))
    val sampleBlocks = rddblocks.sample(withReplacement = false, 0.1).map(_._1.getGeo.getEnvelopeInternal).collect()
    val partiBlocks = RTreePartitioner(sampleBlocks, 10, minMaxBlocks, pointsOnly = false)

    val rddtaxi = sc.textFile("src/test/resources/taxi_sample.csv", 4)
      .map { line => line.split(";") }
      .map { arr => (STObject(arr(1)), arr(0))}

    val minMaxTaxi = GridPartitioner.getMinMax(rddtaxi.map(_._1.getGeo.getEnvelopeInternal))
    val sampleTaxi = rddtaxi.sample(withReplacement = false, 0.1).map(_._1.getGeo.getEnvelopeInternal).collect()
    val partiTaxi = RTreePartitioner(sampleTaxi, 10, minMaxTaxi, pointsOnly = false)

    val matches = for(t <- partiTaxi.partitions;
                      b <- partiBlocks.partitions
                      if t.extent.intersects(b.extent)) yield (t,b)

    matches.length shouldBe >(0)
    //      val _ = new LiveIndexedSpatialRDDFunctions(rdd, 5).join(rddtaxi, JoinPredicate.CONTAINS, None)
  }

  it should "correctly partiton random points" in {
    val rdd = sc.textFile("src/test/resources/points.wkt")
      .map(_.split(";"))
      .map(arr => (STObject(arr(0)),arr(1))).cache()

    val sample = rdd.sample(withReplacement = false, 0.1).map(_._1.getGeo.getEnvelopeInternal).collect()
    val parti = RTreePartitioner(sample, 100, pointsOnly = true)

    val numparts = parti.numPartitions

    val parted = rdd.partitionBy(parti)

    parted.collect().foreach { case (st, name) =>
      try {
        val pNum = parti.getPartition(st)
        withClue(name) { pNum should (be >= 0 and be < numparts)}
      } catch {
        case e:IllegalStateException =>


          fail(s"$name: ${e.getMessage}  xok: xOk  yOk: yOk")
      }
    }
  }

  it should "correctly partiton random points with sampling" taggedAs Sampling in {
    val rdd = sc.textFile("src/test/resources/points.wkt")
      .map(_.split(";"))
      .map(arr => (STObject(arr(0)),arr(1)))

    val parti = RTreePartitioner(rdd.sample(withReplacement = false, 0.1).map(_._1.getGeo.getEnvelopeInternal).collect(), 100, pointsOnly = true)

    val numparts = parti.numPartitions

    val parted = rdd.partitionBy(parti)

    parted.collect().foreach { case (st, name) =>
      try {
        val pNum = parti.getPartition(st)
        withClue(name) { pNum should (be >= 0 and be < numparts)}
      } catch {
        case e:IllegalStateException =>


          fail(s"$name: ${e.getMessage}  xok: xOk  yOk: yOk")
      }
    }
  }

  it should "correctly join with single point and polygon" in {

    val pointsRDD = sc.parallelize(Seq(
      (STObject("POINT (77.64656066894531  23.10247055501927)"),1)))

    val polygonsRDD = sc.parallelize(Seq(
      (STObject("POLYGON ((77.2723388671875 23.332168306311473, 77.8436279296875 23.332168306311473, " +
        "77.8436279296875 22.857194700969636, 77.2723388671875 22.857194700969636, 77.2723388671875 23.332168306311473, " +
        "77.2723388671875 23.332168306311473))"),1)))

    val pointsParti = RTreePartitioner(pointsRDD.map(_._1.getGeo.getEnvelopeInternal).collect(), 10, pointsOnly = true)
    val pointsPart = pointsRDD.partitionBy(pointsParti)

    val polygonsParti = RTreePartitioner(polygonsRDD.map(_._1.getGeo.getEnvelopeInternal).collect(), 10, pointsOnly = true)
    val polygonsPart = polygonsRDD.partitionBy(polygonsParti)

    val joined = polygonsPart.join(pointsPart, JoinPredicate.CONTAINS)

    joined.collect().length shouldBe 1
  }

  it should "correctly join with single point and polygon containedby" in {

    val pointsRDD = sc.parallelize(Seq(
      (STObject("POINT (77.64656066894531  23.10247055501927)"),1)))

    val polygonsRDD = sc.parallelize(Seq(
      (STObject("POLYGON ((77.2723388671875 23.332168306311473, 77.8436279296875 23.332168306311473, " +
        "77.8436279296875 22.857194700969636, 77.2723388671875 22.857194700969636, 77.2723388671875 23.332168306311473, " +
        "77.2723388671875 23.332168306311473))"),1)))

    val pointsParti = RTreePartitioner(pointsRDD.map(_._1.getGeo.getEnvelopeInternal).collect(), 10, pointsOnly = true)
    val pointsPart = pointsRDD.partitionBy(pointsParti)

    val polygonsParti = RTreePartitioner(polygonsRDD.map(_._1.getGeo.getEnvelopeInternal).collect(), 10, pointsOnly = true)
    val polygonsPart = polygonsRDD.partitionBy(polygonsParti)

    val joined = pointsPart.join(polygonsPart, JoinPredicate.CONTAINEDBY)

    joined.collect().length shouldBe 1
  }

  it should "correctly join with single point and polygon intersect point-poly" in {

    val pointsRDD = sc.parallelize(Seq(
      (STObject("POINT (77.64656066894531  23.10247055501927)"),1)))

    val polygonsRDD = sc.parallelize(Seq(
      (STObject("POLYGON ((77.2723388671875 23.332168306311473, 77.8436279296875 23.332168306311473, " +
        "77.8436279296875 22.857194700969636, 77.2723388671875 22.857194700969636, 77.2723388671875 23.332168306311473, " +
        "77.2723388671875 23.332168306311473))"),1)))

    val pointsParti = RTreePartitioner(pointsRDD.map(_._1.getGeo.getEnvelopeInternal).collect(), 10, pointsOnly = true)
    val pointsPart = pointsRDD.partitionBy(pointsParti)

    val polygonsParti = RTreePartitioner(polygonsRDD.map(_._1.getGeo.getEnvelopeInternal).collect(), 10, pointsOnly = true)
    val polygonsPart = polygonsRDD.partitionBy(polygonsParti)

    val joined = pointsPart.join(polygonsPart, JoinPredicate.INTERSECTS)

    joined.collect().length shouldBe 1
  }

  it should "correctly join with single point and polygon intersect poly-point" in {

    val pointsRDD = sc.parallelize(Seq(
      (STObject("POINT (77.64656066894531  23.10247055501927)"),1)))

    val polygonsRDD = sc.parallelize(Seq(
      (STObject("POLYGON ((77.2723388671875 23.332168306311473, 77.8436279296875 23.332168306311473, " +
        "77.8436279296875 22.857194700969636, 77.2723388671875 22.857194700969636, 77.2723388671875 23.332168306311473, " +
        "77.2723388671875 23.332168306311473))"),1)))

    val pointsParti = RTreePartitioner(pointsRDD.map(_._1.getGeo.getEnvelopeInternal).collect(), 10, pointsOnly = true)
    val pointsPart = pointsRDD.partitionBy(pointsParti)

    val polygonsParti = RTreePartitioner(polygonsRDD.map(_._1.getGeo.getEnvelopeInternal).collect(), 10, pointsOnly = true)
    val polygonsPart = polygonsRDD.partitionBy(polygonsParti)

    val joined = polygonsPart.join(pointsPart, JoinPredicate.INTERSECTS)

    joined.collect().length shouldBe 1
  }

  it should "contain all taxi points with sampling" taggedAs Sampling in {
    val rddTaxi = sc.textFile("src/test/resources/taxi_sample.csv", 4)
      .map { line => line.split(";") }
      .map { arr => (STObject(arr(1)), arr(0))}



    val partiTaxi = RTreePartitioner(rddTaxi.sample(withReplacement = false,0.1).map(_._1.getGeo.getEnvelopeInternal).collect(), 100, pointsOnly = true)

    val partedTaxi = rddTaxi.partitionBy(partiTaxi)

    partedTaxi.collect().foreach { case (s,_) =>
      partiTaxi.getPartition(s) should (be >= 0 and be < partiTaxi.numPartitions)
    }
  }

  it should "join in both directions" taggedAs Slow in {
    val rddBlocks = sc.textFile("src/test/resources/blocks.csv", 4)
      .map { line => line.split(";") }
      .map { arr => (STObject(arr(1)), arr(0))}


    val rddTaxi = sc.textFile("src/test/resources/taxi_sample.csv", 4)
      .map { line => line.split(";") }
      .map { arr => (STObject(arr(1)), arr(0))}

    val partiBlocksSample = RTreePartitioner(rddBlocks/*.sample(withReplacement = false, 0.1)*/.map(_._1.getGeo.getEnvelopeInternal).collect(), 10, pointsOnly = false)
    val partiTaxiSample = RTreePartitioner(rddTaxi/*.sample(withReplacement = false, 0.8)*/.map(_._1.getGeo.getEnvelopeInternal).collect(), 10, pointsOnly = true)

    val partedTaxi = rddTaxi.partitionBy(partiTaxiSample).cache()
    val partedBlocks = rddBlocks.partitionBy(partiBlocksSample).cache()

    val start = System.currentTimeMillis()
    val blocksTaxiJoin = partedBlocks.liveIndex(RTreeConfig(order = 5)).join(partedTaxi, JoinPredicate.INTERSECTS, oneToMany = true)
    val taxiBlocksJoin = partedTaxi.liveIndex(RTreeConfig(order = 5)).join(partedBlocks, JoinPredicate.INTERSECTS, oneToMany = true)

    val blocksTaxiCnt = blocksTaxiJoin.sortByKey().collect()
    val taxiBlocksCnt = taxiBlocksJoin.sortByKey().collect()
    val end = System.currentTimeMillis()

    println(s"${end - start} ms")


//    partiTaxiSample.printPartitions("/tmp/parti_taxi")
//    partiBlocksSample.printPartitions("/tmp/parti_blocks")

    try {

      withClue("compare directions block vs taxi") {
        taxiBlocksCnt.map(_.swap) should contain theSameElementsAs blocksTaxiCnt
      }

      val noIdx = rddTaxi.liveIndex(5).join(rddBlocks, JoinPredicate.INTERSECTS).sortByKey().collect()

      println(s"noIdx: ${noIdx.length}")
      println(s"taxi - blocks ${taxiBlocksCnt.length}")
      println(s"block - taxi ${blocksTaxiCnt.length}")

      withClue("greater than zero") { noIdx.length shouldBe >(0) }
//      withClue("compared with noIdx") {
//        taxiBlocksCnt shouldBe noIdx
//        taxiBlocksCnt should contain theSameElementsAs noIdx

        taxiBlocksCnt.mkString("\n") shouldBe noIdx.mkString("\n")
//      }

    } finally {
      rddBlocks.unpersist()
      rddTaxi.unpersist()

      partedBlocks.unpersist()
      partedTaxi.unpersist()
    }
  }

  it should "produce same join results with sampling as without" taggedAs Sampling in {
    val rddBlocks = sc.textFile("src/test/resources/blocks.csv", 4)
      .map { line => line.split(";") }
      .map { arr => (STObject(arr(1)), arr(0))}//.sample(withReplacement = false, 0.5)

    //    BSPartitioner.numCellThreshold = Runtime.getRuntime.availableProcessors()
    val partiBlocksSample = RTreePartitioner(rddBlocks/*.sample(withReplacement = false, 0.1)*/.map(_._1.getGeo.getEnvelopeInternal).collect(), 10, pointsOnly = false)

    val rddTaxi = sc.textFile("src/test/resources/taxi_sample.csv", 4)
      .map { line => line.split(";") }
      .map { arr => (STObject(arr(1)), arr(0))}//.sample(withReplacement = false, 0.5)


    val partiTaxiSample = RTreePartitioner(rddTaxi/*.sample(withReplacement = false, 0.8)*/.map(_._1.getGeo.getEnvelopeInternal).collect(), 10, pointsOnly = true)

    val partedBlocksSample = rddBlocks.partitionBy(partiBlocksSample).cache()
    println(s"blocks done: ${partedBlocksSample.count()}")

    val partedTaxiSample = rddTaxi.partitionBy(partiTaxiSample).cache()

    try {
      println(s"taxi done: ${partedTaxiSample.count()}")

      partiTaxiSample.printPartitions("/tmp/parts_taxi")
      partiBlocksSample.printPartitions("/tmp/parts_blocks")
    } catch {
      case e: Throwable =>
//        import scala.collection.JavaConverters._
//        val fName = Paths.get(System.getProperty("user.home"),"taxi_sample.wkt")
//        val list = partiTaxiSample.theRDD.map { case (o, v) => s"${o.getGeo.toText};$v"}.collect().toList.asJava
//        java.nio.file.Files.write(fName, list, java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.WRITE, java.nio.file.StandardOpenOption.TRUNCATE_EXISTING)
        fail(e.getMessage)
    }


//    val taxiPartiNoSample = new BSPartitioner(rddTaxi, sideLength = 0.1, maxCostPerPartition = 100,
//      pointsOnly = true, sampleFraction = 0)
//
//    val blockPartiNoSample = new BSPartitioner(rddBlocks, sideLength = 0.2, maxCostPerPartition = 100,
//      pointsOnly = false, sampleFraction = 0)

//    val tPartedNoSample = rddTaxi.partitionBy(taxiPartiNoSample)
//    val pPartedNoSample = rddBlocks.partitionBy(blockPartiNoSample)

    val start = System.currentTimeMillis()
    val joinResSam = new LiveIndexedSpatialRDDFunctions(partedTaxiSample, RTreeConfig(order = 10)).join(partedBlocksSample, JoinPredicate.CONTAINEDBY, None)//.collect()
    val joinResSamCnt = joinResSam.count()
    val end = System.currentTimeMillis()


    joinResSamCnt shouldBe > (0L)

    val start2 = System.currentTimeMillis()
    val joinResPlain = new LiveIndexedSpatialRDDFunctions(rddTaxi, RTreeConfig(order = 10)).join(rddBlocks, JoinPredicate.CONTAINEDBY, None)//.collect()
    val joinResPlainCnt = joinResPlain.count()
    val end2 = System.currentTimeMillis()

    joinResPlainCnt shouldBe > (0L)

    val countNoPart = new LiveIndexedSpatialRDDFunctions(rddTaxi, RTreeConfig(order = 5)).join(rddBlocks, JoinPredicate.CONTAINEDBY).count()

    withClue("compared with no partitioning") { joinResSamCnt shouldBe countNoPart}
    withClue("different join results"){joinResSamCnt should equal(joinResPlainCnt)}

    println(s"sampled join: ${end - start} ms: $joinResSamCnt")
    println(s"plain join: ${end2 - start2} ms: $joinResPlainCnt")

    //    joinResSam should contain theSameElementsAs joinResPlain
  }

  ignore should "produce same join results with sampling as without with zip join" taggedAs (Sampling,Slow) in {
    val rddBlocks = sc.textFile("src/test/resources/blocks.csv", 4)
      .map { line => line.split(";") }
      .map { arr => (STObject(arr(1)), arr(0))}

    val rddTaxi = sc.textFile("src/test/resources/taxi_sample.csv", 4)
      .map { line => line.split(";") }
      .map { arr => (STObject(arr(1)), arr(0))}//.sample(withReplacement = false, 0.5)


    val partiBlocksSample = RTreeStrategy(10, sampleFraction = 0.5)

    val blockPartiNoSample = partiBlocksSample.copy(sampleFraction = 0.0)

    val taxiPartiNoSample = RTreeStrategy(10, pointsOnly = true, sampleFraction = 0.0)

    val tPartedNoSample = rddTaxi.partitionBy(taxiPartiNoSample)
    val bPartedNoSample = rddBlocks.partitionBy(blockPartiNoSample)

    val sampleCnt = StarkTestUtils.timing("sampled zip join") {
      val joinResSam = rddBlocks.liveIndex(order = 5).zipJoin(rddTaxi, JoinPredicate.CONTAINS)
      val joinResSamCnt = joinResSam.count()

      println(s"sampled zip join size: $joinResSamCnt")

      withClue("live indexed join partedTaxiSample w/ partedBlocksSample") {
        joinResSamCnt should be > 0L
      }
      joinResSamCnt
    }

    val fullCnt = StarkTestUtils.timing("full join") {
      val joinResPlain = new LiveIndexedSpatialRDDFunctions(tPartedNoSample, RTreeConfig(order = 50)).join(bPartedNoSample, JoinPredicate.CONTAINEDBY, None) //.collect()
      val joinResPlainCnt = joinResPlain.count()

      withClue("live indexed join tPartedNoSample w/ pPartedNoSample") {
        joinResPlainCnt should be > 0L
      }

      joinResPlainCnt
    }

    withClue("sampleCnt vs fullCnt") {
      sampleCnt shouldBe fullCnt
    }

    //    joinResSam should contain theSameElementsAs joinResPlain
  }

}

