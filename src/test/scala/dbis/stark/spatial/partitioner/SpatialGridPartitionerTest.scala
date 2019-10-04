package dbis.stark.spatial.partitioner

import dbis.stark.STObject
import org.apache.spark.SpatialRDD._
import dbis.stark.spatial.{JoinPredicate, StarkUtils}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class SpatialGridPartitionerTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  
  private var sc: SparkContext = _
  
  override def beforeAll() {
    val conf = new SparkConf().setMaster(s"local[${Runtime.getRuntime.availableProcessors() - 1}]").setAppName(getClass.getName)
    sc = new SparkContext(conf)
  }
  
  override def afterAll() {
    if(sc != null)
      sc.stop()
  }
  
  it should "find use cells as partitions for taxi"  in {
    val rdd = sc.textFile("src/test/resources/taxi_sample.csv", 4)
      .map { line => line.split(";") }
//      .filter { arr => !arr(1).contains("0 0")}
      .map { arr => (STObject(arr(1)), arr(0))}

      val minMax = GridPartitioner.getMinMax(rdd.map(_._1.getGeo.getEnvelopeInternal))

      val parti = SpatialGridPartitioner(rdd, partitionsPerDimension = 3, pointsOnly = false, minMax, dimensions = 2, sampleFraction = 0)



      parti.numPartitions shouldBe 9

      rdd.collect().foreach { case (st, name) =>
        try {
          val pNum = parti.getPartition(st)
          withClue(name) { pNum should (be >= 0 and be < 9)}
        } catch {
        case e:IllegalStateException =>

          val xOk = st.getGeo.getCentroid.getX >= minMax._1 && st.getGeo.getCentroid.getX <= minMax._2
          val yOk = st.getGeo.getCentroid.getY >= minMax._3 && st.getGeo.getCentroid.getY <= minMax._4



          fail(s"$name: ${e.getMessage}  xok: $xOk  yOk: $yOk")
        }
      }
  }

  it should "correctly partiton georand" in {
    val rdd = sc.textFile("src/test/resources/points.wkt")
                  .map(_.split(";"))
                    .map(arr => (STObject(arr(0)),arr(1)))

//    val parti = SpatialGridPartitioner(rdd, 5, false)

//    val parted = rdd.partitionBy(parti)

    val gridConfig = GridStrategy(5, pointsOnly = false, sampleFraction = 0)

    val parted = rdd.partitionBy(gridConfig)
    val parti = parted.partitioner match {
      case None => fail("partitioner not set!")
      case Some(p) => p
    }

    parti.numPartitions shouldBe 25

    parted.collect().foreach { case (st, name) =>
      try {
        val pNum = parti.getPartition(st)
        withClue(name) { pNum should (be >= 0 and be < 25)}
      } catch {
        case e:IllegalStateException =>

//          val xOk = st.getGeo.getCentroid.getX >= minMax._1 && st.getGeo.getCentroid.getX <= minMax._2
//          val yOk = st.getGeo.getCentroid.getY >= minMax._3 && st.getGeo.getCentroid.getY <= minMax._4



          fail(s"$name: ${e.getMessage}  xok: xOk  yOk: yOk")
      }
    }
  }

  ignore should "correctly partition wikimapia" in {
    val rdd = sc.textFile("src/test/resources/wiki.wkt")
      .map(_.split(";"))
      .map(arr => (STObject(arr(0)),arr(1)))

    val parti = SpatialGridPartitioner(rdd, 5, pointsOnly = true)

    parti.numPartitions shouldBe 25
    val parted = rdd.partitionBy(parti)

    parted.collect().foreach { case (st, name) =>
      try {
        val pNum = parti.getPartition(st)
        withClue(name) { pNum should (be >= 0 and be < 25)}
      } catch {
        case e:IllegalStateException =>

          //          val xOk = st.getGeo.getCentroid.getX >= minMax._1 && st.getGeo.getCentroid.getX <= minMax._2
          //          val yOk = st.getGeo.getCentroid.getY >= minMax._3 && st.getGeo.getCentroid.getY <= minMax._4



          fail(s"$name: ${e.getMessage}  xok: xOk  yOk: yOk")
      }
    }
  }

  it should "correctly join with single point and polygon" in {

    val thePoint = STObject("POINT (77.64656066894531  23.10247055501927)")
    val thePolygon = STObject("POLYGON ((77.2723388671875 23.332168306311473, 77.8436279296875 23.332168306311473, " +
      "77.8436279296875 22.857194700969636, 77.2723388671875 22.857194700969636, 77.2723388671875 23.332168306311473, " +
      "77.2723388671875 23.332168306311473))")

    withClue("premis: polygon contains point does not hold") { thePolygon contains thePoint}

    val pointsRDD = sc.parallelize(Seq((thePoint,1)))

    val polygonsRDD = sc.parallelize(Seq((thePolygon,1)))



    val pointsGrid = SpatialGridPartitioner(pointsRDD, partitionsPerDimension = 1, pointsOnly = true)
    val pointsPart = pointsRDD.partitionBy(pointsGrid)

//    val pointPartitionId = pointsGrid.getPartition(thePoint)
//    println(pointsGrid.partitions(pointPartitionId)._1)

    val polygonsGrid = SpatialGridPartitioner(polygonsRDD,partitionsPerDimension = 1, pointsOnly = false)
    val polygonsPart = polygonsRDD.partitionBy(polygonsGrid)

//    val polyPartitionId = polygonsGrid.getPartition(thePolygon)
//    println(polygonsGrid.partitions(polyPartitionId)._1)


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

    val pointsGrid = SpatialGridPartitioner(pointsRDD, partitionsPerDimension = 1, pointsOnly = true)
    val pointsPart = pointsRDD.partitionBy(pointsGrid)

    val polygonsGrid = SpatialGridPartitioner(polygonsRDD,partitionsPerDimension = 1, pointsOnly = false)
    val polygonsPart = polygonsRDD.partitionBy(polygonsGrid)

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

    val pointsGrid = SpatialGridPartitioner(pointsRDD, partitionsPerDimension = 5, pointsOnly = true)
    val pointsPart = pointsRDD.partitionBy(pointsGrid)

    val polygonsGrid = SpatialGridPartitioner(polygonsRDD,partitionsPerDimension = 5, pointsOnly = false)
    val polygonsPart = polygonsRDD.partitionBy(polygonsGrid)

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

    val pointsGrid = SpatialGridPartitioner(pointsRDD, partitionsPerDimension = 1, pointsOnly = true)
    val pointsPart = pointsRDD.partitionBy(pointsGrid)

    val polygonsGrid = SpatialGridPartitioner(polygonsRDD,partitionsPerDimension = 1, pointsOnly = false)
    val polygonsPart = polygonsRDD.partitionBy(polygonsGrid)

    val joined = polygonsPart.join(pointsPart, JoinPredicate.INTERSECTS)

    joined.collect().length shouldBe 1
  }

  ignore should "compute correct join contains" in {
    val points = sc.textFile("src/test/resources/points10k.csv")
      .map(_.split(","))
      .map(arr => s"POINT(${arr(1)} ${arr(0)})")
      .map(s => (STObject(s),9))


    val wiki = sc.textFile("src/test/resources/wiki_full.wkt")
      .map(_.split(";"))
      .map(arr => (STObject(arr(0)),arr(1)))


    val jWOPart = wiki.join(points, JoinPredicate.CONTAINS).collect()

    jWOPart should not be empty

    val pointGrid = SpatialGridPartitioner(points, 5, pointsOnly = true)
    val polyGrid = SpatialGridPartitioner(wiki, 5, pointsOnly = false)

    val pointsParted = points.partitionBy(pointGrid)
    val polyParted = wiki.partitionBy(polyGrid)

    val jWPart = polyParted.join(pointsParted, JoinPredicate.CONTAINS).collect()

    println(s"no part: ${jWOPart.length}")
    println(s"w/ part: ${jWPart.length}")

    jWOPart should contain theSameElementsAs jWPart
  }


  ignore should "compute correct join containedby" in {
    val pointsWkt = sc.textFile("src/test/resources/points10k.csv")
      .map(_.split(","))
      .map(arr => s"POINT(${arr(1)} ${arr(0)})")

    val points = pointsWkt.map(s => (STObject(s),9))


    val wiki = sc.textFile("src/test/resources/wiki_full.wkt")
      .map(_.split(";"))
      .map(arr => (STObject(arr(0)),arr(1)))


    val jWOPart = points.join(wiki, JoinPredicate.CONTAINEDBY).collect()

    jWOPart should not be empty

    val pointGrid = SpatialGridPartitioner(points, 5, pointsOnly = true)
    val polyGrid = SpatialGridPartitioner(wiki, 5, pointsOnly = false)

    val pointsParted = points.partitionBy(pointGrid)
    val polyParted = wiki.partitionBy(polyGrid)

    val jWPart = pointsParted.join(polyParted, JoinPredicate.CONTAINEDBY).collect()

    println(s"no part: ${jWOPart.length}")
    println(s"w/ part: ${jWPart.length}")

    jWOPart should contain theSameElementsAs jWPart
  }

  it should "find join result, if point is in extent" in {
    val thePoint = STObject("POINT (1 1)")
    val thePolygon = STObject("POLYGON ((0 0, 10 0, 10 10,  0 10, 0 0))")

    withClue("premis: polygon contains point does not hold") { thePolygon contains thePoint}

    val pointsRDD = sc.parallelize(Seq((thePoint,1)))

    val polygonsRDD = sc.parallelize(Seq((thePolygon,1)))



    val pointsGrid = SpatialGridPartitioner(pointsRDD, partitionsPerDimension = 1, pointsOnly = true)
    val pointsPart = pointsRDD.partitionBy(pointsGrid)

    //    val pointPartitionId = pointsGrid.getPartition(thePoint)
    //    println(pointsGrid.partitions(pointPartitionId)._1)

    val polygonsGrid = SpatialGridPartitioner(polygonsRDD, partitionsPerDimension = 3, pointsOnly = false)
    val polygonsPart = polygonsRDD.partitionBy(polygonsGrid)

    val partitionOfPolygon = 4
    polygonsGrid.partitions(partitionOfPolygon).extent shouldBe StarkUtils.fromEnvelope(thePolygon.getGeo.getEnvelopeInternal)
    withClue("point should NOT be in range"){polygonsGrid.partitions(partitionOfPolygon).range.contains(StarkUtils.fromEnvelope(thePoint.getGeo.getEnvelopeInternal)) shouldBe false}

    withClue("point should be in range"){polygonsGrid.partitions(partitionOfPolygon).extent.contains(StarkUtils.fromEnvelope(thePoint.getGeo.getEnvelopeInternal)) shouldBe true}

    val joined = polygonsPart.join(pointsPart, JoinPredicate.CONTAINS)

    joined.collect().length shouldBe 1
  }
  
}
