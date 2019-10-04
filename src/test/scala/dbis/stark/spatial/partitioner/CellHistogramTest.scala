package dbis.stark.spatial.partitioner

import dbis.stark.spatial.{Cell, NPoint, NRectRange}
import dbis.stark.{STObject, StarkKryoRegistrator}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.locationtech.jts.io.WKTReader
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class CellHistogramTest extends FlatSpec with Matchers with BeforeAndAfterAll {

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

  private def createRDD(points: Seq[String] = List("POINT(2 2)", "POINT(2.5 2.5)", "POINT(2 4)", "POINT(4 2)", "POINT(4 4)")): RDD[(STObject, Long)] =
    sc.parallelize(points,4).zipWithIndex()
      .map { case (string, id) => (new WKTReader().read(string), id) }

  it should "correctly add new point to empty" in {
    var histo = CellHistogram.empty
    histo = histo.add(STObject("POINT(0 0)"), 0,0,9,9,3,3,3,
      pointsOnly = true)


    histo.buckets.size shouldBe 1

    histo.get(0) shouldBe defined
    histo.get(0).get._2 shouldBe 1
  }

  it should "correctly add new point to non-empty" in {
    var histo = CellHistogram.empty
    histo = histo.add(STObject("POINT(0 0)"), 0,0,9,9,3,3,3,
      pointsOnly = true)

    histo = histo.add(STObject("POINT(0 0)"), 0,0,9,9,3,3,3,
      pointsOnly = true)

    histo = histo.add(STObject("POINT(4 4)"), 0,0,9,9,3,3,3,
      pointsOnly = true)


    histo.buckets.size shouldBe 2

    withClue("bucket 0 not defined"){histo.get(0) shouldBe defined}
    withClue("bucket 0 count not 2"){histo.get(0).get._2 shouldBe 2}

    withClue("bucket 4 not defined"){histo.get(4) shouldBe defined}
    withClue("bucket 4 count"){histo.get(4).get._2 shouldBe 1}
  }

  it should "be built correctly from RDD" in {
    val rdd = createRDD()

    val histo = GridPartitioner.buildHistogram(rdd.map(_._1.getGeo.getEnvelopeInternal), pointsOnly = true, 3,3,0,
                              0,9,9,3,3)

    histo.buckets.size shouldBe 9

    withClue("bucket 0 cell"){histo(0)._1 shouldBe Cell(0,NRectRange(NPoint(0,0),NPoint(3,3)))}
    withClue("bucket 1 cell"){histo(1)._1 shouldBe Cell(1,NRectRange(NPoint(3,0),NPoint(6,3)))}
    withClue("bucket 3 cell"){histo(3)._1 shouldBe Cell(3,NRectRange(NPoint(0,3),NPoint(3,6)))}
    withClue("bucket 4 cell"){histo(4)._1 shouldBe Cell(4,NRectRange(NPoint(3,3),NPoint(6,6)))}

    withClue("bucket 0 count"){histo(0)._2 shouldBe 2}
    withClue("bucket 1 count"){histo(1)._2 shouldBe 1}
    withClue("bucket 3 count"){histo(3)._2 shouldBe 1}
    withClue("bucket 4 count"){histo(4)._2 shouldBe 1}

    // other cells should be empty
    histo.buckets.valuesIterator.filter(_._2 == 0).map(_._1.id).toList should contain only (2,5,6,7,8)
  }

}
