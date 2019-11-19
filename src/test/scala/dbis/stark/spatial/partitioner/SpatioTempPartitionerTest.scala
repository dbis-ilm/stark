package dbis.stark.spatial.partitioner

import dbis.stark.{STObject, StarkKryoRegistrator}
import org.apache.spark.SpatialRDD._
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}


class SpatioTempPartitionerTest extends FlatSpec with Matchers with BeforeAndAfterAll {
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



  ignore should "correctly get partitions" in {
    def sentinelReader(str: String): (STObject, String) = {
      val arr = str.split(';')
      (STObject(arr(1), arr(2).toLong, arr(3).toLong), arr(0))
    }
    val file = "src/test/resources/sentinel2.wkt"
    val raw = sc.textFile(file).map(sentinelReader)

    val parti = SpatioTempStrategy(1,5*1000, pointsOnly = false)

    val parted = raw.partitionBy(parti)
//    println(parted.partitioner.get.asInstanceOf[SpatioTempPartitioner[STObject]].numPartitions)
//    print(parted.getNumPartitions)
  }

  it should "find values in the info file" in {

  }
}
