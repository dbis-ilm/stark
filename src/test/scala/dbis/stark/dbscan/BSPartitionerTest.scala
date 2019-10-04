package dbis.stark.dbscan

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class BSPartitionerTest extends FlatSpec with Matchers with BeforeAndAfter {

  var sc: SparkContext = _
  
  before {
    val conf = new SparkConf().setMaster("local").setAppName("paritionertest")
    sc = new SparkContext(conf)
  }
  
  after {
    if(sc != null)
      sc.stop()
  }
  
  def createRDD(sc: SparkContext, file: String = "src/test/resources/new_eventful_flat_1000.csv", sep: Char = ',', numParts: Int = 1) =    
    sc.textFile(file,numParts)
      .map { line => line.split(sep) }
      .flatMap { arr => 
        val pattern = "POINT\\((.+)\\s+(.+)\\)".r
        
        val res = pattern.findAllIn(arr(7)).matchData map { m =>
          (m.group(2).toDouble, m.group(1).toDouble)
        }
        
        res
      }
      .zipWithUniqueId
      .map { case ((x,y),id) => ClusterPoint[Long, Int](id,Vectors.dense(x,y), 0) }
      
  
  "The BSPartitioner" should "create one partition if max cost equals input size" in {
    
    val rdd = createRDD(sc, numParts = Runtime.getRuntime.availableProcessors())
    
    val globalMBB = rdd.map(_.vec).aggregate(MBB.zero)(MBB.mbbSeq, MBB.mbbComb)
    
    val eps = 2.0
    val maxPartitionSize = rdd.count().toInt
    
    val parti = new BSPartitioner()
        .setMBB(globalMBB)
        .setCellSize(eps * 2.0)
        .setMaxNumPoints(maxPartitionSize)
        .computeHistogam(rdd, eps * 2.0)
    
    
  }
  
  it should "return all points in case of one partition" in {
    
    val rdd = createRDD(sc, numParts = Runtime.getRuntime.availableProcessors())
    
    val globalMBB = rdd.map(_.vec).aggregate(MBB.zero)(MBB.mbbSeq, MBB.mbbComb)
    
    val eps = 2.0
    val maxPartitionSize = rdd.count().toInt
    
    val parti = new BSPartitioner()
        .setMBB(globalMBB)
        .setCellSize(eps * 2.0)
        .setMaxNumPoints(maxPartitionSize)
        .computeHistogam(rdd, eps * 2.0)
    
    parti.cellHistogram.map(_._2).sum shouldBe rdd.count()
    
  }
  
  it should "return all points in case of more than one partition" in {
    
    val rdd = createRDD(sc, numParts = Runtime.getRuntime.availableProcessors())
    
    val globalMBB = rdd.map(_.vec).aggregate(MBB.zero)(MBB.mbbSeq, MBB.mbbComb)
    
    val eps = 2.0
    val maxPartitionSize = rdd.count().toInt / 2
    
    val parti = new BSPartitioner()
        .setMBB(globalMBB)
        .setCellSize(eps * 2.0)
        .setMaxNumPoints(maxPartitionSize)
        .computeHistogam(rdd, eps * 2.0)
    
    parti.cellHistogram.map(_._2).sum shouldBe rdd.count()
    
  }
  
}