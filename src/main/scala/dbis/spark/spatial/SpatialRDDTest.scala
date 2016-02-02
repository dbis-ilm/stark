package dbis.spark.spatial

import org.apache.spark.SparkContext
import com.vividsolutions.jts.io.WKTReader
import org.apache.spark.SparkConf
import dbis.spark.spatial.SpatialRDD._
import scala.io.Source
import dbis.spark.spatial.indexed.RTree
import com.vividsolutions.jts.geom.Geometry
import etm.core.monitor.EtmMonitor
import etm.core.configuration.EtmManager
import etm.core.configuration.BasicEtmConfigurator
import etm.core.renderer.SimpleTextRenderer
import org.apache.hadoop.fs.Hdfs
import org.apache.spark.deploy.SparkHadoopUtil


object SpatialRDDTest {
  
  var monitor: EtmMonitor = _
  
  def setupETM() {
    BasicEtmConfigurator.configure(true) // nested
    monitor = EtmManager.getEtmMonitor
    monitor.start()
  }
  
  def stopETM(render: Boolean = true) = {
    
    if(render)
    	monitor.render(new SimpleTextRenderer())
    
    monitor.stop()
  }
  
	def main(args: Array[String]) {

    if(args.length != 2 && args.length != 3) {
      sys.error("""Parameters (in order): 
        |  dataFile  : file to load and process
        |  queryFile : a file containing a query wkt
        |  numRuns   : number of runs (optional, default = 1)
        """)
    }
    
	  val dataFile = args(0)
	  val queryFile = args(1)

	  val numRuns = if(args.length == 3) args(2).toInt else 1
	  
	  println(s"NUMBER OF RUNS: $numRuns")
	    
	  
		val sc = new SparkContext()
	  
		val queryWkt = Source.fromFile(queryFile).getLines().mkString(" ")
		val queryGeom = new WKTReader().read(queryWkt)
		
		setupETM()
		
		val program = monitor.createPoint("program")

		try {
  		(0 until numRuns).foreach { i => 
  
    		// plain filtering - no partitioning, no indexing  
    	  val resPlain = plain(sc, dataFile, queryGeom)
    	             
        // grid filtering - partition the data, but no indexing	               
    	  val resGrid = grid(sc, dataFile, queryGeom)
    
    	  assert(resPlain == resGrid, s"plain vs grid: $resPlain - $resGrid")
    	  
    	  // live indexing - partition data and create index on-the-fly when computing RDD result
    	  val resLive = live(sc, dataFile, queryGeom)              	               
    	  
    	  assert(resGrid == resLive, s"grid vs live: $resGrid - $resLive")
    	  
    	  // persitable index - partition data and create R-Tree for each partition, then compute results on this tree
    	  val resIndex = index(sc, dataFile, queryGeom)              
    
    	  assert(resLive == resIndex, s"live vs index: $resLive - $resIndex")
    	  
        // create and persist index
    	  val persistFile = "/user/stha1in/rtree"
    	  store(sc, dataFile, persistFile)
    	  
        // load persisted index objects		
    		val resLoad = load(sc, persistFile, queryGeom)
    		
    		assert(resIndex == resLoad, s"live vs load: $resLive - $resLoad")
    		
  	  }	   
		} finally {
		  program.collect() 
		}               
		
		
	  stopETM()
  
	               
		sc.stop()
	}
	
	def plain(sc: SparkContext, dataFile: String, queryGeom: Geometry) = {
	  val p = monitor.createPoint("plain")
	  try {
  	  sc.textFile(dataFile)
  	             .map { line => line.split(",")}
                 .filter { arr => arr(7).matches("POINT\\(\\d+\\.?\\d* \\d+\\.?\\d*\\)")}
                 .map { arr => (new WKTReader().read(arr(7)), arr(0)) }
                 .intersect(queryGeom)
                 .count()
	  } finally {
	    p.collect()
	  }
	}
	
  def grid(sc: SparkContext, dataFile: String, queryGeom: Geometry) = {
	  val p = monitor.createPoint("grid")
	  try { 
      sc.textFile(dataFile)
  	             .map { line => line.split(",")}
                 .filter { arr => arr(7).matches("POINT\\(\\d+\\.?\\d* \\d+\\.?\\d*\\)")}
                 .map { arr => (new WKTReader().read(arr(7)), arr(0)) }
                 .grid(3)      // apply partitioning (but no indexing)
                 .intersect(queryGeom)
                 .count()
	  } finally {
	    p.collect()
	  }
  }
  
  def live(sc: SparkContext, dataFile: String, queryGeom: Geometry) = {
	  val p = monitor.createPoint("live")
	  try { 
      sc.textFile(dataFile)
	             .map { line => line.split(",")}
               .filter { arr => arr(7).matches("POINT\\(\\d+\\.?\\d* \\d+\\.?\\d*\\)")}
               .map { arr => (new WKTReader().read(arr(7)), arr(0)) }
               .liveIndex(3)      // live indexing
               .intersect(queryGeom)
               .count()
	  } finally {
	    p.collect()
	  }
  }

               
  def index(sc: SparkContext, dataFile: String, queryGeom: Geometry) = {
	  val p = monitor.createPoint("index")
	  try { 
      sc.textFile(dataFile)
	             .map { line => line.split(",")}
               .filter { arr => arr(7).matches("POINT\\(\\d+\\.?\\d* \\d+\\.?\\d*\\)")}
               .map { arr => (new WKTReader().read(arr(7)), arr(0)) }
               .index(3)      // persistable indexing
               .intersect(queryGeom)
               .count()
	  } finally {
	    p.collect()
	  }
  }
               
  def store(sc: SparkContext, dataFile: String, resultFile: String) = {
    deleteHdfsFile(resultFile, sc)
	  val p = monitor.createPoint("store")
	  try {
      sc.textFile(dataFile)
	             .map { line => line.split(",")}
               .filter { arr => arr(7).matches("POINT\\(\\d+\\.?\\d* \\d+\\.?\\d*\\)")}
               .map { arr => (new WKTReader().read(arr(7)), arr(0)) }
               .index(3)
               .saveAsObjectFile(resultFile)
	  } finally {
	    p.collect()
	  }
  }
               
  def load(sc: SparkContext, resultFile: String, queryGeom: Geometry) = {
	  val p = monitor.createPoint("load")
	  try { 
      sc.objectFile[RTree[Geometry, (Geometry, String)]](resultFile)
	            .intersect(queryGeom)
	            .count()
	  } finally {
	    p.collect()
	  }
  }
  
  def deleteHdfsFile(path: String, sc: SparkContext) = {
    val hdfs = org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration)
    hdfs.delete(new org.apache.hadoop.fs.Path(path), true)
  }
}