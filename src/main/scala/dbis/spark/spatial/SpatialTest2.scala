package dbis.spark.spatial

import etm.core.monitor.EtmMonitor
import etm.core.configuration.BasicEtmConfigurator
import etm.core.configuration.EtmManager
import etm.core.renderer.SimpleTextRenderer
import org.apache.spark.SparkContext
import scala.io.Source
import com.vividsolutions.jts.io.WKTReader

import dbis.spark.spatial.SpatialRDD._
import dbis.spark.spatial.indexed.RTree
import com.vividsolutions.jts.geom.Geometry

object SpatialTest2 {
  
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
        """.stripMargin)
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
  		  
  		  val raw = sc.textFile(dataFile, 8)
  	             .map { line => line.split(",")}
                 .filter { arr => arr(7).matches("POINT\\(\\d+\\.?\\d* \\d+\\.?\\d*\\)")}
                 .map { arr => (new WKTReader().read(arr(7)), arr(0)) }
        
        println(s"input size: ${raw.count()}")                 
                 
        val other = sc.parallelize(Seq(queryGeom), 1).map { g => (g, "Hallo Welt") }.keyBy(_._1)

        val pIdx = monitor.createPoint("pidx")
        try {
        	val cnt = raw.keyBy(_._1).index(cost = 10, cellSize = 10).join(other).count()
    			println(s"idx cnt: $cnt")
        } finally { 
        	pIdx.collect() 
        }
        
        val idxStore = raw.keyBy(_._1).index(cost = 10, cellSize = 10)
        val numParts = idxStore.getNumPartitions
        idxStore.saveAsObjectFile("/tmp/idx_test")
        
        val pIdxLoad = monitor.createPoint("idxLoad")
        try {
        	val idx = sc.objectFile[RTree[Geometry, (Geometry, (Geometry, String))]]("/tmp/idx_test", numParts)
        	val cnt = idx.join(other).count()
    			println(s"idx load cnt: $cnt")
        } finally { 
        	pIdxLoad.collect() 
        } 
        
        val plain = monitor.createPoint("plain")
        try {
        	val cnt = raw.join(other).count()
        	println(s"plain cnt: $cnt")
        } finally { 
        	plain.collect() 
        }
        
        
    		
  	  }	   
		} finally {
		  program.collect() 
		}               
		
		
	  stopETM()
  
	               
		sc.stop()  
    
    
  }
  
}