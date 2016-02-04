package dbis.spark.spatial

import etm.core.monitor.EtmMonitor
import etm.core.configuration.BasicEtmConfigurator
import etm.core.configuration.EtmManager
import etm.core.renderer.SimpleTextRenderer
import org.apache.spark.SparkContext
import scala.io.Source
import com.vividsolutions.jts.io.WKTReader

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

  		  
  		  val raw = sc.textFile(dataFile)
  	             .map { line => line.split(",")}
                 .filter { arr => arr(7).matches("POINT\\(\\d+\\.?\\d* \\d+\\.?\\d*\\)")}
                 .map { arr => (new WKTReader().read(arr(7)), arr(0)) }
                 
        val parti = new BSPartitioner(raw, 10, 100)

//        println(s"num partitions: ${parti.numPartitions}")
//        (0 until parti.numPartitions).foreach { idx => println(parti.getCellBounds(idx))}
                 
      raw.partitionBy(parti)
               .mapPartitionsWithIndex { case (idx, iter) => List((idx, iter.size)).toIterator }
               .foreach(println)
    		
  	  }	   
		} finally {
		  program.collect() 
		}               
		
		
	  stopETM()
  
	               
		sc.stop()  
    
    
  }
  
}