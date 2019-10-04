package dbis.stark
//
//import dbis.stark.ScalarDistance._
//import org.apache.spark.SpatialRDD._
//import dbis.stark.spatial.indexed.{IndexConfig, IntervalTreeConfig, QuadTreeConfig, RTreeConfig}
//import dbis.stark.spatial._
//import dbis.stark.spatial.partitioner.{BSPartitioner, SpatialGridPartitioner, TemporalRangePartitioner}
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql._
//import org.apache.spark.sql.expressions.UserDefinedFunction
//import org.apache.spark.sql.functions.udf
//import org.apache.spark.{SparkConf, SparkContext}
//
//
///**
//  * Created by Jacob on 21.02.17.
//  */
//
//
object TestMain {
//  //-fs src/test/resources/ -nf -gp -ti 10k_1-100.csv
//  def main(args: Array[String]) {
//    println("davor")
//
//
//    new TestMain().mainMethod(args)
//
//  }
//
//  def time[R](block: => R): Long = {
//    val t0 = System.nanoTime()
//    var res = block // call-by-name
//    val t1 = System.nanoTime()
//    (t1 - t0) / 1000000
//  }
//}
//
//class TestMain {
//
//  var filesource = ""
//  var filepath: String = filesource
//  var index = 0
//  var part = 0
//  var point = 0
//  var method = 0
//  var sampelfactor = 0.01
//  var partionsize = 10
//  var prefilter = false
//  var autorange = false
//  var cache = false
//  var secondfilter = false
//  var order = 10
//  var datasetfromrdd = false
//  var searchP = false
//  var listpoints = false
//  var dataset = false
//  val searchsize = 30
//  val searchsize2 = 15
//  var useresult = false
//  val intervalfakt = 1000
//  val searchPolygon: STObject = STObject(s"Polygon((-$searchsize $searchsize, $searchsize $searchsize, $searchsize -$searchsize, -$searchsize -$searchsize, -$searchsize $searchsize))", Interval(3 * intervalfakt, 6 * intervalfakt))
//  val secondPolygon: STObject = STObject(s"Polygon((-$searchsize2 $searchsize2, $searchsize2 $searchsize2, $searchsize2 -$searchsize2, -$searchsize2 -$searchsize2, -$searchsize2 $searchsize2))", Interval(4 * intervalfakt, 5 * intervalfakt))
//  var secondPoint = 1
//  var secondquery = false
//
//
//  var sc: SparkContext = createSparkContext("asd")
//
//  var spark: SparkSession = createSparkSession("asdas")
//
//  def printhelp(): Unit = {
//    println("Jacobs TestUtil")
//    println("[Options] FILE  | DEFAULT ")
//
//    println("example : " + "-p 3 -ps 30 -nf -os 20 -sf 0.01 -ar -tp -ti 200M_1-10000.csv")
//    println("using point 3, make 30 partitions, dont filter result after tree query, order for r-tree: 20, samplefactor for temppart 0.01, use auto range fot remp part, use temporal partitioner, user temporal index")
//    println()
//    println("-h  | --help               for Help")
//    println()
//    println("-ni | --no-index               don't use any index | DEFAULT")
//    println("-ti | --temporal-index         use temporal interval index")
//    println("-si | --spatial-index          use spatial r-tree index")
//    println()
//    println("-np | --no-part                don't use any partitioner | DEFAULT")
//    println("-gp | --grid-part              use spatial  grid partitioner")
//    println("-bp | --bs-part                use spatial  BSPartitioner")
//    println("-tp | --temp-part              use temporal partitioner")
//    println()
//    println("-p NUMBER | --point            use point [x] | DEFAULT " + point)
//    println("-fs PATH | --file-source       use other file source | DEFAULT: " + filesource)
//    println("-nf  | --no-filter             no filtering after tree-query")
//    println("-ps NUMBER | --part-size       partition size | DEFAULT " + partionsize)
//    println("-ar | --auto-range             partition size automatic on | DEFAULT off")
//    println("-sf DOUBLE | --sample-factor   sample factor for auto range | DEFAULT " + sampelfactor)
//    println("-os NUMBER | --order-size      order size | DEFAULT " + order)
//    println("-lp | --list-points     list 20 points ")
//    println()
//    println("-ds | --data-set     use dataset ")
//    println("-sp | --search-polygone     use search Polygone")
//    println("-pf | --pre-filter     use pre filtering for datasets")
//    println("-dsfr | --dataset-from-rdd     create dataset from rdd")
//    println("-sq | --second-query    second query")
//    println("-sf | --second-filter    second filter")
//    println("-ca | --cache    use .cache()")
//    println("-ur | --use-result    use result for second query")
//    println()
//    println("-c  | --contains               use contains as Method | DEFAULT")
//    println("-i  | --intersects             use intersects as Method")
//    println("-cb | --containedby            use containedby as Method")
//    println("-w  | --withinDistance         use withinDistance as Method")
//  }
//
//  def mainMethod(args: Array[String]): Unit = {
////    for (arg <- args) {
////      // println(arg)
////    }
//
//
//
//    if (args.contains("-h") || args.contains("--help") || args.length == 0) {
//      printhelp()
//    } else {
//
//      var i = 0
//      while (i < args.length - 1) {
//
//        val arg = args(i)
//        arg match {
//          case "-fs" =>
//            filesource = args(i + 1)
//            i += 1
//          case "-ni" => index = 0
//          case "-ti" => index = 1
//          case "-si" => index = 2
//          case "-np" => part = 0
//          case "-gp" => part = 1
//          case "-bp" => part = 2
//          case "-tp" => part = 3
//          case "-c" => method = 0
//          case "-i" => method = 1
//          case "-ar" => autorange = true
//          case "-ds" => dataset = true
//          case "-sp" => searchP = true
//          case "-pf" => prefilter = true
//          case "-sq" => secondquery = true
//          case "-sf" => secondfilter = true
//          case "-ca" => cache = true
//          case "-ur" => useresult = true
//          case "-dsfr" => datasetfromrdd = true
//          case "-cb" => method = 2
//          case "-w" => method = 3
//          case "-ps" =>
//            partionsize = args(i + 1).toInt
//            i += 1
//          case "-sa" =>
//            sampelfactor = args(i + 1).toDouble
//            i += 1
//          case "-os" =>
//            order = args(i + 1).toInt
//            i += 1
//          case "-p" =>
//            point = args(i + 1).toInt
//            i += 1
//          case "-lp" => listpoints = true
//          case _ => println("unknown argument: " + arg)
//        }
//
//        i += 1
//      }
//
//      val file = args(args.length - 1)
//      filepath = filesource + file
//      //  if (Files.exists(Paths.get(filepath))) {
//
//      val t = TestMain.time(startProgramm())
//
//
//      println("alltime: " + t)
//
//
//      /*  } else {
//          println("File does not exist: " + filepath)
//          printhelp()
//        }*/
//    }
//  }
//
//  def startProgramm(): Unit = {
//
//    println("Start Programm")
//    println("using file: " + filepath)
//    println("using point : " + point)
//
//
//    if (dataset) {
//      startdatasetProgramm2(spark)
//    } else {
//      startrddProgramm()
//    }
//
//
//  }
//
//
//  def startdatasetProgramm2(sparks: SparkSession): Unit = {
//    //if (!datasetfromrdd) {
//
//    val ds = createIntervalDataSet(spark, filepath)
//    println("createIntervalDataSet")
//  //  } else {
//   //   println("createIntervalDataSetFromRdd")
//    //  createIntervalDataSetFromRdd(sc, spark, filepath)
//   // }
//
//
//    if(cache) {
//      ds.cache()
//    }else{
//      println("no cache")
//    }
//
//
//    println("using dataset")
//
//    implicit val myObjEncoder: Encoder[STObject] = org.apache.spark.sql.Encoders.kryo[(STObject)]
//
//    /*var dsa = ds.repartition(10, ds("start"))
//    var psa = dsa.filter(dsa("test") < 2)*/
//
//
//    var searchData = searchPolygon
//    if (!searchP) {
//      val xs = ds.take(point + 1)(point)
//
//      searchData = STObject(xs.stob, Interval(xs.start, xs.end))
//    }
//    println("search data : " + searchData)
//
//    var psa = ds
//
//    if (prefilter) {
//      psa = TestF.prefilter(method,ds,searchData,secondfilter)
//    } else {
//      println("no prefilter")
//    }
//
//
//    val predicateFunc = TestF.getJP(method)
//
//    val indexTyp: Option[IndexConfig] = index match {
//      case 0 => None
//      case 1 => Some(IntervalTreeConfig())
//      case 2 => Some(RTreeConfig(order))
//      case 3 => Some(QuadTreeConfig(maxDepth = 10, minNum = 1))
//      case _ => throw new IllegalArgumentException(" wrong Index: " + index)
//    }
//
//    var res1: Array[ESTO] = null
//    var res_d: Dataset[ESTO] = null
//    import sparks.implicits._
//    val treeorder = order
//
//    val t1 = TestMain.time({
//      var psa2 = psa
//      if (indexTyp.isDefined) {
//        println("index")
//        psa2 = psa.mapPartitions(TestF.getf(indexTyp.get, treeorder, searchData))
//      }
//      res_d= psa2.filter { x => predicateFunc(STObject(x.stob, Interval(x.start, x.end)), searchData) }
//      res1 = res_d.collect()
//    }
//    )
//    println("\nelapsed time for dataset-method in ms: " + t1)
//
//    if (res1.length < 1) {
//      throw new Exception("found nothing:" + res1.length)
//    } else {
//      println("result size: " + res1.length)
//    }
//
//    //-------------------
//    if (secondquery) {
//      var psa = ds
//
//
//
//      var searchData = secondPolygon
//      if (!searchP) {
//        val xs = ds.take(secondPoint + 1)(secondPoint)
//        searchData = STObject(xs.stob, Interval(xs.start, xs.end))
//      }
//
//      if(useresult){
//        println("using result")
//        psa = res_d
//      }
//
//
//      if (prefilter) {
//       psa = TestF.prefilter(method,psa,searchData,secondfilter)
//
//      } else {
//        println("no prefilter")
//      }
//
//      var res2: Array[ESTO] = null
//      println("search data : " + searchData)
//      val t2 = TestMain.time({
//        var psa2 = psa
//        if (indexTyp.isDefined) {
//          psa2 = psa.mapPartitions(TestF.getf(indexTyp.get, treeorder, searchData))
//        }
//        val tmp7 = psa2.filter { x => predicateFunc(STObject(x.stob, Interval(x.start, x.end)), searchData) }
//        res2 = tmp7.collect()
//      }
//      )
//      println("\nelapsed time2 for dataset-method in ms: " + t2)
//
//      if (res2.length < 1) {
//        throw new Exception("found nothing:" + res2.length)
//      } else {
//        println("result size2: " + res2.length)
//      }
//    }
//
//
//
//
//
//
//    //val psa = ds.filter(ds("start") < xs.end)
//
//
//  }
//
//
//  def startrddProgramm(): Unit = {
//
//    val rddRaw = createIntervalRDD(sc, filepath)
//
//
//
//    var searchData = searchPolygon
//    if (!searchP) {
//      searchData = rddRaw.take(point + 1)(point)._1
//    }
//    println("point-data : " + searchData)
//
//    if (listpoints) {
//      var i = 0
//      rddRaw.take(50).foreach(k => {
//        println(i + "  -  point-data : " + k._1)
//        i += 1
//      })
//
//
//    }
//
//    var rdd = rddRaw
//
//    val t2 = TestMain.time(
//      part match {
//        case 0 => rdd = rddRaw // do nothing
//        case 1 => rdd = rddRaw.partitionBy(SpatialGridPartitioner[STObject](rddRaw, partionsize, false));
//        case 2 => rdd = rddRaw.partitionBy(new BSPartitioner[STObject,(String,STObject)](rddRaw, 0.5, 1000, false))
//        case 3 => rdd = rddRaw.partitionBy(new TemporalRangePartitioner(rddRaw, partionsize, autorange, sampelfactor))
//        case _ => println(" wrong Partitioner: " + part)
//      }
//    )
//    println("time for partitionby: " + t2)
//
//
//    if(cache) {
//      rdd.cache()
//    }else{
//      println("no cache")
//    }
//
//
//    if (rdd.partitioner.isDefined) {
//      println("using " + rdd.partitioner.get.getClass.getSimpleName + " as partitioner (partition-size: " + partionsize + " )")
//      // val d = rdd.mapPartitions(iter => Array(iter.size).iterator, true)
//      // println("partitionsizes: "+d.collect().mkString(","))
//    } else {
//      println("using no partitioner")
//    }
//
//
//
//
//
//
//    /* index match {
//       case 0 => println("using no index")
//       case 1 => println("using interval index")
//       case 2 => println("using spatial index")
//       case _ => println(" wrong Index: " + index)
//     }*/
//
//    var indexData: SpatialRDDFunctions[STObject, (String, STObject)] = index match {
//      case 0 => rdd // do nothing
//      case 1 => rdd.liveIndex(IntervalTreeConfig())
//      case 2 => rdd.liveIndex(order)
//      case 3 => rdd.liveIndex(QuadTreeConfig(maxDepth = 10, minNum = 1))
//      case _ => throw new IllegalArgumentException(" wrong Index: " + index)
//    }
//
//    println("using " + indexData.getClass.getSimpleName + " for indexing (order if spatialindex: " + order + " )")
//
//
//    var res: Array[(STObject, (String, STObject))] = null
//    var res_r: RDD[(STObject, (String, STObject))] = null
//    val t1 = TestMain.time(
//      method match {
//        case 0 =>
//          println("using Method contains")
//          res_r = indexData.contains(searchData)
//          res = res_r.collect()
//        case 1 =>
//          println("using Method intersects")
//          res_r = indexData.intersects(searchData)
//          res = res_r.collect()
//        case 2 =>
//          println("using Method containedby")
//          res_r = indexData.containedby(searchData)
//          res = res_r.collect()
//        case 3 =>
//          println("using Method withinDistance")
//          res_r = indexData.withinDistance(searchData, 0.5, Distance.seuclid)
//          res = res_r.collect()
//        case _ => println(" wrong Method: " + method)
//      }
//    )
//
//    println("\nelapsed time for method in ms: " + t1)
//
//
//
//
//    if (res.length < 1) {
//      throw new Exception("found nothing:" + res.length)
//    } else {
//      println("result size: " + res.length)
//    }
//
//    /* println()
//     println()
//     println()
//     println()
//     println()
//     printhelp()*/
//
//    if (secondquery) {
//      searchData = secondPolygon
//      if (!searchP) {
//        searchData = rddRaw.take(secondPoint + 1)(secondPoint)._1
//      }
//      if(useresult){
//
//        part match {
//          case 0 => rdd = res_r // do nothing
//          case 1 => rdd = res_r.partitionBy(new SpatialGridPartitioner(rddRaw, partionsize, false));
//          case 2 => rdd = res_r.partitionBy(new BSPartitioner(rddRaw, 0.5, 1000, false))
//          case 3 => rdd = res_r.partitionBy(new TemporalRangePartitioner(rddRaw, partionsize, autorange, sampelfactor))
//          case _ => println(" wrong Partitioner: " + part)
//        }
//
//        indexData = index match {
//          case 0 => rdd // do nothing
//          case 1 => rdd.liveIndex(IntervalTreeConfig())
//          case 2 => rdd.liveIndex(order)
//          case 3 => rdd.liveIndex(QuadTreeConfig(maxDepth = 10,minNum = 1))
//          case _ => throw new IllegalArgumentException(" wrong Index: " + index)
//        }
//        println("used result")
//      }
//
//      var res: Array[(STObject, (String, STObject))] = null
//      val t1 = TestMain.time(
//        method match {
//          case 0 =>
//            println("using Method contains")
//            val tmp = indexData.contains(searchData)
//            res = tmp.collect()
//          case 1 =>
//            println("using Method intersects")
//            val tmp = indexData.intersects(searchData)
//            res = tmp.collect()
//          case 2 =>
//            println("using Method containedby")
//            val tmp = indexData.containedby(searchData)
//            res = tmp.collect()
//          case 3 =>
//            println("using Method withinDistance")
//            val tmp = indexData.withinDistance(searchData, 0.5, (g1, g2) => g1.getGeo.distance(g2.getGeo))
//            res = tmp.collect()
//          case _ => println(" wrong Method: " + method)
//        }
//      )
//
//      println("\nelapsed time2 for method in ms: " + t1)
//
//
//
//
//      if (res.length < 1) {
//        throw new Exception("found nothing:" + res.length)
//      } else {
//        println("result size2: " + res.length)
//      }
//    }
//  }
//
//
//  def createIntervalRDD(
//                         sc: SparkContext,
//                         file: String = "src/test/resources/intervaltest.csv",
//                         sep: Char = ';',
//                         numParts: Int = 32,
//                         distinct: Boolean = false): RDD[(STObject, (String, STObject))] = {
//
//    val rdd = sc.textFile(file, if (distinct) 1 else numParts) // let's start with only one partition and repartition later
//      .map { line => line.split(sep) }
//      .map { arr =>
//        (arr(0), STObject(arr(1), Interval(arr(2).toInt, arr(3).toInt)))
//      }
//      .keyBy(_._2)
//
//
//    rdd
//  }
//
//
//
//  def createIntervalDataSet(
//                             sparks: SparkSession,
//                             file: String = "src/test/resources/20_1-100.csv",
//                             sep: String = ";"
//                           ): Dataset[ESTO] = {
//    import sparks.implicits._
//    //import sparkss.implicits._
//    var sparkss = sparks.read.option("inferSchema", "true").option("delimiter", sep).csv(file).toDF("id", "stob", "start", "end")
//    // "minx", "maxx", "miny", "maxy"
//
//    sparkss = sparkss.withColumn("minx",sparkss("id"))
//    sparkss = sparkss.withColumn("maxx",sparkss("id"))
//    sparkss = sparkss.withColumn("miny",sparkss("id"))
//    sparkss = sparkss.withColumn("maxy",sparkss("id"))
//   // sparkss = sparkss.withColumn("stob2", blubb()(sparkss("stob")))
//   // sparkss.show(10)
//
//    //sparkss.as[ESTO]
//    if(secondfilter) {
//      println("load second filter")
//      val extsto = sparkss.map(x => {
//        val s = x.getString(1)
//        val ob = STObject(s)
//        val env = ob.getEnvelopeInternal
//        ESTO(x.getInt(0).toLong,
//          x.getString(1),
//          x.getInt(2).toLong,
//          x.getInt(3).toLong,
//          env.getMinX, env.getMaxX, env.getMinY, env.getMaxY)
//      })
//
//      extsto.as[ESTO]
//    }else{
//
//
//      sparkss.as[ESTO]
//    }
//
//  }
//
//  def blubb(): UserDefinedFunction = udf((stob: String) => {
//    val x = stob
//    val ob = STObject(x)
//    val env = ob.getEnvelopeInternal
//
//    env.getMaxX +""
//  })
//
//
//  def createSparkContext(name: String): SparkContext = {
//    val conf = new SparkConf().setAppName(name)
//    new SparkContext(conf)
//  }
//
//  def createSparkSession(name: String): SparkSession = {
//    SparkSession
//      .builder()
//      .appName(name)
//      .getOrCreate()
//  }
//
//
}

case class STO(id: Long, stob: String, start: Long, end: Long)

case class ESTO(id: Long, stob: String, start: Long, end: Long, minx: Double, maxx: Double, miny: Double, maxy: Double)
