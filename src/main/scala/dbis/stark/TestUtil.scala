package dbis.stark

import java.nio.file.{Files, Paths}

import dbis.stark.spatial.SpatialRDD._
import dbis.stark.spatial._
import dbis.stark.spatial.indexed.live.{LiveIndexedSpatialRDDFunctions, LiveIntervalIndexedSpatialRDDFunctions}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StructType, StringType, StructField}
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.udf

/**
  * Created by Jacob on 21.02.17.
  */


object TestUtil {
  //-fs src/test/resources/ -nf -gp -ti 10k_1-100.csv
  def main(args: Array[String]) {
    println("davor")


    new TestUtil().mainMethod(args)

  }

  def time[R](block: => R): Long = {
    val t0 = System.nanoTime()
    var res = block // call-by-name
    val t1 = System.nanoTime()
    (t1 - t0) / 1000000
  }
}

class TestUtil {

  var filesource = ""
  var filepath = filesource
  var index = 0
  var part = 0
  var point = 0
  var method = 0
  var sampelfactor = 0.01
  var partionsize = 10
  var prefilter = false;
  var autorange = false
  var order = 10
  var datasetfromrdd = false;
  var searchP = false;
  var listpoints = false;
  var dataset = false;
  val searchsize = 30;
  val intervalfakt = 1000;
  val searchPolygon: STObject = STObject(s"Polygon((-$searchsize $searchsize, $searchsize $searchsize, $searchsize -$searchsize, -$searchsize -$searchsize, -$searchsize $searchsize))", Interval(2 * intervalfakt, 5 * intervalfakt))
  val secondPolygon: STObject = STObject(s"Polygon((-$searchsize $searchsize, $searchsize $searchsize, $searchsize -$searchsize, -$searchsize -$searchsize, -$searchsize $searchsize))", Interval(6 * intervalfakt, 7 * intervalfakt))
  var secondPoint = 1
  var secondquery = false;


  var sc = createSparkContext("asd")

  var spark = createSparkSession("asdas");

  def printhelp(): Unit = {
    println("Jacobs TestUtil")
    println("[Options] FILE  | DEFAULT ")

    println("example : " + "-p 3 -ps 30 -nf -os 20 -sf 0.01 -ar -tp -ti 200M_1-10000.csv")
    println("using point 3, make 30 partitions, dont filter result after tree query, order for r-tree: 20, samplefactor for temppart 0.01, use auto range fot remp part, use temporal partitioner, user temporal index")
    println()
    println("-h  | --help               for Help")
    println()
    println("-ni | --no-index               don't use any index | DEFAULT")
    println("-ti | --temporal-index         use temporal interval index")
    println("-si | --spatial-index          use spatial r-tree index")
    println()
    println("-np | --no-part                don't use any partitioner | DEFAULT")
    println("-gp | --grid-part              use spatial  grid partitioner")
    println("-bp | --bs-part                use spatial  BSPartitioner")
    println("-tp | --temp-part              use temporal partitioner")
    println()
    println("-p NUMBER | --point            use point [x] | DEFAULT " + point)
    println("-fs PATH | --file-source       use other file source | DEFAULT: " + filesource)
    println("-nf  | --no-filter             no filtering after tree-query")
    println("-ps NUMBER | --part-size       partition size | DEFAULT " + partionsize)
    println("-ar | --auto-range             partition size automatic on | DEFAULT off")
    println("-sf DOUBLE | --sample-factor   sample factor for auto range | DEFAULT " + sampelfactor)
    println("-os NUMBER | --order-size      order size | DEFAULT " + order)
    println("-lp | --list-points     list 20 points ")
    println()
    println("-ds | --data-set     use dataset ")
    println("-sp | --search-polygone     use search Polygone")
    println("-pf | --pre-filter     use pre filtering for datasets")
    println("-dsfr | --dataset-from-rdd     create dataset from rdd")
    println("-sq | --second-query    second query")
    println()
    println("-c  | --contains               use contains as Method | DEFAULT")
    println("-i  | --intersects             use intersects as Method")
    println("-cb | --containedby            use containedby as Method")
    println("-w  | --withinDistance         use withinDistance as Method")
  }

  def mainMethod(args: Array[String]): Unit = {
    for (arg <- args) {
      // println(arg)
    }



    if (args.contains("-h") || args.contains("--help") || args.size == 0) {
      printhelp()
    } else {

      var i = 0;
      while (i < args.size - 1) {

        val arg = args(i)
        arg match {
          case "-fs" => {
            filesource = args(i + 1)
            i += 1
          }
          case "-ni" => index = 0
          case "-ti" => index = 1
          case "-si" => index = 2
          case "-np" => part = 0
          case "-gp" => part = 1
          case "-bp" => part = 2
          case "-tp" => part = 3
          case "-c" => method = 0
          case "-i" => method = 1
          case "-ar" => autorange = true
          case "-ds" => dataset = true
          case "-sp" => searchP = true
          case "-pf" => prefilter = true
          case "-sq" => secondquery = true
          case "-dsfr" => datasetfromrdd = true
          case "-nf" => {
            LiveIntervalIndexedSpatialRDDFunctions.skipFilter = true
            LiveIndexedSpatialRDDFunctions.skipFilter = true
          }
          case "-cb" => method = 2
          case "-w" => method = 3
          case "-ps" => {
            partionsize = args(i + 1).toInt
            i += 1
          }
          case "-sf" => {
            sampelfactor = args(i + 1).toDouble
            i += 1
          }
          case "-os" => {
            order = args(i + 1).toInt
            i += 1
          }
          case "-p" => {
            point = args(i + 1).toInt
            i += 1
          }
          case "-lp" => listpoints = true
          case _ => println("unknown argument: " + arg)
        }

        i += 1
      }

      val file = args(args.size - 1)
      filepath = filesource + file
      //  if (Files.exists(Paths.get(filepath))) {

      val t = TestUtil.time(startProgramm())


      println("alltime: " + t)


      /*  } else {
          println("File does not exist: " + filepath)
          printhelp()
        }*/
    }
  }

  def startProgramm(): Unit = {

    println("Start Programm")
    println("using file: " + filepath)
    println("using point : " + point)


    if (dataset) {
      startdatasetProgramm2()
    } else {
      startrddProgramm()
    }


  }


  def blubb() = udf((start: Long) => (math.ceil(start / 1000).toInt).toLong)

  def startdatasetProgramm2(): Unit = {
    val ds = if (!datasetfromrdd) {
      println("createIntervalDataSet")
      createIntervalDataSet(spark, filepath)
    } else {
      println("createIntervalDataSetFromRdd")
      createIntervalDataSetFromRdd(sc, spark, filepath)
    }


    println("using dataset")

    implicit val myObjEncoder = org.apache.spark.sql.Encoders.kryo[(STObject)]

    /*var dsa = ds.repartition(10, ds("start"))
    var psa = dsa.filter(dsa("test") < 2)*/


    var searchData = searchPolygon
    if (!searchP) {
      val xs = ds.take(point + 1)(point)
      searchData = STObject(xs.stob, Interval(xs.start, xs.end))
    }
    println("search data : " + searchData)

    var psa = ds

    if (prefilter) {
      val t = searchData.getTemp.get
      val t2 = TestUtil.time(
        method match {
          case 0 => {
            println("time filter contains")
            psa = ds.where(ds("start") <= t.start.value and ds("end") >= t.end.get.value)
          }
          case 1 => {
            println("time filter intersects")
            psa = ds.where((ds("start") <= t.start.value and ds("end") >= t.start.value) or (ds("start") >= t.start.value and ds("start") <= t.end.get.value))
          }
          case 2 => {
            println("time filter containedby")
            psa = ds.where(ds("start") >= t.start.value and ds("end") <= t.end.get.value)
          }
          case _ => println(" wrong Method: " + method)
        }
      )

      println("\nelapsed time for dataset-pre-filter in ms: " + t2)

    } else {
      println("no prefilter")
    }


    var predicateFunc = JoinPredicate.predicateFunction(JoinPredicate.CONTAINS)
    var res1: Array[STO] = null
    method match {
      case 0 => {
        println("using Method contains")
        predicateFunc = JoinPredicate.predicateFunction(JoinPredicate.CONTAINS)
      }
      case 1 => {
        println("using Method intersects")
        predicateFunc = JoinPredicate.predicateFunction(JoinPredicate.INTERSECTS)
      }
      case 2 => {
        println("using Method containedby")
        predicateFunc = JoinPredicate.predicateFunction(JoinPredicate.CONTAINEDBY)
      }
      case _ => println(" wrong Method: " + method)
    }
    val t1 = TestUtil.time({
      val tmp = psa.filter { x => predicateFunc(STObject(x.stob, Interval(x.start, x.end)), searchData) }
      res1 = tmp.collect()
    }
    )
    println("\nelapsed time for dataset-method in ms: " + t1)

    if (res1.size < 1) {
      throw new Exception("found nothing:" + res1.size)
    } else {
      println("result size: " + res1.size)
    }
    //-------------------
    if (secondquery) {
      var psa = ds
      var searchData = secondPolygon
      if (!searchP) {
        val xs = ds.take(secondPoint + 1)(secondPoint)
        searchData = STObject(xs.stob, Interval(xs.start, xs.end))
      }
      val t = searchData.getTemp.get

      method match {
        case 0 => {
          println("time filter contains")
          psa = ds.where(ds("start") <= t.start.value and ds("end") >= t.end.get.value)
        }
        case 1 => {
          println("time filter intersects")
          psa = ds.where((ds("start") <= t.start.value and ds("end") >= t.start.value) or (ds("start") >= t.start.value and ds("start") <= t.end.get.value))
        }
        case 2 => {
          println("time filter containedby")
          psa = ds.where(ds("start") >= t.start.value and ds("end") <= t.end.get.value)
        }
        case _ => println(" wrong Method: " + method)
      }


      var res2: Array[STO] = null
      println("search data : " + searchData)
      val t2 = TestUtil.time({
        val tmp = psa.filter { x => predicateFunc(STObject(x.stob, Interval(x.start, x.end)), searchData) }
        res2 = tmp.collect()
      }
      )
      println("\nelapsed time2 for dataset-method in ms: " + t2)

      if (res2.size < 1) {
        throw new Exception("found nothing:" + res2.size)
      } else {
        println("result size2: " + res2.size)
      }
    }






    //val psa = ds.filter(ds("start") < xs.end)


  }


  def startdatasetProgramm(): Unit = {
    val ds = createIntervalDataSet(spark, filepath)







    val predicateFunc = JoinPredicate.predicateFunction(JoinPredicate.CONTAINS)


    implicit val myObjEncoder = org.apache.spark.sql.Encoders.kryo[STObject]

    /* val newds = ds.map(x => {
       STObject(x.stob, Interval(x.start, x.end))
     })*/
    val xs = ds.take(point + 1)(point)
    val searchData = STObject(xs.stob, Interval(xs.start, xs.end))

    //STObject("POINT (15.292502748164168 63.93914390076469)", Interval(617, 1670)) //newds.take(point + 1)(point)
    println("point-data : " + searchData)
    var res2: Array[STO] = null
    val t3 = TestUtil.time({
      //  val tmp = newds.filter { xs => predicateFunc(xs, searchData) }
      val tmp = ds.filter { x => predicateFunc(STObject(x.stob, Interval(x.start, x.end)), searchData) }
      res2 = tmp.collect()
    })
    println("\nelapsed time for dataset-method1 in ms: " + t3)
    println("result2 size: " + res2.size)
  }

  def startrddProgramm(): Unit = {

    val rddRaw = createIntervalRDD(sc, filepath)




    var searchData = searchPolygon
    if (!searchP) {
      searchData = rddRaw.take(point + 1)(point)._1
    }
    println("point-data : " + searchData)

    if (listpoints) {
      var i = 0;
      rddRaw.take(50).foreach(k => {
        println(i + "  -  point-data : " + k._1)
        i += 1
      })


    }

    var rdd = rddRaw

    val t2 = TestUtil.time(
      part match {
        case 0 => rdd = rddRaw // do nothing
        case 1 => rdd = rddRaw.partitionBy(new SpatialGridPartitioner(rddRaw, partionsize));
        case 2 => rdd = rddRaw.partitionBy(new BSPartitioner(rddRaw, 0.5, 1000))
        case 3 => rdd = rddRaw.partitionBy(new TemporalRangePartitioner(rddRaw, partionsize, autorange, sampelfactor))
        case _ => println(" wrong Partitioner: " + part)
      }
    )



    println("time for partitionby: " + t2)
    if (rdd.partitioner.isDefined) {
      println("using " + rdd.partitioner.get.getClass.getSimpleName + " as partitioner (partition-size: " + partionsize + " )")
      // val d = rdd.mapPartitions(iter => Array(iter.size).iterator, true)
      // println("partitionsizes: "+d.collect().mkString(","))
    } else {
      println("using no partitioner")
    }



    var indexData: SpatialRDDFunctions[STObject, (String, STObject)] = null



    /* index match {
       case 0 => println("using no index")
       case 1 => println("using interval index")
       case 2 => println("using spatial index")
       case _ => println(" wrong Index: " + index)
     }*/

    index match {
      case 0 => indexData = rdd // do nothing
      case 1 => indexData = rdd.liveIntervalIndex()
      case 2 => indexData = rdd.liveIndex(order)
      case _ => println(" wrong Index: " + index)
    }

    println("using " + indexData.getClass.getSimpleName + " for indexing (order if spatialindex: " + order + " )")
    println("skipping filter on spatial/interval index: " + LiveIntervalIndexedSpatialRDDFunctions.skipFilter)


    var res: Array[(STObject, (String, STObject))] = null
    val t1 = TestUtil.time(
      method match {
        case 0 => {
          println("using Method contains")
          val tmp = indexData.contains2(searchData)
          res = tmp.collect()
        }
        case 1 => {
          println("using Method intersects")
          val tmp = indexData.intersects2(searchData)
          res = tmp.collect()
        }
        case 2 => {
          println("using Method containedby")
          val tmp = indexData.containedby2(searchData)
          res = tmp.collect()
        }
        case 3 => {
          println("using Method withinDistance")
          val tmp = indexData.withinDistance(searchData, 0.5, (g1, g2) => g1.getGeo.distance(g2.getGeo))
          res = tmp.collect()
        }
        case _ => println(" wrong Method: " + method)
      }
    )

    println("\nelapsed time for method in ms: " + t1)




    if (res.size < 1) {
      throw new Exception("found nothing:" + res.size)
    } else {
      println("result size: " + res.size)
    }

    /* println()
     println()
     println()
     println()
     println()
     printhelp()*/

    if(secondquery){
      searchData = secondPolygon
      if (!searchP) {
        searchData = rddRaw.take(secondPoint + 1)(secondPoint)._1
      }
      var res: Array[(STObject, (String, STObject))] = null
      val t1 = TestUtil.time(
        method match {
          case 0 => {
            println("using Method contains")
            val tmp = indexData.contains2(searchData)
            res = tmp.collect()
          }
          case 1 => {
            println("using Method intersects")
            val tmp = indexData.intersects2(searchData)
            res = tmp.collect()
          }
          case 2 => {
            println("using Method containedby")
            val tmp = indexData.containedby2(searchData)
            res = tmp.collect()
          }
          case 3 => {
            println("using Method withinDistance")
            val tmp = indexData.withinDistance(searchData, 0.5, (g1, g2) => g1.getGeo.distance(g2.getGeo))
            res = tmp.collect()
          }
          case _ => println(" wrong Method: " + method)
        }
      )

      println("\nelapsed time2 for method in ms: " + t1)




      if (res.size < 1) {
        throw new Exception("found nothing:" + res.size)
      } else {
        println("result size2: " + res.size)
      }
    }
  }


  def createIntervalRDD(
                         sc: SparkContext,
                         file: String = "src/test/resources/intervaltest.csv",
                         sep: Char = ';',
                         numParts: Int = 32,
                         distinct: Boolean = false): RDD[(STObject, (String, STObject))] = {

    val rdd = sc.textFile(file, if (distinct) 1 else numParts) // let's start with only one partition and repartition later
      .map { line => line.split(sep) }
      .map { arr =>
        (arr(0), STObject(arr(1), Interval(arr(2).toInt, arr(3).toInt)))
      }
      .keyBy(_._2)


    rdd
  }


  def createIntervalDataSetFromRdd(
                                    sc: SparkContext,
                                    sparks: SparkSession,
                                    file: String = "src/test/resources/intervaltest.csv",
                                    sep: Char = ';',
                                    numParts: Int = 32,
                                    distinct: Boolean = false) = {
    import collection.JavaConverters._
    import sparks.implicits._
    val rdd1 = sc.textFile(file, if (distinct) 1 else numParts) // let's start with only one partition and repartition later
    val rdd = rdd1.map { line => Row.fromSeq(line.split(sep)) }
    val fields = "id,stob,start,end".split(",").map(fieldName => StructField(fieldName, if (fieldName.equals("stob")) {
      StringType
    } else {
      LongType
    }, nullable = true))
    val schema = StructType(fields)
    var ds = spark.createDataFrame(rdd, schema).as[STO]

    ds
  }


  def createIntervalDataSet(
                             sparks: SparkSession,
                             file: String = "src/test/resources/20_1-100.csv",
                             sep: String = ";"
                           ) = {
    import sparks.implicits._
    //import sparkss.implicits._
    val sparkss = sparks.read.option("inferSchema", "true").option("delimiter", sep).csv(file).toDF("id", "stob", "start", "end")
    //sparkss.withColumn("test", blubb()(sparkss("start")))
    sparkss.as[STO];

  }


  def createSparkContext(name: String) = {
    val conf = new SparkConf().setAppName(name)
    new SparkContext(conf)
  }

  def createSparkSession(name: String) = {
    SparkSession
      .builder()
      .appName(name)
      .getOrCreate()
  }


}

case class STO(id: Long, stob: String, start: Long, end: Long)
