package dbis.stark

import dbis.stark.spatial.indexed._
import dbis.stark.spatial.JoinPredicate
import org.apache.spark.sql.Dataset

/**
  * Created by Jacob on 28.03.2017.
  */
object TestF extends Serializable {
  def prefilter(method: Int, ds: Dataset[ESTO], searchData: STObject,secondfilter : Boolean): Dataset[ESTO] = {
    var psa = ds
    val t = searchData.getTemp.get
    val env = searchData.getGeo.getEnvelopeInternal
    method match {


      case 0 =>
        println("pre time filter contains")
        psa = ds.where(ds("start") <= t.start.value and ds("end") >= t.end.get.value)
        if(secondfilter) {
          println("second filter contains")
          psa = psa.where(ds("minx") < env.getMinX)
            .where(ds("maxx") > env.getMaxX)
            .where(ds("miny") < env.getMinY)
            .where(ds("maxy") > env.getMaxY)
        }
      case 1 =>
        println("pre time filter intersects")
        psa = ds.where((ds("start") <= t.start.value and ds("end") >= t.start.value) or (ds("start") >= t.start.value and ds("start") <= t.end.get.value))
      case 2 =>
        println("pre time filter containedby")
        psa = ds.where(ds("start") >= t.start.value and ds("end") <= t.end.get.value)
        if(secondfilter) {
          println("second filter containedby")
          psa = psa.where(ds("minx") > env.getMinX)
            .where(ds("maxx") < env.getMaxX)
            .where(ds("miny") > env.getMinY)
            .where(ds("maxy") < env.getMaxY)
        }
      case _ => println(" wrong Method: " + method)
    }
    psa

  }

  def getJP(method: Int): (STObject, STObject) => Boolean = {
    var predicateFunc = JoinPredicate.predicateFunction(JoinPredicate.CONTAINS)

    method match {
      case 0 =>
        println("using Method contains")
        predicateFunc = JoinPredicate.predicateFunction(JoinPredicate.CONTAINS)
      case 1 =>
        println("using Method intersects")
        predicateFunc = JoinPredicate.predicateFunction(JoinPredicate.INTERSECTS)
      case 2 =>
        println("using Method containedby")
        predicateFunc = JoinPredicate.predicateFunction(JoinPredicate.CONTAINEDBY)
      case _ => println(" wrong Method: " + method)
    }
    predicateFunc
  }


  def getf2(indexConfig: IndexConfig, order: Int, searchData: STObject): Iterator[ESTO] => Iterator[ESTO] = (s: Iterator[ESTO]) => {
    s
  }


  def getf(indexConfig: IndexConfig, order: Int, searchData: STObject): Iterator[ESTO] => Iterator[ESTO] = (s: Iterator[ESTO]) => {

    val tree = IndexFactory.get[(STObject, ESTO)](indexConfig)

    s.foreach{x =>
      val ob = STObject(x.stob, Interval(x.start, x.end))
      tree.insert(ob, (ob, x))
    }

    tree.build()
    tree.query(searchData).map(x => x._2)
  }
}
