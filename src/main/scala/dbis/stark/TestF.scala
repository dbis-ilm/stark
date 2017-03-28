package dbis.stark

import dbis.stark.spatial.JoinPredicate
import dbis.stark.spatial.indexed.{IntervalTree1, RTree}
import dbis.stark.spatial.plain.IndexTyp
import dbis.stark.spatial.plain.IndexTyp
import dbis.stark.spatial.plain.IndexTyp.IndexTyp
import spire.syntax.order

/**
  * Created by Jacob on 28.03.2017.
  */
object TestF extends Serializable{
  def getJP(method: Int): (STObject, STObject) => Boolean = {
    var predicateFunc = JoinPredicate.predicateFunction(JoinPredicate.CONTAINS)

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
    predicateFunc
  }


  def getf2(indexTyp:IndexTyp, order :Int, searchData:STObject) = (s: Iterator[STO]) => {
        s
  }


  def getf(indexTyp:IndexTyp, order :Int, searchData:STObject) = (s: Iterator[STO]) => {
      indexTyp match {
        case IndexTyp.SPATIAL => {
        println("using spatial index")
          val tree = new RTree[STObject, (STObject, STO)](order)

          s.foreach(x => {val ob = STObject(x.stob, Interval(x.start, x.end))
            tree.insert(ob, (ob, x))})

          /* while (s.hasNext) {
             val x = s.next()
             val ob = STObject(x.stob, Interval(x.start, x.end))
             tree.insert(ob, (ob, x))
           }*/

          tree.build()
          tree.query(searchData).map(x => x._2)

        }
        case IndexTyp.TEMPORAL => {

          println("using temporal index")
          val tree = new IntervalTree1[STObject, STO]()

          s.foreach(x => {val ob = STObject(x.stob, Interval(x.start, x.end))
            tree.insert(ob, (ob, x))})

         /* while (s.hasNext) {
            val x = s.next()
            val ob = STObject(x.stob, Interval(x.start, x.end))
            tree.insert(ob, (ob, x))
          }*/
          tree.query(searchData).map(x => x._2)

        }
        case _ => {
          println("using no index")
          s
        }
      }
  }
}
