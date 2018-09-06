package dbis.stark

import org.locationtech.jts.index.intervalrtree.SortedPackedIntervalRTree
import dbis.stark.spatial.indexed.{Data, IntervalTree1}
import org.apache.spark.sql.{Dataset, SparkSession, SQLContext}
import utils.InvertavlTreeVisitor


/**
  * Created by Jacob on 21.03.2017.
  */
object DatasetTest {
//  def main(args: Array[String]) {
//
//    val spark = SparkSession
//      .builder()
//      .appName("Spark SQL basic example")
//      .master("local")
//      .getOrCreate()
//    import spark.implicits._
//
//    val dataframe = spark.read.option("inferSchema", "true").option("delimiter",";").csv("src/test/resources/20_1-100.csv").toDF("id","stob","start","end");
//
//    dataframe.show()
//
//
//
//
//    var dataset = dataframe.as[STO]
//
//
//
//
//
//
//    import scala.collection.JavaConversions.asScalaBuffer
//
//    dataset.describe("id").show()
//    dataset.explain()
//    dataset = dataset.repartition(5)
//      dataset.explain()
//
//
//
//    dataset = dataset.mapPartitions(x => {
//      val tree = new SortedPackedIntervalRTree()
//
//      x.foreach( d => tree.insert(d.start,d.end,d))
//
//      val visitor: InvertavlTreeVisitor = new InvertavlTreeVisitor()
//      tree.query(80, 85, visitor)
//
//     val s=  visitor.getVisitedItems.map(_.asInstanceOf[STO]).iterator
//
//     // println(s.mkString(" , "))
//      s
//    })
//
//    dataset.show()
//
//
//   // dataset.filter { x => predicateFunc(g, qry) }
//
//  }
}
/*


 val indexTree = new IntervalTree1[G, V]()
        // insert everything into the tree
        parent.iterator(split, context).foreach { case (g, v) => indexTree.insert(g, (g, v)) }
        indexTree.query(qry).filter { case (g, _) => predicateFunc(g, qry) }

 */






