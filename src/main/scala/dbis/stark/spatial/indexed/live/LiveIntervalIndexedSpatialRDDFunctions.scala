package dbis.stark.spatial.indexed.live

import dbis.stark.spatial.JoinPredicate.JoinPredicate
import dbis.stark.{TestUtil, STObject}
import dbis.stark.spatial.JoinPredicate.JoinPredicate
import dbis.stark.spatial.{JoinPredicate, SpatialPartitioner}
import dbis.stark.spatial.indexed.{IntervalTree1, RTree}
import dbis.stark.spatial.plain.{IndexTyp, SpatialFilterRDD, PlainSpatialRDDFunctions}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag


object LiveIntervalIndexedSpatialRDDFunctions {
  var skipFilter = false

}

class LiveIntervalIndexedSpatialRDDFunctions[G <: STObject : ClassTag, V: ClassTag](
                                                                                     rdd: RDD[(G, V)]
                                                                                   ) extends PlainSpatialRDDFunctions[G, V](rdd) with Serializable {


  override def intersects2(o: G) = new SpatialFilterRDD[G,V](rdd, o, JoinPredicate.INTERSECTS,IndexTyp.TEMPORAL)
  override def containedby2(o: G) = new SpatialFilterRDD[G,V](rdd, o, JoinPredicate.CONTAINEDBY,IndexTyp.TEMPORAL)
  override def contains2(o: G) = new SpatialFilterRDD[G,V](rdd, o, JoinPredicate.CONTAINS,IndexTyp.TEMPORAL)

  override def doWork[G <: STObject : ClassTag, V: ClassTag](
                                                              qry: G,
                                                              iter: Iterator[(G, V)],
                                                              predicate: (STObject, STObject) => Boolean
                                                            ) = {
    val indexTree = new IntervalTree1[G, V]()

    val t1 = TestUtil.time(
      // Build our index live on-the-fly
      iter.foreach { case (geom, data) =>
        indexTree.insert(geom, (geom, data))
      }
    )
    //println("time for treebuilt: " + t1)

    var result: Iterator[(G, V)] = null
    // now query the index
    val t2 = TestUtil.time(
      result = indexTree.query(qry)
    )
   // println("time for tree-query: " + t2)

    if (!LiveIntervalIndexedSpatialRDDFunctions.skipFilter) {
      var filteredResult: Iterator[(G, V)] = null
      val t3 = TestUtil.time(
        filteredResult = result.filter { case (g, _) =>
          // println(g+"->"+qry+"  |  "+"spat: "+g.containsSpatial(qry) + " | "+"temp: "+g.containsTemporal(qry))
          predicate(g, qry)

        }
      )
     // println("time for resultfilter: " + t3)
      filteredResult

    } else {
      //println("skip filter")
      result
    }
  }


  //TODO
  override def withinDistance(
                               qry: G,
                               maxDist: Double,
                               distFunc: (STObject, STObject) => Double
                             ) = rdd.mapPartitions({ iter =>
    // we don't know how the distance function looks like and thus have to scan all partitions
    //      LiveIndexedSpatialRDDFunctions.doWork(qry, iter, Predicates.withinDistance(maxDist, distFunc) _, capacity)

    val indexTree = new RTree[G, (G, V)](4)

    // Build our index live on-the-fly
    iter.foreach { case (geom, data) =>
      /* we insert a pair of (geom, data) because we want the tupled
       * structure as a result so that subsequent RDDs build from this
       * result can also be used as SpatialRDD
       */
      indexTree.insert(geom, (geom, data))
    }
    indexTree.build()

    indexTree.withinDistance(qry, distFunc, maxDist)

  })


  override def kNN(qry: G, k: Int): RDD[(G, (Double, V))] = ???

  /**
    * Perform a spatial join using the given predicate function.
    * When using this variant partitions cannot not be pruned. And basically a cartesian product has
    * to be computed and filtered<br><br>
    *
    * <b>NOTE</b> This method will <b>NOT</b> use an index as the given predicate function may want to find elements that are not returned
    * by the index query (which does an intersect)
    *
    * @param other The other RDD to join with
    * @param pred  A function to compute the join predicate. The first parameter is the geometry of the left input RDD (i.e. the RDD on which this function is called)
    *              and the parameter is the geometry of <code>other</code>
    * @return Returns an RDD containing the Join result
    */
  override def join[V2: ClassTag](other: RDD[(G, V2)], pred: (STObject, STObject) => Boolean) =
    new PlainSpatialRDDFunctions(rdd).join(other, pred)

  //    new LiveIndexedSpatialCartesianJoinRDD(rdd.sparkContext, rdd, other, pred, capacity)


  def join1[V2: ClassTag](other: RDD[(G, V2)], pred: JoinPredicate, partitioner: Option[SpatialPartitioner] = None) = ???

  override def cluster[KeyType](
                                 minPts: Int,
                                 epsilon: Double,
                                 keyExtractor: ((G, V)) => KeyType,
                                 includeNoise: Boolean = true,
                                 maxPartitionCost: Int = 10,
                                 outfile: Option[String] = None
                               ): RDD[(G, (Int, V))] = ???


}

