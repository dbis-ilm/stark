package dbis.stark.spatial.indexed.live

import dbis.stark.spatial.JoinPredicate.JoinPredicate
import dbis.stark.{TestUtil, STObject}
import scala.reflect.ClassTag
import dbis.stark.spatial._
import org.apache.spark.rdd.RDD
import dbis.stark.spatial.indexed.RTree

import dbis.stark.spatial.JoinPredicate.JoinPredicate
import dbis.stark.spatial.JoinPredicate._
import dbis.stark.spatial.plain.{IndexTyp, SpatialFilterRDD, PlainSpatialRDDFunctions}


object LiveIndexedSpatialRDDFunctions {

  var skipFilter = false


}

class LiveIndexedSpatialRDDFunctions[G <: STObject : ClassTag, V: ClassTag](
                                                                             rdd: RDD[(G, V)],
                                                                             capacity: Int
                                                                           ) extends PlainSpatialRDDFunctions[G, V](rdd) with Serializable {

  override def doWork[G <: STObject : ClassTag, V: ClassTag](
                                                              qry: G,
                                                              iter: Iterator[(G, V)],
                                                              predicate: (STObject, STObject) => Boolean
                                                            ) = {


    // println("using capacity for r-tree: "+capacity)
    val indexTree = new RTree[G, (G, V)](capacity)
    val t1 = TestUtil.time(
      {

        // Build our index live on-the-fly
        iter.foreach { case (geom, data) =>
          /* we insert a pair of (geom, data) because we want the tupled
         * structure as a result so that subsequent RDDs build from this
         * result can also be used as SpatialRDD
         */
          indexTree.insert(geom, (geom, data))
        }
        indexTree.build()

      }
    )
    //  println("time for treebuilt: " + t1)

    var result: Iterator[(G, V)] = null
    val t2 = TestUtil.time(
      // now query the index
      result = indexTree.query(qry)

    )
    //   println("time for tree-query: " + t2)
    /* The result of a r-tree query are all elements that
     * intersect with the MBB of the query region. Thus,
     * for all result elements we need to check if they
     * really intersect with the actual geometry
     */
    if (!LiveIndexedSpatialRDDFunctions.skipFilter) {
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
  override def intersects2(o: G) = new SpatialFilterRDD[G,V](rdd, o, JoinPredicate.INTERSECTS,IndexTyp.SPATIAL,capacity)
  override def containedby2(o: G) = new SpatialFilterRDD[G,V](rdd, o, JoinPredicate.CONTAINEDBY,IndexTyp.SPATIAL,capacity)
  override def contains2(o: G) = new SpatialFilterRDD[G,V](rdd, o, JoinPredicate.CONTAINS,IndexTyp.SPATIAL,capacity)

  override def withinDistance(
                               qry: G,
                               maxDist: Double,
                               distFunc: (STObject, STObject) => Double
                             ) = rdd.mapPartitions({ iter =>
    // we don't know how the distance function looks like and thus have to scan all partitions
    //      LiveIndexedSpatialRDDFunctions.doWork(qry, iter, Predicates.withinDistance(maxDist, distFunc) _, capacity)

    val indexTree = new RTree[G, (G, V)](capacity)

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


  override def kNN(qry: G, k: Int): RDD[(G, (Double, V))] = {
    val r = rdd.mapPartitionsWithIndex({ (idx, iter) =>
      val partitionCheck = rdd.partitioner.map { p =>
        p match {
          case sp: SpatialPartitioner => Utils.toEnvelope(sp.partitionBounds(idx).extent).intersects(qry.getGeo.getEnvelopeInternal)
          case _ => true
        }
      }.getOrElse(true)

      if (partitionCheck) {
        val tree = new RTree[G, (G, V)](capacity)

        iter.foreach { case (g, v) => tree.insert(g, (g, v)) }

        tree.build()

        val result = tree.kNN(qry, k)
        result
      }
      else
        Iterator.empty

    })

      .map { case (g, v) => (g, (g.distance(qry.getGeo), v)) }
      .sortBy(_._2._1, ascending = true)
      .take(k)

    rdd.sparkContext.parallelize(r)
  }

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
  override  def join[V2: ClassTag](other: RDD[(G, V2)], pred: (STObject, STObject) => Boolean) =
    new PlainSpatialRDDFunctions(rdd).join(other, pred)

  //    new LiveIndexedSpatialCartesianJoinRDD(rdd.sparkContext, rdd, other, pred, capacity)

  /**
    * Perform a spatial join using the given predicate and a partitioner.
    * The input RDDs are both partitioned using the provided partitioner. (If they were already partitoned by the same
    * partitioner nothing is changed).
    * This method uses the fact of the same partitioning of both RDDs and prunes partitiones that cannot contribute to the
    * join
    *
    * @param other       The other RDD to join with
    * @param pred        The join predicate
    * @param partitioner The partitioner to partition both RDDs with
    * @return Returns an RDD containing the Join result
    */
  override def join[V2: ClassTag](other: RDD[(G, V2)], pred: JoinPredicate, partitioner: Option[SpatialPartitioner] = None) = {

    new LiveIndexedJoinSpatialRDD(
      if (partitioner.isDefined) rdd.partitionBy(partitioner.get) else rdd,
      if (partitioner.isDefined) other.partitionBy(partitioner.get) else other,
      pred,
      capacity)
  }

  override  def cluster[KeyType](
                                  minPts: Int,
                                  epsilon: Double,
                                  keyExtractor: ((G, V)) => KeyType,
                                  includeNoise: Boolean = true,
                                  maxPartitionCost: Int = 10,
                                  outfile: Option[String] = None
                                ): RDD[(G, (Int, V))] = ???


}

