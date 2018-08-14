package dbis.stark.spatial.indexed.live

import dbis.stark.spatial.JoinPredicate.JoinPredicate
import dbis.stark.spatial.indexed._
import dbis.stark.spatial.partitioner.SpatialPartitioner
import dbis.stark.spatial.{SpatialFilterRDD, _}
import dbis.stark.{Distance, STObject}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class LiveIndexedSpatialRDDFunctions[G <: STObject : ClassTag, V: ClassTag](
         rdd: RDD[(G, V)],
         indexConfig: IndexConfig
       ) extends SpatialRDDFunctions[G, V](rdd) with Serializable {

  def intersects(qry: G) = new SpatialFilterRDD[G, V](rdd, qry, JoinPredicate.INTERSECTS, Some(indexConfig))

  def contains(qry: G) = new SpatialFilterRDD[G, V](rdd, qry, JoinPredicate.CONTAINS, Some(indexConfig))

  def containedby(qry: G) = new SpatialFilterRDD[G, V](rdd, qry, JoinPredicate.CONTAINEDBY, Some(indexConfig))

  def withinDistance(
		  qry: G,
		  maxDist: Distance,
		  distFunc: (STObject,STObject) => Distance
	  ): RDD[(G, V)] = rdd.mapPartitions { iter =>
          // we don't know how the distance function looks like and thus have to scan all partitions

          val tree = IndexFactory.get[G,(G,V)](indexConfig)

          require(tree.isInstanceOf[WithinDistanceIndex[_]], s"index must support withinDistance, but is: ${tree.getClass}")

          val idxTree = tree.asInstanceOf[Index[(G,V)] with WithinDistanceIndex[(G,V)]]


          // Build our index live on-the-fly
          iter.foreach{ case (geom, data) =>
            /* we insert a pair of (geom, data) because we want the tupled
             * structure as a result so that subsequent RDDs build from this
             * result can also be used as SpatialRDD
             */
            idxTree.insert(geom, (geom,data))
          }
          idxTree.build()

          idxTree.withinDistance(qry, distFunc, maxDist)

        }


  def kNN(qry: G, k: Int, distFunc: (STObject, STObject) => Distance): RDD[(G,(Distance,V))] = {
    val r = rdd.mapPartitionsWithIndex({(idx,iter) =>
              val partitionCheck = rdd.partitioner.forall { p =>
                p match {
                  case sp: SpatialPartitioner => Utils.toEnvelope(sp.partitionBounds(idx).extent).intersects(qry.getGeo.getEnvelopeInternal)
                  case _ => true
                }
              }

              if(partitionCheck) {

                val tree = IndexFactory.get[G,(G,V)](indexConfig)

                require(tree.isInstanceOf[KnnIndex[_]], s"index must support kNN, but is: ${tree.getClass}")

                val idxTree = tree.asInstanceOf[Index[(G,V)] with KnnIndex[(G,V)]]

                iter.foreach{ case (g,v) => tree.insert(g,(g,v)) }

                idxTree.build()

                idxTree.kNN(qry, k, distFunc)
              }
              else
                Iterator.empty

            })

          .map { case (g,v) => (g, (distFunc(g,qry), v)) }
          .sortBy(_._2._1, ascending = true)
          .take(k)

    rdd.sparkContext.parallelize(r)
  }

  /**
   * Perform a spatial join using the given predicate function.
   * When using this variant partitions cannot not be pruned. And basically a cartesian product has
   * to be computed and filtered
   *
   * ==NOTE==
   * This method will <b>NOT</b> use an index as the given predicate function may want to find elements that are not returned
   * by the index query (which does an intersect)
   *
   * @param other The other RDD to join with
   * @param pred A function to compute the join predicate. The first parameter is the geometry of the left input RDD (i.e. the RDD on which this function is called)
   * and the parameter is the geometry of other
   * @return Returns an RDD containing the Join result
   */
  def join[V2: ClassTag](other: RDD[(G,V2)], pred: (G,G) => Boolean) = {
    new SpatialJoinRDD(rdd, other, pred)
  }

  /**
   * Perform a spatial join using the given predicate and a partitioner.
   * The input RDDs are both partitioned using the provided partitioner. (If they were already partitoned by the same
   * partitioner nothing is changed).
   * This method uses the fact of the same partitioning of both RDDs and prunes partitiones that cannot contribute to the
   * join
   *
   * @param other The other RDD to join with
   * @param pred The join predicate
   * @param partitioner The partitioner to partition both RDDs with
   * @return Returns an RDD containing the Join result
   */
  def join[V2 : ClassTag](other: RDD[(G, V2)], pred: JoinPredicate, partitioner: Option[SpatialPartitioner] = None) = {
      new SpatialJoinRDD(
          if(partitioner.isDefined) rdd.partitionBy(partitioner.get) else rdd,
          if(partitioner.isDefined) other.partitionBy(partitioner.get) else other,
          pred,
        Some(indexConfig))
  }
  
  def cluster[KeyType](
		  minPts: Int,
		  epsilon: Double,
		  keyExtractor: ((G,V)) => KeyType,
		  includeNoise: Boolean = true,
		  maxPartitionCost: Int = 10,
		  outfile: Option[String] = None
		  ) : RDD[(G, (Int, V))] = ???

  override def skyline(ref: STObject,
                       distFunc: (STObject, STObject) => (Distance, Distance),
                       dominates: (STObject, STObject) => Boolean,
                       ppD: Int,
                       allowCache: Boolean): RDD[(G, V)] = {

    /*
     * Branch & Bound Skyline, Papadias SIGMOD 2003
     * Nearest Neighbor Search, Kossman VLDB 2002
     */
    ???
  }

  override def skylineAgg(ref: STObject,
                          distFunc: (STObject, STObject) => (Distance, Distance),
                          dominates: (STObject, STObject) => Boolean
                         ): RDD[(G,V)] = {

    //    def combine(sky: Skyline[(G,V)], tuple: (G,V)): Skyline[(G,V)] = {
    //      val dist = Distance.euclid(tuple._1, ref)
    //      val distObj = STObject(dist._1.value, dist._2.value)
    //      sky.insert((distObj, tuple))
    //      sky
    //    }
    //
    //    def merge(sky1: Skyline[(G,V)], sky2: Skyline[(G,V)]): Skyline[(G,V)] = {
    //      sky1.merge(sky2)
    //    }


    ???
  }
}

