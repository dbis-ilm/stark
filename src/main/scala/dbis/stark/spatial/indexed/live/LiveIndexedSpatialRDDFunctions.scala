package dbis.stark.spatial.indexed.live

import dbis.stark.spatial.JoinPredicate.JoinPredicate
import dbis.stark.spatial._
import dbis.stark.spatial.indexed._
import dbis.stark.spatial.partitioner.GridPartitioner
import dbis.stark.{Distance, STObject}
import org.apache.spark.SpatialFilterRDD
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class LiveIndexedSpatialRDDFunctions[G <: STObject : ClassTag, V: ClassTag](
                                                                             self: RDD[(G, V)],
                                                                             indexConfig: IndexConfig
       ) extends SpatialRDDFunctions[G, V](self) with Serializable {

  val o = Ordering.fromLessThan[(G,(Distance,V))](_._2._1 < _._2._1)

  override def intersects(qry: G) = new SpatialFilterRDD[G, V](self, qry, JoinPredicate.INTERSECTS, Some(indexConfig))

  override def contains(qry: G) = new SpatialFilterRDD[G, V](self, qry, JoinPredicate.CONTAINS, Some(indexConfig))

  override def covers(qry: G) = new SpatialFilterRDD[G,V](self, qry, JoinPredicate.COVERS, Some(indexConfig))
  override def coveredby(qry: G) = new SpatialFilterRDD[G,V](self, qry, JoinPredicate.COVEREDBY, Some(indexConfig))

  override def containedby(qry: G) = new SpatialFilterRDD[G, V](self, qry, JoinPredicate.CONTAINEDBY, Some(indexConfig))

  override def withinDistance(
		  qry: G,
		  maxDist: Distance,
		  distFunc: (STObject,STObject) => Distance
	  ): RDD[(G, V)] = self.mapPartitions { iter =>
          // we don't know how the distance function looks like and thus have to scan all partitions

          val tree = IndexFactory.get[(G,V)](indexConfig)

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


  override def kNN(qry: G, k: Int, distFunc: (STObject, STObject) => Distance): RDD[(G,(Distance,V))] = {
    val r = self.mapPartitionsWithIndex({ (_, iter) =>

      val tree = IndexFactory.get[(G,V)](indexConfig)
      require(tree.isInstanceOf[KnnIndex[_]], s"index must support kNN, but is: ${tree.getClass}")

      val idxTree = tree.asInstanceOf[Index[(G,V)] with KnnIndex[(G,V)]]
      iter.foreach{ case (g,v) =>
        idxTree.insert(g,(g,v))
      }

      idxTree.build()
      idxTree.kNN(qry, k, distFunc)

    }).map{ case ((g,v),d) =>
      (g,(d,v))
    }
      .takeOrdered(k)(o)

    self.sparkContext.parallelize(r)
  }

  override def knnAgg(qry: G, k: Int, distFunc: (STObject, STObject) => Distance): RDD[(G,(Distance,V))] = {
    val knns = self.mapPartitions({iter =>
      val tree = IndexFactory.get[(G,V)](indexConfig)

      require(tree.isInstanceOf[KnnIndex[_]], s"index must support kNN, but is: ${tree.getClass}")

      val idxTree = tree.asInstanceOf[Index[(G,V)] with KnnIndex[(G,V)]]

      iter.foreach{ case (g,v) => tree.insert(g,(g,v)) }

      idxTree.build()

      val knnIter = idxTree.kNN(qry, k, distFunc)
                            .map(_.swap)
                            .toArray

      val knn = new KNN[(G,V)](k)

      knn.set(knnIter)
      Iterator.single(knn)

    }, true)
        .reduce(_.merge(_))


    self.sparkContext.parallelize(knns.iterator.map{ case (d,(g,v)) => (g,(d,v))}.toSeq)
  }

  override def knnTake(qry: G, k: Int, distFunc: (STObject, STObject) => Distance) = kNN(qry, k, distFunc)

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
  override def join[V2: ClassTag](other: RDD[(G,V2)], pred: (G,G) => Boolean, oneToManyPartitioning: Boolean) = {
    new SpatialJoinRDD(self, other, pred, oneToMany = oneToManyPartitioning)
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
  override def join[V2 : ClassTag](other: RDD[(G, V2)], pred: JoinPredicate, partitioner: Option[GridPartitioner] = None, oneToMany: Boolean = false) = {
      new SpatialJoinRDD(
        if (partitioner.isDefined) self.partitionBy(partitioner.get) else self,
        if (partitioner.isDefined) other.partitionBy(partitioner.get) else other,
        pred,
        Some(indexConfig), oneToMany = oneToMany)
  }

  def zipJoin[V2 : ClassTag](other: RDD[(G,V2)], pred: JoinPredicate): RDD[(V, V2)] = {

    self.zipPartitions(other, preservesPartitioning = true){ (leftIter,rightIter) =>

      if(!leftIter.hasNext || !rightIter.hasNext) {
        Iterator.empty
      } else {

        val index = IndexFactory.get[(G, V)](indexConfig)
        leftIter.foreach { case (g, v) => index.insert(g, (g, v)) }
        index.build()

        val predFunc = JoinPredicate.predicateFunction(pred)
        rightIter.flatMap { case (rg, rv) =>
          index.query(rg)
            .filter { case (lg, _) => predFunc(lg, rg) }
            .map { case (_, lv) => (lv, rv) }
        }
      }
    }.distinct()
  }


  /**
    * Performs a broadcast join. The relation "other" is broadcasted to all partitions of this RDD and thus, "other"
    * should be the smaller one and fit into memory!
    * @param other The smaller relation to join with - will be broadcasted
    * @param pred The join predicate
    * @tparam V2 Payload type in second relation
    * @return Returns an RDD with payload values from left and right
    */
  override def broadcastJoinL[V2 : ClassTag](other: RDD[(G, V2)], pred: JoinPredicate): RDD[(V, V2)] = {
    val otherArray = other.collect()
    val otherBC = self.sparkContext.broadcast(otherArray)

    self.mapPartitions{iter =>
      val predFunc = JoinPredicate.predicateFunction(pred)
      val tree = IndexFactory.get[(G,V)](indexConfig)

      iter.foreach{ case (g,v) => tree.insert(g, (g,v))}
      tree.build()

      otherBC.value.iterator.flatMap{ right =>
        tree.query(right._1)
          .filter{ case (g,_) => predFunc(g, right._1) }
          .map{ case (_,v) => (v, right._2) }
      }
    }
  }

  /**
    * Performs a broadcast join. The relation "other" is broadcasted to all partitions of this RDD and thus, "other"
    * should be the smaller one and fit into memory!
    * @param other The smaller relation to join with - will be broadcasted
    * @param pred The join predicate
    * @tparam V2 Payload type in second relation
    * @return Returns an RDD with payload values from left and right
    */
  override def broadcastJoin[V2 : ClassTag](other: RDD[(G, V2)], pred: JoinPredicate): RDD[(V, V2)] = {
    val otherArray = other.collect()
    val otherBC = self.sparkContext.broadcast(otherArray)

    self.mapPartitions{iter =>
      val predFunc = JoinPredicate.predicateFunction(pred)
      val tree = IndexFactory.get[(G,V2)](indexConfig)
      otherBC.value.iterator.foreach{ case (g,v2) => tree.insert(g,(g,v2))}
      tree.build()

      iter.flatMap{ left =>
        tree.query(left._1)
          .filter{ case (g,_) => predFunc(left._1, g) }
          .map{ case (_,v2) => (left._2, v2) }
      }
    }
  }

  def broadcastJoinWithIndex[V2 : ClassTag](other: RDD[Index[(G,V2)]], pred: JoinPredicate): RDD[(V, V2)] = {
    val otherArray = other.collect()
    val otherBC = self.sparkContext.broadcast(otherArray)

    self.mapPartitions{iter =>
      val predFunc = JoinPredicate.predicateFunction(pred)
      val otherArr = otherBC.value

      iter.flatMap{ left =>
        otherArr.iterator.flatMap{ idx =>
          idx.query(left._1)
            .filter{ case (g,_) => predFunc(left._1, g) }
            .map{ case (_,v2) => (left._2, v2) }
        }
      }
    }
  }

  override def knnJoin[V2: ClassTag](other: RDD[Index[V2]], k: Int, distFunc: (STObject,STObject) => Distance): RDD[(V,V2)] = {
    new SpatialKnnJoinRDD(self, other, k, distFunc)
  }

  override def cluster[KeyType](
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

  override def knnAggIter(ref: G, k: Int, distFunc: (STObject, STObject) => Distance): Iterator[(G, (Distance, V))] = ???
  override def knnAgg2Iter(ref: G, k: Int, distFunc: (STObject, STObject) => Distance): Iterator[(G, (Distance, V))] = ???
  override def knn2(qry: G, k: Int, distFunc: (STObject, STObject) => Distance): Iterator[(G, (Distance, V))] = ???
}

