package dbis.stark.spatial.indexed.persistent

import java.nio.file.Paths

import dbis.stark.STObject.MBR
import dbis.stark.spatial.JoinPredicate.JoinPredicate
import dbis.stark.spatial._
import dbis.stark.spatial.indexed.{Index, KnnIndex, WithinDistanceIndex}
import dbis.stark.spatial.partitioner.GridPartitioner
import dbis.stark.{Distance, STObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SpatialRDD, TaskContext}

import scala.reflect.ClassTag



class PersistedIndexedSpatialRDDFunctions[G <: STObject : ClassTag, V: ClassTag](
    @transient private val self: RDD[Index[(G,V)]]) extends SpatialRDDFunctions[G,V](self.flatMap(_.items)) with Serializable {

  implicit val ord = Ordering.fromLessThan[((G,V),Distance)](_._2 < _._2)

  val o = Ordering.fromLessThan[(G,(Distance,V))](_._2._1 < _._2._1)

  override def contains(qry: G) = //self.flatMap { tree => tree.query(qry).filter{ c => c._1.contains(qry) } }
    new SpatialIndexedRDD(self, qry, JoinPredicate.CONTAINS )

  override def containedby(qry: G) = {/*
    self.mapPartitions({ iter =>

      val res = ListBuffer.empty[(G,V)]

      iter.foreach { tree =>
        val candidates = tree.queryL(qry)
        var i = 0
        while(i < candidates.length) {
          if(candidates(i)._1.containedBySpatial(qry))
            res += candidates(i)
          i += 1
        }
      }
      res.iterator
    }, preservesPartitioning = true)*/
        new SpatialIndexedRDD(self, qry, JoinPredicate.CONTAINEDBY )
  }

  override def intersects(qry: G) =
    new SpatialIndexedRDD(self, qry, JoinPredicate.INTERSECTS )

  override def covers(qry: G) = new SpatialIndexedRDD(self, qry, JoinPredicate.COVERS )

  override def coveredby(qry: G) = new SpatialIndexedRDD(self, qry, JoinPredicate.COVEREDBY )

  override def join[V2 : ClassTag](other: RDD[(G, V2)], pred: (G,G) => Boolean, oneToMany: Boolean) =
    new PersistentIndexedSpatialJoinRDD(self, other, pred, oneToMany)

  def joinT[V2 : ClassTag](other: RDD[(G, V2)], pred: JoinPredicate, oneToMany: Boolean) =
    new PersistentIndexedSpatialJoinRDD(self, other, pred, oneToMany)

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

    self.mapPartitions{ iter =>
      val predFunc = JoinPredicate.predicateFunction(pred)
      val otherArr = otherBC.value
      iter.flatMap{ idx =>
        otherArr.iterator.flatMap { right =>
          idx.query(right._1)
            .filter{case (lg,_) => predFunc(lg, right._1)}
            .map{ case (_,lv) => (lv,right._2)}
        }
      }
    }
  }

  override def broadcastJoinL[V2 : ClassTag](other: RDD[(G, V2)], pred: JoinPredicate): RDD[(V, V2)] = 
    broadcastJoin(other, pred)

//  def zipJoin[V2 : ClassTag](other: RDD[(G,V2)], pred: JoinPredicate): RDD[(V, V2)] = {
//    println(self.partitioner)
//    println(other.partitioner)
//
//    require(self.partitioner.isDefined && self.partitioner.get.isInstanceOf[SpatialGridPartitioner[G]],"zip join only for spatial grid partitioners")
//    //    require(self.partitioner == other.partitioner, "zip join only works for same spatial partitioners")
//
//    println("zip join")
//
//    self.zipPartitions(other, preservesPartitioning = true){ (left,right) =>
//
//      val predFunc = JoinPredicate.predicateFunction(pred)
//
//      left.flatMap { tree =>
//        right.flatMap { case (rg, rv) =>
//          tree.query(rg)
//            .filter { case (lg, _) => predFunc(lg, rg) }
//            .map { case (_, lv) => (lv, rv) }
//        }
//      }
//    }
//  }


  def broadcastJoinWithIndex[V2 : ClassTag](other: RDD[Index[(G,V2)]], pred: JoinPredicate): RDD[(V, V2)] = ???

  override def join[V2: ClassTag](right: RDD[(G, V2)], predicate: JoinPredicate, partitioner: Option[GridPartitioner] = None, oneToMany: Boolean = false): RDD[(V, V2)] = {

    if(SpatialRDD.isSpatialParti(self.partitioner) && self.partitioner == right.partitioner) {
      self.zipPartitions(right) { case (leftIter, rightIter) =>

        if(leftIter.isEmpty || rightIter.isEmpty)
          Seq.empty[(V,V2)].iterator
        else {
          val predicateFunction = JoinPredicate.predicateFunction(predicate)
          leftIter.flatMap { index =>
            rightIter.flatMap { case (rg, rv) =>
              index.query(rg)
                .filter { case (lg, _) => predicateFunction(lg, rg) }
                .map { case (_, lv) => (lv, rv) }
            }
          }
        }
      }
    } else
      new PersistentIndexedSpatialJoinRDD(self, right, predicate, oneToMany)
  }


  override def kNN(qry: G, k: Int, distFunc: (STObject, STObject) => Distance) = {

    val nn = self.mapPartitions({ trees =>
        trees.flatMap { tree =>
          require(tree.isInstanceOf[KnnIndex[_]], s"kNN function requires KnnIndex but got: ${tree.getClass}")
          tree.asInstanceOf[KnnIndex[(G,V)]].kNN(qry, k, distFunc)
        }
    }, true)
//    .map { case (g,v) => (g, (distFunc(g,qry), v)) }
//    .sortBy(_._2._1, ascending = true)
//    .take(k)
      .map{ case ((g,v), dist) =>
//        println(s"$g --> $dist")
        (g, (dist, v))}
      .takeOrdered(k)(o)

//    println(s"taken: ${nn.mkString("\n")}")

    self.sparkContext.parallelize(nn)
  }

  def knnAggIter(qry: G, k: Int, distFunc: (STObject, STObject) => Distance): Iterator[(G, (Distance, V))] = {

    val knns = self.mapPartitions({ trees =>
      trees.flatMap { tree =>
        require(tree.isInstanceOf[KnnIndex[_]], s"kNN function requires KnnIndex but got: ${tree.getClass}")
        val knnIter = tree.asInstanceOf[KnnIndex[(G,V)]].kNN(qry, k, distFunc)
//          .map{ case (g,v) => (distFunc(g,qry), (g,v))}
          .toIndexedSeq

        val knn = new KNN[(G,V)](k)
        knn.set(knnIter.map(_.swap))

        Iterator.single(knn)

      }
    }, true)
        .reduce(_.merge(_))

    knns.iterator.map{ case (d,(g,v)) => (g,(d,v))}
  }

  override def knnAgg(qry: G, k: Int, distFunc: (STObject, STObject) => Distance): RDD[(G, (Distance, V))] =
    self.sparkContext.parallelize(knnAggIter(qry,k,distFunc).toSeq)

  override def knnTake(qry: G, k: Int, distFunc: (STObject, STObject) => Distance) = {
    val knns = self.mapPartitions({ trees =>
      trees.flatMap { tree =>
        require(tree.isInstanceOf[KnnIndex[_]], s"kNN function requires KnnIndex but got: ${tree.getClass}")
        tree.asInstanceOf[KnnIndex[(G,V)]].kNN(qry, k, distFunc)
//          .map{ case (g,v) => (distFunc(g,qry), (g,v))}
      }
    }, true)

//    implicit val ord = new Ordering[(Distance,(G,V))] {
//      override def compare(x: (Distance,(G,V)), y: (Distance,(G,V))) = if(x._1 < y._1) -1 else if(x._1 > y._1) 1 else 0
//    }
    val theKnn = knns.takeOrdered(k)(ord).toSeq

    self.sparkContext.parallelize(theKnn).map{ case ((g,v),d) => (g,(d,v))}
  }


  def knnAgg2Iter(qry: G, k: Int, distFunc: (STObject, STObject) => Distance): Iterator[(G, (Distance, V))] = {
    val knns = self.mapPartitions({ trees =>
      trees.flatMap { tree =>
        require(tree.isInstanceOf[KnnIndex[_]], s"kNN function requires KnnIndex but got: ${tree.getClass}")
        tree.asInstanceOf[KnnIndex[(G,V)]].kNN(qry, k, distFunc)
//          .map{ case (g,v) => (distFunc(g,qry), (g,v))}
      }
    }, true)


    knns.takeOrdered(k)(ord).iterator.map{ case ((g,v),d) => (g,(d,v))}
  }

  def knnAgg2(qry: G, k: Int, distFunc: (STObject, STObject) => Distance): RDD[(G, (Distance, V))] =
    self.sparkContext.parallelize(knnAgg2Iter(qry,k,distFunc).toSeq)

  def knn2(qry: G, k: Int, distFunc: (STObject, STObject) => Distance): Iterator[(G,(Distance, V))] = self.partitioner match {
    case Some(p: GridPartitioner) =>

      // get partition of query point
      val partitionOfQry = p.getPartition(qry)

      // a function to get knnInPartition
      def getPartitionsKNN(context: TaskContext, iter: Iterator[Index[(G,V)]]) = {
        if(iter.nonEmpty && context.partitionId() == partitionOfQry)
          iter.flatMap{ index => index.asInstanceOf[KnnIndex[(G,V)]].kNN(qry, k, distFunc)}.toArray
        else
          Array.empty[((G,V),Distance)]
      }

      // the kNNs in the same partition as the reference point
      val knnsInPart = self.sparkContext.runJob(self, getPartitionsKNN _,Seq(partitionOfQry)).apply(0)

      // get the maximum distance of all points in the ref's partition
      val maxDist = if (knnsInPart.isEmpty) 0 // none found
      else if (knnsInPart.length == 1)
        knnsInPart(0)._2.maxValue // only one
      else //order by distance to get last one. minValue is from intervaldistance
        knnsInPart.toStream.maxBy(_._2)._2.maxValue

      // make a box around ref point and get all intersecting partitions
      val qryPoint = qry.getGeo.getCentroid.getCoordinate
      val mbr = new MBR(qryPoint.x - maxDist, qryPoint.x + maxDist, qryPoint.y - maxDist, qryPoint.y + maxDist)
      val env = StarkUtils.makeGeo(mbr)
      val intersectinPartitions = p.getIntersectingPartitionIds(env)

      if(knnsInPart.length == k && intersectinPartitions.length == 1) {
        // we found k points in the only partition that overlaps with the box --> final result
        knnsInPart.iterator.map { case ((g,v),d) => (g,(d,v))}
      } else if(knnsInPart.length > k && intersectinPartitions.length == 1) {
        // we found more than k points in the only partition --> get first k ones
        knnsInPart.toStream.sortBy(_._2.minValue).take(k).iterator.map { case ((g,v),d) => (g,(d,v))}
      } else {
        /* not enough points in current partition OR box overlaps with more partitions
           If the maxdist is 0 then there are only duplicates of ref and we have to do another search method
           without relying on the box
         */
        if (maxDist <= 0.0) {
          // for maxDist == 0 the box would be a point and not find any addtitional kNNs
          // we maybe should iteratively increase the box size
          knnAgg2Iter(qry, k, distFunc)
        }
        else {
          // execute query for all intersecting partitions
          val kNNsOfIntersecting = self.sparkContext.runJob(self, getPartitionsKNN _, intersectinPartitions)

          val numCandidates = kNNsOfIntersecting.iterator.map(_.length).sum

          val result: Iterator[(G,(Distance,V))] = if(numCandidates < k) {
            // we still havn't found enough...

            val knns = this.containedby(STObject(env).asInstanceOf[G])
                            .map { case (g, v) => (g, (distFunc(qry, g), v)) }
                            .takeOrdered(k)(o.reverse)

            knns.iterator
          } else {

            KNN.merge(kNNsOfIntersecting, k)

          }

          result
        }
      }

    case _ =>
      //println("no partitioner set")
      knnAgg2Iter(qry,k,distFunc)
  }

  override def zipJoin[V2 : ClassTag](other: RDD[(G,V2)], pred: JoinPredicate): RDD[(V, V2)] = {
    //    require(self.partitioner.isDefined && self.partitioner.get.isInstanceOf[SpatialGridPartitioner[G]],"zip join only for spatial grid partitioners")
    //    require(self.partitioner == other.partitioner, "zip join only works for same spatial partitioners")


    self.zipPartitions(other, preservesPartitioning = true){ (leftIter,rightIter) =>
      if(!leftIter.hasNext || !rightIter.hasNext)
        Iterator.empty
      else {
        val predFunc = JoinPredicate.predicateFunction(pred)
        val tree = leftIter.next()
        rightIter.flatMap { case (rg, rv) =>
          tree.query(rg)
            .filter { case (lg, _) => predFunc(lg, rg) }
            .map { case (_, lv) => (lv, rv) }
        }
      }
    }.distinct()
  }

  override def withinDistance(qry: G, maxDist: Distance, distFunc: (STObject,STObject) => Distance) =
    self.mapPartitions({ trees =>
    trees.flatMap{ tree =>
      require(tree.isInstanceOf[WithinDistanceIndex[_]], s"withinDistance function requires WithinDistanceIndex but got: ${tree.getClass}")
      //  tree.query(qry, Predicates.withinDistance(maxDist, distFunc) _)
      tree.asInstanceOf[WithinDistanceIndex[(G,V)]].withinDistance(qry, distFunc, maxDist)
    }
  }, true) // preserve partitioning



  override def knnJoin[V2: ClassTag](other: RDD[Index[V2]], k: Int, distFunc: (STObject, STObject) => Distance) = {
    new SpatialKnnJoinRDD(self.flatMap(_.items), other, k, distFunc)
  }

  override def skyline(ref: STObject, distFunc: (STObject, STObject) => (Distance, Distance), dominates: (STObject, STObject) => Boolean, ppD: Int, allowCache: Boolean) = ???

  override def skylineAgg(ref: STObject, distFunc: (STObject, STObject) => (Distance, Distance), dominates: (STObject, STObject) => Boolean) = ???

  override def cluster[KeyType](minPts: Int, epsilon: Double, keyExtractor: ((G,V)) => KeyType,
                        includeNoise: Boolean = true, maxPartitionCost: Int = 10,outfile: Option[String] = None) : RDD[(G, (Int, V))] = ???



  def saveAsStarkObjectFile(path: String): Unit = self.partitioner.flatMap{
    case sp: GridPartitioner => Some(sp)
    case _ =>
      None
  } match {
    case Some(sp) =>
      val wkts = self.partitions.indices.map{ i =>
        Array(sp.partitionExtent(i).wkt,"","","part-%05d".format(i)).mkString(STSparkContext.PARTITIONINFO_DELIM)
      }

      self.saveAsObjectFile(path)
      self.sparkContext.parallelize(wkts).saveAsTextFile(Paths.get(path,STSparkContext.PARTITIONINFO_FILE).toString)
    case _ =>
      self.saveAsObjectFile(path)
  }
}
