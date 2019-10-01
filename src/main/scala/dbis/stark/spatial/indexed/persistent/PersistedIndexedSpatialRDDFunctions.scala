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

  override def containedby(qry: G) = //self.flatMap{ tree => tree.query(qry).filter{ c => c._1.containedBy(qry)} }
    new SpatialIndexedRDD(self, qry, JoinPredicate.CONTAINEDBY )

  override def intersects(qry: G) =
    new SpatialIndexedRDD(self, qry, JoinPredicate.INTERSECTS )

  override def covers(qry: G) = new SpatialIndexedRDD(self, qry, JoinPredicate.COVERS )

  override def coveredby(qry: G) = new SpatialIndexedRDD(self, qry, JoinPredicate.COVEREDBY )

  override def join[V2 : ClassTag](other: RDD[(G, V2)], pred: (G,G) => Boolean, oneToMany: Boolean) =
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

      val partitionOfQry = p.getPartition(qry)

      def blubb(context: TaskContext, iter: Iterator[Index[(G,V)]]) = {
        if(iter.nonEmpty && context.partitionId() == partitionOfQry)
          iter.flatMap{ index => index.asInstanceOf[KnnIndex[(G,V)]].kNN(qry, k, distFunc)}.toArray
        else
          Array.empty[((G,V),Distance)]
      }

      val knnsInPart = self.sparkContext.runJob(self, blubb _,Seq(partitionOfQry)).flatten

//      val knnsInPart = self.mapPartitionsWithIndex({(idx, iter) =>
//        if(idx == partitionOfQry) {
//          iter.flatMap{ index => index.asInstanceOf[KnnIndex[(G,V)]].kNN(qry, k, distFunc)}
//        } else {
//          Iterator.empty
//        }
//      }, true).collect()


//      val maxDist = knnsInPart.maxBy(_._2)._2.minValue
      val maxDist = if(knnsInPart.isEmpty) 0
      else if(knnsInPart.length == 1) knnsInPart.head._2.minValue
      else knnsInPart.toStream.maxBy(_._2)._2.minValue //tail.head._2.minValue

      if(maxDist <= 0.0) {
        // for maxDist == 0 the box would be a point and not find any addtitional kNNs
        // we maybe should iteratively increase the box size
//        println("fallback to knnAgg")
        knnAgg2Iter(qry, k, distFunc)
      }
      else {


        val qryPoint = qry.getGeo.getCentroid.getCoordinate

        val env = StarkUtils.makeGeo(new MBR(qryPoint.x - maxDist, qryPoint.x + maxDist, qryPoint.y - maxDist, qryPoint.y + maxDist))

        val knns = this.containedby(STObject(env).asInstanceOf[G]).map { case (g, v) => (g, (distFunc(qry, g), v)) }.takeOrdered(k)(o.reverse)

        val result = (knns ++ knnsInPart.map{case ((g,v),d) => (g,(d,v))}).sorted(o).take(k)

//        self.sparkContext.parallelize(knns)
        result.iterator
      }

    case _ =>
      println("no partitioner set")
      knnAgg2Iter(qry,k,distFunc)
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