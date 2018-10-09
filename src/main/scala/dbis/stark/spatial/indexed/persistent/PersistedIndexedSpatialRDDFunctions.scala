package dbis.stark.spatial.indexed.persistent

import dbis.stark.STObject.MBR
import dbis.stark.spatial.JoinPredicate.JoinPredicate
import dbis.stark.spatial.indexed.{Index, KnnIndex, WithinDistanceIndex}
import dbis.stark.spatial.partitioner.GridPartitioner
import dbis.stark.spatial._
import dbis.stark.{Distance, STObject}
import org.apache.spark.{SpatialRDD, TaskContext}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class PersistedIndexedSpatialRDDFunctions[G <: STObject : ClassTag, V: ClassTag](
    @transient private val self: RDD[Index[(G,V)]]) extends SpatialRDDFunctions[G,V](self.flatMap(_.items)) with Serializable {

  implicit val ord = Ordering.fromLessThan[((G,V),Distance)](_._2 < _._2)

  val o = Ordering.fromLessThan[(G,(Distance,V))](_._2._1 < _._2._1)

  override def contains(qry: G) = self.flatMap { tree => tree.query(qry).filter{ c => c._1.contains(qry) } }

  override def containedby(qry: G) = self.flatMap{ tree => tree.query(qry).filter{ c => c._1.containedBy(qry)} }

  override def intersects(qry: G) = self.flatMap { tree =>
    tree.query(qry).filter{ c => c._1.intersects(qry)}
  }

  override def covers(qry: G) = self.flatMap{ tree =>
    tree.query(qry).filter{ c => c._1.covers(qry)}
  }

  override def coveredby(qry: G) = self.flatMap{ tree =>
    tree.query(qry).filter{ c => c._1.coveredBy(qry)}
  }

  override def join[V2 : ClassTag](other: RDD[(G, V2)], pred: (G,G) => Boolean, oneToMany: Boolean) =
    new PersistentIndexedSpatialJoinRDD(self, other, pred, oneToMany)


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
      .map{ case ((g,v), dist) => (g, (dist, v))}
      .takeOrdered(k)(o)

    self.sparkContext.parallelize(nn)
  }

  override def knnAgg(qry: G, k: Int, distFunc: (STObject, STObject) => Distance): RDD[(G, (Distance, V))] = {

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

    self.sparkContext.parallelize(knns.iterator.map{ case (d,(g,v)) => (g,(d,v))}.toSeq)
  }

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

  def knnAgg2(qry: G, k: Int, distFunc: (STObject, STObject) => Distance): RDD[(G, (Distance, V))] = {
    val knns = self.mapPartitions({ trees =>
      trees.flatMap { tree =>
        require(tree.isInstanceOf[KnnIndex[_]], s"kNN function requires KnnIndex but got: ${tree.getClass}")
        tree.asInstanceOf[KnnIndex[(G,V)]].kNN(qry, k, distFunc)
//          .map{ case (g,v) => (distFunc(g,qry), (g,v))}
      }
    }, true)


    val theKnn = knns.takeOrdered(k)(ord).toSeq

    self.sparkContext.parallelize(theKnn).map{ case ((g,v),d) => (g,(d,v))}
  }

  def knn2(qry: G, k: Int, distFunc: (STObject, STObject) => Distance): RDD[(G,(Distance, V))] = self.partitioner match {
    case Some(p: GridPartitioner) =>

      val partitionOfQry = p.getPartition(qry)

      def blubb(context: TaskContext, iter: Iterator[Index[(G,V)]]) = {
        iter.flatMap{ index => index.asInstanceOf[KnnIndex[(G,V)]].kNN(qry, k, distFunc)}.toArray
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
      var maxDist = knnsInPart.toStream.sortBy(_._2).tail.head._2.minValue
      if(maxDist <= 0.0) {
        // for maxDist == 0 the box would be a point and not find any addtitional kNNs
        // we maybe should iteratively increase the box size
        println("fallback to knnAgg")
        knnAgg(qry, k, distFunc)
      }
      else {


        val qryPoint = qry.getGeo.getCentroid.getCoordinate

        val env = Utils.makeGeo(new MBR(qryPoint.x - maxDist, qryPoint.x + maxDist, qryPoint.y - maxDist, qryPoint.y + maxDist))

        val knns = this.intersects(STObject(env).asInstanceOf[G]).map { case (g, v) => (g, (distFunc(qry, g), v)) }.takeOrdered(k)(o.reverse)

        self.sparkContext.parallelize(knns)
      }

    case _ => kNN(qry,k,distFunc)
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


}