package dbis.stark.spatial.indexed.persistent

import dbis.stark.spatial.JoinPredicate.JoinPredicate
import dbis.stark.spatial.indexed.{Index, KnnIndex, WithinDistanceIndex}
import dbis.stark.spatial.partitioner.GridPartitioner
import dbis.stark.spatial.{JoinPredicate, KNN, SpatialKnnJoinRDD, SpatialRDDFunctions}
import dbis.stark.{Distance, STObject}
import org.apache.spark.SpatialRDD
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class PersistedIndexedSpatialRDDFunctions[G <: STObject : ClassTag, V: ClassTag](
    @transient private val self: RDD[Index[(G,V)]]) extends SpatialRDDFunctions[G,V](self.flatMap(_.items)) with Serializable {

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
    .map { case (g,v) => (g, (distFunc(g,qry), v)) }
    .sortBy(_._2._1, ascending = true)
    .take(k)

    self.sparkContext.parallelize(nn)
  }

  override def knnAgg(qry: G, k: Int, distFunc: (STObject, STObject) => Distance): RDD[(G, (Distance, V))] = {

//    val knns = self.mapPartitions({ trees =>
//      trees.flatMap { tree =>
//        require(tree.isInstanceOf[KnnIndex[_]], s"kNN function requires KnnIndex but got: ${tree.getClass}")
//        val knnIter = tree.asInstanceOf[KnnIndex[(G,V)]].kNN(qry, k, distFunc)
//          .map{ case (g,v) => (distFunc(g,qry), (g,v))}
//          .toIndexedSeq
//
//        val knn = new KNN[(G,V)](k)
//        knn.set(knnIter)
//
//        Iterator.single(knn)
//
//      }
//    }, true)
//        .reduce(_.merge(_))
//
//    self.sparkContext.parallelize(knns.iterator.map{ case (d,(g,v)) => (g,(d,v))}.toSeq)
val knns = self.mapPartitions({ trees =>
  trees.flatMap { tree =>
    require(tree.isInstanceOf[KnnIndex[_]], s"kNN function requires KnnIndex but got: ${tree.getClass}")
    tree.asInstanceOf[KnnIndex[(G,V)]].kNN(qry, k, distFunc)
      .map{ case (g,v) => (distFunc(g,qry), (g,v))}
  }
}, true)

    implicit val ord = new Ordering[(Distance,(G,V))] {
      override def compare(x: (Distance,(G,V)), y: (Distance,(G,V))) = if(x._1 < y._1) -1 else if(x._1 > y._1) 1 else 0
    }
    val theKnn = knns.takeOrdered(k)(ord).toSeq

    self.sparkContext.parallelize(theKnn).map{ case (d,(g,v)) => (g,(d,v))}
  }


  def knnAgg2(qry: G, k: Int, distFunc: (STObject, STObject) => Distance): RDD[(G, (Distance, V))] = {
    val knns = self.mapPartitions({ trees =>
      trees.flatMap { tree =>
        require(tree.isInstanceOf[KnnIndex[_]], s"kNN function requires KnnIndex but got: ${tree.getClass}")
        tree.asInstanceOf[KnnIndex[(G,V)]].kNN(qry, k, distFunc)
          .map{ case (g,v) => (distFunc(g,qry), (g,v))}
      }
    }, true)

    implicit val ord = new Ordering[(Distance,(G,V))] {
      override def compare(x: (Distance,(G,V)), y: (Distance,(G,V))) = if(x._1 < y._1) -1 else if(x._1 > y._1) 1 else 0
    }
    val theKnn = knns.takeOrdered(k)(ord.reverse).toSeq

    self.sparkContext.parallelize(theKnn).map{ case (d,(g,v)) => (g,(d,v))}
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