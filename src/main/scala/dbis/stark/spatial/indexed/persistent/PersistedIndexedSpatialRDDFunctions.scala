package dbis.stark.spatial.indexed.persistent

import dbis.stark.spatial.JoinPredicate
import dbis.stark.spatial.JoinPredicate.JoinPredicate
import dbis.stark.spatial.indexed.{Index, KnnIndex, WithinDistanceIndex}
import dbis.stark.{Distance, STObject}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class PersistedIndexedSpatialRDDFunctions[G <: STObject : ClassTag, V: ClassTag](
    @transient private val self: RDD[Index[(G,V)]]) extends Serializable {

  def contains(qry: G) = self.flatMap { tree => tree.query(qry).filter{ c => c._1.contains(qry) } }

  def containedby(qry: G) = self.flatMap{ tree => tree.query(qry).filter{ c => c._1.containedBy(qry)} }

  def intersects(qry: G) = self.flatMap { tree =>
    tree.query(qry).filter{ c => c._1.intersects(qry)}
  }

  def covers(qry: G) = self.flatMap{ tree =>
    tree.query(qry).filter{ c => c._1.covers(qry)}
  }

  def coveredby(qry: G) = self.flatMap{ tree =>
    tree.query(qry).filter{ c => c._1.coveredBy(qry)}
  }

  def join[V2 : ClassTag](other: RDD[(G, V2)], pred: (G,G) => Boolean, oneToMany: Boolean) =
    new PersistentIndexedSpatialJoinRDD(self, other, pred, oneToMany)


  def join[V2 : ClassTag](right: RDD[(G, V2)], pred: JoinPredicate, oneToMany: Boolean = false): RDD[(V, V2)] = {

    if(self.partitioner.isDefined && self.partitioner == right.partitioner) {
      self.zipPartitions(right) { case (leftIter, rightIter) =>

        if(leftIter.isEmpty || rightIter.isEmpty)
          Seq.empty[(V,V2)].iterator
        else {
          val predicateFunction = JoinPredicate.predicateFunction(pred)
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
      new PersistentIndexedSpatialJoinRDD(self, right, pred, oneToMany)
  }


  def kNN(qry: STObject, k: Int, distFunc: (STObject, STObject) => Distance) = {

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

  def withinDistance(qry: STObject, maxDist: Distance, distFunc: (STObject,STObject) => Distance) =
    self.mapPartitions({ trees =>
    trees.flatMap{ tree =>
      require(tree.isInstanceOf[WithinDistanceIndex[_]], s"withinDistance function requires WithinDistanceIndex but got: ${tree.getClass}")
      //  tree.query(qry, Predicates.withinDistance(maxDist, distFunc) _)
      tree.asInstanceOf[WithinDistanceIndex[(G,V)]].withinDistance(qry, distFunc, maxDist)
    }
  }, true) // preserve partitioning

}