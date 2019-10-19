package dbis.stark.spatial.indexed.persistent

import dbis.stark.STObject
import dbis.stark.spatial.JoinPredicate.JoinPredicate
import dbis.stark.spatial.indexed.Index
import dbis.stark.spatial.partitioner.{OneToManyPartition, OneToOnePartition}
import dbis.stark.spatial.{JoinPredicate, JoinRDD}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag


class PersistentIndexedSpatialJoinRDD[G <: STObject : ClassTag, V: ClassTag, V2: ClassTag](
    _left: RDD[Index[(G,V)]],
    _right: RDD[(G,V2)],
    predicateFunction: (G,G) => Boolean,
    private val checkParties: Boolean,
    oneToMany: Boolean = false
    )  extends JoinRDD[Index[(G,V)],(G,V2),(V,V2)](_left,_right, oneToMany, checkParties) {
  

  def this(left: RDD[Index[(G,V)]], right:RDD[(G,V2)], predicate: JoinPredicate, oneToMany:Boolean) =
    this(left, right, JoinPredicate.predicateFunction(predicate), checkParties = true, oneToMany = oneToMany)

  override protected def computeWithOneToOnePartition(partition: OneToOnePartition, context: TaskContext) = {
    val rightArray =right.iterator(partition.rightPartition, context).toArray

    left.iterator(partition.leftPartition, context).flatMap{ tree =>
      /*
       * Returns:
       * an Envelope (for STRtrees), an Interval (for SIRtrees), or other object
       * (for other subclasses of AbstractSTRtree)
       *
       * http://www.atetric.com/atetric/javadoc/com.vividsolutions/jts-core/1.14.0/com/vividsolutions/jts/index/strtree/Boundable.html#getBounds--
       */

      rightArray.flatMap { case (rg,rv) =>
        tree.query(rg)
          .filter { case (lg,_) => predicateFunction(lg,rg)}
          .map{ case (_,lv) => (lv,rv) }
      }
    }
  }

  override protected def computeWithOneToMany(partition: OneToManyPartition, context: TaskContext) = {
    left.iterator(partition.leftPartition, context).flatMap { tree =>

      partition.rightPartitions.iterator.flatMap{ rp =>
        right.iterator(rp, context).flatMap { case (rg,rv) =>
          tree.query(rg)
            .filter { case (lg,_) => predicateFunction(lg,rg)}
            .map{ case (_,lv) => (lv,rv) }
        }
      }
    }
  }
}

