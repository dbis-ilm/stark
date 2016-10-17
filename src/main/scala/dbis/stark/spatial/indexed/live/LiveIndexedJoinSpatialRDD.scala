package dbis.stark.spatial.indexed.live


import scala.reflect.ClassTag
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.Partition
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.InterruptibleIterator

import dbis.stark.STObject
import dbis.stark.spatial.indexed.RTree
import dbis.stark.spatial.SpatialPartitioner
import dbis.stark.spatial.plain.CartesianPartition
import dbis.stark.spatial.JoinPredicate
import org.apache.spark.Dependency
import org.apache.spark.NarrowDependency


/**
 * A live indexed spatial join. 
 * <br><br>
 * Entries from left RDD are put into an R-tree upon execution. This tree
 * is queried for each STObject in right RDD.
 */
class LiveIndexedJoinSpatialRDD[G <: STObject : ClassTag, V: ClassTag, V2: ClassTag](
    var left: RDD[(G,V)], 
    var right: RDD[(G,V2)],
    predicate: JoinPredicate.JoinPredicate,
    capacity: Int
    )  extends RDD[(V,V2)](left.context, Nil) {
  
  val predicateFunction = JoinPredicate.predicateFunction(predicate)
  val numPartitionsInRdd2 = right.getNumPartitions
  
  /**
   * Get the partitions by this operator. We create the cartesian product for all
   * partition combinations of left and right RDDs and prune those combinations
   * that cannot contain join results, i.e., partitions that do not intersect 
   */
  override def getPartitions =  {
    val parts = ArrayBuffer.empty[CartesianPartition]
    
    val checkPartitions = leftParti.isDefined && rightParti.isDefined
    
    logDebug(s"apply partition pruning: $checkPartitions")
    var idx = 0
    for (
        s1 <- left.partitions; 
        s2 <- right.partitions
        if(!checkPartitions || leftParti.get.partitionExtent(s1.index).intersects(rightParti.get.partitionExtent(s2.index)))) {
      
      parts += new CartesianPartition(idx, left, right, s1.index, s2.index)
      idx += 1
    }
    logDebug(s"partition combinations: ${parts.size} (of ${left.partitions.size * right.partitions.size})")
    parts.toArray
  }
  
  private lazy val leftParti = {
    val p = left.partitioner 
      if(p.isDefined) {
        p.get match {
          case sp: SpatialPartitioner => Some(sp)
          case _ => None
        }
      } else 
        None
  }
  private lazy val rightParti = {
    val p = right.partitioner 
      if(p.isDefined) {
        p.get match {
          case sp: SpatialPartitioner => Some(sp)
          case _ => None
        }
      } else 
        None
  }
  
  @DeveloperApi
  override def compute(s: Partition, context: TaskContext): Iterator[(V,V2)] = {
    val split = s.asInstanceOf[CartesianPartition]
    
    val tree = new RTree[G,(G,V)](capacity)
    
    left.iterator(split.s1, context).foreach{ case (lg,lv) => tree.insert(lg, (lg,lv)) }

    tree.build()
    

  	right.iterator(split.s2, context).flatMap{ case (rg, rv) => 
      tree.query(rg)  // for each entry in right query the index
        .filter{ case (lg, _) => predicateFunction(lg,rg) } // index returns candidates only -> prune by checking predicate again
        .map { case (_, lv) => (lv,rv) } // emit join pairs
  	}
  }
  
  override def getPreferredLocations(split: Partition): Seq[String] = {
    val currSplit = split.asInstanceOf[CartesianPartition]
    (left.preferredLocations(currSplit.s1) ++ right.preferredLocations(currSplit.s2)).distinct
  }
  
  override def getDependencies: Seq[Dependency[_]] = List(
    new NarrowDependency(left) {
      def getParents(id: Int): Seq[Int] = List(id / numPartitionsInRdd2)
    },
    new NarrowDependency(right) {
      def getParents(id: Int): Seq[Int] = List(id % numPartitionsInRdd2)
    }
  )
  
  override def clearDependencies() {
    super.clearDependencies()
    left = null
    right = null
  }
}
