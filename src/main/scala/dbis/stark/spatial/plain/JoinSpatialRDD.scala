package dbis.stark.spatial.plain

import dbis.stark.STObject
import dbis.stark.spatial.{JoinPredicate, SpatialPartitioner}
import org.apache.spark.{Dependency, NarrowDependency, Partition, TaskContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class JoinSpatialRDD[G <: STObject : ClassTag, V: ClassTag, V2: ClassTag](
    var left: RDD[(G,V)], 
    var right: RDD[(G,V2)],
    predicate: (G,G) => Boolean
    )  extends RDD[(V,V2)](left.context, Nil) {
  
  val numPartitionsInRdd2 = right.getNumPartitions
  
  def this(left: RDD[(G,V)], right: RDD[(G,V2)], predicate: JoinPredicate.JoinPredicate) = 
    this(left, right, JoinPredicate.predicateFunction(predicate))
  
  override def getPartitions = {
    val parts = ArrayBuffer.empty[CartesianPartition]
    
    val checkPartitions = leftParti.isDefined && rightParti.isDefined
    var idx = 0
    for (
        s1 <- left.partitions; 
        s2 <- right.partitions
        if(!checkPartitions || leftParti.get.partitionExtent(s1.index).intersects(rightParti.get.partitionExtent(s2.index)))) {
      
      parts += new CartesianPartition(idx, left, right, s1.index, s2.index)
      idx += 1
    }
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
  
  
  @DeveloperApi
  override def compute(s: Partition, context: TaskContext): Iterator[(V,V2)] = {
    val split = s.asInstanceOf[CartesianPartition]
    
    val rightList = right.iterator(split.s2, context).toArray

    left.iterator(split.s1, context).flatMap{ case (lg, lv) =>
        rightList.view.filter{ case (rg, _) => predicate(lg,rg)}.map{ case (_,rv) => (lv,rv) }

//      rightList.filter{ case (rg, _) => predicate(lg,rg)}.map{ case (_,rv) => (lv,rv) }
    }
    
    
    
//    for(
//      l <- left.iterator(split.s1, context);
//      r <- right.iterator(split.s2, context)
//      if(predicate(l._1, r._1))) yield(l._2,r._2)
        
  }
  
  
  override def clearDependencies() {
    super.clearDependencies()
    left = null
    right = null
  }
  
}