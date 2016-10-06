package dbis.stark.spatial.plain

import scala.reflect.ClassTag
import scala.collection.mutable.ListBuffer

import org.apache.spark.Partition
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.InterruptibleIterator

import dbis.stark.spatial.SpatialRDD
import dbis.stark.STObject
import dbis.stark.spatial.SpatialPartitioner
import dbis.stark.spatial.Utils
import dbis.stark.spatial.JoinPredicate

class JoinSpatialRDD[G <: STObject : ClassTag, V: ClassTag, V2: ClassTag](
    var left: RDD[(G,V)], 
    var right: RDD[(G,V2)],
    predicate: (G,G) => Boolean
    )  extends RDD[(V,V2)](left.context, Nil) {
  
  val numPartitionsInRdd2 = right.getNumPartitions
  
  def this(left: RDD[(G,V)], right: RDD[(G,V2)], predicate: JoinPredicate.JoinPredicate) = 
    this(left, right, JoinPredicate.predicateFunction(predicate))
  
  override def getPartitions = {
    val parts = ListBuffer.empty[CartesianPartition]
    
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
  
  
  @DeveloperApi
  override def compute(s: Partition, context: TaskContext): Iterator[(V,V2)] = {
    val split = s.asInstanceOf[CartesianPartition]
    
//    val map = SpatialRDD.createExternalMap[G,V,V2]
    
    
    val pairs = for(
        l <- left.iterator(split.s1, context);
        r <- right.iterator(split.s2, context)
        if(predicate(l._1, r._1))) yield(l._1, (l._2,r._2))
        
//    map.insertAll(pairs)
    
//    val f = map.iterator.flatMap{ case (g, l) => l}
    
//    new InterruptibleIterator(context, f)
      pairs.map{ case (_, p) => p }
  }
  
  
  override def clearDependencies() {
    super.clearDependencies()
    left = null
    right = null
  }
  
}