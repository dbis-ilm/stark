package dbis.stark.spatial.plain

import scala.reflect.ClassTag
import org.apache.spark.Partition
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.annotation.DeveloperApi
import dbis.stark.spatial.SpatialRDD
import org.apache.spark.Dependency
import org.apache.spark.OneToOneDependency
import dbis.stark.spatial.indexed.persistent.JoinPartition
import dbis.stark.spatial.indexed.persistent.NarrowIndexJoinSplitDep
import org.apache.spark.util.collection.ExternalAppendOnlyMap
import scala.collection.mutable.ListBuffer
import org.apache.spark.InterruptibleIterator
import dbis.stark.STObject
import dbis.stark.spatial.SpatialPartitioner
import dbis.stark.spatial.Utils
import org.apache.spark.NarrowDependency

class JoinSpatialRDD[G <: STObject : ClassTag, V: ClassTag, V2: ClassTag](
    @transient val left: RDD[(G,V)], 
    @transient val right: RDD[(G,V2)],
    predicate: (G,G) => Boolean
//    part: SpatialPartitioner
//  )  extends SpatialRDD[G,(V,V2)](left.context, Seq(new OneToOneDependency(left), new OneToOneDependency(right))) {
    )  extends RDD[(V,V2)](left.context, Seq(new OneToOneDependency(left), new OneToOneDependency(right))) {
  
  override def getPartitions = Array.tabulate(left.getNumPartitions){ i =>  
    
    val leftExtent = leftParti.map { p => Utils.toEnvelope(p.partitionBounds(i).extent) }    
    new JoinPartition(i, 
		  new NarrowIndexJoinSplitDep(
	      left, 
	      i, 
	      left.partitions(i)),
//		    Array.tabulate(right.getNumPartitions){ j => 
//		      new NarrowIndexJoinSplitDep(right, j, right.partitions(j))  
//        }
	      // create only right dependencies if the partition bound intersects with the left bounds
	      (0 until right.getNumPartitions).filter { j =>
	        if(leftExtent.isDefined && rightParti.isDefined) {
	          val rightExtent = Utils.toEnvelope(rightParti.get.partitionBounds(j).extent)
	          leftExtent.get.intersects(rightExtent)
	        } else
	          true
	        
        }.map( j => new NarrowIndexJoinSplitDep(right, j, right.partitions(j))).toArray 
		 )
  }
  
  private lazy val leftParti = {
    val p = left.partitioner 
      if(p.isDefined) {
        p.get match {
          case sp: SpatialPartitioner[G,V] => Some(sp)
          case _ => None
        }
      } else 
        None
  }
  private lazy val rightParti = {
    val p = right.partitioner 
      if(p.isDefined) {
        p.get match {
          case sp: SpatialPartitioner[G,V2] => Some(sp)
          case _ => None
        }
      } else 
        None
  }
  
  
  @DeveloperApi
  override def compute(s: Partition, context: TaskContext): Iterator[(V,V2)] = {
    val split = s.asInstanceOf[JoinPartition]
    val leftIter  = split.leftDep.rdd.iterator(split.leftDep.split, context).asInstanceOf[Iterator[(G,V)]].toList
    
    // again check if the right geom lies within the extent of left
    // specially useful if only left was partitioned
    val rightIter = split.rightDeps.flatMap( d => d.rdd.iterator(d.split, context).asInstanceOf[Iterator[(G,V2)]])
                      .filter { case (rg,_) => 
                        leftParti.map { p => 
                          rg.getEnvelopeInternal.intersects(Utils.toEnvelope(p.partitionBounds(s.index).extent))
                        }.getOrElse(true)
                      }

    val map = createExternalMap
    
    for((lg,lv) <- leftIter) {
      
      val res = rightIter
                  .filter { case (rg, rv) => predicate(lg, rg) }
                  .map { case (rg, rv) => (lg,(lv, rv)) }
      
  		map.insertAll(res)
      
    }
    
//    val f = map.iterator.flatMap{ case (g, l) => l.map { case (lv, rv) => (g,(lv,rv)) } }
    val f = map.iterator.flatMap{ case (g, l) => l}
    
    new InterruptibleIterator(context, f)
  }
  
  type ValuePair = (V,V2)
  type Combiner = ListBuffer[ValuePair]
  
  private def createExternalMap(): ExternalAppendOnlyMap[G, ValuePair, Combiner] = {
    val createCombiner: ( ValuePair => Combiner) = pair => ListBuffer(pair)
    
    val mergeValue: (Combiner, ValuePair) => Combiner = (list, value) => list += value
    
    val mergeCombiners: ( Combiner, Combiner ) => Combiner = (c1, c2) => c1 ++= c2
    
    new ExternalAppendOnlyMap[G, ValuePair, Combiner](createCombiner, mergeValue, mergeCombiners)
    
  }  
  
  
}