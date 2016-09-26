package dbis.stark.spatial.indexed.persistent

import scala.reflect.ClassTag
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer

import java.io.IOException
import java.io.ObjectOutputStream

import org.apache.spark.Dependency
import org.apache.spark.util.collection.ExternalAppendOnlyMap
import org.apache.spark.InterruptibleIterator

import org.apache.spark.rdd.RDD
import org.apache.spark.Partition
import org.apache.spark.TaskContext
import org.apache.spark.OneToOneDependency
import org.apache.spark.SparkEnv

import dbis.stark.spatial.SpatialRDD
import dbis.stark.spatial.indexed.RTree
import dbis.stark.spatial.SpatialPartitioner
import dbis.stark.STObject
import dbis.stark.spatial.Utils


protected[spatial] case class NarrowIndexJoinSplitDep(
    rdd: RDD[_],
    splitIndex: Int,
    var split: Partition
  ) extends Serializable {

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = { 
      // Update the reference to parent split at the time of task serialization
      split = rdd.partitions(splitIndex)
      oos.defaultWriteObject()
  }
}

protected[spatial] class JoinPartition(
    idx: Int, 
    val leftDep: NarrowIndexJoinSplitDep,
    val rightDeps: Array[NarrowIndexJoinSplitDep]
  ) extends Partition with Serializable {
  
  override def index = idx
  override def hashCode() = idx 
}



class PersistantIndexedSpatialJoinRDD[G <: STObject : ClassTag, V: ClassTag, V2: ClassTag](
    @transient val left: RDD[RTree[G, (G,V)]], //IndexedSpatialRDD[G,V] 
    @transient val right: RDD[(G,V2)],
    pred: (G,G) => Boolean
    )  extends RDD[(V,V2)](left.context, Nil) {
  
  
  override def getDependencies: Seq[Dependency[_]] = Seq(
    new OneToOneDependency(left),
    new OneToOneDependency(right)
  ) 
  
//  override def getPartitions = Array.tabulate(left.getNumPartitions){ i =>
//    new JoinPartition(i, 
//      new NarrowIndexJoinSplitDep(left, i, left.partitions(i)),
//      Array.tabulate(right.getNumPartitions)(j => new NarrowIndexJoinSplitDep(right, j, right.partitions(j))))
//  }
  
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
  
   
  
  override def compute(s: Partition, context: TaskContext): Iterator[(V,V2)] = {
    val split = s.asInstanceOf[JoinPartition]
    
    val leftIter = split.leftDep.rdd.iterator(split.leftDep.split, context).asInstanceOf[Iterator[RTree[G, (G,V)]]]
    
    val iters = ListBuffer.empty[Iterator[(G,Combiner)]] 
      
//		println(s"left size: ${left.size}")
    for(idx <- leftIter) {
//      println(s"left idx contains: ${idx.itemsTree().size()} ")
      val map = createExternalMap
     
      for(rDep <- split.rightDeps) {
        val rightIter = rDep.rdd.iterator(rDep.split, context).asInstanceOf[Iterator[(G,V2)]]
        
//        println(s"\tright size ${rightList.size}")
        
        
        val res = rightIter.flatMap{ case (rg, rv) =>
    		  idx.query(rg) // query R-Tree and get matching MBBs
        		  .filter{ case (lg, _) => pred(lg, rg) } // check if real geom also matches
        		  .map { case (lg, lv) => (lg,(lv, rv))  }    // key is the left geom to make it a spatial RDD again
        }
        
        map.insertAll(res)
      }
      
      iters += map.iterator
    }

//    val f = iters.flatten.flatMap { case (g, l) => l.map { case (lv,rv) => (g,(lv, rv)) }}
    val f = iters.flatten.flatMap { case (g, l) => l}
    
    // return an interruptable iterator of all our produced iterators
    new InterruptibleIterator(context, f.iterator)
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

