package dbis.stark.spatial.indexed.persistent

import org.apache.spark.rdd.CoGroupedRDD
import dbis.stark.spatial.SpatialRDD
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import org.apache.spark.Partition
import org.apache.spark.TaskContext
import org.apache.spark.rdd.CoGroupPartition
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.OneToOneDependency
import org.apache.spark.ShuffleDependency
import org.apache.spark.SparkEnv
import dbis.stark.spatial.indexed.live.IndexedSpatialRDD
import dbis.stark.spatial.indexed.RTree
import dbis.stark.spatial.indexed.IndexedPartition
import java.io.IOException
import java.io.ObjectOutputStream
import dbis.stark.spatial.SpatialPartitioner
import org.apache.spark.Dependency
import org.apache.spark.util.collection.ExternalAppendOnlyMap
import org.apache.spark.InterruptibleIterator
import org.apache.spark.util.collection.ExternalAppendOnlyMap
import scala.collection.mutable.ListBuffer
import dbis.stark.spatial.SpatialGridPartitioner
import dbis.stark.spatial.BSPartitioner
import dbis.stark.STObject


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
  
  override def getPartitions = Array.tabulate(left.getNumPartitions){ i =>
    new JoinPartition(i, 
      new NarrowIndexJoinSplitDep(left, i, left.partitions(i)),
      Array.tabulate(right.getNumPartitions)(j => new NarrowIndexJoinSplitDep(right, j, right.partitions(j))))
  }
  
  override def compute(s: Partition, context: TaskContext): Iterator[(V,V2)] = {
    val split = s.asInstanceOf[JoinPartition]
    
    val left  = split.leftDep.rdd.iterator(split.leftDep.split, context).asInstanceOf[Iterator[RTree[G, (G,V)]]]
    
    val iters = ListBuffer.empty[Iterator[(G,Combiner)]] 
      
//		println(s"left size: ${left.size}")
    for(idx <- left) {
//      println(s"left idx contains: ${idx.itemsTree().size()} ")
      val map = createExternalMap
     
      for(rDep <- split.rightDeps) {
        val right = rDep.rdd.iterator(rDep.split, context).asInstanceOf[Iterator[(G,V2)]]
        
//        println(s"\tright size ${rightList.size}")
        
        
        val res = right.flatMap{ case (rg, rv) =>
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

