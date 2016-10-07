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
import dbis.stark.spatial.JoinPredicate
import dbis.stark.spatial.plain.CartesianPartition
import org.apache.spark.NarrowDependency
import com.vividsolutions.jts.geom.Envelope


class PersistantIndexedSpatialJoinRDD[G <: STObject : ClassTag, V: ClassTag, V2: ClassTag](
    var left: RDD[RTree[G, (G,V)]], //IndexedSpatialRDD[G,V] 
    var right: RDD[(G,V2)],
    pred: JoinPredicate.JoinPredicate
    )  extends RDD[(V,V2)](left.context, Nil) {
  
  val numPartitionsInright = right.partitions.length

  val predicateFunction = JoinPredicate.predicateFunction(pred)

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
  
  override def getPartitions: Array[Partition] = {
//    // create the cross product split
//    val array = new Array[Partition](left.partitions.length * right.partitions.length)
//    for (s1 <- left.partitions; s2 <- right.partitions) {
//      val idx = s1.index * numPartitionsInright + s2.index
//      array(idx) = new CartesianPartition(idx, left, right, s1.index, s2.index)
//    }
//    array
    
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

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val currSplit = split.asInstanceOf[CartesianPartition]
    (left.preferredLocations(currSplit.s1) ++ right.preferredLocations(currSplit.s2)).distinct
  }

  override def compute(split: Partition, context: TaskContext): Iterator[(V, V2)] = {
    val currSplit = split.asInstanceOf[CartesianPartition]

//    val map = SpatialRDD.createExternalMap[G,V,V2]
    
    val theIter = left.iterator(currSplit.s1, context).flatMap{ tree => 
      
      /*
       * Returns:
  		 * an Envelope (for STRtrees), an Interval (for SIRtrees), or other object 
  		 * (for other subclasses of AbstractSTRtree)
  		 * 
  		 * http://www.atetric.com/atetric/javadoc/com.vividsolutions/jts-core/1.14.0/com/vividsolutions/jts/index/strtree/Boundable.html#getBounds--
       */
//      val indexBounds = tree.getRoot.getBounds.asInstanceOf[Envelope]
//      
//      val partitionCheck = rightParti.map { p => 
//            indexBounds.intersects(Utils.toEnvelope(p.partitionExtent(currSplit.s2.index)))
//      }.getOrElse(true)
//      
//      if(partitionCheck) {
        right.iterator(currSplit.s2, context)
                .flatMap { case (rg,rv) =>
                  tree.query(rg)
                    .filter { case (lg,_) => predicateFunction(lg,rg)}
                    .map{ case (_,lv) => (lv,rv) }
//                    .map{ case (lg,lv) => (lg,(lv,rv)) }
//              .foreach { case (g,v) => map.insert(g,v) }
                }
//        } else {
//          Iterator.empty
//        }
      }
    
//    val f = map.iterator.flatMap{ case (g, l) => l}
    
//    new InterruptibleIterator(context, theIter)
    theIter
    
  }

  override def getDependencies: Seq[Dependency[_]] = List(
    new NarrowDependency(left) {
      def getParents(id: Int): Seq[Int] = List(id / numPartitionsInright)
    },
    new NarrowDependency(right) {
      def getParents(id: Int): Seq[Int] = List(id % numPartitionsInright)
    }
  )

  override def clearDependencies() {
    super.clearDependencies()
    left = null
    right = null
  }  
}

