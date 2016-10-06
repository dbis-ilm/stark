package dbis.stark.spatial.indexed.live


import scala.reflect.ClassTag
import scala.collection.mutable.ListBuffer

import org.apache.spark.Partition
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.InterruptibleIterator

import dbis.stark.STObject
import dbis.stark.spatial.indexed.RTree
import dbis.stark.spatial.Utils
import dbis.stark.spatial.SpatialPartitioner
import dbis.stark.spatial.SpatialRDD
import dbis.stark.spatial.plain.CartesianPartition

import com.vividsolutions.jts.geom.Envelope
import dbis.stark.spatial.JoinPredicate

class LiveIndexedJoinSpatialRDD[G <: STObject : ClassTag, V: ClassTag, V2: ClassTag](
    var left: RDD[(G,V)], 
    var right: RDD[(G,V2)],
    predicate: JoinPredicate.JoinPredicate,
    capacity: Int
    )  extends RDD[(V,V2)](left.context, Nil) {
  
  val predicateFunction = JoinPredicate.predicateFunction(predicate)
  
  
  override def getPartitions =  {
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
    val split = s.asInstanceOf[CartesianPartition]
    
    val tree = new RTree[G,(G,V)](capacity)
    
    left.iterator(split.s1, context).foreach{ case (lg,lv) => tree.insert(lg, (lg,lv)) }

    tree.build()
    
    /*
     * Returns:
		 * an Envelope (for STRtrees), an Interval (for SIRtrees), or other object 
		 * (for other subclasses of AbstractSTRtree)
		 * 
		 * http://www.atetric.com/atetric/javadoc/com.vividsolutions/jts-core/1.14.0/com/vividsolutions/jts/index/strtree/Boundable.html#getBounds--
     */
    val indexBounds = tree.getRoot.getBounds.asInstanceOf[Envelope]
          
    if(indexBounds == null)
      throw new IllegalStateException("tree root bounds is null")
    
    val partitionCheck = rightParti.map { p => 
//            indexBounds.intersects(Utils.toEnvelope(p.partitionExtent(split.s2.index)))
        if(leftParti.isDefined) {
          val lextent = leftParti.get.partitionExtent(split.s1.index)
          val rextent = p.partitionExtent(split.s2.index)
          
          val lenv = Utils.toEnvelope(lextent)
          val renv = Utils.toEnvelope(rextent)
          
          lenv.intersects(renv)
        } else
          true
      }.getOrElse(true)
      
    
    val res = if(partitionCheck) {
//    	val map = SpatialRDD.createExternalMap[G,V,V2]        

    	right.iterator(split.s2, context).flatMap{ case (rg, rv) => 
        tree.query(rg)  // for each entry in right query the index
          .filter{ case (lg, _) => predicateFunction(lg,rg) } // index returns candidates only -> prune by checking predicate again
          .map { case (_, lv) => (lv,rv) }
//          .map { case (lg, lv) => (lg, (lv, rv)) }    // transform to structure for the external map
//          .foreach { case (g, v) => map.insert(g, v)  } // insert into external map
    	}
    	
//    	map.iterator.flatMap{ case (g, l) => l} // when done, return entries of the map
    	
    } else
      Iterator.empty
    
//    new InterruptibleIterator(context, res)
      res
  }
  
  override def clearDependencies() {
    super.clearDependencies()
    left = null
    right = null
  }
}