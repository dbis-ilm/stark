package org.apache.spark

import com.vividsolutions.jts.geom.Geometry
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import org.apache.spark.annotation.DeveloperApi
import com.vividsolutions.jts.index.strtree.STRtree
import com.vividsolutions.jts.index.strtree.AbstractSTRtree
import java.io.IOException
import java.io.ObjectOutputStream
import org.apache.spark.util.Utils
import org.apache.spark.serializer.JavaSerializer
import java.io.ObjectInputStream
import dbis.spatialspark.RTree
import dbis.spatialspark.IndexedRDD

class RTreePartition(
  var rddId: Long,
  var slice: Int,
  var values: RTree) extends Partition with Serializable {
  
  override def hashCode(): Int = (41 * (41 + rddId) + slice).toInt

  override def equals(other: Any): Boolean = other match {
    case that: RTreePartition =>
      this.rddId == that.rddId && this.slice == that.slice
    case _ => false
  }

  override def index: Int = slice

  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {

    val sfactory = SparkEnv.get.serializer

    // Treat java serializer with default action rather than going thru serialization, to avoid a
    // separate serialization header.

    sfactory match {
      case js: JavaSerializer => out.defaultWriteObject()
      case _ =>
        out.writeLong(rddId)
        out.writeInt(slice)

        val ser = sfactory.newInstance()
        Utils.serializeViaNestedStream(out, ser)(_.writeObject(values))
    }
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {

    val sfactory = SparkEnv.get.serializer
    sfactory match {
      case js: JavaSerializer => in.defaultReadObject()
      case _ =>
        rddId = in.readLong()
        slice = in.readInt()

        val ser = sfactory.newInstance()
        Utils.deserializeViaNestedStream(in, ser)(ds => values = ds.readObject[RTree]())
    }
  }
  
}


class NewIndexedSpatialRDD[T <: Geometry : ClassTag](
    sc: SparkContext,
    @transient private val data: Seq[T])
  extends IndexedRDD[T](sc) {
  
  
  
  /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   */
  @DeveloperApi
  def compute(split: Partition, context: TaskContext): Iterator[T] = ???

  /**
   * Implemented by subclasses to return the set of partitions in this RDD. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   */
  protected def getPartitions: Array[Partition] = {
    
    val rtree = new RTree
    
    data.map { geom => (geom, geom.getEnvelopeInternal) }
        .foreach { case (geom, mbr) => rtree.insert(mbr, geom)}    
    
    rtree.build()
    
    val executors = sc.getConf.getInt("", sc.defaultParallelism)
    
    val subtrees = rtree.getSubTree(executors)
                        .zipWithIndex
                        .map { case (node, slice) => 
                          val tree = new RTree(node)
                          new RTreePartition(id, slice, tree)
                        }
    
    subtrees.toArray
    
  }

  /**
   * Optionally overridden by subclasses to specify placement preferences.
   */
  override protected def getPreferredLocations(split: Partition): Seq[String] = Nil

  /** Optionally overridden by subclasses to specify how they are partitioned. */
  @transient override val partitioner: Option[Partitioner] = None
}