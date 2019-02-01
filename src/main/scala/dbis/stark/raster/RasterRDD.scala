package dbis.stark.raster

import dbis.stark.STObject
import dbis.stark.spatial.JoinPredicate
import dbis.stark.spatial.JoinPredicate.JoinPredicate
import dbis.stark.spatial.indexed.{Index, IndexConfig}
import dbis.stark.spatial.partitioner.GridStrategy
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer

import scala.reflect.ClassTag

/**
  * A base class for representing raster data as RDD
  * @param _parent The parent (input) RDD
  * @param _partitioner The optional partitioner that _was_ used
  */
class RasterRDD[U : ClassTag](@transient private val _parent: RDD[Tile[U]],
                              private val _partitioner: Option[Partitioner] = None)(implicit ord: Ordering[U])
  extends RDD[Tile[U]](_parent) {


  /**
    * The partitioner that was used on this RDD
    */
  override val partitioner = _partitioner

  /**
    * Own implementation of partitionBy for RasterRDD.
    *
    * In Spark, partititionBy is only available for Pair-RDDs. Thus, here we simulate this
    * by enhancing it to (tile, 0x0) i.e. we append a zero byte to create a pair RDD.
    * On this we perform the partitioning and then re-transform it back to only tile
    *
    * @param partitioner The partitioner to use
    * @return Returns a partitioned RasterRDD
    */
  def partitionBy(partitioner: RasterGridPartitioner): RasterRDD[U] = {
    val zero: Byte = 0.toByte // the zero byte
    // transform to key-value form where Tile is key and the zero byte our aux-value
    val res = this.map(t => (t, zero))
                  .partitionBy(partitioner) // partition
                  .map(_._1) // transform back to remove the aux byte

    // make it a raster RDD with the given partitioner information
    new RasterRDD(res, Some(partitioner))
  }


  // methods needed to implement - otherwise RasterRDD must be abstract
  override def compute(split: Partition, context: TaskContext) = firstParent[Tile[U]].compute(split, context)
  override protected def getPartitions = firstParent.partitions

  /**
    * Filter the RasterRDD so that it contains only Tiles intersecting with the given region
    * @param qry The query region
    * @return Returns
    */
  def filter(qry: STObject, pixelDefault: U, predicate: JoinPredicate = JoinPredicate.INTERSECTS) =
    new RasterFilterVectorRDD(qry, this, predicate, pixelDefault)

//  def join(other: RDD[STObject], predicate: JoinPredicate, indexConf: Option[IndexConfig] = None): RasterRDD[U] =
//    new RasterJoinVectorRDD(this, other, predicate, indexConf)

  def join[P : ClassTag](other: RDD[(STObject, P)], pixelDefault: U, predicate: JoinPredicate,
                        indexConf: Option[IndexConfig] = None, oneToMany: Boolean = false): RDD[(Tile[U],P)] =
    new RasterJoinVectorRDD(this, other, predicate, pixelDefault, indexConf, oneToMany)

  def joinWithAggregate[P : ClassTag, R](other: RDD[(STObject, P)], pixelDefault: U, predicate: JoinPredicate, aggregate: Tile[U] => R, indexConf: Option[IndexConfig] = None, oneToMany: Boolean = false): RDD[(R,P)] =
    new RasterJoinVectorRDD(this, other, predicate, pixelDefault, indexConf, oneToMany).map{ case (tile, p) => (aggregate(tile), p)}

  def join[P: ClassTag](other: RDD[Index[P]], pixelDefault: U, predicate: JoinPredicate, oneToMany: Boolean): RDD[(Tile[U],P)] = {
    RasterJoinIndexedVectorRDD(this, other, predicate, pixelDefault, oneToMany)
  }

  def joinWithAggregate[P: ClassTag, R](other: RDD[Index[P]], pixelDefault: U, predicate: JoinPredicate, oneToMany: Boolean, aggregate: Tile[U] => R): RDD[(R,P)] = {
    RasterJoinIndexedVectorRDD(this, other, predicate, pixelDefault, oneToMany).map{ case (tile, p) =>
      (aggregate(tile), p)
    }
  }

  def join[P, R](other: RasterRDD[P], predicate: JoinPredicate, func: (U,P) => R, oneToMany: Boolean)(implicit ev$1: Ordering[P], ev$2: Ordering[R], classTag$P: ClassTag[P], classTag$R: ClassTag[R]) =
    RasterJoinRDD(this, other, predicate, func, oneToMany)


  override def saveAsTextFile(path: String) = {
    this.map{ t =>
      s"${t.ulx},${t.uly},${t.width},${t.height},${t.pixelWidth},${t.data.mkString(",")}${t.sma.map(","+_.toString).getOrElse("")}"
    }.saveAsTextFile(path)
  }

  def saveAsObjectFile(path: String, partitions: Int, conf: SparkConf): Unit = {

    val parti = GridStrategy(partitions, pointsOnly = true, minmax = None, sampleFraction = 0)

    val mapped = this.mapPartitions { iter =>

      val ser = new KryoSerializer(conf)

      val output = ser.newKryoOutput()
      val kryo = ser.newKryo()

      val res = iter.map { t =>

        kryo.writeObject(output, t)
        output.flush()
        val objBytes = output.getBuffer


//        val smaBytes: Option[(Array[Byte], Array[Byte], Double)] = t.sma map {
//          case SMA(min, max, avg) =>
//
//            oos.writeObject(min)
//            oos.flush()
//            bos.flush()
//            oos.reset()
//            bos.reset()
//            val minBytes = bos.toByteArray
//
//
//            oos.writeObject(max)
//            oos.flush()
//            bos.flush()
//            oos.reset()
//            bos.reset()
//            val maxBytes = bos.toByteArray
//
//            (minBytes, maxBytes,avg)
//        }
//
//        bos.reset()
//        oos.reset()
//
//        val smaBytesLength = smaBytes.map{ case (minBytes, maxBytes,_) =>
//            INT + minBytes.length + // numbytes + actual bytes
//            INT + maxBytes.length + // numbytes + actual bytes
//            DOUBLE // average
//        }.getOrElse(0) // no SMA present
//
//        var i = 0
//
//        var numDataBytes = 0
//        val dataBytes = mutable.ListBuffer.empty[Array[Byte]]
//
//        while (i < t.data.length) {
////          buf.putInt(t.data(i).asInstanceOf[Int])
//
//          oos.writeObject(t.data(i))
//          oos.flush()
//          val pixelBytes = bos.toByteArray
//          numDataBytes += pixelBytes.length
//          dataBytes += pixelBytes
//
//          bos.reset()
//          oos.reset()
//
//          i += 1
//        }
//
//        val buf = ByteBuffer.allocate(DOUBLE + DOUBLE + // ulx, uly
//          INT + INT + DOUBLE + // width, height, pixelwidth
//          1 + smaBytesLength + // SMA indicator (yes or no) + sma bytes
//          INT + dataBytes.length * INT + numDataBytes) // num entries, length per entry, total bytes
//
//        buf.putDouble(t.ulx)
//        buf.putDouble(t.uly)
//        buf.putInt(t.width)
//        buf.putInt(t.height)
//        buf.putDouble(t.pixelWidth)
//        //      buf.putInt(t.data.length)
//
//
//        smaBytes match {
//          case Some((minBytes, maxBytes, avg)) =>
//            buf.put(1.toByte)
//            buf.putInt(minBytes.length)
//            buf.put(minBytes)
//            buf.putInt(maxBytes.length)
//            buf.put(maxBytes)
//            buf.putDouble(avg)
//          case None =>
//            buf.put(0.toByte)
//        }
//
//
//        i = 0
//        while(i < dataBytes.length) {
//          // TODO could be left out if all pixels have same amount, e.g. Int, ...
//          buf.putInt(dataBytes(i).length)
//          buf.put(dataBytes(i))
//          i += 1
//        }

        output.clear()

        (STObject(RasterUtils.tileToGeo(t)), objBytes)

      }

      output.close()

      res
    }

    val parted = new PlainSpatialRDDFunctions(mapped).partitionBy(parti)

    new PlainSpatialRDDFunctions(parted).saveAsStarkObjectFile(path)
  }

  def countValue(x: U): Long = {
    this.aggregate(0)({(c, t) =>
      c + t.countValue(x)
    },{(c1,c2) => c1 + c2})
  }

  def hasValue(v: U): Boolean = {
    this.mapPartitions{iter =>
      Iterator.single(iter.exists(t => t.hasValue(v)))
    }.aggregate(false)((b1,b2) => b1 || b2, (b1,b2) => b1 || b2)
  }

  def histogram() = {

  }
}


object  RasterRDD {
  implicit def toRasterRDD[U](rdd: RDD[Tile[U]])(implicit ord: Ordering[U], classTag$U: ClassTag[U]) = new RasterRDD[U](rdd)
//  implicit def toDrawableDouble(rdd: RDD[Tile[Double]]) = new DrawableRasterRDDFunctionsDouble(rdd)
//  implicit def toDrawableInt(rdd: RDD[Tile[Int]]) = new DrawableRasterRDDFunctionsInt(rdd)
}