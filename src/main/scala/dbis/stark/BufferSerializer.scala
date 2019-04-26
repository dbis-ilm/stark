package dbis.stark

import java.nio.{ByteBuffer, ByteOrder}

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import dbis.stark.spatial.partitioner.OneToManyPartition
import org.locationtech.jts.geom.Point

/* according to https://dzone.com/articles/serialization-using-bytebuffer
 * writing to a normal buffer is fastest
 */


object BufferSerializer {

  val BYTE_SIZE = 1
  val SHORT_SIZE = 2
  val INT_SIZE = 4
  val LONG_SIZE = 8
  val DOUBLE_SIZE = 8

  def forDouble(d: Double): ByteBuffer = allocate(DOUBLE_SIZE).putDouble(d)


  def allocate(size: Int): ByteBuffer = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN)

  def fromArray(arr: Array[Byte]): ByteBuffer = ByteBuffer.wrap(arr).order(ByteOrder.LITTLE_ENDIAN)

  def getDouble(implicit input: Input): Double =
    fromArray(input.readBytes(DOUBLE_SIZE)).getDouble

  def getInt(implicit input: Input): Int =
    fromArray(input.readBytes(INT_SIZE)).getInt()

  def getLong(implicit input: Input): Long =
    fromArray(input.readBytes(LONG_SIZE)).getLong()

  def getByte(implicit input: Input): Byte =
    input.readByte()
}

class STObjectSerializerBuffer extends Serializer[STObject] {

  override def write(kryo: Kryo, output: Output, obj: STObject): Unit = {
    val buffer = BufferSerializer.allocate(obj.determineByteSize)
    obj.serialize(buffer)
//    buffer.putDouble(1.0)
//    buffer.putDouble(1.0)

    buffer.flip()
//    println(buffer.limit())
//    println(buffer.capacity())

    output.writeBytes(buffer.array())



  }

  override def read(kryo: Kryo, input: Input, `type`: Class[STObject]): STObject = {

    STObject.deserialize(input)
//    val x = BufferSerializer.getDouble(input)
//    val y = BufferSerializer.getDouble(input)
//    STObject(x,y)
  }
}

class PairSerializerBuffer extends Serializer[(STObject, Byte)] {
  val soSerializer = new STObjectSerializer
  override def write(kryo: Kryo, output: Output, pair: (STObject, Byte)): Unit = {
    kryo.writeObject(output, pair._1, soSerializer)
//    kryo.writeClassAndObject(output, pair._2)
    output.writeByte(pair._2)
  }

  override def read(kryo: Kryo, input: Input, `type`: Class[(STObject, Byte)]): (STObject, Byte) = {
    val so = kryo.readObject(input, classOf[STObject],soSerializer)
//    val payload = kryo.readClassAndObject(input)
    val payload = input.readByte()
    (so, payload)
  }
}

class ScalarDistanceSerializerBuffer extends Serializer[ScalarDistance] {

  override def write(kryo: Kryo, output: Output, dist: ScalarDistance): Unit = {
    output.writeByte(dist.value.toByte)
  }

  override def read(kryo: Kryo, input: Input, `type`: Class[ScalarDistance]): ScalarDistance = {
    ScalarDistance(input.readByte().toDouble)
  }
}

class OneToMayPartitionSerializerBuffer extends Serializer[OneToManyPartition] {
  override def write(kryo: Kryo, output: Output, partition: OneToManyPartition): Unit = {
    val size = BufferSerializer.INT_SIZE + BufferSerializer.INT_SIZE + partition.rightIndex.length * BufferSerializer.INT_SIZE

    val buffer = BufferSerializer.allocate(size)
      .putInt(partition.idx)
      .putInt(partition.leftIndex)
      .putInt(partition.rightIndex.length)


    val iter = partition.rightIndex.iterator
    while(iter.hasNext) {
      buffer.putInt(iter.next())
    }

    val arr = buffer.array()
    output.write(arr, 0, arr.length)

  }

  override def read(kryo: Kryo, input: Input, `type`: Class[OneToManyPartition]): OneToManyPartition = {  val idx = BufferSerializer.getInt(input)
    val leftIdx = BufferSerializer.getInt(input)
    val num = BufferSerializer.getInt(input)

    var i = 0

    val arr = new Array[Int](num)
    while(i < num) {
      arr(i) = BufferSerializer.getInt(input)
      i += 1
    }

    OneToManyPartition(idx, null, null, leftIdx, arr)
  }
}
