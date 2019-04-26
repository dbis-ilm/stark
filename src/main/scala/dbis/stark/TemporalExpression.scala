package dbis.stark

import java.nio.ByteBuffer

trait StarkSerializable {
  def determineByteSize: Int
  def serialize(buffer: ByteBuffer): Unit
}

/**
 * A basic temporal expression class
 */
trait TemporalExpression extends BaseExpression[TemporalExpression] with StarkSerializable {

  def intersects(t: TemporalExpression): Boolean
  
  def contains(t: TemporalExpression): Boolean
  
  def containedBy(t: TemporalExpression): Boolean

  def center: Option[Instant]
  
  def length: Option[Long]
  
  def start: Instant
  
  def end: Option[Instant]
  
  def <(o: TemporalExpression): Boolean 
  
  def <=(o: TemporalExpression): Boolean
  
  def >(o: TemporalExpression): Boolean 
  
  def >=(o: TemporalExpression): Boolean
}
