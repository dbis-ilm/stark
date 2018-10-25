package dbis.stark
import java.nio.ByteBuffer

/**
 * Represents an interval in time with a start and an optional end
 * 
 * @param _start The start point (inclusive)
 * @param _end The optional end. 
 */
case class Interval(
    private val _start: Instant, 
    private val _end: Option[Instant]
  ) extends TemporalExpression {

  //  require(_end.isEmpty || _start < _end.get, "Right Open Interval criteria not met! _start must be smaller than _end")
  require(_end.isEmpty || _start <= _end.get, "Start must be <= End")

  override def determineByteSize: Int = BufferSerializer.BYTE_SIZE + BufferSerializer.LONG_SIZE + BufferSerializer.BYTE_SIZE + (if(_end.isDefined) BufferSerializer.LONG_SIZE else 0)

  override def serialize(buffer: ByteBuffer): Unit = {
    buffer.put(Interval.INTERVAL_TYPE)
    buffer.putLong(_start.value)
    if(_end.isDefined) {
      buffer.put(Interval.HAS_END)
      buffer.putLong(_end.get.value)
    } else {
      buffer.put(Interval.NO_END)
    }
  }


  def intersects(t: TemporalExpression): Boolean = 
    (start <= t.start && (end.isEmpty || (end.isDefined && end.get >= t.start))) ||
    (t.start <= start && (t.end.isEmpty || (t.end.isDefined && t.end.get >= start)))
  
  
  def contains(t: TemporalExpression): Boolean = 
    t.end.isDefined && (t.start >= start && (end.isEmpty || t.end.get <= end.get)) 
  
  def containedBy(t: TemporalExpression): Boolean = t contains this
  
  def center: Option[Instant] = end.map { end => Instant(start.value + ((end.value - start.value) / 2)) }
  
  def length: Option[Long] = end.map { e => e.value - start.value }
  
  def start = _start
  def end = _end
  
  
  override def <(o: TemporalExpression): Boolean = {
    end.isDefined && (start.value < o.start.value 
        && (o.end.isEmpty || end.get.value < o.end.get.value ))
  }
  
  override def <=(o: TemporalExpression): Boolean =
    end.isDefined && (start.value <= o.start.value 
        && (o.end.isEmpty || end.get.value <= o.end.get.value))
  
  override def >(o: TemporalExpression): Boolean = o < this
  
  override def >=(o: TemporalExpression): Boolean = o <= this
  
}

object Interval {

  protected[stark] val INTERVAL_TYPE: Byte = 2

  protected[stark] val NO_END: Byte = 0
  protected[stark] val HAS_END: Byte = 1

  def apply(o: Interval): Interval= Interval(Instant(o.start), Instant(o.end))
  
  def apply(start: Long, end: Long): Interval = Interval(Instant(start), Some(Instant(end)))
  
  def apply(start: Instant, end: Instant): Interval = Interval(start, Some(end))

}
