package dbis.spark

/**
 * An interval represents a range in time with a given start point and an optional end.
 * 
 * @param _start The start of the interval (inclusive)
 * @param _end The optional end of the interval (exclusive, right open interval). If the interval has no end, the end is None
 */
case class Interval(
    private val _start: Instant, 
    private val _end: Option[Instant]
  ) extends TemporalExpression {
  
//  require(_end.isEmpty || _start < _end.get, "Right Open Interval criteria not met! _start must be smaller than _end")
  require(_end.isEmpty || _start <= _end.get, "Start must be <= End")
  
  def isValidAt(t: TemporalExpression): Boolean = this.intersects(t) 
  
  def intersects(t: TemporalExpression): Boolean = ! (this < t || this > t)  
  
  def contains(t: TemporalExpression): Boolean = t >= this && t <= this
  
  def containedBy(t: TemporalExpression): Boolean = t contains this
  
  def center: Option[Instant] = end.map { end => Instant(start.value + ((end.value - start.value) / 2)) }
  
  def length: Option[Long] = end.map { e => e.value - start.value - 1 }
  
  def start = _start
  def end = _end
  
  
  def ~(o: Interval): Interval = new Interval( 
      Instant(Instant.min(start, o.start)), 
      Instant(Instant.max(end, o.end) ) 
    )

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
  
  def apply(o: Interval): Interval= Interval(Instant(o.start), Instant(o.end))
  
  def apply(start: Long, end: Long): Interval = Interval(Instant(start), Instant(end))
  
  def apply(start: Instant, end: Instant): Interval = Interval(start, Some(end))
  
}