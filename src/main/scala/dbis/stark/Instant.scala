package dbis.stark

/**
 * An instant represents a point in time.
 * <br><br>
 * Internally, an instant is represented as an interval with (l, l+1], i.e. we're 
 * simply adding one to the instants value
 * 
 * @param l The actual time as a long, e.g. seconds since Epoch. 
 * 
 */
case class Instant(
    private val _l: Long
  ) extends TemporalExpression {
  
  
  def value = _l
  
  def start = this
  def end = Some(this)
  
  def intersects(t: TemporalExpression): Boolean = ! (this < t || this > t)  
  
  def contains(t: TemporalExpression): Boolean = t >= this && t <= this
  
  def containedBy(t: TemporalExpression): Boolean = t contains this
  
  def center: Option[Instant] = Some(Instant(value))
  
  def length = Some(0)
  
  override def <(o: TemporalExpression): Boolean = value < o.start.value 
    
  
  override def <=(o: TemporalExpression): Boolean = value <= o.start.value
  
  override def >(o: TemporalExpression): Boolean = o < this
  
  override def >=(o: TemporalExpression): Boolean = o <= this
}

  

object Instant {

  def apply(o: Instant): Instant = Instant(o.value)
  
  def apply(o: Option[Instant]): Option[Instant] = o.map { v => Instant(v) }
  
  
  def max(l: Instant, r: Instant): Instant = if(l > r) l else r
  def max(l: Option[Instant], r: Option[Instant]): Option[Instant] = {
    if(l.isEmpty)
      r
    else if (r.isEmpty)
      l
    else if(l.get > r.get) 
      l       
    else 
      r
  }
  
  def min(l: Instant, r: Instant): Instant = if(l < r) l else r
  def min(l: Option[Instant], r: Option[Instant]): Option[Instant] = {
    if(l.isEmpty)
      r
    else if (r.isEmpty)
      l
    else if(l.get < r.get) 
      l       
    else 
      r
  }
}