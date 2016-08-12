package dbis.stark

/**
 * A basic temporal expression class
 */
trait TemporalExpression extends BaseExpression[TemporalExpression] {
  

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
