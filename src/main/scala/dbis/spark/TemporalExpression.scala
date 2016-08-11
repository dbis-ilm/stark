package dbis.spark

/**
 * A basic temporal expression class
 */
trait TemporalExpression extends BaseExpression[TemporalExpression] {
  

  def isValidAt(t: TemporalExpression): Boolean
  
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
  
  
  
//  def ~(o: TemporalExpression): Interval
  
//  def ext_end(o: TemporalExpression): Interval = this ~ o
  
  
}
