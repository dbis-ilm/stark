package dbis.spark.spatial

case class NRectRange(id: Int, ll: NPoint, ur: NPoint) {
    
    require(ll.dim == ur.dim, "ll and ur points must be of same dimension")
    
    def contains(p: NPoint): Boolean = 
      p.c.zipWithIndex.forall { case(e, idx) => e >= ll(idx) && e < ur(idx) }
    
    def contains(r: NRectRange): Boolean =
      this.contains(r.ll) && r.ur.c.zipWithIndex.forall { case (e, idx) => e <= ur(idx) }
    
    def dim = ll.dim
    
    lazy val lengths = (0 until dim).map { i => ur(i) - ll(i) }.toArray 
    
    lazy val volume = lengths.reduceLeft(_ * _)
    
    override def equals(that: Any): Boolean = that match {
      case NRectRange(id, l, u) => (this.ll equals l) && (this.ur equals u)
      case _ => false
    }
  }
  object NRectRange {
    
    def apply(ll: NPoint, ur: NPoint): NRectRange = NRectRange(-1, ll, ur)
    
  }