
/*
 * The JTS Topology Suite is a collection of Java classes that
 * implement the fundamental operations required to validate a given
 * geo-spatial data set to a known topological specification.
 *
 * Copyright (C) 2001 Vivid Solutions
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 * For more information, contact:
 *
 *     Vivid Solutions
 *     Suite #1A
 *     2328 Government Street
 *     Victoria BC  V8T 5G5
 *     Canada
 *
 *     (250)385-6040
 *     www.vividsolutions.com
 */
package org.locationtech.jts.index.strtree;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.util.PriorityQueue;
import scala.Tuple2;

import java.util.*;

/**
 *  A query-only R-tree created using the Sort-Tile-Recursive (STR) algorithm.
 *  For two-dimensional spatial data.
 * <P>
 *  The STR packed R-tree is simple to implement and maximizes space
 *  utilization; that is, as many leaves as possible are filled to capacity.
 *  Overlap between nodes is far less than in a basic R-tree. However, once the
 *  tree has been built (explicitly or on the first call to #query), items may
 *  not be added or removed.
 * <P>
 * Described in: P. Rigaux, Michel Scholl and Agnes Voisard.
 * <i>Spatial Databases With Application To GIS</i>.
 * Morgan Kaufmann, San Francisco, 2002.
 * <p>
 * This class is thread-safe.  Building the tree is synchronized, 
 * and querying is stateless.
 * <p>
 * Originally this class was implemented in JTS <a href="http://tsusiatsoftware.net/jts/main.html">http://tsusiatsoftware.net/jts/main.html</a>
 * JTSPlus <a href="https://github.com/jiayuasu/JTSplus">https://github.com/jiayuasu/JTSplus</a> added support for k nearest neighbor search.
 * <br>
 * STARK took the implementation of JTSPlus and modified it in the following ways:<ul>
 * 	<li> Let the JTSPlus STRtree extend the original STRtree
 *  <li> Remove methods that are unchanged in JTSPlus STRtree and thus use original implementation
 *  <li> Make STRtree generic to avoid having to cast to correct type in calling functions
 *  <li> k nearest neighbor search returns list of T instead of only the Envelope so that we obtain the data and not only useless bounds
 * </ul>
 *
 * @version 1.7
 */
public class STRtreePlus<T> extends STRtree
{

  /**
   *
   */
  private static final long serialVersionUID = 259274702368956900L;


  private static final int DEFAULT_NODE_CAPACITY = 10;

  /**
   * Constructs an STRtree with the default node capacity.
   */
  public STRtreePlus()
  {
    this(DEFAULT_NODE_CAPACITY);
  }

  /**
   * Constructs an STRtree with the given maximum number of child nodes that
   * a node may have.
   * <p>
   * The minimum recommended capacity setting is 4.
   *
   */
  public STRtreePlus(int nodeCapacity) {
    super(nodeCapacity);
  }

  public void setRoot(AbstractNode root) {
    super.root = root;
  }

  public ResultIterator<T> iteratorQuery(Envelope env) {
    super.build();
    return new ResultIterator<>(this, env);
  }

  /**
   * Finds the item in this tree which is nearest to the given {@link Object}, 
   * using {@link ItemDistance} as the distance metric.
   * A Branch-and-Bound tree traversal algorithm is used
   * to provide an efficient search.
   * <p>
   * The query <tt>object</tt> does <b>not</b> have to be 
   * contained in the tree, but it does 
   * have to be compatible with the <tt>itemDist</tt> 
   * distance metric. 
   *
   * @param env the envelope of the query item
   * @param item the item to find the nearest neighbour of
   * @param itemDist a distance metric applicable to the items in this tree and the query item
   * @return the nearest item in this tree
   */

  public List<Tuple2<T,Double>> kNearestNeighbour(Envelope env, Object item, ItemDistance itemDist, int k)
  {
    Boundable bnd = new ItemBoundable(env, item);
    BoundablePair bp = new BoundablePair(this.getRoot(), bnd, itemDist);
    return nearestNeighbour(bp,k);
  }

  private List<Tuple2<T,Double>> nearestNeighbour(BoundablePair initBndPair, int k)
  {
    return nearestNeighbour(initBndPair, Double.POSITIVE_INFINITY,k);
  }

  @SuppressWarnings("unchecked")
  private List<Tuple2<T,Double>> nearestNeighbour(BoundablePair initBndPair, double maxDistance, int k)
  {
    double distanceLowerBound = maxDistance;

    // initialize internal structures
    PriorityQueue priQ = new PriorityQueue();

    // initialize queue
    priQ.add(initBndPair);

    List<T> kNearestNeighbors = new ArrayList<> ();
    List<Double> kNearestDistances = new ArrayList<>();
    while (! priQ.isEmpty() && distanceLowerBound >= 0.0) {
      // pop head of queue and expand one side of pair
      BoundablePair bndPair = (BoundablePair) priQ.poll();
      double currentDistance = bndPair.getDistance();


      /*
       * If the distance for the first node in the queue
       * is >= the current maximum distance in the k queue , all other nodes
       * in the queue must also have a greater distance.
       * So the current minDistance must be the true minimum,
       * and we are done.
       */

      if (currentDistance >= distanceLowerBound && kNearestDistances.size()>=k){
        break;
      }

      /*
       * If the pair members are leaves
       * then their distance is the exact lower bound.
       * Update the distanceLowerBound to reflect this
       * (which must be smaller, due to the test
       * immediately prior to this).
       */
      if (bndPair.isLeaves()) {
        // assert: currentDistance < minimumDistanceFound

        //distanceLowerBound = currentDistance;
        if(kNearestDistances.size()>0 && kNearestDistances.size()<k){

          int position=Collections.binarySearch(kNearestDistances, currentDistance);
          if(position<0)
          {
            position=-position-1;
          }
          kNearestNeighbors.add(position,(T)((ItemBoundable)bndPair.getBoundable(0)).getItem());
          kNearestDistances.add(position,currentDistance);
        }
        else if(kNearestDistances.size()>=k)
        {

          if(currentDistance<kNearestDistances.get(kNearestDistances.size()-1))
          {
            int position=Collections.binarySearch(kNearestDistances, currentDistance);
            if(position<0)
            {
              position=-position-1;
            }
            kNearestNeighbors.add(position,(T)((ItemBoundable)bndPair.getBoundable(0)).getItem());
            kNearestDistances.add(position,currentDistance);
            assert kNearestNeighbors.size()>k;
            kNearestNeighbors.remove(kNearestNeighbors.size()-1);
            kNearestDistances.remove(kNearestDistances.size()-1);
          }
        }
        else if(kNearestDistances.size()==0)
        {
          kNearestNeighbors.add((T)((ItemBoundable)bndPair.getBoundable(0)).getItem());
          kNearestDistances.add(currentDistance);
        }
        else
        {
          try {
            throw new Exception("Should never reach here");
          } catch (Exception e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
          }
        }


        distanceLowerBound = kNearestDistances.get(kNearestDistances.size()-1);
        //System.out.println("distanceLowerBound is " + distanceLowerBound);



      }
      else {
        // testing - does allowing a tolerance improve speed?
        // Ans: by only about 10% - not enough to matter
        /*
        double maxDist = bndPair.getMaximumDistance();
        if (maxDist * .99 < lastComputedDistance) 
          return;
        //*/

        /*
         * Otherwise, expand one side of the pair,
         * (the choice of which side to expand is heuristically determined)
         * and insert the new expanded pairs into the queue
         */
        bndPair.expandToQueue(priQ, distanceLowerBound);
      }
    }
    // done - return items with min distance

    Iterator<T> neighborIter = kNearestNeighbors.iterator();
    Iterator<Double> distIter = kNearestDistances.iterator();
    List<Tuple2<T,Double>> result = new LinkedList<>();
    while(neighborIter.hasNext()) {
      T neighbor = neighborIter.next();
      Double dist = distIter.next();

      result.add(new Tuple2<>(neighbor, dist));
    }

    return result;
  }

  /* return leaf? nodes
   */
  public List<Envelope> queryBoundary()
  {
    build();
    List<Envelope> boundaries = new ArrayList<>();
    if (isEmpty()) {
      //Assert.isTrue(root.getBounds() == null);
      //If the root is empty, we stop traversing. This should not happen.
      return boundaries;
    }

    queryBoundary(root, boundaries);

    return boundaries;
  }
  /**
   * This function is to traverse the children of the root.
   * @param node
   * @param boundaries
   */
  private void queryBoundary(AbstractNode node, List<Envelope> boundaries) {
    List childBoundables = node.getChildBoundables();
    boolean flagLeafnode=true;
    for (Object childBoundable2 : childBoundables) {
      Boundable childBoundable = (Boundable) childBoundable2;
      if (childBoundable instanceof AbstractNode) {
        //We find this is not a leaf node.
        flagLeafnode = false;
        break;

      }
    }
    if(flagLeafnode)
    {
      boundaries.add((Envelope)node.getBounds());
    }
    else
    {
      for (Object childBoundable1 : childBoundables) {
        Boundable childBoundable = (Boundable) childBoundable1;
        if (childBoundable instanceof AbstractNode) {
          queryBoundary((AbstractNode) childBoundable, boundaries);
        }

      }
    }
  }

}


