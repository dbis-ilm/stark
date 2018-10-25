package org.locationtech.jts.index.strtree;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.index.strtree.*;

import java.util.Iterator;
import java.util.Objects;
import java.util.Stack;

public class ResultIterator<T> implements Iterator<T> {

    private static final class Pair {
        protected final AbstractNode b;
        protected int i;

        public Pair(AbstractNode b, int i) {
            this.b = b;
            this.i = i;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Pair pair = (Pair) o;
            return i == pair.i &&
                    Objects.equals(b, pair.b);
        }

        @Override
        public int hashCode() {
            return Objects.hash(b, i);
        }

        @Override
        public String toString() {
            return "Pair{" +
                    "b=" + b.getBounds() +
                    ", i=" + i +
                    '}';
        }
    }

    private final Envelope searchBounds;
    private final AbstractSTRtree.IntersectsOp iOp;

    private final Stack<Pair> stack = new Stack<>();


    private T next = null;

    ResultIterator(STRtreePlus<?> index, Envelope env) {

        this.searchBounds = env;
        this.iOp = index.getIntersectsOp();
        this.stack.push(new Pair(index.root, 0));
    }

    @Override
    public boolean hasNext() {

        findNextObject();

        //noinspection unchecked
        return !Objects.isNull(next);
    }

    @Override
    public T next() {
        if(Objects.isNull(next))
            throw new IllegalStateException("empty iterator!");

        return next;
    }

    private void findNextObject() {

        T found = null;

        while(found == null && !stack.isEmpty()) {
            // get the next element to process ...
            Pair pair = stack.pop();

            // ... and iterate over all its children
            int i;
            for (i = pair.i; i < pair.b.getChildBoundables().size(); ++i) {
                Boundable child = (Boundable) pair.b.getChildBoundables().get(i);

                // the current child does not intersect with the search region,
                // we do not need to consider it further
                if (!iOp.intersects(child.getBounds(), searchBounds)) {
                    continue;
                }

                // if the current child is an inner node add it to our to-do stack
                if (child instanceof AbstractNode) {
                    Pair p = new Pair((AbstractNode) child, 0);
                    if (!stack.contains(p)) {
                        stack.push(p);
                    }
                } else if (child instanceof ItemBoundable) {
                    /* it is a leaf node, we know from the previous check that it intersects
                     * with the search region. we can stop the search for now and return this
                     * child as the next element for the iterator
                     */

                    //noinspection unchecked
                    found = (T) ((ItemBoundable) child).getItem();
                    break;
                }
            }

            // the for loop finished completely if i has the same
            // value as the number of children of the current node
            // if i is smaller, than we left the loop using a break
            if(i < pair.b.getChildBoundables().size()) {
                pair.i = i+1;
                stack.push(pair);
            }

            // continue the search if nothing was found yet but there are other nodes to process
        }

        // return the found value - or null if nothing was left to find
        next = found;
    }
}
