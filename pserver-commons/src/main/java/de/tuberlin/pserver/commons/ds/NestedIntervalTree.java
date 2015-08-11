package de.tuberlin.pserver.commons.ds;


import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;

public class NestedIntervalTree<T> {

    public static class Point extends Interval { public Point(int x) { super(x, x); } }
    public static class Interval implements Comparable<Interval> {

        public final int low;

        public final int high;

        public Interval(int left, int right) {
            if (left <= right) {
                this.low  = left;
                this.high = right;
            }
            else throw new IllegalStateException();
        }

        public boolean contains(int x) { return (low <= x) && (x <= high); }

        public boolean contains(Interval x) { return (low <= x.low) && (x.high <= high); }

        public boolean equals(Interval x) { return (low == x.low) && (x.high == high); }

        public boolean intersects(Interval that) {
            if (that.high < this.low) return false;
            if (this.high < that.low) return false;
            return true;
        }

        public int compareTo(Interval that) {
            if      (this.low  < that.low)  return -1;
            else if (this.low  > that.low)  return +1;
            else if (this.high < that.high) return -1;
            else if (this.high > that.high) return +1;
            else                            return  0;
        }

        public String toString() { return "[" + low + ", " + high + "]"; }
    }

    // ---------------------------------------------------

    private class Node {

        Interval        interval;
        T               value;
        Node            parent;
        List<Node>      children;

        Node(Node parent, Interval interval, T value) {
            this.parent    = parent;
            this.interval  = interval;
            this.value     = value;
            this.children  = new ArrayList<>();
        }
    }

    // ---------------------------------------------------

    private final Node root;

    // ---------------------------------------------------

    public NestedIntervalTree(final Interval interval, final T value) {
        root = new Node(null, interval, value);
    }

    // ---------------------------------------------------

    public boolean put(final Interval interval, final T value) {
        Preconditions.checkNotNull(interval);
        Preconditions.checkNotNull(value);
        final Node parent = find(root, interval);
        if (parent == null)
            return false;

        /*for (final Node c : parent.children) {
            if (interval.intersects(c.interval))
                return false;
        }*/

        final Node n = new Node(parent, interval, value);
        return parent.children.add(n);
    }

    //public Node find(final Interval interval) { return find(root, interval); }
    private Node find(final Node n, final Interval interval) {
        if (n.interval.contains(interval)) {
            for (Node c : n.children) {
                final Node r = find(c, interval);
                if (r != null)
                    return r;
            }
            return n;
        } else
            return null;
    }

    public boolean exist(final Interval interval) { return exist(root, interval); }
    private boolean exist(final Node n, final Interval interval) {
        if (n.interval.contains(interval)) {
            if (n.interval.equals(interval))
                return true;
            for (Node c : n.children) {
                if (exist(c, interval))
                    return true;
            }
        }
        return false;
    }

    public boolean isValid(final Interval interval) { return isValid(root, interval); }
    private boolean isValid(final Node n, final Interval interval) {
        if (n.interval.contains(interval)) {
            for (Node c : n.children) {
                if (c.interval.intersects(interval))
                    return false;
                else
                    isValid(interval);
            }
            return true;
        } else
            return false;
    }

    public Pair<Interval,T> get(final Interval interval) {
        Preconditions.checkNotNull(interval);
        final Node n = find(root, interval);
        if (n == null)
            return null;
        return Pair.of(n.interval, n.value);
    }

    public boolean remove(final Interval interval) {
        Preconditions.checkNotNull(interval);
        final Node n = find(root, interval);
        if (n == null)
            return false;
        if (n.parent != null)
            n.parent.children.remove(n);
        return true;
    }

    // ---------------------------------------------------

    public static void main(final String[] args) {

        final NestedIntervalTree<Integer> nit = new NestedIntervalTree<>(new Interval(0, 7), 1);

        nit.put(new Interval(0, 4), 2);

        nit.put(new Interval(0, 2), 3);

        System.out.println(nit.get(new Point(1)).getKey().toString());

        System.out.println(nit.put(new Interval(0, 3), 4));
    }
}
