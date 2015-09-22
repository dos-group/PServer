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
            if (this.low <= that.low  && that.low <= this.high && that.high > this.high)  return true;
            //if (that.low <= this.low  && this.low <= that.high && this.high > that.high)  return true;
            //if (this.low <= that.high && that.high <= this.high) return true;
            return false;
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

    public synchronized boolean put(final Interval interval, final T value) {
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
    private synchronized Node find(final Node n, final Interval interval) {
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

    public synchronized boolean exist(final Interval interval) { return exist(root, interval); }
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

    public synchronized boolean isValid(final Interval interval) { return isValid(root, interval); }
    private boolean isValid(final Node n, final Interval interval) {
        if (n.interval.contains(interval)) {
            for (Node c : n.children) {
                if (c.interval.intersects(interval) || interval.contains(c.interval))
                    return false;
            }
            for (Node c : n.children) {
                if (c.interval.contains(interval))
                    return isValid(c, interval);
            }
            return true;
        } else
            return false;
    }

    public synchronized Pair<Interval,T> get(final Interval interval) {
        Preconditions.checkNotNull(interval);
        final Node n = find(root, interval);
        if (n == null)
            return null;
        return Pair.of(n.interval, n.value);
    }

    public synchronized boolean remove(final Interval interval) {
        Preconditions.checkNotNull(interval);
        final Node n = find(root, interval);
        if (n == null)
            return false;
        if (n.parent != null)
            n.parent.children.remove(n);
        return true;
    }

    public synchronized boolean isDeepestInterval(final Interval interval) {
        Preconditions.checkNotNull(interval);
        final Node n = find(root, interval);
        if (n == null)
            return false;
        if (n.children.size() == 0)
            return true;
        else
            return false;
    }


    @Override
    public String toString() {
        return toString(root);
    }

    public String toString(final Node parent) {
        final StringBuilder strBuilder = new StringBuilder();
        strBuilder.append("[" + parent.interval.low + ", " + parent.interval.high + "] -> ");
        for (final Node n : parent.children) {
            strBuilder.append(toString(n));
        }
        return strBuilder.toString();
    }

    // ---------------------------------------------------

    public static void main(final String[] args) {

        final NestedIntervalTree<Integer> nestedIntervalTree = new NestedIntervalTree<>(new NestedIntervalTree.Interval(0, 3), 0);


        /*NestedIntervalTree.Interval in1 = new NestedIntervalTree.Interval(1, 3);
        System.out.println(in1 + " - " + nestedIntervalTree.isValid(in1));
        nestedIntervalTree.put(in1, 1);


        NestedIntervalTree.Interval in2 = new NestedIntervalTree.Interval(1, 2);
        System.out.println(in2 + " - " + nestedIntervalTree.isValid(in2));
        nestedIntervalTree.put(in2, 2);


        NestedIntervalTree.Interval inX = new NestedIntervalTree.Interval(0, 3);
        System.out.println("+ " + inX + " - " + nestedIntervalTree.isValid(inX));


        NestedIntervalTree.Interval in3 = new NestedIntervalTree.Interval(2, 2);
        System.out.println(in3 + " - " + nestedIntervalTree.isValid(in3));
        nestedIntervalTree.put(in3, 3);


        NestedIntervalTree.Interval in4 = new NestedIntervalTree.Interval(3, 3);
        System.out.println(in4 + " - " + nestedIntervalTree.isValid(in4));
        nestedIntervalTree.put(in4, 4);*/


        NestedIntervalTree.Interval in1 = new NestedIntervalTree.Interval(0, 3);
        System.out.println(in1 + " - " + nestedIntervalTree.isValid(in1));
        nestedIntervalTree.put(in1, 1);


        NestedIntervalTree.Interval in2 = new NestedIntervalTree.Interval(1, 3);
        System.out.println(in2 + " - " + nestedIntervalTree.isValid(in2));
        nestedIntervalTree.put(in2, 2);


        NestedIntervalTree.Interval inX = new NestedIntervalTree.Interval(2, 3);
        System.out.println("+ " + inX + " - " + nestedIntervalTree.isValid(inX));


        NestedIntervalTree.Interval in3 = new NestedIntervalTree.Interval(3, 3);
        System.out.println(in3 + " - " + nestedIntervalTree.isValid(in3));
        nestedIntervalTree.put(in3, 3);


        NestedIntervalTree.Interval in4 = new NestedIntervalTree.Interval(0, 3);
        System.out.println(in4 + " - " + nestedIntervalTree.isValid(in4));
        nestedIntervalTree.put(in4, 4);

/*
        System.out.println("is deepest interval - " + nestedIntervalTree.isDeepestInterval(in2));
*/
    }
}
