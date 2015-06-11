package de.tuberlin.pserver.examples.graphs;


import de.tuberlin.pserver.app.types.DMatrixValue;
import de.tuberlin.pserver.math.DMatrix;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class AdjMatrixDigraph extends DMatrixValue {

    private int V;
    private int E;

    // random graph with V vertices and E edges
    public AdjMatrixDigraph(int V, int E) {
        super(V, V, false, DMatrix.MemoryLayout.ROW_LAYOUT);

        this.V = V;

        if (E < 0) throw new RuntimeException("Number of edges must be nonnegative");
        if (E > V * V) throw new RuntimeException("Too many edges");

        // can be inefficient
        // can be inefficient
        while (this.E != E) {
            int v = (int) (V * Math.random());
            int w = (int) (V * Math.random());
            addEdge(v, w);
        }
    }

    // number of vertices and edges
    public int V() {
        return V;
    }

    public int E() {
        return E;
    }

    // add directed edge v->w
    public void addEdge(int v, int w) {
        if (matrix.get(v, w) == 0.0) E++;
        matrix.set(v, w, 1.0);
    }

    // return list of neighbors of v
    public Iterable<Integer> adj(int v) {
        return new AdjIterator(v);
    }

    // support iteration over graph vertices
    private class AdjIterator implements Iterator<Integer>, Iterable<Integer> {
        private int v, w = 0;

        AdjIterator(int v) {
            this.v = v;
        }

        public Iterator<Integer> iterator() {
            return this;
        }

        public boolean hasNext() {
            while (w < V) {
                if (matrix.get(v, w) == 1.0) return true;
                w++;
            }
            return false;
        }

        public Integer next() {
            if (hasNext()) {
                return w++;
            } else {
                throw new NoSuchElementException();
            }
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    // string representation of Graph - takes quadratic time
    public String toString() {
        String NEWLINE = System.getProperty("line.separator");
        StringBuilder s = new StringBuilder();
        s.append(V + " " + E + NEWLINE);
        for (int v = 0; v < V; v++) {
            s.append(v + ": ");
            for (int w : adj(v)) {
                s.append(w + " ");
            }
            s.append(NEWLINE);
        }
        return s.toString();
    }
}