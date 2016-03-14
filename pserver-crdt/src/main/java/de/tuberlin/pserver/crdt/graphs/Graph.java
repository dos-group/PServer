package de.tuberlin.pserver.crdt.graphs;

import de.tuberlin.pserver.crdt.CRDT;

public interface Graph<T> extends CRDT {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    boolean addVertex(Vertex<T> v);

    boolean removeVertex(Vertex<T> v);

    boolean addEdge(Edge e);

    boolean removeEdge(Edge e);
}
