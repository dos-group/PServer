package de.tuberlin.pserver.crdt.graphs;

import de.tuberlin.pserver.crdt.CRDT;

public interface Graph<T> extends CRDT {

    public boolean addVertex(Vertex<T> v);
    public boolean removeVertex(Vertex<T> v);

    public boolean addEdge(Edge e);
    public boolean removeEdge(Edge e);
}
