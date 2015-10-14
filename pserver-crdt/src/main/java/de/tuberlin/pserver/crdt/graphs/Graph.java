package de.tuberlin.pserver.crdt.graphs;

public interface Graph<T> {

    public boolean addVertex(Vertex<T> v);
    public boolean removeVertex(Vertex<T> v);

    public boolean addEdge(Edge e);
    public boolean removeEdge(Edge e);
}
