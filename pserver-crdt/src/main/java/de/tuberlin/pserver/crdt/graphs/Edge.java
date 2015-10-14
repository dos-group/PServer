package de.tuberlin.pserver.crdt.graphs;

import java.io.Serializable;

// Immutable class
public class Edge implements Serializable {
    private final Vertex source;
    private final Vertex sink;

    public Edge(Vertex source, Vertex sink) {
        this.source = source;
        this.sink = sink;
    }

    public Vertex getSource() {
        return this.source;
    }

    public Vertex getSink() {
        return this.sink;
    }

    @Override
    public String toString() {
        return "Edge{" +
                "source=" + source.getId() +
                ", sink=" + sink.getId() +
                '}';
    }
}
