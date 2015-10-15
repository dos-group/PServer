package de.tuberlin.pserver.crdt.graphs;

import de.tuberlin.pserver.crdt.AbstractCRDT;
import de.tuberlin.pserver.crdt.sets.TwoPSet;
import de.tuberlin.pserver.runtime.RuntimeManager;

import java.util.HashSet;
import java.util.Set;

public abstract class AbstractGraph<T> extends AbstractCRDT implements Graph<T> {
    protected final TwoPSet<Vertex<T>> vertices;
    protected final TwoPSet<Edge> edges;

    public AbstractGraph(String id, RuntimeManager runtimeManager) {
        super(id, runtimeManager);
        this.vertices = new TwoPSet<>("vertices", runtimeManager);
        this.edges = new TwoPSet<>("edges", runtimeManager);
    }

    public Set<Vertex<T>> getVertices() {
        return vertices.getSet();
    }

    public Set<Edge> getEdges() {
        return edges.getSet();
    }
}
