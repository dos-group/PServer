package de.tuberlin.pserver.crdt.graphs;

import de.tuberlin.pserver.crdt.AbstractCRDT;
import de.tuberlin.pserver.crdt.sets.TwoPSet;
import de.tuberlin.pserver.runtime.DataManager;

import java.util.HashSet;
import java.util.Set;

public abstract class AbstractGraph<T> extends AbstractCRDT implements Graph<T> {
    protected final TwoPSet<Vertex<T>> vertices;
    protected final TwoPSet<Edge> edges;

    public AbstractGraph(String id, DataManager dataManager) {
        super(id, dataManager);
        this.vertices = new TwoPSet<>("vertices", dataManager);
        this.edges = new TwoPSet<>("edges", dataManager);
    }

    public Set<Vertex<T>> getVertices() {
        return vertices.getSet();
    }

    public Set<Edge> getEdges() {
        return edges.getSet();
    }
}
