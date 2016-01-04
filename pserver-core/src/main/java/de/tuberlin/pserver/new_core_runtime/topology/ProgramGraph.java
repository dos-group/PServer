package de.tuberlin.pserver.new_core_runtime.topology;

import java.util.*;

public final class ProgramGraph {

    private final Map<Integer, ProgramVertex> vertices;

    private final Map<Integer, ProgramEdge> edges;

    private ProgramVertex root;

    public ProgramGraph() {

        this.vertices = new HashMap<>();

        this.edges = new HashMap<>();
    }

    public void addEdge(ProgramEdge edge) {
        edges.put(edge.edgeID, edge);
        edge.src.addOutEdge(edge);
        edge.dst.addInEdge(edge);
    }

    public void addVertex(ProgramVertex vertex) {
        if (vertex.vertexID == 0)
            root = vertex;
        vertices.put(vertex.vertexID, vertex);
    }

    public Map<Integer, ProgramVertex> getVertices() {
        return Collections.unmodifiableMap(vertices);
    }

    public Map<Integer, ProgramEdge> getEdges() {
        return Collections.unmodifiableMap(edges);
    }

    public ProgramVertex root() { return root; }
}