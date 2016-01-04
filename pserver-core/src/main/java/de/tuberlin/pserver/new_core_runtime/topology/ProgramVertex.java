package de.tuberlin.pserver.new_core_runtime.topology;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public final class ProgramVertex {

    public final int vertexID;

    public final int level;

    private List<ProgramEdge> in;

    private List<ProgramEdge> out;

    public boolean visited = false;

    public Object data;

    public ProgramVertex(int vertexID, int level) {

        this.vertexID = vertexID;

        this.level = level;

        this.in = new ArrayList<>();

        this.out = new ArrayList<>();
    }

    public void addInEdge(ProgramEdge edge) {
        if (in.contains(edge)) return;
        in.add(edge);
    }

    public void addOutEdge(ProgramEdge edge) {
        if (out.contains(edge)) return;
        out.add(edge);
    }

    public List<ProgramEdge> getInEdges() { return Collections.unmodifiableList(in); }

    public List<ProgramEdge> getOutEdges() { return Collections.unmodifiableList(out); }

    public String toString() { return "V(" + vertexID + "," + level + ")"; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProgramVertex vertex = (ProgramVertex) o;
        return level == vertex.level && vertexID == vertex.vertexID;
    }

    @Override
    public int hashCode() {
        int result = vertexID;
        result = 31 * result + level;
        return result;
    }
}
