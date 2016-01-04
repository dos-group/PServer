package de.tuberlin.pserver.new_core_runtime.topology;


public final class ProgramEdge {

    public final ProgramVertex src;

    public final ProgramVertex dst;

    public final int edgeID;

    public ProgramEdge(ProgramVertex src, ProgramVertex dst) {

        this.src = src;

        this.dst = dst;

        this.edgeID = hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProgramEdge edge = (ProgramEdge) o;
        return dst.equals(edge.dst) && src.equals(edge.src);
    }

    @Override
    public int hashCode() {
        int result = src.hashCode();
        result = 31 * result + dst.hashCode();
        return result;
    }
}
